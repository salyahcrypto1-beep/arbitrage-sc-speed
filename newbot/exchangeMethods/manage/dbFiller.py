import asyncio
import sys
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
import multiprocessing as mp
import threading
import dbloader
import exchanges

class BaseFiller:
    def __init__(self) -> None:
        pass
    

    class MarketsLoader:
        def __init__(self) -> None:
            pass

        async def loadMarkets(self):
            sem = asyncio.Semaphore(10000)
            await asyncio.gather(*[ex.loadMarkets(sem=sem) for ex in exchanges.exchanges])

    class CurrencyLoader:
        def __init__(self) -> None:
            pass

        async def currency(self):
            loader = dbloader.Mongoloader()
            symbols = await loader.loadFromMongo(key='Markets')
            sem = asyncio.Semaphore(2000)
            proc = []
            # Раньше одна проблемная биржа роняла загрузку ЦЕЛИКОМ:
            # symbols[ex.name] вычислялся прямо в цикле (KeyError рвал перебор
            # на первой же отсутствующей бирже), а gather без
            # return_exceptions отменял остальные задачи по первой ошибке.
            # Теперь каждая биржа изолирована и отвечает только за себя.
            names = []
            for ex in exchanges.exchanges:
                ex_symbols = symbols.get(ex.name) if isinstance(symbols, dict) else None
                if not ex_symbols:
                    print(f'Currency: пропуск {ex.name} — нет символов в Markets')
                    continue
                proc.append(asyncio.create_task(ex.currency(ex_symbols, sem)))
                names.append(ex.name)

            results = await asyncio.gather(*proc, return_exceptions=True)
            for name, result in zip(names, results):
                if isinstance(result, BaseException):
                    print(f'Currency {name} err: {type(result).__name__}: {result}')



if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    markets = BaseFiller.MarketsLoader()
    currency = BaseFiller.CurrencyLoader()
    ASD = dbloader.Mongoloader()

    scheduler.add_job(ASD.collect_all_symbols_data,'interval', seconds=15)
    scheduler.add_job(markets.loadMarkets, 'interval', seconds=10)
    scheduler.add_job(currency.currency, 'interval', seconds=45) 

    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
