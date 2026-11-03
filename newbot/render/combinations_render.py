import asyncio
from datetime import datetime
from typing import Dict
from motor.motor_asyncio import AsyncIOMotorClient
import os
from typing import Dict, List, Optional, Set, Tuple
import sys
import gzip, pickle
from pymemcache. client import base

cache = base.Client(('memcached', 11211))

processesPID = []
sem = asyncio.Semaphore(10000)

sys.path.append(os.path.join(os.getcwd(),'..'))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges import exchanges


# Порог прибыли для отбора связки (как в исходной логике)
PROFIT_THRESHOLD = 0.05

# Биржи, полностью исключённые из расчёта связок
EXCLUDED_EXCHANGES = frozenset(('OKX',))

# Пары бирж, исключённые из расчёта (в обе стороны)
EXCLUDED_PAIRS = frozenset((
    ('CoinEx', 'Bitmart'),
    ('Bitmart', 'CoinEx'),
))

# Правила форматирования символа под каждую биржу.
# Раньше эти словари пересобирались ДВАЖДЫ на каждую комбинацию бирж
# (~1.5 млн строковых операций за цикл). Теперь — одна функция на биржу,
# а результат кэшируется на символ.
SYMBOL_FORMATTERS = {
    'OKX': lambda s: s.replace('/', '-'),
    'Kucoin': lambda s: s.replace('/', '-'),
    'Gate': lambda s: s.replace('/', '_'),
    'MEXC Global': lambda s: s.replace('/', ''),
    'Bitget': lambda s: s.replace('/', ''),
    'Bybit': lambda s: s.replace('/', ''),
    'BingX': lambda s: s.replace('/', '-'),
    'HTX': lambda s: s.replace('/', '').lower(),
    'CoinEx': lambda s: s.replace('/', ''),
    'Bitmart': lambda s: s.replace('/', '_'),
}

# Сколько раз пытаться получить тикеры с биржи, прежде чем пропустить её на этот цикл.
TICKER_ATTEMPTS = 2
# Таймаут на одну попытку получения тикеров, секунды.
TICKER_TIMEOUT = 20


def _price(raw) -> Optional[float]:
    """Приводит сырое значение цены к float.

    Возвращает None, если цены нет, она пустая, нечисловая или неположительная,
    — ровно те же случаи, что отсеивались в исходной поштучной проверке.
    """
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


class ArbitrageBot:
    def __init__(self):
        self.client = AsyncIOMotorClient('mongo', 27017)
        self.globalBlackList: Set[str] = set()  # Будет инициализирован позже
        self.local_block_array: List[List] = []
        self.black_list = self.client.renderBase.black_list
        self.db_renderBase = self.client.renderBase

    async def get_all_symbols_data(self) -> Dict[str, str]:
        all_symbols_data =  await self.db_renderBase.AllSymbolsData.find().to_list(length=None)

        return {item['symbol']: item['currency'] for item in all_symbols_data}

    def collect_blocks(self, symbol: str, exBuy: str, exSell: str, ex_networks_asks: Dict, ex_networks_bids: Dict) -> None:
        """Складывает блоки по общим сетям пары бирж.

        Раньше на каждую сеть создавалась отдельная asyncio-задача, хотя работа
        чисто синхронная — планировщик стоил дороже самой работы.
        """
        bids_keys = ex_networks_bids.keys()
        append = self.local_block_array.append
        for network in ex_networks_asks.keys():
            if (network in bids_keys
                    and ex_networks_asks[network].get('withdraw') is True
                    and ex_networks_bids[network].get('deposit') is True
                    and ex_networks_asks[network].get('fee') is not None
                    and not (ex_networks_asks[network].get('contract') and ex_networks_bids[network].get('contract')
                             and ex_networks_asks[network]['contract'].lower() != ex_networks_bids[network]['contract'].lower())):
                append([
                    f'block1_{symbol}_{exBuy}_ob_{exSell}_ob_{network}',
                    [symbol, exBuy, exSell, network, ex_networks_asks, ex_networks_bids],
                ])

    async def update_cache(self):
        try:
            # Записываем все данные в кэш под одним ключом 'blocks'
            cache.set('blocks', gzip.compress(pickle.dumps(self.local_block_array)), 10)  # TTL 10 секунд

            # Очищаем локальный массив
            self.local_block_array.clear()
        except Exception as e:
            print('update_cache', e)

    def process_symbol(self, symbol: str, symbol_data: Dict, tickers: Dict) -> None:
        """Считает связки по одному символу.

        Алгоритм: сначала один проход по биржам (E лукапов вместо 2*E*(E-1)),
        затем дешёвая отсечка по лучшей возможной прибыли — и только у
        выживших символов полный перебор пар по уже готовым float'ам.
        """
        try:
            # Символ из чёрного списка не даёт блоков ни при какой прибыли,
            # поэтому отсекаем его до всех расчётов, а не после.
            if symbol in self.globalBlackList:
                return

            # --- Проход 1: по одному лукапу цены на биржу ---
            asks: List[Tuple[str, float]] = []   # где можно купить
            bids: List[Tuple[str, float]] = []   # где можно продать
            best_ask = None
            best_bid = None

            for exchange in symbol_data.keys():
                if exchange in EXCLUDED_EXCHANGES:
                    continue
                formatter = SYMBOL_FORMATTERS.get(exchange)
                if formatter is None:
                    continue

                ticker = tickers.get(exchange)
                if not ticker:
                    continue
                entry = ticker.get(formatter(symbol))
                if not entry:
                    continue

                ask = _price(entry.get('ask'))
                if ask is not None:
                    asks.append((exchange, ask))
                    if best_ask is None or ask < best_ask:
                        best_ask = ask

                bid = _price(entry.get('bid'))
                if bid is not None:
                    bids.append((exchange, bid))
                    if best_bid is None or bid > best_bid:
                        best_bid = bid

            if best_ask is None or best_bid is None:
                return

            # --- Отсечка: если даже лучшая пара не проходит порог,
            # то ни одна пара по этому символу его не пройдёт. ---
            if best_bid / best_ask * 100 - 100 <= PROFIT_THRESHOLD:
                return

            # --- Проход 2: только выжившие символы, только float-сравнения ---
            for exBuy, ask in asks:
                # Лучший из возможных bid не даёт прибыли с этим ask — пропускаем биржу целиком
                if best_bid / ask * 100 - 100 <= PROFIT_THRESHOLD:
                    continue
                networks_asks = symbol_data[exBuy].get('networks')
                if not networks_asks:
                    continue
                for exSell, bid in bids:
                    if exBuy == exSell or (exBuy, exSell) in EXCLUDED_PAIRS:
                        continue
                    if bid / ask * 100 - 100 <= PROFIT_THRESHOLD:
                        continue
                    networks_bids = symbol_data[exSell].get('networks')
                    if not networks_bids:
                        continue
                    self.collect_blocks(symbol, exBuy, exSell, networks_asks, networks_bids)

        except Exception as e:
            print(f"Error in process_symbol {symbol}: {e}")


async def looper(func, sem, name):
    """Получает тикеры с биржи с ограниченным числом попыток.

    Раньше здесь был `while param is None or param == {}` — бесконечный ретрай.
    Одна нерабочая биржа (например, Bybit с 403 из дата-центра) навсегда
    подвешивала asyncio.gather, из-за чего 'blocks' не публиковался и вставал
    ВЕСЬ пайплайн. Теперь нерабочая биржа просто пропускается на этот цикл.
    """
    for attempt in range(TICKER_ATTEMPTS):
        try:
            param = await asyncio.wait_for(func(sem), timeout=TICKER_TIMEOUT)
        except asyncio.TimeoutError:
            print(f'{name}: timeout (попытка {attempt + 1}/{TICKER_ATTEMPTS})')
            continue
        except Exception as e:
            print(f'{name}: ошибка {e} (попытка {attempt + 1}/{TICKER_ATTEMPTS})')
            continue

        if param:
            return param, name
        print(f'{name}: пустой ответ (попытка {attempt + 1}/{TICKER_ATTEMPTS})')

    print(f'{name}: биржа недоступна, пропускаем цикл')
    return {}, name

async def fetch_ticker_with_timing(exchange, sem):
    start_time = datetime.now()
    result = await looper(exchange.fetchTickers, sem, exchange.name)
    end_time = datetime.now()
    return {
        'data': result,
        'exchange': exchange.name,
        'time': end_time - start_time
    }

async def generate_combinations(bot:ArbitrageBot):
    print('start generator')
    while True:
        try:
            start = datetime.now()

            black_list_docs = await bot.black_list.find().to_list(length=None)
            # set вместо list: проверка вхождения O(1) вместо O(n) на каждый символ
            bot.globalBlackList = set(black_list_docs[0]['list']) if black_list_docs else set()
            print(f'Black list collected. Len: {len(bot.globalBlackList)}')

            ticker_start = datetime.now()
            tasks = [asyncio.create_task(fetch_ticker_with_timing(ex, sem)) for ex in exchanges]
            tickers_data = await asyncio.gather(*tasks)

            # Вывод времени для каждой биржи
            skipped = []
            for ticker_info in tickers_data:
                status = '' if ticker_info['data'][0] else '  [ПРОПУЩЕНА]'
                if not ticker_info['data'][0]:
                    skipped.append(ticker_info['exchange'])
                print(f"Exchange {ticker_info['exchange']}: {ticker_info['time']}{status}")
            if skipped:
                print(f'Недоступные биржи в этом цикле: {", ".join(skipped)}')

            # Преобразование в нужный формат для дальнейшей обработки
            tickers = {ticker['data'][1]: ticker['data'][0] for ticker in tickers_data}
            ticker_end = datetime.now()
            print(f'Total tickers collection time: {ticker_end-ticker_start}')

            allSymbolsData = await bot.get_all_symbols_data()
            print('All symbols data collected')
            # Очищаем локальный массив перед новым циклом
            bot.local_block_array.clear()

            # Расчёт чисто CPU-bound: раньше на него создавалось ~77 000 задач
            # asyncio за цикл. Прямой цикл делает ту же работу без планировщика.
            calc_start = datetime.now()
            for symbol, symbol_data in allSymbolsData.items():
                bot.process_symbol(symbol, symbol_data, tickers)
            print(f'Tasks completed. Blocks: {len(bot.local_block_array)}, calc time: {datetime.now()-calc_start}')

            # Обновляем кэш после завершения всех расчетов
            await bot.update_cache()

            end = datetime.now()
            cache.set('time_blocks', gzip.compress(pickle.dumps(str(end-start))), 10)
            print(f'Time circle: {end-start} seconds')
        except Exception as e:
            print('Tasks error: ', e)
            await asyncio.sleep(1)
            continue

async def main() -> None:
    bot = ArbitrageBot()
  # Инициализация Redis

    await asyncio.create_task(generate_combinations(bot))

if __name__ == '__main__':
    asyncio.run(main())
