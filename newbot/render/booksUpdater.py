import aiohttp
import aiohttp_socks
import asyncio
import motor.motor_asyncio
from datetime import datetime
import sys
import os
import diskcache as dc  # Import diskcache
import gzip
import pickle
from multiprocessing import Process

cache = dc.Cache('../tmp/cache')

sys.path.append(os.path.join(os.getcwd(),'..'))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchangeMethods.manage.constants import Constants
from exchangeMethods.exchanges import bingx ,bitget,coinex,okex,bybit,gate,mexc,htx, kucoin

class MexcOrderBookUpdater:
    def __init__(self):
        self.proxies = [None]

        self.proxy_username = 'sccrypto_uulpx'
        self.proxy_password = ''

        self.url = 'https://api.mexc.com/api/v3/depth?symbol=ACUSDT&limit=100'
        self.MONGO_URI = "mongodb://mongo:27017/"
        self.MONGO_DATABASE_TOKENS = "renderBase"
        self.MONGO_COLLECTION_TOKENS = "tradePairs"

        self.mongo_client_tokens = motor.motor_asyncio.AsyncIOMotorClient(self.MONGO_URI)
        self.mongo_db_tokens = self.mongo_client_tokens[self.MONGO_DATABASE_TOKENS]
        self.mongo_collection_tokens = self.mongo_db_tokens[self.MONGO_COLLECTION_TOKENS]

        self.symbols = {}
        self.exchanges = ['MEXC Global']

    def get_proxy_url(self, entry_point, ip, port):
        return fNone


    async def formatPair(self, pair):
        return str(pair)

    async def get_trading_pairs_from_mongodb(self):
        for exchange in self.exchanges:
            trade_pair = await self.mongo_collection_tokens.find_one({"id": exchange})
            if trade_pair and "tokens" in trade_pair:
                local_symbols = await asyncio.gather(*[self.formatPair(pair) for pair in trade_pair["tokens"]])
                self.symbols[exchange] = local_symbols
        
        return self.symbols
    
    async def set_order_book_in_cache(self, exchange, symbol, order_book):
        try:
            item_dict = {
                "exchange": exchange,
                "symbol": symbol,
                "bids": order_book["bids"],
                "asks": order_book["asks"],
                "lastUpdate": datetime.now()
            }
            cache_key = f'orderbook_{exchange}_{symbol}'
            # Сериализация данных с помощью pickle и сжатие с помощью gzip
            serialized_data = pickle.dumps(item_dict)
            cached_data = gzip.compress(serialized_data)
            cache.set(cache_key, cached_data, expire=300)  # Кэшировать данные на 5 минут
        except Exception as e:
            print(f"Error updating order book cache for {symbol}: {e}")

    async def my_coroutine(self, session, symbol, exchange):
        symbolOrderBook = []
        try:
            match exchange:
                case 'OKX':
                    symbolOrderBook = await okex.OKX().fetchOrderBook(symbol, session)
                case 'Kucoin':
                    symbolOrderBook = await kucoin.KuCoin().fetchOrderBook(symbol, session)
                case 'Gate':
                    symbolOrderBook = await gate.Gate().fetchOrderBook(symbol, session)
                case 'MEXC Global':
                    symbolOrderBook = await mexc.MEXC().fetchOrderBook(symbol, session)
                case 'Bitget':
                    symbolOrderBook = await bitget.Bitget().fetchOrderBook(symbol, session)
                case 'Bybit':
                    symbolOrderBook = await bybit.Bybit().fetchOrderBook(symbol, session)
                case 'BingX':
                    symbolOrderBook = await bingx.BingX().fetchOrderBook(symbol, session)
                case 'HTX':
                    symbolOrderBook = await htx.HTX().fetchOrderBook(symbol, session)
                case 'CoinEx':
                    symbolOrderBook = await coinex.CoinEx().fetchOrderBook(symbol, session)

        except Exception as e:
            print(f'Error {exchange}: {e}')
            return 0, 1

        if symbolOrderBook is not None:
            try:
                cache_key = f'orderbook_{exchange}_{symbol}'
                cache.evict(cache_key)
                await self.set_order_book_in_cache(exchange, symbol, symbolOrderBook)
                
            except Exception as e:
                print(f"Error updating order books in MongoDB: {e}")
            return 1, 0
        else:
            return 0, 1

    async def main_container(self, proxy, symbols, exchange):
        #proxy = self.get_proxy_url(proxy_data['entryPoint'], proxy_data['ip'], proxy_data['port'])
        connector = (aiohttp_socks.ProxyConnector.from_url(proxy) if proxy else None)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector) as session:
            results = await asyncio.gather(*[self.my_coroutine(session, symbol, exchange) for symbol in symbols])
            success_count = sum(result[0] for result in results)
            error_count = sum(result[1] for result in results)
            return success_count, error_count  
        
    async def runner_every_ex(self, symbols_exchange, exchange):
        total_success = 0
        total_error = 0

        start = datetime.now()
        num_proxies = len(self.proxies)
        num_symbols = len(symbols_exchange)

        if num_symbols > num_proxies:
            part_size = num_symbols // num_proxies
            symbols_parts = [
                symbols_exchange[i:i+part_size]
                for i in range(0, num_symbols, part_size)
            ]

            results = await asyncio.gather(*[
                self.main_container(self.proxies[i], symbols_parts[i], exchange)
                for i in range(0, num_proxies)
            ])

            for success, error in results:
                total_success += success
                total_error += error
        else:
            results = await asyncio.gather(*[
                self.main_container(self.proxies[i], symbols_exchange[i], exchange)
                for i in range(0, num_symbols)
            ])

            for success, error in results:
                total_success += success
                total_error += error

        print(f"Total Success {exchange}: {total_success}")
        print(f"Total Errors: {exchange} {total_error}")

        end = datetime.now()
        print(f'End recived: {exchange}', end-start)

    def runner_every_ex_container(self, symbols_exchange, exchange):
        asyncio.run(self.runner_every_ex(symbols_exchange, exchange))

    async def run(self):
        symbols = await self.get_trading_pairs_from_mongodb()

        start = datetime.now()

        processes = [
            Process(target=self.runner_every_ex_container, args=(symbols[exchange], exchange))
            for exchange in self.exchanges
        ]

        for process in processes:
            process.start()

    
        for process in processes:
            process.join()
        
        end = datetime.now()
        print('Global end: ', end-start)
            

if __name__ == '__main__':
    order_book_updater = MexcOrderBookUpdater()
    asyncio.run(order_book_updater.run())
