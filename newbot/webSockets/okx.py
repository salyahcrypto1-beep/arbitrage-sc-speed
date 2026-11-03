import time
import json
import asyncio
import aiohttp
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gzip
import pickle
from pymemcache.client import base

# Настройка логирования
log_file = "logs/okx.log"
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

cache = base.Client(('memcached', 11211))

class OKXOrderBookUpdater:
    def __init__(self):
        self.order_book_updates = {}
        self.active_trading_pairs = set()
        self.semaphore = asyncio.Semaphore(50)  # Лимит на 50 одновременных соединений
        self.reconnect_delay = 5  # Задержка перед переподключением

    async def format_pair(self, key):
        # Форматируем торговую пару в соответствии с требованиями OKX
        return key.split('_')[1].replace('/', '-')

    async def get_trading_pairs_from_cache(self):
        substring = 'OKX_ob'
        try:
            cached_data = cache.get('blocks')
            if not cached_data:
                logger.warning("No cached data found for trading pairs.")
                return []

            data = pickle.loads(gzip.decompress(cached_data))
            keys = [key[0] for key in data if substring in key[0]]
            return [await self.format_pair(key) for key in keys]
        except Exception as e:
            logger.error(f"Error fetching trading pairs from cache: {e}")
            return []

    async def handle_message(self, symbol, message):
        try:
            data = json.loads(message)
            logger.info(f"Got data for {symbol}: {data}.")
            if data.get("event") == "error":
                logger.error(f"Error for {symbol}: {data['msg']}")
                return

            if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                book_data = data["data"][0]
                bids = [[float(price), float(quantity)] for price, quantity, *_ in book_data.get("bids", [])]
                asks = [[float(price), float(quantity)] for price, quantity, *_ in book_data.get("asks", [])]
                time_stamp = datetime.now()
                self.order_book_updates[symbol] = {
                    "bids": bids,
                    "asks": asks,
                    "lastUpdate": time_stamp
                }

                # Сохраняем данные в кэш
                try:
                    all_order_books = {
                        symbol: {
                            "exchange": "OKX",
                            "symbol": symbol,
                            "bids": bids,
                            "asks": asks,
                            "lastUpdate": time_stamp.isoformat()
                        }
                    }
                    serialized_data = pickle.dumps(all_order_books)
                    compressed_data = gzip.compress(serialized_data)
                    cache.set(f'orderbook_OKX_{symbol}', compressed_data, 10)
                    logger.info(f"Order book for {symbol} saved successfully.")
                except Exception as e:
                    logger.error(f"Error saving order book to cache for {symbol}: {e}")
            else:
                logger.warning(f"Unexpected data format for {symbol}: {data}")
        except Exception as e:
            logger.error(f"Error handling message for {symbol}: {e}")

    async def handle_connection(self, symbol):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect("wss://ws.okx.com:8443/ws/v5/public") as websocket:
                        logger.info(f"Connected to WebSocket for {symbol}")
                        subscribe_message = {
                            "op": "subscribe",
                            "args": [
                                {
                                    "channel": "books5",
                                    "instId": symbol
                                }
                            ]
                        }
                        await websocket.send_str(json.dumps(subscribe_message))

                        async for msg in websocket:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.handle_message(symbol, msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning(f"WebSocket closed for {symbol}. Removing and waiting for reactivation...")
                                self.active_trading_pairs.discard(symbol)
                                self.order_book_updates.pop(symbol, None)
                                break
            except Exception as e:
                logger.error(f"Error for {symbol}: {e}")
                await asyncio.sleep(self.reconnect_delay)

    async def check_new_tokens(self):
        while True:
            try:
                current_trading_pairs = set(await self.get_trading_pairs_from_cache())
                new_trading_pairs = current_trading_pairs - self.active_trading_pairs

                for trading_pair in new_trading_pairs:
                    logger.info(f"New trading pair found: {trading_pair}")
                    self.active_trading_pairs.add(trading_pair)
                    asyncio.create_task(self.handle_connection(trading_pair))
            except Exception as e:
                logger.error(f"Error in check_new_tokens: {e}")
            await asyncio.sleep(5)

    async def initialize_cache(self):
        try:
            # Добавьте начальные данные в кэш, если его содержимое пусто
            if not cache.get('blocks'):
                trading_pairs = ["BTC-USDT", "ETH-USDT"]  # Пример начальных пар
                serialized_data = gzip.compress(pickle.dumps(trading_pairs))
                cache.set('blocks', serialized_data)
                logger.info("Initialized cache with trading pairs.")
        except Exception as e:
            logger.error(f"Error initializing cache: {e}")

    async def run(self):
        try:
            await self.initialize_cache()
            symbols = await self.get_trading_pairs_from_cache()
            self.active_trading_pairs.update(symbols)

            scheduler = AsyncIOScheduler()
            scheduler.add_job(self.check_new_tokens, 'interval', seconds=10)
            scheduler.start()

            await asyncio.Future()  # Keep the process running
        except Exception as e:
            logger.error(f"Error in run: {e}")

if __name__ == "__main__":
    order_book_updater = OKXOrderBookUpdater()
    asyncio.run(order_book_updater.run())
