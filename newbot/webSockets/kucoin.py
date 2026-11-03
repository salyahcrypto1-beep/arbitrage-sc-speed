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
log_file = "logs/kucoin.log"
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.WARNING)

cache = base.Client(('memcached', 11211))

class KucoinOrderBookUpdater:
    def __init__(self):
        self.order_book_updates = {}
        self.raw_updates = {}
        self.active_trading_pairs = set()
        self.semaphore = asyncio.Semaphore(50)  # Ограничение на 50 одновременных WebSocket-соединений

    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/', '-')

    async def get_trading_pairs_from_mongodb(self):
        substring = 'Kucoin_ob'
        
        raw_data = cache.get('blocks')
        if raw_data is None:
            logger.error("Cache returned None for 'blocks'. Check if Redis/Memcached is running and storing data correctly.")
            return []

        try:
            data = pickle.loads(gzip.decompress(raw_data))
            keys = [key[0] for key in data if substring in key[0]]
            return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])
        except Exception as e:
            logger.error(f"Error while loading trading pairs from cache: {e}")
            return []

    async def get_ws_token(self):
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.kucoin.com/api/v1/bullet-public") as response:
                if response.status == 200:
                    data = await response.json()
                    return data['data']['token']
                else:
                    logger.error(f"Failed to get KuCoin WebSocket token. Status: {response.status}")
                    return None

    async def handle_message(self, symbol, message):
        try:
            # 02.10 оптимизация: лента level2Depth50 шлёт полный снимок ~10 раз/с на пару, а в memcached уходит
            # только последний раз в 2 с — сырое сообщение запоминаем, разбираем при записи (_parse_pending)
            if '/spotMarket/level2Depth50:' in message:
                self.raw_updates[symbol] = (message, datetime.now())
        except Exception as e:
            logger.error(f"Error handling message for {symbol}: {e}")

    def _parse_pending(self):
        pending = self.raw_updates
        self.raw_updates = {}
        for symbol, (message, ts) in pending.items():
            try:
                data = json.loads(message)
                if "topic" in data and data["topic"].startswith('/spotMarket/level2Depth50:'):
                    self.order_book_updates[symbol] = {
                        'bids': [[float(price), float(quantity)] for price, quantity in data['data']['bids']],
                        'asks': [[float(price), float(quantity)] for price, quantity in data['data']['asks']],
                        'lastUpdate': ts
                    }
            except Exception as e:
                logger.error(f"Error handling message for {symbol}: {e}")

    async def handle_connection(self, symbol):
        while True:
            token = await self.get_ws_token()
            if not token:
                logger.error(f"Failed to retrieve WebSocket token for {symbol}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
                continue

            ws_endpoint = f"wss://ws-api.kucoin.com?token={token}"

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_endpoint) as websocket:
                        logger.info(f"Connected to WebSocket for {symbol}")
                        subscribe_message = {
                            "id": str(int(time.time())),
                            "type": "subscribe",
                            "topic": f"/spotMarket/level2Depth50:{symbol}",
                            "response": True,
                            "privateChannel": False
                        }
                        await websocket.send_str(json.dumps(subscribe_message))

                        async def send_ping():
                            while True:
                                if not websocket.closed:
                                    await websocket.send_str(json.dumps({"id": str(int(time.time())), "type": "ping"}))
                                    logger.info(f"Sent PING for {symbol}")
                                    await asyncio.sleep(15)
                                else:
                                    break

                        asyncio.create_task(send_ping())

                        async for msg in websocket:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.handle_message(symbol, msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning(f"WebSocket closed for {symbol}. Removing and waiting for reactivation...")
                                break  # Выход из цикла для очистки и ожидания повторного запуска через check_new_tokens

            except Exception as e:
                logger.error(f"Error for {symbol}: {e}")

            # Удаляем символ, но не запускаем его заново – это сделает check_new_tokens
            self.active_trading_pairs.discard(symbol)
            self.order_book_updates.pop(symbol, None)
            self.raw_updates.pop(symbol, None)
            return  # Прерываем выполнение, чтобы новый WebSocket открылся через check_new_tokens

    async def update_database(self):
        while True:
            try:
                self._parse_pending()
                current_snapshot = self.order_book_updates.copy()

                if current_snapshot:
                    try:
                        all_order_books = {}
                        for symbol, order_book in current_snapshot.items():
                            all_order_books[symbol] = {
                                "exchange": "Kucoin",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat()
                            }
                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data, 1)
                        cache.set('orderbook_Kucoin_full_snapshot', compressed_data, 10)
                        logger.info("Order book update in Redis completed.")
                    except Exception as e:
                        logger.error(f"Error updating order books in Redis: {e}")
                else:
                    logger.info("No order books to update in Redis.")
            except Exception as e:
                logger.error(f"Error in update_database: {e}")
            await asyncio.sleep(2)

    async def check_new_tokens(self):
        try:
            current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())

            if not current_trading_pairs:
                logger.warning("No trading pairs found. Check if the data source is working correctly.")
                await asyncio.sleep(1)

            new_trading_pairs = current_trading_pairs - self.active_trading_pairs
            for trading_pair in new_trading_pairs:
                logger.info(f"new_trading_pairs: {trading_pair}")

            for trading_pair in new_trading_pairs:
                if trading_pair not in self.active_trading_pairs:
                    logger.info(f"New trading pair found: {trading_pair}")
                    self.active_trading_pairs.add(trading_pair)
                    asyncio.create_task(self.handle_connection(trading_pair))

        except Exception as e:
            logger.error(f"Error in check_new_tokens: {e}")


    async def run(self):
        try:
            symbols = await self.get_trading_pairs_from_mongodb()

            scheduler = AsyncIOScheduler()
            scheduler.add_job(self.check_new_tokens, 'interval', seconds=3)
            scheduler.start()

            database_task = asyncio.create_task(self.update_database())
            await asyncio.gather(database_task)
        except Exception as e:
            logger.error(f"Error in run: {e}")

if __name__ == "__main__":
    order_book_updater = KucoinOrderBookUpdater()
    asyncio.run(order_book_updater.run())
