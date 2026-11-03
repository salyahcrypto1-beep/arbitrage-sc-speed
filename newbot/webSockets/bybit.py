import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
import random
from urllib.parse import urlparse
import aiohttp
import motor.motor_asyncio
import time
from datetime import datetime
import os
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gzip
import pickle
from pymemcache.client import base


cache = base.Client(('memcached', 11211))

log_file = "logs/bybit.log"
os.makedirs("logs", exist_ok=True)
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

MAX_ORDERS_IN_DB = 50
UPDATE_INTERVAL = 2

class BybitOrderBookUpdater:
    def __init__(self):
        self.temp_order_books = {}  # Используем единый словарь для всех снепшотов
        self.reconnect_delay = 25
        self.active_trading_pairs = set()
        self.last_update_time = defaultdict(lambda: datetime(1970, 1, 1))
        
        self.cache = {}
        self.proxies = [None]

    async def handle_message(self, message):
        try:
            if "type" in message:
                if message["type"] == "snapshot":
                    symbol = message["data"]["s"]
                    bids = [[float(price), float(size)] for price, size in message["data"]["b"]]
                    asks = [[float(price), float(size)] for price, size in message["data"]["a"]]
                    
                    # Обновляем снепшот для данного символа в общем словаре
                    self.temp_order_books[symbol] = {"bids": bids, "asks": asks}
                    self.last_update_time[symbol] = datetime.now()
                    logger.info(f"Snapshot received for {symbol}")
                elif message["type"] == "delta":
                    symbol = message["data"]["s"]
                    self.last_update_time[symbol] = datetime.now()
                    
                    # Обновляем order book, используя данные из общего словаря
                    if symbol in self.temp_order_books:
                        await self.update_order_book(self.temp_order_books[symbol], message["data"]["b"], message["data"]["a"], symbol)
                    else:
                        logger.warning(f"Delta message received for {symbol} before snapshot.")
            elif message['ret_msg'] == 'subscribe' and message['success']:
                logger.info(f"Subscription confirmation received: {message}")
            elif message['ret_msg'] == 'pong' and message['success']:
                logger.info(f"Received PONG message")
            else:
                logger.warning(f"Received unexpected message: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def update_order_book(self, order_book, bids, asks, symbol):
        # Обновляем bids 
        for price, qty in bids:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                order_book["bids"] = [bid for bid in order_book["bids"] if bid[0] != price]
            else:
                updated = False
                for i in range(len(order_book["bids"])):
                    if order_book["bids"][i][0] == price:
                        order_book["bids"][i][1] = qty
                        updated = True
                        break
                if not updated:
                    order_book["bids"].append([price, qty])

        # Сортируем bids в порядке убывания цены
        order_book["bids"].sort(key=lambda x: x[0], reverse=True)

        # Обновляем asks 
        for price, qty in asks:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                order_book["asks"] = [ask for ask in order_book["asks"] if ask[0] != price]
            else:
                updated = False
                for i in range(len(order_book["asks"])):
                    if order_book["asks"][i][0] == price:
                        order_book["asks"][i][1] = qty
                        updated = True
                        break
                if not updated:
                    order_book["asks"].append([price, qty])

        # Сортируем asks в порядке возрастания цены
        order_book["asks"].sort(key=lambda x: x[0])

        # Ограничиваем количество ордеров в order book
        order_book["bids"] = order_book["bids"][:MAX_ORDERS_IN_DB]
        order_book["asks"] = order_book["asks"][:MAX_ORDERS_IN_DB]


        return order_book["bids"], order_book["asks"]

    async def handle_token(self, symbol, depth=50):
        lasttime = int(time.time() * 1000)
        while True:
            try:
                conn = aiohttp.TCPConnector(
                    limit=0,
                    limit_per_host=0,
                    keepalive_timeout=10,
                    force_close=False,
                    enable_cleanup_closed=True,
                    use_dns_cache=True,
                    ttl_dns_cache=15,
                )
                proxy = random.choice(self.proxies)
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=conn) as session:
                    async with session.ws_connect(f"wss://stream.bybit.com/v5/public/spot", timeout=30, proxy=proxy) as websocket:
                        await websocket.send_json({"op": "subscribe", "args": [f"orderbook.{depth}.{symbol}"]})

                        logger.info(f"Subscribed to orderbook.{depth}.{symbol}")
                        async for msg in websocket:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = msg.json()
                                    await self.handle_message(data)
                                    diff = int(time.time() * 1000) - lasttime
                                    if diff > 10000:
                                        await websocket.send_json(({"req_id": str(int(time.time() * 1000)), "op": "ping"}))
                                        lasttime = int(time.time() * 1000)
                                except Exception as e:
                                    logger.error(f"Error handling message for {symbol}: {e}")
                                    break  # Выходим из цикла обработки сообщений при ошибке
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"WebSocket error for {symbol}: {websocket.exception()}")
                                break  # Выходим из цикла обработки сообщений при ошибке
                            if websocket.closed:
                                logging.warning(f'Connection closed for {symbol}. Wait for new one')
                                break  # Выходим из цикла обработки сообщений при ошибке

                    # Если мы здесь, значит произошла ошибка и нужно переподключиться
                    logger.info(f"Reconnecting to {symbol}...")
                    
                    # Удаляем токен из объекта снепшотов при проблемах с каналом
                    try:
                        if symbol in self.temp_order_books.items():
                            del self.temp_order_books[symbol]
                            del self.last_update_time[symbol]
                    except KeyError:
                        pass 
                    
            except (aiohttp.ClientConnectorError, aiohttp.WSServerHandshakeError) as e:
                logger.error(f"Error connecting to WebSocket for {symbol}: {e}")
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                logger.error(f"Unhandled exception for {symbol}: {e}")
                await asyncio.sleep(self.reconnect_delay)

    async def formatPair(self, key):
        pair = str(key).split('_')
        first, next = str(pair[1]).split('/')
        return f'{first}{next}'

    async def get_trading_pairs_from_mongodb(self):
        substring = 'Bybit_ob'
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])

    async def update_database(self):
        while True:
            try:
                logger.info("Starting order book update in Redis...")
                current_snapshot = self.temp_order_books.copy()

                if current_snapshot:
                    try:
                        all_order_books = {}
                        for symbol, order_book in current_snapshot.items():
                            all_order_books[symbol] = {
                                "exchange": "Bybit",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": self.last_update_time[symbol] .isoformat()
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data)
                        cache.set('orderbook_Bybit_full_snapshot', compressed_data, 10)
                        logger.info("Order book update in Redis completed.")
                    except Exception as e:
                        logger.error(f"Error updating order books in Redis: {e}")
                else:
                    logger.info("No order books to update in Redis.")

            except Exception as e:
                logger.error(f"Error in update_database: {e}")
            await asyncio.sleep(2)

    async def check_new_tokens(self):
        current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())
        new_trading_pairs = current_trading_pairs - self.active_trading_pairs

        for trading_pair in new_trading_pairs:
            logger.info(f"New trading pair found: {trading_pair}")
            self.active_trading_pairs.add(trading_pair)
            asyncio.create_task(self.handle_token(trading_pair))

    async def run_websocket(self):
        logger.info("Starting order book update tasks...")

        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.check_new_tokens, 'interval', seconds=2)
        scheduler.start()

        trading_pairs = await self.get_trading_pairs_from_mongodb()
        self.active_trading_pairs = set(trading_pairs)

        tasks = [asyncio.create_task(self.handle_token(symbol)) for symbol in trading_pairs]
        update_task = asyncio.create_task(self.update_database())

        await asyncio.gather(*tasks, update_task)

async def main():
    order_book_updater = BybitOrderBookUpdater()
    await order_book_updater.run_websocket()

if __name__ == "__main__":
    asyncio.run(main())
