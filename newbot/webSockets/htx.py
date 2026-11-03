import time
import json
import asyncio
import aiohttp
import random
from urllib.parse import urlparse
import gzip
import logging
from logging.handlers import RotatingFileHandler
import motor.motor_asyncio
from datetime import datetime
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gzip
import pickle
from pymemcache.client import base


cache = base.Client(('memcached', 11211))

log_file = "logs/htx.log"
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.WARNING)

WS_URL = "wss://api.huobi.pro/ws"

class HTXOrderBookUpdater:
    def __init__(self):
        self.temp_order_books = defaultdict(dict)
        self.raw_books = {}
        self.depth_limit = 50
        self.active_trading_pairs = set()
        self.last_snapshot = {}
        self.message_time = {}
        self.reconnect_delay = 5  # Задержка перед переподключением

    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/','').lower()

    async def get_trading_pairs_from_mongodb(self):
        substring = 'HTX_ob'
        
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])
    
    async def handle_ping(self, websocket, msg_data,symbol):
        pong_msg = {
            "pong": msg_data["ping"]
        }
        await websocket.send_str(json.dumps(pong_msg))
        logger.info(f'Sent PONG for {symbol}, heartbeat is normal')

    async def write_order_book(self,msg_data, ts=None):
        trading_pair = msg_data["ch"].split(".")[1]
        bids = sorted([[float(price), float(quantity)] for price, quantity in msg_data["tick"]["bids"]], key=lambda x: x[0], reverse=True)[:self.depth_limit]
        asks = sorted([[float(price), float(quantity)] for price, quantity in msg_data["tick"]["asks"]], key=lambda x: x[0])[:self.depth_limit]
        book = {
            "symbol": trading_pair,
            "exchange": "HTX",
            "bids": bids,
            "asks": asks,
            "lastUpdate": ts or datetime.now()
        }
        self.temp_order_books[trading_pair] = book


    async def handle_message(self, websocket, symbol, message):
        try:
            text = gzip.decompress(message).decode('utf-8')
            if '"ping"' in text:
                msg_data = json.loads(text)
                if "ping" in msg_data:
                    await self.handle_ping(websocket, msg_data,symbol)
                    return
            if '"tick"' in text:
                # 02.10 оптимизация: полный снимок стакана запоминаем сырым, разбираем при записи раз в 2 с
                self.raw_books[symbol] = (text, datetime.now())
                return
        except Exception as e:
            logger.error(f"Error handling message for {symbol}: {e}")
            # Удаляем токен из активных и очищаем его данные при ошибке
            self.active_trading_pairs.remove(symbol)
            if symbol in self.temp_order_books:
                del self.temp_order_books[symbol]
            logger.warning(f'Pair {symbol} removed from active pairs due to error.')

    def _parse_pending(self):
        pending = self.raw_books
        self.raw_books = {}
        for symbol, (text, ts) in pending.items():
            try:
                msg_data = json.loads(text)
                if "tick" in msg_data:
                    coro = self.write_order_book(msg_data, ts)
                    try:
                        coro.send(None)   # внутри нет await — выполняется целиком
                    except StopIteration:
                        pass
            except Exception as e:
                logger.error(f"Error handling message for {symbol}: {e}")

    async def on_message(self, websocket, symbol):
        async for message in websocket:
            if message.type == aiohttp.WSMsgType.TEXT:
                await self.handle_message(websocket, symbol, message.data)
                self.message_time[symbol] = int(datetime.now().timestamp())
            elif message.type == aiohttp.WSMsgType.BINARY:
                await self.handle_message(websocket, symbol, message.data)
                self.message_time[symbol] = int(datetime.now().timestamp())
            else:
                logger.warning(f"Unexpected message type: {message.type}")
        # Если мы здесь, значит соединение было закрыто
        logger.warning(f'Connection closed for {symbol}. Wait for new one')
        self.active_trading_pairs.remove(symbol)

    async def check_connect(self,symbol):
        while True:
            try:
                diff_time = int(datetime.now().timestamp())-self.message_time.get(symbol, 0)
                if diff_time>10:
                    self.active_trading_pairs.remove(symbol)
                    if symbol in self.temp_order_books:
                        del self.temp_order_books[symbol]
                    logger.warning(f'Last message time for {symbol} was wasted. Pair was deleted from active pairs')
                    return
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f'Error in check_connect for {symbol}: {e}')
                return

    async def reconnect_websocket(self, symbol):
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

                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=conn) as session:
                    async with session.ws_connect(WS_URL, timeout=30) as websocket:
                        subscribe_msg = {
                            "sub": f"market.{symbol}.depth.step0",
                            "id": f"id_{symbol}"
                        }
                        await websocket.send_str(json.dumps(subscribe_msg))
                        self.message_time[symbol] = int(datetime.now().timestamp())
                        asyncio.create_task(self.check_connect(symbol))
                        await self.on_message(websocket, symbol)
            except (ConnectionResetError, aiohttp.ClientConnectorError) as e:
                logger.error(f"Connection error for {symbol}: {e}")
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {e}")
                await asyncio.sleep(self.reconnect_delay)

            # Удаляем токен из активных при ошибке, чтобы не дублировать подписки
            self.active_trading_pairs.remove(symbol)

    async def start_websocket(self, symbol):
        if symbol in self.active_trading_pairs:
            return
        self.active_trading_pairs.add(symbol)
        await self.reconnect_websocket(symbol)

    
    async def update_database(self):
        while True:
            try:
                self._parse_pending()
                current_snapshot = self.temp_order_books.copy()

                if current_snapshot:
                    try:
                        all_order_books = {}
                        for symbol, order_book in current_snapshot.items():
                            all_order_books[symbol] = {
                                "exchange": "HTX",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat()
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data, 1)
                        cache.set('orderbook_HTX_full_snapshot', compressed_data, 10)
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
            
            asyncio.create_task(self.start_websocket(trading_pair))
            await asyncio.sleep(1)

    async def run_websocket(self):
        
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.check_new_tokens, 'interval', seconds=2)
        scheduler.add_job(self.update_database, 'interval', seconds=2)
        scheduler.start()
        while True:
            await asyncio.sleep(1)


async def main():
    order_book_updater = HTXOrderBookUpdater()
    await order_book_updater.run_websocket()

if __name__ == "__main__":
    asyncio.run(main())
