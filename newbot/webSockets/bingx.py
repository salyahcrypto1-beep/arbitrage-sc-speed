import json
import gzip
import io
import asyncio
import aiohttp
import logging
from logging.handlers import RotatingFileHandler
import motor.motor_asyncio
from datetime import datetime
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import sys
import io
import gzip
import pickle
  # Import diskcache
import time

import diskcache as dc
from pymemcache. client import base


cache = base.Client(('memcached', 11211))


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

URL = "wss://open-api-ws.bingx.com/market"

class BingXOrderBookUpdater:
    def __init__(self):
        self.log_file = "logs/bingx.log"
        self.log_handler = RotatingFileHandler(self.log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
        self.log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_handler.setFormatter(self.log_formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(self.log_handler)
        self.logger.setLevel(logging.WARNING)
        self.raw_books = {}
        self.logger.setLevel(logging.INFO)
        self.temp_order_books = {} # Используем обычный словарь для хранения всех снепшотов под одним ключом
        self.active_trading_pairs = set()
        self.tasks = []
        self.cache = {}



    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/','-')


    async def get_trading_pairs_from_cache(self):
        self.logger.info("Retrieving trading pairs from cache...")
        substring = 'BingX_ob'
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])

    def handle_data(self, data, symbol, ts=None):
        try:
            data_dict = json.loads(data)
            if "data" in data_dict:
                bids = [[float(order[0]), float(order[1])] for order in data_dict["data"]["bids"]] if "bids" in data_dict["data"] else []
                asks = [[float(order[0]), float(order[1])] for order in data_dict["data"]["asks"]] if "asks" in data_dict["data"] else []

                time = ts or datetime.now()
                self.temp_order_books[symbol] = {"bids": bids, "asks": list(reversed(asks)), "lastUpdate": time}

        except json.JSONDecodeError:
            self.logger.error(f"JSON decoding error for {symbol}: {data}")

    def _parse_pending(self):
        # 02.10 оптимизация: вместо json.loads→dumps→loads на каждое сообщение — один разбор при записи раз в 2 с
        pending = self.raw_books
        self.raw_books = {}
        for symbol, (raw, ts) in pending.items():
            self.handle_data(raw, symbol, ts)

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
                                "exchange": "BingX",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat()
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data, 1)
                        cache.set('orderbook_BingX_full_snapshot', compressed_data, 10)
                        self.logger.info("Order book update in Redis completed.")
                    except Exception as e:
                        self.logger.error(f"Error updating order books in Redis: {e}")
                else:
                    self.logger.info("No order books to update in Redis.")

            except Exception as e:
                self.logger.error(f"Error in update_database: {e}")
            await asyncio.sleep(2)

    async def handle_connection(self, channel, session):
        try:
            async with session.ws_connect(URL) as websocket:
                subStr = json.dumps(channel)
                await websocket.send_str(subStr)
                self.logger.info(f"Subscribed to {subStr}")
                asyncio.create_task(self.check_connection(websocket,channel))
                async for message in websocket:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        data = message.data
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        compressed_data = gzip.GzipFile(fileobj=io.BytesIO(message.data), mode='rb')
                        decompressed_data = compressed_data.read()
                        data = decompressed_data.decode('utf-8')
                    elif message.type == aiohttp.WSMsgType.CLOSED:
                        self.logger.error(f"WebSocket closed for {channel['dataType']}")
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        self.logger.error(f"WebSocket error for {channel['dataType']}: {message.data}")
                        break
                    else:
                        continue
                    
                    if '"ping"' in data:
                        await websocket.send_str('Pong')
                    elif '"data"' in data:
                        symbol = channel["dataType"].split("@")[0]
                        if "-" in symbol:
                            parts = symbol.split("-")
                            symbol = "-".join([parts[0].upper(), parts[1].upper()])
                        else:
                            token = symbol[:-4]
                            symbol = f"{token.upper()}-USDT"

                        self.raw_books[symbol] = (data, datetime.now())
                    else:
                        pass

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.logger.error(f"Connection error for {channel['dataType']}: {e}")
            await asyncio.sleep(2)
            asyncio.create_task(self.handle_trading_pair(channel['dataType'].split("@")[0]))
                
        except Exception as e:
            self.logger.error(f"Error in handle_connection for {channel['dataType']}: {e}")
            await asyncio.sleep(2)
            asyncio.create_task(self.handle_trading_pair(channel['dataType'].split("@")[0]))         
    
    async def check_connection(self, websocket, channel):
        while True:
            await asyncio.sleep(2)
            if websocket.closed:
                self.logger.error(f"WebSocket closed for {channel['dataType']}")
                pair = channel['dataType'].split("@")[0]
                self.active_trading_pairs.discard(pair)
                try:
                    if pair in self.temp_order_books.itmes():
                        del self.temp_order_books[pair] # Удаляем токен из объекта снепшотов
                except KeyError:
                    self.logger.warning(f"Trading pair {pair} not found in temp_order_books")
                break 
            else:
                self.logger.info(f"WebSocket keep-alive for {channel['dataType']}")




    async def close_and_restart(self, trading_pair):
        try:
            await self.close()
        except Exception as e:
            self.logger.error(f"Error closing WebSocket connection for instrument {trading_pair}: {e}")
        reconnect_delay = 5
        while True:
            self.logger.info(f"Reconnecting in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)
            try:
                asyncio.create_task(self.handle_trading_pair(trading_pair))
                break  
            except Exception as e:
                self.logger.error(f"Reconnection attempt failed: {e}")
                reconnect_delay = min(reconnect_delay * 2, 300)

        


    async def handle_trading_pair(self, trading_pair):
        while True:
            conn = aiohttp.TCPConnector(
                    limit=0,
                    limit_per_host=0,
                    keepalive_timeout=10,  
                    force_close=False,  
                    enable_cleanup_closed=True,  
                    use_dns_cache=True,
                    ttl_dns_cache=15,
                )

            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=conn,timeout=aiohttp.ClientTimeout(total=3600, connect=10, sock_read=300)) as session:
                    channel = {"id": "e745cd6d-d0f6-4a70-8d5a-043e4c741b40", "reqType": "sub", "dataType": f"{trading_pair}@depth50"}
                    await self.handle_connection(channel, session)

            except Exception as e:
                self.logger.error(f"Error in handle_trading_pair for {trading_pair}: {e}")
                await self.close_and_restart(trading_pair)

            else:
                break
 

    async def check_new_tokens(self):
        current_trading_pairs = set(await self.get_trading_pairs_from_cache())
        new_trading_pairs = current_trading_pairs - self.active_trading_pairs

        for trading_pair in new_trading_pairs:
            self.active_trading_pairs.add(trading_pair)
            self.logger.info(f"New trading pair found: {trading_pair}")
            asyncio.create_task(self.handle_trading_pair(trading_pair))
            await asyncio.sleep(1)
            

    async def run_websocket(self):
        self.logger.info("Starting order book update tasks...")

        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.check_new_tokens, 'interval', seconds=2)
        scheduler.start()


if __name__ == "__main__":
    order_book_updater = BingXOrderBookUpdater()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    update_task = loop.create_task(order_book_updater.update_database())
    websocket_task = loop.create_task(order_book_updater.run_websocket())

    loop.run_until_complete(asyncio.gather(update_task, websocket_task))
