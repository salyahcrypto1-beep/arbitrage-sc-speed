import time
import json
import asyncio
import threading
from urllib.parse import urlparse
import aiohttp
import time
import logging
from logging.handlers import RotatingFileHandler
import motor.motor_asyncio
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import gzip
import pickle
from collections import OrderedDict
import threading as t
from pymemcache. client import base


cacheDC = base.Client(('memcached', 11211))

logging.basicConfig(filename='logs/coinex.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

class CoinExOrderBookUpdater:
    def __init__(self):
        self.sequence_numbers = {}
        self.order_book_updates = {}
        self.active_trading_pairs = []
        self.cache = {}
        self.order_book = {"bids": [], "asks": []}
        

    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/','')

    async def get_trading_pairs_from_mongodb(self):
        substring = 'CoinEx_ob'
        data = pickle.loads(gzip.decompress(cacheDC.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])


    def update_order_book(self, symbol, data):
        self.order_book[symbol] = {
            "bids": [[float(price), float(amount)] for price, amount in data["bids"]],
            "asks": [[float(price), float(amount)] for price, amount in data["asks"]],
            "lastUpdate": datetime.now()
        }
        self.order_book_updates[symbol] = self.order_book[symbol]

    def update_order_book_incrementally(self, symbol, data):
        
        if symbol not in self.order_book:
            return

        sides = ["bids", "asks"]
        for side in sides:
            if side in data:
                for price, amount in data[side]:
                    price, amount = float(price), float(amount)
                    if amount == 0:
                        try:
                            for pair in self.order_book[symbol][side]:
                                if float(pair[0])==price:
                                    self.order_book[symbol][side].remove(pair)
                        except Exception as e:
                            pass
                    else:
                        try:
                            for pair in self.order_book[symbol][side]:
                                check_arr = False
                                if price == pair[0]:
                                    self.order_book[symbol][side][self.order_book[symbol][side].index(pair)][1]=amount
                                    check_arr = True
                            if not check_arr:
                                self.order_book[symbol][side].append([price,amount])
                        except Exception as e:
                            pass
                            
                self.order_book[symbol][side] = OrderedDict(
                    sorted(self.order_book[symbol][side], 
                        key=lambda x: x[0], reverse=(side == "bids"))
                )
                

        self.order_book[symbol]["lastUpdate"] = datetime.now()
        self.order_book_updates[symbol] = self.order_book[symbol]

    async def handle_message(self, symbol, message):
        try:
            data = json.loads(message)
            if "method" in data and data["method"] == "depth.update":
                params = data["params"]
                is_full = params[0]
                depth_data = params[1]
                
                
                if is_full:
                    self.update_order_book(symbol, depth_data)

                else:
                    self.update_order_book_incrementally(symbol, depth_data)

            elif "result" in data and "error" in data:
                logging.info(f"Rec PONG for {symbol}")
                        

                    
        except Exception as e:
            logging.error(f"Error handling message for {symbol}: {e}")

    async def handle_ping(self, ws,pair):
        while True:
            try:
                await asyncio.sleep(20)
                if not ws.closed:
                    param = {
            "id": int(time.time() * 1000),
            "method": "server.ping",
            "params": []
        }
                    await ws.send_str(json.dumps(param))
                    logging.info(f"Sent PING for {pair}")
                else:
                    break
            except Exception as e:
                logging.error(f"Error sending Ping message: {e}")
                break

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
                    async with session.ws_connect('wss://socket.coinex.com/') as ws:
                        logging.info(f"Connected to WebSocket for {symbol}")
                        subscribe_message = {
                                                "id": int(time.time() * 1000),
                                                "method": "depth.subscribe",
                                                "params": [symbol, 20, "0",True]        
                                            }
                        logging.info(f"Sending subscription message for {symbol}: {subscribe_message}")
                        await ws.send_str(json.dumps(subscribe_message))
                        
                        ping_task = asyncio.create_task(self.handle_ping(ws,symbol))

                        async for msg in ws:
                            if ws.closed:
                                logging.warning(f"Connection closed for {symbol}, waiting for new one")
                                try:
                                    del self.order_book_updates[symbol]
                                    cacheDC.hdel('order_book_snapshots', symbol)
                                except Exception:
                                    return
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.handle_message(symbol, msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logging.error(f'WebSocket error for {symbol}: {ws.exception()}')
                        await ping_task
            except Exception as e:
                logging.error(f"Error for {symbol}: {e}")
            await asyncio.sleep(5)

    async def start_websocket(self, symbol):
        while True:
            try:
                await self.reconnect_websocket(symbol)
            except Exception as e:
                logging.error(f"Error in start_websocket for {symbol}: {e}")
                await asyncio.sleep(5)

    async def update_database(self):
        while True:
            try:
                logging.info("Starting order book update in Redis...")
                current_snapshot = self.order_book_updates.copy()

                if current_snapshot:
                    try:
                        all_order_books = {}
                        for symbol, order_book in current_snapshot.items():
                            all_order_books[symbol] = {
                                "exchange": "CoinEx",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat()
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data)
                        cacheDC.set('orderbook_CoinEx_full_snapshot', compressed_data, 10)
                        logging.info("Order book update in Redis completed.")
                    except Exception as e:
                        logging.error(f"Error updating order books in Redis: {e}")
                else:
                    logging.info("No order books to update in Redis.")

            except Exception as e:
                logging.error(f"Error in update_database: {e}")
            await asyncio.sleep(2)

    async def check_new_tokens(self):
            try:
                current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())
                new_trading_pairs = current_trading_pairs - set(self.active_trading_pairs)

                for trading_pair in new_trading_pairs:
                    logging.info(f"New trading pair found: {trading_pair}")
                    self.active_trading_pairs.append(trading_pair)
                    thread = t.Thread(target=asyncio.run, args=[self.start_websocket(trading_pair)])
                    thread.start()
                    # asyncio.create_task(self.start_websocket(trading_pair))
                    await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Error in check_new_tokens: {e}")
            await asyncio.sleep(2)

    async def run(self):
        try:
            # symbols = await self.get_trading_pairs_from_mongodb()
            # self.active_trading_pairs = symbols

            scheduler = AsyncIOScheduler()
            scheduler.add_job(self.check_new_tokens, 'interval', seconds=2)
            scheduler.start()

            # websocket_tasks = [asyncio.create_task(self.start_websocket(symbol)) for symbol in symbols]
            database_task = asyncio.create_task(self.update_database())

            await asyncio.gather(database_task)

        except Exception as e:
            logging.error(f"Error in run: {e}")

if __name__ == "__main__":
    order_book_updater = CoinExOrderBookUpdater()
    asyncio.run(order_book_updater.run())
