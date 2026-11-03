import asyncio
import json
import logging
import os
from datetime import datetime
import time
import aiohttp
import gzip
import pickle
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import random



logging.basicConfig(filename='logs/bitmart.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

from pymemcache.client import base


cache = base.Client(('memcached', 11211))

class BitmartOrderBookManager:
    def __init__(self):
        self.WS_PUBLIC_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
        self.PING_INTERVAL = 18
        self.order_books = {}
        self.ws_connections = {}
        self.subscribed_symbols = set()
        self.last_snapshot = {}
        
        self.proxies = [None]
        

    

    async def subscribe(self, ws, symbol):
        if symbol in self.subscribed_symbols:
            logging.info(f"Already subscribed to {symbol}")
            return

        subscribe_message = {
            "op": "subscribe",
            "args": [f"spot/depth50:{symbol}"]
        }
        await ws.send_str(json.dumps(subscribe_message))
        self.subscribed_symbols.add(symbol)
        logging.info(f"Sent subscription message for {symbol}")

        # Ответ на подписку НЕ вычитываем здесь.
        #
        # Раньше после каждой подписки стоял блокирующий ws.receive_json().
        # Bitmart подключается к сжимающему эндпоинту (ws-manager-compress),
        # и часть кадров приходит не текстом — receive_json спотыкался
        # («Expecting value: line 1 column 2», «257:None is not WSMsgType.TEXT»),
        # соединение уходило в закрытие, и следующая же подписка падала с
        # «Cannot write to closing transport». В результате ВСЯ пачка символов
        # оставалась неподписанной — из 67 нужных пар в стакан попадало 43.
        #
        # Ответы на подписку разбирает основной цикл `async for msg in ws`
        # через handle_message — так же, как это сделано в остальных фидах.


    async def handle_message(self, ws, message):
        try:
            data = json.loads(message)
            if "event" in data and data["event"] == "subscribe":
                logging.info(f"Subscription confirmed: {data['topic']}")
            elif "table" in data and data["table"].startswith("spot/depth"):
                #logging.info(f"Received order book update for {data['data'][0]['symbol']}")
                await self.handle_orderbook_update(data)
            elif "event" in data and data["event"] == "error":
                logging.error(f"Error message received: {data}")
            else:
                logging.warning(f"Unexpected message: {message}")
        except json.JSONDecodeError:
            logging.error(f"Failed to parse message as JSON: {message}")
        except Exception as e:
            logging.error(f"Error handling message: {e}")

    async def handle_orderbook_update(self, data):
        symbol = data["data"][0]["symbol"]
        bids = [[float(price), float(size)] for price, size in data["data"][0]["bids"]]
        asks = [[float(price), float(size)] for price, size in data["data"][0]["asks"]]
        
        self.order_books[symbol] = {
            "bids": bids,
            "asks": asks,
            "lastUpdate": datetime.now()
        }
        
        self.last_snapshot[symbol] = {
            "symbol": symbol,
            "exchange": "Bitmart",
            "bids": bids,
            "asks": asks,
            "lastUpdate": datetime.now()
        }

    async def send_ping(self, ws):
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            if not ws.closed:
                await ws.send_str('ping')
                logging.info(f"Sent ping message for connection {id(ws)}")
            else:
                logging.warning(f"WebSocket {id(ws)} closed. Stopping ping.")
                break

    async def connect_and_subscribe(self, symbols):
        # Прямой режим: пул прокси мёртв, Bitmart отвечает напрямую.
        # Прокси передаётся аргументом proxy=, общий патч сюда не достаёт.
        # (в старой ветке был выход за границу списка: randint включает
        #  верхнюю границу, поэтому здесь len-1)
        if os.environ.get("BOT_DIRECT_CONNECTION") == "1":
            proxy = None
        else:
            proxy = self.proxies[random.randint(0, len(self.proxies) - 1)]
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_PUBLIC_URL, proxy=proxy) as ws:
                self.ws_connections[tuple(symbols)] = ws
                logging.info(f"Connected for symbols: {symbols}")
                
                ping_task = asyncio.create_task(self.send_ping(ws))
                
                try:
                    valid_symbols = []
                    for symbol in symbols:
                        await self.subscribe(ws, symbol)
                        if symbol in self.subscribed_symbols:
                            valid_symbols.append(symbol)
                    
                    if not valid_symbols:
                        logging.warning("No valid symbols to subscribe. Closing connection.")
                        return

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == 'pong':
                                logging.info("Received pong message")
                            else:
                                await self.handle_message(ws, msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error(f"WebSocket connection closed with error: {ws.exception()}")
                            break
                except Exception as e:
                    logging.error(f"Error in WebSocket connection: {e}")
                finally:
                    ping_task.cancel()
                    if not ws.closed:
                        await ws.close()
                    del self.ws_connections[tuple(symbols)]
                    for symbol in symbols:
                        self.subscribed_symbols.discard(symbol)

    async def update_cache(self):
        try:
            logging.info("Starting order book update in Redis...")
            current_snapshot = self.last_snapshot.copy()

            if current_snapshot:
                try:
                    all_order_books = {}
                    for symbol, order_book in current_snapshot.items():
                        all_order_books[symbol] = {
                            "exchange": "Bitmart",
                            "symbol": symbol,
                            "bids": order_book["bids"],
                            "asks": order_book["asks"],
                            "lastUpdate": order_book["lastUpdate"].isoformat()
                        }

                    serialized_data = pickle.dumps(all_order_books)
                    compressed_data = gzip.compress(serialized_data)
                    cache.set('orderbook_Bitmart_full_snapshot', compressed_data, 10)
                    logging.info("Order book update in Redis completed.")
                except Exception as e:
                    logging.error(f"Error updating order books in Redis: {e}")
            else:
                logging.info("No order books to update in Redis.")

        except Exception as e:
            logging.error(f"Error in update_database: {e}")

    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/','_')

    async def get_trading_pairs_from_cache(self):
        logging.info("Retrieving trading pairs from cache...")
        substring = 'Bitmart_ob'
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])
    
    async def check_new_tokens(self):
        try:
            new_symbols = await self.get_trading_pairs_from_cache()
            existing_symbols = set(symbol for group in self.ws_connections.keys() for symbol in group)
            symbols_to_add = set(new_symbols) - existing_symbols
            logging.info(f"New symbols to add: {symbols_to_add}")
            
            if symbols_to_add:
                asyncio.create_task(self.connect_and_subscribe(list(symbols_to_add)))
        except Exception as e:
            logging.error(f"Error in check_new_tokens: {e}")

    async def run(self):
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self.update_cache, 'interval', seconds=2)
        scheduler.add_job(self.check_new_tokens, 'interval', seconds=2)
        scheduler.start()

        try:
            # Keep the coroutine running
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Shutdown the scheduler on exit
            scheduler.shutdown()


if __name__ == "__main__":
    manager = BitmartOrderBookManager()
    asyncio.run(manager.run())
