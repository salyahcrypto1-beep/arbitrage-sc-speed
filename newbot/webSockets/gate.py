import asyncio
import json
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
import websockets
from websockets.client import WebSocketClientProtocol
import gzip
import pickle
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pymemcache.client import base
import time
import random

cache = base.Client(('memcached', 11211))

log_file = "logs/gate.log"
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

class GateOrderBookUpdater:
    def __init__(self):
        self.WS_URL = "wss://api.gateio.ws/ws/v4/"
        self.order_book_updates = {}
        self.loop = asyncio.get_event_loop()
        self.active_trading_pairs = set()
        self._last_ping = None
        self.active_connections = []
        self.proxies = [None]
        

    async def formatPair(self, key):
        pair = str(key).split('_')
        return str(pair[1]).replace('/', '_')

    async def get_trading_pairs_from_mongodb(self):
        substring = 'Gate_ob'
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])
    
    async def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    async def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")

    async def on_open(self, ws):
        logger.info("WebSocket connection established")
        # Отправляем сообщение подписки
        subscribe_message = {
            "time": int(time.time()),
            "channel": "spot.order_book",
            "event": "subscribe",
            "payload": [ws.symbol, "50", "100ms"]
        }
        ws.send(json.dumps(subscribe_message))


    async def update_side(self, side, update_orders, clean):
        current = {float(price): float(amount) for price, amount in side} if not clean else {}
        
        for price, amount in update_orders:
            price = float(price)
            amount = float(amount)
            
            if amount == 0:
                current.pop(price, None)
            else:
                current[price] = amount
                
        return current

    def trim_side(self, side_dict, is_bid=True):
        # Приводим dict к отсортированному списку и берем первые 50 элементов
        sorted_items = sorted(side_dict.items(), key=lambda x: x[0], reverse=is_bid)
        return [[price, amount] for price, amount in sorted_items[:50]]

    async def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('result', {}).get('s')
            
            if data.get('channel') == 'spot.order_book':
                book_data = data.get('result', {})
                # Получаем и обрабатываем данные книги ордеров
                bids = self.trim_side({float(price): float(amount) for price, amount 
                                     in book_data.get('bids', [])}, is_bid=True)
                asks = self.trim_side({float(price): float(amount) for price, amount 
                                     in book_data.get('asks', [])}, is_bid=False)
                
                self.order_book_updates[symbol] = {
                    "bids": bids,
                    "asks": asks,
                    "lastUpdate": datetime.now()
                }
                
        except Exception as e:
            logger.error(f"Error in on_message: {e}")


    async def _send_ping(self, interval, event):
        while not event.wait(interval):
            self.last_ping_tm = time.time()
            if self.sock:
                try:
                    self.sock.ping()
                except Exception as ex:
                    logger.warning("send_ping routine terminated: {}".format(ex))
                    break
                try:
                    self._request("spot.ping", auth_required=False)
                except Exception as e:
                    raise e


    async def handle_connection(self, symbol):
        try:
            # Прокси передаётся аргументом proxy=, общий патч сюда не достаёт.
            proxy = None if os.environ.get("BOT_DIRECT_CONNECTION") == "1" else random.choice(self.proxies)
            async with websockets.connect(self.WS_URL,proxy=proxy) as ws:
                self.active_connections.append(ws)
                try:
                    subscribe_message = {
                        "time": int(time.time()),
                        "channel": "spot.order_book",
                        "event": "subscribe",
                        "payload": [symbol, "50", "1000ms"]
                    }
                    await ws.send(json.dumps(subscribe_message))
                    
                    while True:
                        message = await ws.recv()
                        await self.on_message(ws, message)
                        await asyncio.sleep(0.1)
                        
                except Exception as e:
                    logger.error(f"Error in WebSocket 1 loop for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Error in WebSocket 2 loop for {symbol}: {e}")
        finally:
            logger.error(f"Error in WebSocket 3 loop for {symbol}")
            if symbol in self.order_book_updates:
                del self.order_book_updates[symbol]
            self.active_trading_pairs.discard(symbol)
            return None


    """ async def handle_connection(self, symbol):
        try:
            ws = WebSocketApp(
                self.WS_URL,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            ws.symbol = symbol
            self.active_connections.append(ws)
            
            while True:
                try:
                    # Отправляем сообщение подписки
                    subscribe_message = {
                        "time": int(time.time()),
                        "channel": "spot.order_book",
                        "event": "subscribe",
                        "payload": [symbol, "50", "100ms"]
                    }
                    await ws.send(json.dumps(subscribe_message))
                    
                    # Обработка входящих сообщений
                    message = await ws.recv()
                    await self.on_message(ws, message)
                    
                    await asyncio.sleep(0.1)  # Небольшая задержка для предотвращения перегрузки
                    
                except Exception as e:
                    logger.error(f"Error in WebSocket loop for {symbol}: {e}")
                    await asyncio.sleep(5)  # Пауза перед повторным подключением
                    
        except Exception as e:
            logger.error(f"Connection error for {symbol}: {e}")
        finally:
            if symbol in self.order_book_updates:
                del self.order_book_updates[symbol]
            self.active_trading_pairs.discard(symbol)
            if ws in self.active_connections:
                self.active_connections.remove(ws) """

    

    async def update_database(self):
        while True:
            try:
                current_snapshot = self.order_book_updates.copy()
                if current_snapshot:
                    all_order_books = {
                        symbol: {
                            "exchange": "Gate",
                            "symbol": symbol,
                            "bids": book["bids"],
                            "asks": book["asks"],
                            "lastUpdate": book["lastUpdate"].isoformat()
                        } for symbol, book in current_snapshot.items()
                    }
                    
                    serialized_data = pickle.dumps(all_order_books)
                    compressed_data = gzip.compress(serialized_data)
                    cache.set('orderbook_Gate_full_snapshot', compressed_data, 10)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error in update_database: {e}")
                await asyncio.sleep(2)

    async def check_new_tokens(self):
        while True:
            try:
                current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())
                new_trading_pairs = current_trading_pairs - self.active_trading_pairs
                if len(new_trading_pairs) > 0:
                    for pair in new_trading_pairs:
                        self.active_trading_pairs.add(pair)
                        logger.info(f"check_new_tokens {pair}")
                        asyncio.create_task(self.handle_connection(pair)) 
                        
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error in check_new_tokens: {e}")
                await asyncio.sleep(3) 

    async def run(self):
        check_new = asyncio.create_task(self.check_new_tokens())
        database_task = asyncio.create_task(self.update_database())
        await asyncio.gather(database_task, check_new)

if __name__ == "__main__":
    updater = GateOrderBookUpdater()
    asyncio.run(updater.run())
