import asyncio
import re
import json
import logging
import os
from datetime import datetime
from collections import defaultdict
import aiohttp
import gzip
import pickle
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import random
from pymemcache.client import base

# Настройка логирования
logging.basicConfig(filename='logs/bitget.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

cache = base.Client(('memcached', 11211))

_INSTID_RE = re.compile(r'"instId"\s*:\s*"([^"]+)"')
logging.getLogger().setLevel(logging.WARNING)


class BitgetOrderBookUpdater:
    def __init__(self):
        self.WS_URL = "wss://ws.bitget.com/v2/ws/public"
        self.temp_order_books = defaultdict(dict)
        self.raw_books = {}
        self.active_trading_pairs = set()
        self.max_subscriptions_per_connection = 1000
        self.connections = []
        
        # Прокси для использования
        self.proxies = [None]

    async def formatPair(self, key):
        pair = str(key).split('_')
        first, next = str(pair[1]).split('/')
        return f'{first}{next}'

    async def get_trading_pairs_from_mongodb(self):
        substring = 'Bitget_ob'
        data = pickle.loads(gzip.decompress(cache.get('blocks')))
        keys = [key[0] for key in data if substring in key[0]]
        # Проверяем наличие ключей, чтобы избежать передачи пустого списка
        return await asyncio.gather(*[self.formatPair(key) for key in keys if keys])

    def handle_message(self, message, trading_pair, ts=None):
        try:
            data = json.loads(message)
            if "data" in data and data["data"]:
                book_data = data["data"][0]
                if "bids" in book_data and "asks" in book_data:
                    # Обработка ордеров
                    book = {"bids": [], "asks": []}
                    book["bids"] = [[float(bid[0]), float(bid[1])] for bid in book_data["bids"]]
                    book["asks"] = [[float(ask[0]), float(ask[1])] for ask in book_data["asks"]]

                    book["bids"].sort(key=lambda x: x[0], reverse=True)
                    book["asks"].sort(key=lambda x: x[0])

                    mongo_book = {
                        "bids": book["bids"][:15],
                        "asks": book["asks"][:15],
                        "lastUpdate": ts or datetime.now()
                    }

                    self.temp_order_books[trading_pair] = mongo_book
        except Exception as e:
            logging.error(f"Error handling message for {trading_pair}: {e}")

    def _parse_pending(self):
        # 02.10 оптимизация: сырые сообщения (books15 — полный снимок) разбираем при записи раз в 2 с
        pending = self.raw_books
        self.raw_books = {}
        for pair, (message, ts) in pending.items():
            self.handle_message(message, pair, ts)

    async def close_connection(self, ws, pairs):
        def delete_tokets(tokens):
            for pair in tokens:
                self.temp_order_books.pop(pair, None)
                self.raw_books.pop(pair, None)
                self.active_trading_pairs.discard(pair)
                logging.info(f'Removed {pair} from active trading pairs due to closed connection.')

        """Закрытие соединения и удаление токенов."""
        if ws.closed:
            delete_tokets(pairs)
            return

        await ws.close()

        # Обработка удаления токенов
        delete_tokets(pairs)

    async def send_ping(self, ws):
        while True:
            await ws.send_str('ping')
            logging.info('Sent ping message.')
            await asyncio.sleep(5)

    async def create_connection(self, trading_pairs):
        try:
            # отвечает с этого сервера напрямую. Прокси здесь передаётся
            # аргументом proxy=, а не через ProxyConnector, поэтому общий
            # патч прямого режима сюда не достаёт — гасим явно.
            if os.environ.get("BOT_DIRECT_CONNECTION") == "1":
                proxy = None
            else:
                proxy = self.proxies[random.randint(0, len(self.proxies) - 1)]
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(self.WS_URL, proxy=proxy) as websocket:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [
                            {
                                "instType": "SPOT",
                                "channel": "books15",
                                "instId": pair
                            } for pair in trading_pairs
                        ]
                    }
                    await websocket.send_str(json.dumps(subscribe_msg))
                    ping_task = asyncio.create_task(self.send_ping(websocket))
                    while True:
                        try:
                            msg = await asyncio.wait_for(websocket.receive(), timeout=10)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                message = msg.data
                                if message == 'pong':
                                    logging.info(f"Received Pong for {trading_pairs}")
                                else:
                                    m = _INSTID_RE.search(message)
                                    if m and '"data"' in message:
                                        self.raw_books[m.group(1)] = (message, datetime.now())
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                await self.close_connection(websocket, trading_pairs)
                                raise Exception("WebSocket connection closed")
                        except asyncio.TimeoutError:
                            logging.warning(f'Timeout waiting for message or Pong for {websocket.path}. Closing connection.')
                            await self.close_connection(websocket, trading_pairs)
                            break
                        except Exception as e:
                            logging.error(f"Error in message handling: {e}")
                            await self.close_connection(websocket, trading_pairs)
                            break
                    ping_task.cancel()
        except Exception as e:
            logging.error(f"Error in create_connection: {e}")
            for pair in trading_pairs:
                self.temp_order_books.pop(pair, None)
                self.raw_books.pop(pair, None)
                self.active_trading_pairs.discard(pair)
                logging.info(f'Removed {pair} from active trading pairs due to closed connection.')


    async def check_new_tokens(self):
        while True:
            try:
                current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())
                new_trading_pairs = current_trading_pairs - self.active_trading_pairs
                logging.info(f"Current_trading_pairs found: {current_trading_pairs}")
                if new_trading_pairs:
                    self.active_trading_pairs.update(new_trading_pairs)
                    logging.info(f"New trading pairs found: {new_trading_pairs}")
                    asyncio.create_task(self.create_connection(list(new_trading_pairs)))
            
            except Exception as e:
                logging.error(f"Error in check_new_tokens: {e}")
            
            await asyncio.sleep(3)

    async def run_websocket(self):
        logging.info("Starting order book update tasks...")

        check_new = asyncio.create_task(self.check_new_tokens())
        database_task = asyncio.create_task(self.update_database())
        await asyncio.gather(database_task, check_new)

        logging.info("All order book update tasks completed.")
        await asyncio.sleep(2)

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
                                "exchange": "Bitget",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat()
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data, 1)
                        cache.set('orderbook_Bitget_full_snapshot', compressed_data, 10)
                        logging.info("Order book update in Redis completed.")
                    except Exception as e:
                        logging.error(f"Error updating order books in Redis: {e}")
                else:
                    logging.info("No order books to update in Redis.")

            except Exception as e:
                logging.error(f"Error in update_database: {e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    order_book_updater = BitgetOrderBookUpdater()
    asyncio.run(order_book_updater.run_websocket())
