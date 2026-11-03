import asyncio
import json
import logging
import gzip
import pickle
import random
from datetime import datetime
from logging.handlers import RotatingFileHandler

import aiohttp
from aiohttp import WSMsgType
import aiohttp_socks

from pymemcache.client import base as memcache_base

# === Protobuf ===
# Файлы *_pb2.py должны быть сгенерированы из официальных .proto (см. инструкцию выше)
# и быть доступны в PYTHONPATH рядом со скриптом.
# mexc.py — в самом верху, до "from PushDataV3ApiWrapper_pb2 import ..."
import os, sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_PY_DIR = os.path.join(BASE_DIR, "mexc_ws_proto")
if PROTO_PY_DIR not in sys.path:
    sys.path.insert(0, PROTO_PY_DIR)

from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper
from google.protobuf.json_format import MessageToDict

# ---------------- Logging ----------------
log_file = "logs/mexc.log"
log_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.WARNING)

# ---------------- Memcache ----------------
cache = memcache_base.Client(('memcached', 11211))

# ---------------- Constants ----------------
WS_ENDPOINT = "wss://wbs-api.mexc.com/ws"  # новый эндпоинт (не wbs.mexc.com)
CHANNEL_TEMPLATE = "spot@public.limit.depth.v3.api.pb@{symbol}@{levels}"
LEVELS = 20  # 5/10/20
PING_INTERVAL = 30  # сек. — согласно доке, без активности сервер дропает линк
MAX_SUBS_PER_CONN = 30  # ограничение MEXC на 1 соединение (на будущее). :contentReference[oaicite:2]{index=2}


class MEXCOrderBookUpdater:
    def __init__(self):
        self.order_book_updates = {}
        self.raw_updates = {}
        self.active_trading_pairs = []

        # HTTP‑прокси Oxylabs DDC (aiohttp_socks умеет HTTP tunneling).
        # При желании можно оставить только один — код сам будет выбирать случайный.
        self.proxies = [None]

    # ---------------- Utils ----------------
    async def formatPair(self, key: str) -> str:
        # В твоих ключах обычно так: "MEXC Global_ob_BTC/USDT"
        pair = str(key).split("_")
        return str(pair[1]).replace("/", "") if len(pair) > 1 else str(key).replace("/", "")

    async def get_trading_pairs_from_mongodb(self):
        # На самом деле берёшь из memcached → оставляю как было, но делаю безопасно.
        substring = "MEXC Global_ob"
        try:
            raw = cache.get("blocks")
            if not raw:
                logger.warning("Memcache key 'blocks' is empty — нет символов для подписки.")
                return []

            data = pickle.loads(gzip.decompress(raw))
            keys = [key[0] for key in data if substring in key[0]]
            return await asyncio.gather(*[self.formatPair(key) for key in keys]) if keys else []
        except Exception as e:
            logger.error(f"get_trading_pairs_from_mongodb error: {e}")
            return []

    # ---------------- Message handling ----------------
    def _upsert_snapshot_from_pb_dict(self, symbol: str, pb_dict: dict, ts=None):
        """
        pb_dict — результат MessageToDict(wrapper)
        ищем блок publicLimitDepths/publiclimitdepths и собираем bids/asks.
        """
        d = (
            pb_dict.get("publiclimitdepths")
            or pb_dict.get("publicLimitDepths")
            or pb_dict.get("public_limit_depths")
        )
        if not d:
            return

        # asks/bids могут называться asksList/bidsList в JSON‑представлении доки.
        asks_list = d.get("asksList") or d.get("asks") or []
        bids_list = d.get("bidsList") or d.get("bids") or []

        try:
            asks = [[float(item["price"]), float(item["quantity"])] for item in asks_list]
            bids = [[float(item["price"]), float(item["quantity"])] for item in bids_list]
        except Exception as e:
            logger.error(f"Price/qty parse error for {symbol}: {e}")
            return

        ts = ts or datetime.now()
        self.order_book_updates[symbol] = {"bids": bids, "asks": asks, "lastUpdate": ts}

    async def handle_text_message(self, symbol: str, text: str):
        """
        Обрабатываем только служебные JSON-ответы: ACK на SUBSCRIPTION и PONG.
        Данные рынка приходят в BINARY и парсятся отдельно.
        """
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return

        # Примеры из доки: {"code":0,"msg":"PONG"} либо {"code":0,"msg":"spot@..."}
        if isinstance(data, dict):
            if data.get("code") == 0 and data.get("msg") == "PONG":
                logger.info(f"Received PONG for {symbol}")
            elif data.get("code") == 0 and isinstance(data.get("msg"), str):
                logger.info(f"Subscribed OK: {data['msg']}")
            elif data.get("code") != 0:
                logger.warning(f"WS server returned error for {symbol}: {data}")
        # остальное игнорируем

    async def handle_binary_message(self, symbol: str, blob: bytes):
        # 02.10 оптимизация: разбор protobuf — при записи снимка раз в 2 с, а не на каждое из ~2000 сообщений/с
        self.raw_updates[symbol] = (blob, datetime.now())

    def _parse_pending(self):
        pending = self.raw_updates
        self.raw_updates = {}
        for symbol, (blob, ts) in pending.items():
            try:
                wrapper = PushDataV3ApiWrapper()
                wrapper.ParseFromString(blob)
                d = None
                try:
                    if wrapper.HasField("publicLimitDepths"):
                        d = wrapper.publicLimitDepths
                except (ValueError, AttributeError):
                    d = None
                if d is not None:
                    # те же строки price/quantity, что отдавал MessageToDict → те же float
                    try:
                        asks = [[float(item.price), float(item.quantity)] for item in d.asks]
                        bids = [[float(item.price), float(item.quantity)] for item in d.bids]
                    except Exception as e:
                        logger.error(f"Price/qty parse error for {symbol}: {e}")
                        continue
                    self.order_book_updates[symbol] = {"bids": bids, "asks": asks, "lastUpdate": ts}
                else:
                    pb_dict = MessageToDict(wrapper, preserving_proto_field_name=False)
                    self._upsert_snapshot_from_pb_dict(symbol, pb_dict, ts)
            except Exception as e:
                logger.error(f"Protobuf parse error for {symbol}: {e}")

    async def handle_ping(self, ws: aiohttp.ClientWebSocketResponse, pair: str):
        ping_msg = {"method": "PING"}
        while True:
            try:
                await asyncio.sleep(PING_INTERVAL)
                if ws.closed:
                    break
                await ws.send_json(ping_msg)
                logger.info(f"Sent PING for {pair}")
            except Exception as e:
                logger.error(f"Error sending PING for {pair}: {e}")
                break

    # ---------------- WS lifecycle ----------------
    async def reconnect_websocket(self, symbol: str):
        """
        Открываем WS, подписываемся на partial depth (20 уровней), парсим бинарные сообщения.
        При любой ошибке выходим — выше нас перезапустят.
        """
        connected = False  # 02.10 backoff
        while True:
            try:
                proxy_url = random.choice(self.proxies)
                connector = aiohttp_socks.ProxyConnector.from_url(proxy_url) if proxy_url else aiohttp.TCPConnector()

                timeout = aiohttp.ClientTimeout(total=0, sock_connect=20, sock_read=0)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.ws_connect(WS_ENDPOINT, autoping=False, heartbeat=None) as ws:
                        logger.info(f"Connected to WebSocket for {symbol}")
                        connected = True

                        channel = CHANNEL_TEMPLATE.format(symbol=symbol, levels=LEVELS)
                        subscribe_message = {"method": "SUBSCRIPTION", "params": [channel]}

                        await ws.send_str(json.dumps(subscribe_message))

                        ping_task = asyncio.create_task(self.handle_ping(ws, symbol))

                        async for msg in ws:
                            if msg.type == WSMsgType.TEXT:
                                await self.handle_text_message(symbol, msg.data)
                            elif msg.type == WSMsgType.BINARY:
                                await self.handle_binary_message(symbol, msg.data)
                            elif msg.type == WSMsgType.ERROR:
                                logger.error(f"WebSocket error for {symbol}: {ws.exception()}")
                                break
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                                break

                        # завершаем пинг‑таску
                        try:
                            ping_task.cancel()
                        except Exception:
                            pass

            except Exception as e:
                # чистим состояние по этому символу
                self.order_book_updates.pop(symbol, None)
                # 02.10 backoff: символ остаётся в active_trading_pairs — за него уже отвечает start_websocket,
                # иначе check_new_tokens каждые 5 с заводил вторую такую же задачу (шторм переподключений)
                logger.error(f"Error for {symbol}: {e}")
                return connected

            # Если вышли из цикла чтения без исключения — подождать и попробовать снова
            await asyncio.sleep(5)

    async def start_websocket(self, symbol: str):
        delay = 5
        while True:
            try:
                ok = await self.reconnect_websocket(symbol)
                # 02.10 backoff: при неудаче пауза растёт до 60 с, после удачного соединения — снова 5 с
                delay = 5 if ok else min(delay * 2, 60)
            except Exception as e:
                logger.error(f"Error in start_websocket for {symbol}: {e}")
                delay = min(delay * 2, 60)
            await asyncio.sleep(delay)

    # ---------------- Book aggregation & persistence ----------------
    async def check_new_tokens(self):
        while True:
            try:
                current_trading_pairs = set(await self.get_trading_pairs_from_mongodb())
                new_trading_pairs = current_trading_pairs - set(self.active_trading_pairs)

                for trading_pair in new_trading_pairs:
                    logger.info(f"New trading pair found: {trading_pair}")
                    self.active_trading_pairs.append(trading_pair)
                    asyncio.create_task(self.start_websocket(trading_pair))
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in check_new_tokens: {e}")

            await asyncio.sleep(5)

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
                                "exchange": "MEXC Global",
                                "symbol": symbol,
                                "bids": order_book["bids"],
                                "asks": order_book["asks"],
                                "lastUpdate": order_book["lastUpdate"].isoformat(),
                            }

                        serialized_data = pickle.dumps(all_order_books)
                        compressed_data = gzip.compress(serialized_data, 1)
                        cache.set("orderbook_MEXC_full_snapshot", compressed_data, 10)
                        logger.info(f"Order book update in Redis completed.")
                    except Exception as e:
                        logger.error(f"Error updating order books in Redis: {e}")
                else:
                    logger.info("No order books to update in Redis.")
            except Exception as e:
                logger.error(f"Error in update_database: {e}")

            await asyncio.sleep(2)  # обновляем раз в 2 сек

    async def run(self):
        try:
            symbols = await self.get_trading_pairs_from_mongodb()
            self.active_trading_pairs = list(symbols)

            database_task = asyncio.create_task(self.update_database())
            check_new_tokens_task = asyncio.create_task(self.check_new_tokens())

            websocket_tasks = [asyncio.create_task(self.start_websocket(symbol)) for symbol in symbols]

            await asyncio.gather(*websocket_tasks, database_task, check_new_tokens_task)
        except Exception as e:
            logger.error(f"Error in run: {e}")


if __name__ == "__main__":
    updater = MEXCOrderBookUpdater()
    asyncio.run(updater.run())
