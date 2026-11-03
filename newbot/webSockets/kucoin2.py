import asyncio
import json
import time
import zmq
import zmq.asyncio
import uvloop
import websocket
import base64
import hmac
import hashlib
import requests
import threading
from datetime import datetime
import diskcache as dc
import pickle
import gzip

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

cache = dc.Cache('../tmp/cache')

class KucoinProxy:
    def __init__(self):
        self.api_key = ""
        self.api_secret = ""
        self.api_passphrase = ""
        self.base_url = "https://api.kucoin.com"
        self.ws_endpoint = "wss://ws-api.kucoin.com"
        self.loop = asyncio.get_event_loop()
        self.context = zmq.asyncio.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind("tcp://*:5555")
        self.last_snapshot_time = 0
        self.last_ping_time = 0
        self.symbols = ['BTC-USDT']
        self.ws = None
        self.ws_connected = False
        self.ws_lock = threading.Lock()
        # Локальное хранилище для стаканов
        self.orderbooks = {}

    async def handle_evt(self, msg):
        print(f"Получен стакан для {msg['topic']}")
        if msg['topic'].startswith('/spotMarket/level2Depth50:'):
            symbol = msg['topic'].split(':')[1]
            await self.handle_level2_depth50_update(symbol, msg['data'])

    async def handle_level2_depth50_update(self, symbol, data):
        depth_data = {
            'symbol': symbol,
            'bids': data['bids'],
            'asks': data['asks']
        }
        await self.publisher.send_multipart([f"{symbol}_depth".encode(), json.dumps(depth_data).encode()])

        # Обновляем локальное хранилище стаканов
        self.update_local_orderbook(symbol, data)

    def update_local_orderbook(self, symbol, data):
        """Обновляет локальное хранилище стаканов."""
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {
                'bids': [],
                'asks': []
            }
        self.orderbooks[symbol]['bids'] = data['bids']
        self.orderbooks[symbol]['asks'] = data['asks']

    async def update_orderbook_cache(self):
        """Обновляет кэш стаканов раз в 5 секунд."""
        while True:
            for symbol, data in self.orderbooks.items():
                snapshot = {
                    "symbol": symbol,
                    "exchange": "Kucoin",
                    "bids": [[float(price), float(size), 0, 0] for price, size in data['bids']],
                    "asks": [[float(price), float(size), 0, 0] for price, size in data['asks']],
                    "lastUpdate": datetime.utcnow()
                }
                cache_key = f'orderbook_Kucoin_{symbol}'
                serialized_data = pickle.dumps(snapshot)
                cached_data = gzip.compress(serialized_data)
                cache.set(cache_key, cached_data, expire=300)
            await asyncio.sleep(5)

    async def ping_pong(self):
        while True:
            current_time = time.time()
            if current_time - self.last_ping_time >= 30:
                with self.ws_lock:
                    if self.ws_connected:
                        try:
                            self.ws.send(json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"}))
                            self.last_ping_time = current_time
                        except Exception as e:
                            print(f"Ошибка при отправке ping: {e}")
                    else:
                        print("WebSocket соединение закрыто. Пропуск отправки ping.")
            await asyncio.sleep(1)

    def get_ws_token(self):
        now = int(time.time() * 1000)
        str_to_sign = str(now) + 'GET' + '/api/v1/bullet-public'
        signature = base64.b64encode(
            hmac.new(self.api_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()
        )
        passphrase = base64.b64encode(
            hmac.new(self.api_secret.encode('utf-8'), self.api_passphrase.encode('utf-8'), hashlib.sha256).digest()
        )
        headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": str(now),
            "KC-API-KEY": self.api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2"
        }
        return headers

    async def start(self):
        max_retries = 5
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.post(f"{self.base_url}/api/v1/bullet-public", headers=self.get_ws_token())
                print(f"Статус код получения токена: {response.status_code}")
                if response.status_code == 200:
                    data = response.json()
                    token = data['data']['token']
                    ws_endpoint = f"{self.ws_endpoint}?token={token}"

                    self.ws = websocket.WebSocketApp(
                        ws_endpoint,
                        on_message=self.on_message,
                        on_error=self.on_error,
                        on_close=self.on_close,
                        on_open=self.on_open
                    )

                    websocket_thread = threading.Thread(target=self.ws.run_forever)
                    websocket_thread.start()
                    return
            except Exception as e:
                print(f"Ошибка при создании WebSocket соединения: {e}")
                if attempt < max_retries - 1:
                    print(f"Повторная попытка через {retry_delay} секунд...")
                    await asyncio.sleep(retry_delay)
                else:
                    print("Достигнуто максимальное количество попыток. Завершение работы.")
                    return

    def on_message(self, ws, message):
        data = json.loads(message)
        if 'data' in data:
            asyncio.run_coroutine_threadsafe(self.handle_evt(data), self.loop)

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket connection closed: {close_status_code} - {close_msg}")
        with self.ws_lock:
            self.ws_connected = False

    def on_open(self, ws):
        print("WebSocket connection opened")
        with self.ws_lock:
            self.ws_connected = True

        # Подписываемся на стаканы изначально заданных токенов
        for symbol in self.symbols:
            self.subscribe_to_symbol(symbol)

    def subscribe_to_symbol(self, symbol):
        """Подписывается на обновления стакана для заданного символа."""
        with self.ws_lock:
            if self.ws_connected:
                self.ws.send(json.dumps({
                    "id": str(int(time.time() * 1000)),
                    "type": "subscribe",
                    "topic": f"/spotMarket/level2Depth50:{symbol}",
                    "privateChannel": False,
                    "response": True
                }))
                print(f"Подписка на {symbol} отправлена")

    def formatPair(self, pair):
        return str(pair).replace('/','-')
    async def get_trading_pairs_from_cache(self):
        substring = 'Kucoin_ob'
        
        filtered_keys = [key for key in cache.iterkeys() if substring in key]
        
        filtered_values = []
        filtered_values= [cache.get(key) for key in filtered_keys]
        
        if filtered_values and len(filtered_values)>0:
            return [self.formatPair(pair[0]) for pair in filtered_values]
        return []
    
    async def check_new_tokens(self):
        """Проверяет наличие новых токенов в кэше и подписывается на них."""
        while True:
            new_symbols = await self.get_trading_pairs_from_cache()
            
            for symbol in new_symbols:
                if symbol not in self.symbols:
                    self.symbols.append(symbol)
                    self.subscribe_to_symbol(symbol)
            await asyncio.sleep(5)

    def run(self):
        self.loop.create_task(self.start())
        self.loop.create_task(self.ping_pong())
        self.loop.create_task(self.check_new_tokens())
        # Запускаем задачу по обновлению кэша стаканов
        self.loop.create_task(self.update_orderbook_cache())
        self.loop.run_forever()

if __name__ == "__main__":
    proxy = KucoinProxy()
    proxy.run()
