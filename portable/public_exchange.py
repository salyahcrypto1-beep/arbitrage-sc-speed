"""Public-only compatibility adapter. Never configures exchange credentials."""
import asyncio
import os
import time
import ccxt.pro as ccxt
from portable.rest_fallback import RestClient

EXCHANGES = {'kucoin': 'Kucoin', 'gate': 'Gate', 'bitget': 'Bitget',
             'mexc': 'MEXC Global', 'bybit': 'Bybit', 'bingx': 'BingX',
             'htx': 'HTX', 'coinex': 'CoinEx', 'bitmart': 'Bitmart', 'okx': 'OKX'}

def native(name, symbol):
    if name in ('Kucoin', 'BingX', 'OKX'): return symbol.replace('/', '-')
    if name in ('Gate', 'Bitmart'): return symbol.replace('/', '_')
    if name == 'HTX': return symbol.replace('/', '').lower()
    return symbol.replace('/', '')

class PublicExchange:
    def __init__(self, exchange_id):
        self.id = exchange_id
        self.name = EXCHANGES[exchange_id]
        self.aiohttp_proxy = None  # compatibility; use OS VPN, never owner's proxy pool
        self._client = None
        self._tickers = {}
        self._ticker_at = 0
        self._lock = None

    @property
    def client(self):
        if self._client is None:
            self._client = getattr(ccxt, self.id)({
                'enableRateLimit': True, 'timeout': 20000,
                'options': {'defaultType': 'spot', 'fetchCurrencies': False},
                'newUpdates': True,
            }) if hasattr(ccxt, self.id) else RestClient(self.id)
            self._lock = asyncio.Lock()
        return self._client

    async def markets(self):
        return await self.client.load_markets()

    async def fetchTickers(self, sem=None):
        client = self.client
        async with self._lock:
            if time.monotonic() - self._ticker_at < 5:
                return self._tickers
            tickers = await client.fetch_tickers(params={'type': 'spot'})
            self._tickers = {native(self.name, symbol): {'ask': t.get('ask'), 'bid': t.get('bid')}
                             for symbol, t in tickers.items() if symbol.endswith('/USDT') and ':' not in symbol}
            if self.id == 'coinex':
                import gzip, pickle
                from pymemcache.client.base import Client
                cache = Client(('memcached', 11211), connect_timeout=2, timeout=2)
                try:
                    raw = cache.get(f'orderbook_{self.name}_full_snapshot')
                    for symbol, book in (pickle.loads(gzip.decompress(raw)) if raw else {}).items():
                        if book.get('bids') and book.get('asks'):
                            self._tickers[symbol] = {'bid': book['bids'][0][0], 'ask': book['asks'][0][0]}
                finally:
                    cache.close()
            self._ticker_at = time.monotonic()
            return self._tickers

    async def fetchOrderBookBot(self, symbol):
        markets = await self.markets()
        unified = next((s for s in markets if native(self.name, s) == symbol or s == symbol), None)
        if unified is None:
            return None
        result = await self.client.fetch_order_book(unified, limit=20)
        return {'asks': result['asks'], 'bids': result['bids']}

    async def fetchOrderBook(self, symbol, session=None):
        return await self.fetchOrderBookBot(symbol)

    async def close(self):
        if self._client is not None:
            await self._client.close()


def configured_exchanges():
    ids = [x.strip() for x in os.getenv('EXCHANGES', 'kucoin,gate,bitget,mexc,bybit,bingx,htx,coinex,bitmart,okx').split(',') if x.strip()]
    unknown = set(ids) - EXCHANGES.keys()
    if unknown:
        raise ValueError('Unknown exchanges: ' + ', '.join(sorted(unknown)))
    return [PublicExchange(x) for x in ids]
