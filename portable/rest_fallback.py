"""Native public REST market discovery for CoinEx and BitMart."""
import asyncio
import time
import aiohttp

class RestClient:
    has = {'watchOrderBook': False}
    def __init__(self, exchange_id):
        self.id = exchange_id
        self.session = None
        self._markets = None
        self.lock = asyncio.Lock()
        self.last_request = 0

    async def get(self, path, params=None):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        host = 'https://api.coinex.com/v2' if self.id == 'coinex' else 'https://api-cloud.bitmart.com'
        async with self.lock:
            await asyncio.sleep(max(0, .3 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            async with self.session.get(host + path, params=params) as response:
                response.raise_for_status()
                result = await response.json()
        if result.get('code') not in (0, 1000):
            raise RuntimeError(f'{self.id}: public API returned {result.get("code")}')
        return result['data']

    async def load_markets(self):
        if self._markets is not None: return self._markets
        data = await self.get('/spot/market' if self.id == 'coinex' else '/spot/v1/symbols/details')
        items = data if self.id == 'coinex' else data['symbols']
        self._markets = {}
        for m in items:
            base, quote = m['base_ccy'] if self.id == 'coinex' else m['base_currency'], m['quote_ccy'] if self.id == 'coinex' else m['quote_currency']
            symbol = base + '/' + quote
            self._markets[symbol] = {'symbol': symbol, 'base': base, 'quote': quote, 'spot': True,
                                     'active': m.get('status', 'trading') in ('online', 'trading'),
                                     'id': m['market'] if self.id == 'coinex' else m['symbol']}
        return self._markets

    async def fetch_tickers(self, params=None):
        markets = await self.load_markets()
        byid = {v['id']: k for k, v in markets.items()}
        data = await self.get('/spot/ticker' if self.id == 'coinex' else '/spot/quotation/v3/tickers')
        result = {}
        for item in data:
            if self.id == 'coinex':
                symbol = byid.get(item['market'])
                # CoinEx ticker has no best bid/ask: omit instead of using last trade.
                if symbol: result[symbol] = {'bid': None, 'ask': None, 'quoteVolume': float(item.get('value') or 0)}
            else:
                symbol = byid.get(item[0])
                if symbol: result[symbol] = {'bid': float(item[8] or 0), 'ask': float(item[10] or 0), 'quoteVolume': float(item[3] or 0)}
        return result

    async def fetch_order_book(self, symbol, limit=20):
        markets = await self.load_markets(); market = markets[symbol]['id']
        if self.id == 'coinex':
            data = await self.get('/spot/depth', {'market': market, 'limit': limit, 'interval': '0'})
            data = data['depth']
        else:
            data = await self.get('/spot/quotation/v3/books', {'symbol': market, 'limit': limit})
        return {side: [[float(v[0]), float(v[1])] for v in data[side]] for side in ('asks', 'bids')}

    async def close(self):
        if self.session: await self.session.close()
