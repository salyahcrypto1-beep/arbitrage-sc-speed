"""Small public REST transport for market discovery; exchange WebSockets stay native."""
import asyncio
import time
import aiohttp
from portable.rest_fallback import RestClient

TICKERS = {
 'kucoin': ('https://api.kucoin.com/api/v1/market/allTickers', ('data','ticker'), 'symbol','buy','sell','volValue'),
 'gate': ('https://api.gateio.ws/api/v4/spot/tickers', (), 'currency_pair','highest_bid','lowest_ask','quote_volume'),
 'bitget': ('https://api.bitget.com/api/v2/spot/market/tickers', ('data',), 'symbol','bidPr','askPr','usdtVolume'),
 'mexc': ('https://api.mexc.com/api/v3/ticker/bookTicker', (), 'symbol','bidPrice','askPrice',None),
 'bybit': ('https://api.bybit.com/v5/market/tickers?category=spot', ('result','list'), 'symbol','bid1Price','ask1Price','turnover24h'),
 'bingx': ('https://open-api.bingx.com/openApi/spot/v1/ticker/bookTicker', ('data',), 'symbol','bidPrice','askPrice',None),
 'htx': ('https://api.huobi.pro/market/tickers', ('data',), 'symbol','bid','ask','vol'),
 'okx': ('https://www.okx.com/api/v5/market/tickers?instType=SPOT', ('data',), 'instId','bidPx','askPx','volCcy24h'),
}

def unified(raw):
    compact = raw.upper().replace('-', '').replace('_', '')
    return compact[:-4] + '/USDT' if compact.endswith('USDT') and len(compact)>4 else None

class NativeRest:
    def __init__(self, exchange_id):
        self.id=exchange_id
        self.session=None
        self.fallback=RestClient(exchange_id) if exchange_id in ('coinex','bitmart') else None
        self.lock=asyncio.Lock()
        self.last=0
    async def fetch_tickers(self, params=None):
        if self.fallback: return await self.fallback.fetch_tickers()
        if self.session is None:
            self.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        url,path,key,bid,ask,volume=TICKERS[self.id]
        async with self.lock:
            await asyncio.sleep(max(0,1-(time.monotonic()-self.last)))
            self.last=time.monotonic()
            async with self.session.get(url) as r:
                r.raise_for_status(); data=await r.json()
        for part in path: data=data[part]
        if isinstance(data,dict): data=data.get('bookTicker', [data])
        result={}
        for item in data:
            symbol=unified(item[key])
            if symbol:
                result[symbol]={'bid':item.get(bid),'ask':item.get(ask),
                                'quoteVolume':float(item.get(volume) or 0) if volume else 0}
        return result
    async def load_markets(self):
        if self.fallback: return await self.fallback.load_markets()
        tickers=await self.fetch_tickers()
        return {s:{'symbol':s,'base':s.split('/')[0],'quote':'USDT','spot':True,'active':True} for s in tickers}
    async def close(self):
        if self.session: await self.session.close()
        if self.fallback: await self.fallback.close()
