"""Launch an original exchange-specific WebSocket implementation in its own process."""
import asyncio
import gzip
import importlib
import os
from pathlib import Path
import pickle
import sys
from datetime import datetime
from pymongo import MongoClient
from pymemcache.client.base import Client
from portable.public_exchange import EXCHANGES
from portable.freshness import book_is_fresh

CLASSES={'kucoin':'KucoinOrderBookUpdater','gate':'GateOrderBookUpdater',
 'bitget':'BitgetOrderBookUpdater','mexc':'MEXCOrderBookUpdater','bybit':'BybitOrderBookUpdater',
 'bingx':'BingXOrderBookUpdater','htx':'HTXOrderBookUpdater','coinex':'CoinExOrderBookUpdater',
 'bitmart':'BitmartOrderBookManager','okx':'OKXOrderBookUpdater'}
METHODS={'kucoin':'run','gate':'run','bitget':'run_websocket','mexc':'run',
 'bybit':'run_websocket','bingx':'run_websocket','htx':'run_websocket','coinex':'run',
 'bitmart':'run','okx':'run'}

async def main(exchange_id):
    Path('logs').mkdir(exist_ok=True)
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'newbot/webSockets'))
    os.environ['BOT_DIRECT_CONNECTION']='1'
    module=importlib.import_module('newbot.webSockets.'+exchange_id)
    updater=getattr(module,CLASSES[exchange_id])()
    mongo=MongoClient('mongodb://mongo:27017',serverSelectionTimeoutMS=3000)
    cache=Client(('memcached',11211),connect_timeout=3,timeout=3)
    name=EXCHANGES[exchange_id]
    async def pairs():
        # Market selection is independent of route profitability: MM needs its own universe.
        doc=await asyncio.to_thread(mongo.renderBase.tradePairs.find_one, {'id':name})
        return doc.get('tokens',[]) if doc else []
    for method in ('get_trading_pairs_from_cache','get_trading_pairs_from_mongodb'):
        if hasattr(updater,method): setattr(updater,method,pairs)
    async def mirror():
        # OKX historically wrote per-symbol keys; calculations consume full snapshots.
        while True:
            if exchange_id=='okx':
                books={}
                for symbol,book in updater.order_book_updates.copy().items():
                    b=dict(book)
                    if isinstance(b.get('lastUpdate'),datetime): b['lastUpdate']=b['lastUpdate'].isoformat()
                    if book_is_fresh(b): books[symbol]=b
                cache.set('orderbook_OKX_full_snapshot',gzip.compress(pickle.dumps(books),1),10)
            elif exchange_id=='mexc':
                raw=cache.get('orderbook_MEXC_full_snapshot')
                if raw: cache.set('orderbook_MEXC Global_full_snapshot',raw,10)
            await asyncio.sleep(1)
    try:
        if exchange_id=='okx':
            # Original run() marked pairs active without connecting them.
            await asyncio.gather(updater.check_new_tokens(),mirror())
        elif exchange_id=='bingx':
            await asyncio.gather(updater.run_websocket(),updater.update_database(),mirror())
        else:
            await asyncio.gather(getattr(updater,METHODS[exchange_id])(),mirror())
    finally:
        cache.close(); mongo.close()

if __name__=='__main__': asyncio.run(main(sys.argv[1]))
