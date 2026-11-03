"""Run inside the container: python -m portable.doctor [--network]."""
import asyncio
import gzip
import json
import os
import pickle
import sys
from datetime import datetime
from pymongo import MongoClient
from pymemcache.client.base import Client
from portable.public_exchange import configured_exchanges

async def network():
    async def check(ex):
        try:
            tickers = await asyncio.wait_for(ex.client.fetch_tickers(), timeout=35)
            return {'exchange': ex.id, 'public_tickers': len(tickers)}
        except Exception as exc:
            return {'exchange': ex.id, 'error': type(exc).__name__, 'hint': 'Check network, DNS, VPN and exchange availability'}
        finally: await ex.close()
    return await asyncio.gather(*(check(ex) for ex in configured_exchanges()))

def main():
    mongo = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=3000)
    cache = Client(('memcached',11211),connect_timeout=3,timeout=3)
    result = {'telegram_token_configured': bool(os.getenv('TELEGRAM_BOT_TOKEN'))}
    try:
        mongo.admin.command('ping')
        result['mongo'] = 'ok'
        result['owner_paired'] = bool(mongo.userdata.local_owner.find_one({'_id':'owner'}))
        result['symbols'] = mongo.renderBase.AllSymbolsData.count_documents({})
        snapshots = {}
        for ex in configured_exchanges():
            raw = cache.get(f'orderbook_{ex.name}_full_snapshot')
            books = pickle.loads(gzip.decompress(raw)) if raw else {}
            snapshots[ex.id] = sum(0 <= (datetime.now()-datetime.fromisoformat(b['lastUpdate'])).total_seconds() <= 30 for b in books.values())
        result['fresh_books'] = snapshots
        if '--network' in sys.argv: result['network'] = asyncio.run(network())
        print(json.dumps(result,ensure_ascii=False,indent=2))
        if not any(snapshots.values()): raise SystemExit(2)
    finally:
        mongo.close();cache.close()

if __name__ == '__main__':main()
