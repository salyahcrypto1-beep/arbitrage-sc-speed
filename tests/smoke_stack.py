"""Disposable Compose integration test. No exchange or Telegram calls."""
import asyncio
import gzip
import os
from pathlib import Path
import pickle
import subprocess
import sys
import time
from datetime import datetime
from types import SimpleNamespace
from pymongo import MongoClient
from pymemcache.client.base import Client
import redis

ROOT = Path(__file__).resolve().parents[1]


def main():
    mongo = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=5000)
    mongo.admin.command('ping')
    assert redis.Redis(host='redis').ping()
    cache = Client(('memcached',11211),connect_timeout=5,timeout=5)
    cache.set('smoke',b'ok'); assert cache.get('smoke')==b'ok'
    from portable.access import pair_user, is_owner
    class FakeBot:
        async def send_message(self, *args, **kwargs): pass
    os.environ['PAIRING_CODE']='local-integration-code'
    async def check_pairing():
        user=SimpleNamespace(chat=SimpleNamespace(id=12345,type='private'),text='/start wrong')
        assert not await pair_user(user,FakeBot())
        user.text='/start local-integration-code'
        assert await pair_user(user,FakeBot())
        assert is_owner(12345)
        other=SimpleNamespace(chat=SimpleNamespace(id=67890,type='private'),text=user.text)
        assert not await pair_user(other,FakeBot())
        assert not is_owner(67890)
    asyncio.run(check_pairing())
    stamp=datetime.now().isoformat()
    for exchange,key,price in [('Kucoin','TEST-USDT',10.),('Bitget','TESTUSDT',11.)]:
        book={key:{'asks':[[price+.01,100000]],'bids':[[price,100000]],'lastUpdate':stamp}}
        cache.set(f'orderbook_{exchange}_full_snapshot',gzip.compress(pickle.dumps(book)),expire=30)
    net={'ERC20':{'fee':.01,'withdraw':True,'deposit':True,'contract':'0x123','adminFee':0}}
    blocks=[['fixture',['TEST/USDT','Kucoin','Bitget','ERC20',net,net]]]
    cache.set('blocks',gzip.compress(pickle.dumps(blocks)),expire=30)
    process=subprocess.Popen([sys.executable,'-u',str(ROOT/'newbot/render/signalrender.py')],cwd=ROOT/'newbot/render')
    try:
        for _ in range(20):
            if process.poll() is not None: raise AssertionError('Calculation process exited')
            value=cache.get('volumes')
            if value:
                rows=pickle.loads(gzip.decompress(value))
                if rows:
                    assert rows[0][1]['best_result'][1]>0
                    print('PASS: MongoDB, Redis, Memcached, owner pairing, live calculation worker → volumes')
                    break
            time.sleep(1)
        else:raise AssertionError('No computed volume produced from fixture')
    finally:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        cache.close();mongo.close()

if __name__=='__main__':main()
