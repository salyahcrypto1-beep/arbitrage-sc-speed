import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import threading
from multiprocessing.managers import BaseManager, NamespaceProxy
from asgiref.sync import sync_to_async
import json
from bson import ObjectId
from datetime import datetime
import gzip
import pickle
import diskcache as dc
from redis import asyncio as aioredis

cache = dc.Cache('../tmp/cache')


async def setup_redis():
    print('setup_redis')
    return await aioredis.from_url("redis://redis")

redis = asyncio.run(setup_redis())
print('312')

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class OrderbookUsersSnapshot:
    def __init__(self):
        self.client = AsyncIOMotorClient('mongo', 27017)
        self.db_orderbooks = self.client.orderbooks
        self.db_userdata = self.client.userdata
        self.arbitrage = self.client.arbitrage
        self.redis = redis

    async def update_users_snapshots_scheduler(self):
        while True:
            print('123')
            try:
                users = await self.db_userdata.user.find().to_list(length=None)
                
                async for key in self.redis.scan_iter(match='user_*'):
                    await self.redis.delete(key)
                
                if users:
                    for user in users:
                        compressed_data = gzip.compress(pickle.dumps(user))
                        cache_key = f'user_{user["id"]}'
                        await self.redis.set(cache_key, compressed_data, ex=300)
            except Exception as e:
                print('Error update_users_snapshots_scheduler', e)
                pass
    
            await asyncio.sleep(30)

    def start_update_scheduler(self):
        try:
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=self._run_update_scheduler, args=(loop,))
            thread.start()
        except Exception as e:
                print('Error start_update_scheduler', e)
                pass

    def _run_update_scheduler(self, loop):
        try:
            asyncio.set_event_loop(loop)
            user = asyncio.ensure_future(self.update_users_snapshots_scheduler(), loop=loop)
            #signals = asyncio.ensure_future(self.load_signals_scheduler(), loop=loop)

            async def run_tasks():
                await asyncio.gather(user)

            loop.run_until_complete(run_tasks())
        except Exception as e:
                print('Error _run_update_scheduler', e)
                pass

class OrderbookUsersSnapshotProxy(NamespaceProxy):
    _exposed_ = ('__getattribute__', '__setattr__', '__delattr__', 'get', 'start_update_scheduler')

    def get(self, exchange, symbol):
        return self._callmethod('get', (exchange, symbol))

    def start_update_scheduler(self):
        return self._callmethod('start_update_scheduler')

class AllSymbolsDataSnapshot:
    def __init__(self, botShed, check_new_tokens_func):
        self.client = AsyncIOMotorClient('mongo', 27017)
        self.db_renderBase = self.client.renderBase
        self.data_updated_event = asyncio.Event()
        self.check_new_tokens_func = check_new_tokens_func
        self.botShed = botShed
        self.previous_allsymbolsdata = None
        self.processed_tokens = set()

    def start_update_scheduler(self):
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=self._run_update_scheduler, args=(loop,))
        thread.start()

    def _run_update_scheduler(self, loop):
        asyncio.set_event_loop(loop)

    def set_arbitrage_bot(self, arbitrage_bot):
        self.arbitrage_bot = arbitrage_bot

class AllSymbolsDataSnapshotProxy(NamespaceProxy):
    _exposed_ = ('__getattribute__', '__setattr__', '__delattr__', 'start_update_scheduler')

    def start_update_scheduler(self):
        return self._callmethod('start_update_scheduler')

class MyManager(BaseManager):
    pass

MyManager.register('OrderbookUsersSnapshot', OrderbookUsersSnapshot, OrderbookUsersSnapshotProxy)
MyManager.register('AllSymbolsDataSnapshot', AllSymbolsDataSnapshot, AllSymbolsDataSnapshotProxy)
