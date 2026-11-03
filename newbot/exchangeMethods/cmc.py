import json,asyncio,aiohttp
from motor.motor_asyncio import AsyncIOMotorClient as mongoclient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
logging.basicConfig(filename='logs/cmc.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
class CMC:
    def __init__(self) -> None:
        self.client = mongoclient("mongodb://mongo:27017")
        self.cmc_api = ""  # Legacy optional enrichment is not launched.
        self.cmc_pattern = {'rank':'',
                            'symbol':'',
                            'slug':'',
                            'token_address':'',
                            'chain_token':'',}
        self.cmc_list = []

    
    async def sort_collected_data(self,data):
        self.cmc_list = []
        await asyncio.gather(*[self.prepare_object_for_load(obj) for obj in data['data']])
        await self.client.renderBase.cmc.delete_many({})
        await self.client.renderBase.cmc.insert_many(self.cmc_list)
        logging.info(f'Object CMC was loaded with {len(self.cmc_list)} len')

    async def prepare_object_for_load(self,obj:dict):
        base_obj = self.cmc_pattern.copy()
        base_obj['rank']=obj['rank']
        base_obj['symbol']=obj['symbol']
        base_obj['slug']=obj['slug']
        if not obj['platform'] is None:
            base_obj['token_address']=obj['platform']['token_address']
            base_obj['chain_token']=obj['platform']['symbol']
        else:
            base_obj['token_address']=''
            base_obj['chain_token']=obj['symbol']
        self.cmc_list.append(base_obj)
    
    async def collect_tokens_data(self):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
        async with aiohttp.ClientSession() as session:
            headers = {'X-CMC_PRO_API_KEY':self.cmc_api,
                       'Accepts': 'application/json', 
                'Content-Type': 'application/json', 
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
            response = await session.get(url=url,headers=headers)
            if response.status == 200:
                result = await response.json()
                await self.sort_collected_data(data=result)
            else:
                result =await response.text()
                logging.error(f'error happend while request: {result}')


async def main():
    cmc = CMC()
    sched = AsyncIOScheduler()
    sched.add_job(cmc.collect_tokens_data, 'interval', seconds=60)
    sched.start()
    logging.info('Collect started')
    while True:
        await asyncio.sleep(1)
        
asyncio.run(main())