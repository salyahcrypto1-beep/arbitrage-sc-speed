import asyncio
from datetime import datetime
from signalrender import ArbitrageBot, MathSignalsBot
from shanpot_manager import MyManager
from multiprocessing import Process
from django.core.cache import cache
from typing import Dict
from motor.motor_asyncio import AsyncIOMotorClient
import gzip
import pickle
import sys
import os
from datetime import datetime

from redis import asyncio as aioredis

sys.path.append(os.path.join(os.getcwd(),'..'))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges import exchanges

async def create_redis_pool() -> aioredis.Redis:
    return await aioredis.Redis(connection_pool=aioredis.ConnectionPool.from_url('redis://redis'))

redis = None

sem = asyncio.Semaphore(1000)
client = AsyncIOMotorClient('mongo', 27017)
arbitrage = client.arbitrage

async def run_symbols_for_every_part(allSymbolsData: Dict[str, Dict], botShed: MyManager.OrderbookUsersSnapshot, tikers: Dict[str, Dict]) -> None:
    for symbol, symbol_data in allSymbolsData.items():
        for exBuy in symbol_data.keys():
            for exSell in symbol_data.keys():
                try:
                    ex_networks_asks = symbol_data[exBuy]['networks']
                    ex_networks_bids = symbol_data[exSell]['networks']
                    await asyncio.gather(*[ArbitrageBot().netExLoops(symbol, exBuy, exSell, network, ex_networks_asks, ex_networks_bids, tikers) for network, _ in ex_networks_asks.items()])

                except Exception as e:
                    pass

async def run_symbols(allSymbolsData: Dict[str, Dict], botShed: MyManager.OrderbookUsersSnapshot, tikers: Dict[str, Dict]) -> None:
    num_processes = 40
    
    symbols = list(allSymbolsData.keys())
    part_size = len(symbols) // num_processes
    print('allSymbolsData', len(symbols))
    print('part_size', part_size)
    allSymbolsData_parts = [
        {symbol: allSymbolsData[symbol] for symbol in symbols[i:i+part_size]}
        for i in range(0, len(symbols), part_size)
    ]

    await asyncio.gather(*[ run_symbols_for_every_part(allSymbolsData_parts[i], botShed, tikers) for i in range(0, 30)])

processesPID = []

def process_symbols(allSymbolsData: Dict[str, Dict], botShed: MyManager.OrderbookUsersSnapshot, tikers: Dict[str, Dict]) -> None:
    processesPID = os.getpid()
    asyncio.run(run_symbols_for_every_part(allSymbolsData, botShed, tikers))

exist_signal_combinations = []
global_signal_combinations = []
global_signal_combinations_all_time = []
order_books = {
    'OKX': {}, 'Kucoin': {}, 'Gate': {}, 'MEXC Global': {}, 'Bitget': {},
    'Bybit': {}, 'BingX': {}, 'HTX': {}, 'CoinEx': {}, 'Bitmart': {},
}

async def checking_exist(key):
    if key.startswith('block1_'):
        data = await redis.get(key)
        if data:
            data = pickle.loads(data)
            min_combination = [data[0], data[1]]
            max_combination = [data[0], data[2]]
            if not (min_combination in exist_signal_combinations):
                exist_signal_combinations.append(min_combination)
            if not (max_combination in exist_signal_combinations):
                exist_signal_combinations.append(max_combination)
            
            global_signal_combinations.append(data)

async def load_signals():
    try:
        signals = []
        async for key in redis.scan_iter(match='signal_*'):
            compressed_data = await redis.get(key)
            if compressed_data:
                signal = pickle.loads(gzip.decompress(compressed_data))
                signals.append(signal)

        await arbitrage.data.delete_many({})
        await arbitrage.data.insert_many(signals)
        async for key in redis.scan_iter(match='signal_*'):
            await redis.delete(key)

    except Exception as e:
        pass

async def math_part_proccess(symbol, exMin, exMax, network, ex_networks_asks, ex_networks_bids):
    await MathSignalsBot().signalLoops(symbol, network, ex_networks_asks, ex_networks_bids, exMin, exMax)
    
def math_part_proccess_container(symbol, exMin, exMax, network, ex_networks_asks, ex_networks_bids):
    asyncio.run(math_part_proccess(symbol, exMin, exMax, network, ex_networks_asks, ex_networks_bids))

async def init_redis():
    global redis
    redis = await create_redis_pool()
         
async def main() -> None:
    global global_signal_combinations
    global exist_signal_combinations
    global global_signal_combinations_all_time
    
    await init_redis()  # Инициализация Redis
    
    manager = MyManager()
    manager.start()
    botShed = manager.OrderbookUsersSnapshot()

    bot = ArbitrageBot()

    botShed.start_update_scheduler()

    await asyncio.sleep(15)
    while True:
        try:
            start = datetime.now()
            allSymbolsData = await bot.get_all_symbols_data()
            tikers = {
                'OKX': {}, 'Kucoin': {}, 'Gate': {}, 'MEXC Global': {}, 'Bitget': {},
                'Bybit': {}, 'BingX': {}, 'HTX': {}, 'CoinEx': {}, 'Bitmart':{}
            }
            
            tickers = await asyncio.gather(*[looper(ex.fetchTickers,sem,ex.name) for ex in exchanges])
            for ticker in tickers:
                tikers[ticker[1]] = ticker[0]
            
            print(len(tickers))
            num_processes = 100
            symbols = list(allSymbolsData.keys())
            print(len(symbols))
            part_size = len(symbols) // num_processes
            
            allSymbolsData_parts = [
                {symbol: allSymbolsData[symbol] for symbol in symbols[i:i+part_size]}
                for i in range(0, len(symbols), part_size)
            ]
            
            async for key in redis.scan_iter(match='block1_*'):
                await redis.delete(key)
            
            processes = [
                Process(target=process_symbols, args=(allSymbolsData_part, botShed, tikers))
                for allSymbolsData_part in allSymbolsData_parts
            ]
            
            for process in processes:
                process.start()
                
            for process in processes:
                process.join()

            async for key in redis.scan_iter(match='block1_*'):
                await checking_exist(key) 
            
            print('global_signal_combinations: ', len(global_signal_combinations))
            print('exist_signal_combinations: ', len(exist_signal_combinations))

            end = datetime.now()
            print('End circle: ', end-start) 

            
            global_signal_combinations = []
            exist_signal_combinations = []

            await load_signals()

        except Exception as e:
            print('Tasks error: ', e)  
            break                

async def looper(func,sem,name):
    param = {}
    while param is None or param == {}:
        param =await func(sem)
    return param,name
    

if __name__ == '__main__':
    asyncio.run(main())
