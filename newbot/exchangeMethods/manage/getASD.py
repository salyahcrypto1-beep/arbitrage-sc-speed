import asyncio
import json
import gzip
import pickle
from pymemcache.client import base
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict
from datetime import datetime
import motor.motor_asyncio

CACHE_KEY_BLOCKS = 'blocks'
CACHE_KEY_VOLUMES = 'volumes'
CACHE_KEY_SIGNALS = 'signals'
SYMBOLS_FILE = "allSymbolsData.json"
BLOCKS_FILE = "blocks_results.json"
VOLUMES_FILE = "volumes_results.json"
SIGNALS_FILE = "cached_signals.json"

# Функция для преобразования объекта datetime в строку
def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

class ArbitrageBot:
    def __init__(self):
        CONNECTION_STRING = "mongodb://mongo:27017"
        self.client = motor.motor_asyncio.AsyncIOMotorClient(CONNECTION_STRING)
        self.db_renderBase = self.client.renderBase


    async def get_all_symbols_data(self) -> Dict[str, str]:
        cursor = self.db_renderBase.AllSymbolsData.find()
        all_symbols_data = {}
        
        async for item in cursor:
            all_symbols_data[item['symbol']] = item['currency']
        
        return all_symbols_data


async def update_symbols_file(bot: ArbitrageBot):
    while True:
        all_symbols_data = await bot.get_all_symbols_data()
        try:
            with open(SYMBOLS_FILE, "w", encoding="utf-8") as f:
                json.dump(all_symbols_data, f, ensure_ascii=False, indent=4, default=default_serializer)
            print("[INFO] Symbols data updated in allSymbolsData.json")
        except Exception as e:
            print(f"[ERROR] Error updating {SYMBOLS_FILE}: {e}")
        await asyncio.sleep(10)  # Обновление каждые 10 секунд

async def fetch_and_save_cache(cache_key: str, output_file: str):
    client = base.Client(('memcached', 11211))
    while True:
        try:
            data = client.get(cache_key)
            if data:
                decompressed_data = pickle.loads(gzip.decompress(data))
                try:
                    with open(output_file, 'w', encoding="utf-8") as f:
                        json.dump(decompressed_data, f, ensure_ascii=False, indent=4, default=default_serializer)
                    print(f"[INFO] Cache saved to {output_file}")
                except Exception as e:
                    print(f"[ERROR] Error saving {output_file}: {e}")
            else:
                print(f"[INFO] No data in cache for {cache_key}")
        except Exception as e:
            print(f"[ERROR] {e}")
        
        await asyncio.sleep(10)  # Обновление каждые 10 секунд

async def main():
    bot = ArbitrageBot()
    # Обновление данных символов
    asyncio.create_task(update_symbols_file(bot))
    
    # Обновление остальных файлов из кэша
    asyncio.create_task(fetch_and_save_cache(CACHE_KEY_BLOCKS, BLOCKS_FILE))
    asyncio.create_task(fetch_and_save_cache(CACHE_KEY_VOLUMES, VOLUMES_FILE))
    asyncio.create_task(fetch_and_save_cache(CACHE_KEY_SIGNALS, SIGNALS_FILE))

    # Бесконечный цикл
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
