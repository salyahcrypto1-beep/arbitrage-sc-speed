import asyncio
from bisect import bisect_left, bisect_right
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import gzip
import pickle
import diskcache as dc
import sys
import os
import numpy as np
import concurrent.futures
from decimal import Decimal, getcontext
import math
from signalrender import MathSignalsBot
import multiprocessing
import random
from pymemcache.client import base
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
# aioredis нигде не используется (setup_redis ниже не вызывается), а на
# Python 3.12 пакет не импортируется вовсе — «duplicate base class TimeoutError».
# Импорт сделан необязательным, чтобы не тянуть сломанную зависимость.
try:
    import aioredis
except Exception:
    aioredis = None


sys.path.append(os.path.join(os.getcwd(),'..'))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/users_render.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

cache = base.Client(('memcached', 11211))

client = AsyncIOMotorClient('mongo', 27017)

async def setup_redis():
    return await aioredis.from_url("redis://redis")

last_keys_opened = set()

class MemcachedConnectionPool:
    def __init__(self, host="localhost", port=11211, pool_size=20):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.pool = []
        self.lock = asyncio.Lock()
        
    async def get_connection(self):
        async with self.lock:
            if not self.pool:
                # Create a new connection if pool is empty
                return base.Client((self.host, self.port), connect_timeout=2, timeout=2)
            else:
                # Get a connection from the pool
                return self.pool.pop()
    
    async def release_connection(self, connection):
        try:
            # Test if connection is still valid
            test = await asyncio.to_thread(lambda: connection.stats())
            if test:
                async with self.lock:
                    if len(self.pool) < self.pool_size:
                        self.pool.append(connection)
                    else:
                        # Pool is full, discard connection
                        pass
        except Exception:
            # Connection is invalid, discard it
            pass


class UserVolumeIndex:
    """Индекс пользователей по объёмам, строится ОДИН РАЗ за цикл.

    Раньше на КАЖДЫЙ сигнал делалось три полных прохода по всему списку
    пользователей (group1/group2/group3) — сложность O(сигналы x пользователи).
    При 500 сигналах и 1000 юзерах это 1.5 млн итераций за цикл, и росло
    линейно с числом пользователей.

    Здесь пользователи один раз группируются по РАЗЛИЧНЫМ значениям объёмов
    (их сильно меньше, чем юзеров — объёмы задаются из меню), значения
    сортируются, и выборка группы делается бинарным поиском по границам.
    Стоимость обработки сигнала перестаёт зависеть от числа пользователей.

    Каждый пользователь хранится парой (порядковый_номер, user), чтобы любая
    выборка могла быть восстановлена в исходном порядке списка users —
    тогда суммы объёмов складываются в том же порядке, что и раньше, и
    средние значения совпадают побитово.
    """

    def __init__(self, users: List[Dict]):
        by_max: Dict[float, List[Tuple[int, Dict]]] = {}
        by_min: Dict[float, List[Tuple[int, Dict]]] = {}
        intervals: Dict[Tuple[float, float], List[Tuple[int, Dict]]] = {}

        for position, user in enumerate(users):
            try:
                min_volume = user['minVolume']
                max_volume = user['maxVolume']
            except KeyError:
                continue
            item = (position, user)
            by_max.setdefault(max_volume, []).append(item)
            by_min.setdefault(min_volume, []).append(item)
            intervals.setdefault((min_volume, max_volume), []).append(item)

        self.max_values = sorted(by_max)
        self.max_nodes = [by_max[value] for value in self.max_values]

        self.min_values = sorted(by_min)
        self.min_nodes = [by_min[value] for value in self.min_values]

        # Для group2 нужен «прокалывающий» запрос: интервалы, содержащие точку.
        # Сортируем по нижней границе — тогда кандидаты это префикс массива.
        interval_keys = sorted(intervals)
        self.interval_lows = [key[0] for key in interval_keys]
        self.interval_highs = [key[1] for key in interval_keys]
        self.interval_nodes = [intervals[key] for key in interval_keys]

        self.total_users = len(users)

    def group1_nodes(self, min_boundary, best_volume):
        """min_boundary <= maxVolume < best_volume"""
        left = bisect_left(self.max_values, min_boundary)
        right = bisect_left(self.max_values, best_volume)
        return self.max_values[left:right], self.max_nodes[left:right]

    def group3_nodes(self, best_volume, max_boundary):
        """best_volume < minVolume <= max_boundary"""
        left = bisect_right(self.min_values, best_volume)
        right = bisect_right(self.min_values, max_boundary)
        return self.min_values[left:right], self.min_nodes[left:right]

    def group2(self, best_volume) -> List[Dict]:
        """minVolume <= best_volume <= maxVolume"""
        limit = bisect_right(self.interval_lows, best_volume)
        highs = self.interval_highs
        nodes = self.interval_nodes
        collected: List[Tuple[int, Dict]] = []
        for index in range(limit):
            if highs[index] >= best_volume:
                collected.extend(nodes[index])
        collected.sort(key=_position_of)
        return [user for _, user in collected]


def _position_of(item: Tuple[int, Dict]) -> int:
    return item[0]


def _bucket(values, nodes, key_func) -> List[List[Dict]]:
    """Собирает подгруппы по ключу округления объёма.

    Возвращает подгруппы, в каждой пользователи стоят в том же порядке, что и
    в исходном списке users. Благодаря этому последующее `sum(...) / len(...)`
    складывает те же числа в том же порядке — средние совпадают побитово.
    """
    buckets: Dict[float, List[Tuple[int, Dict]]] = {}
    for value, node in zip(values, nodes):
        buckets.setdefault(key_func(value), []).extend(node)

    subgroups = []
    for members in buckets.values():
        members.sort(key=_position_of)
        subgroups.append([user for _, user in members])
    return subgroups


def group1_volume_key(volume):
    if volume <= 200: return round(volume / 2) * 2
    elif volume <= 400: return round(volume / 5) * 5
    elif volume <= 800: return round(volume / 10) * 10
    elif volume <= 1400: return round(volume / 20) * 20
    else: return round(volume / 40) * 40


def group3_volume_key(volume):
    if volume <= 200: return round(volume / 2) * 2
    else: return round(volume / 5) * 5


class UsersignalsBot:
    allExchanges: List[str] = ['OKX', 'Kucoin', 'Gate', 'MEXC', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx','Bitmart']

    def __init__(self):
        self.orderbook_cache = {}
        # Дедуплицированное хранение сигналов.
        #
        # Раньше здесь лежал словарь с ОТДЕЛЬНОЙ записью на каждую пару
        # «сигнал × пользователь»: полезная нагрузка (25 полей, ~716 байт)
        # копировалась каждому получателю. При 5000 пользователей это 210 000
        # словарей и ~143 МБ в памяти процесса, которые вдобавок пересобирались
        # и пиклились каждый цикл.
        #
        # Теперь нагрузка хранится ОДИН раз на сигнал, а получатели — отдельным
        # компактным словарём {user_id: datetime первого показа}. Те же 5000
        # пользователей занимают ~8 МБ вместо 143 МБ.
        #
        # base_key = 'signal_{min_exchange}_{max_exchange}_{symbol}_{network}'
        # — тот же префикс, что раньше стоял перед '_ID_{user}', поэтому
        # очистка по префиксу продолжает работать без изменений.
        self.signal_payloads: Dict[str, Dict] = {}
        self.signal_recipients: Dict[str, Dict[int, datetime]] = {}
        self.local_orderbooks = {}
        self.client = AsyncIOMotorClient('mongo', 27017)
        self.signals_lock = asyncio.Lock()  # Add lock for thread-safe dict operations
        # Один общий экземпляр на весь процесс. Раньше MathSignalsBot()
        # создавался на КАЖДУЮ подгруппу КАЖДОГО сигнала, а в его __init__
        # поднимается новый pymongo.MongoClient — то есть новый пул соединений
        # к Mongo тысячи раз за цикл.
        self.math_bot = MathSignalsBot()
        # Тиры Bitmart раньше запрашивались из Mongo при каждом вызове
        # get_spread_for_group. Теперь — один запрос на цикл.
        self.tiers: List[Dict] = []
        
    async def load_all_signals(self):
        try:
            async with self.signals_lock:
                # Копируем компактно: нагрузка по ссылке (она неизменяема
                # после сборки), получатели — поверхностной копией.
                payloads = dict(self.signal_payloads)
                recipients = {key: dict(users)
                              for key, users in self.signal_recipients.items()
                              if users}

            # Оставляем только те сигналы, у которых есть хотя бы один получатель
            payloads = {key: value for key, value in payloads.items() if key in recipients}

            if not payloads:
                return

            total = sum(len(users) for users in recipients.values())
            print(f'{len(payloads)} сигналов на {total} получателей')

            snapshot = {
                'version': 2,
                'payloads': payloads,
                'recipients': recipients,
            }
            compressed_data = await asyncio.to_thread(lambda: gzip.compress(pickle.dumps(snapshot)))
            signal_key = 'signals'
            await asyncio.to_thread(cache.set, signal_key, compressed_data, 100)
        except Exception as e:
            print(f"Error in load_all_signals: {e}")
        

    async def load_signal_every_user(self, users, min_exchange, max_exchange, contract, deposit, withdraw, countOrderPrice, volumeSellSumm, symbol, feeVal, networkSumm, result_dict, summasksVol, summbidsVol, summProfit, summDollarSpread):
        try:
            # Полезная нагрузка одинакова для ВСЕХ получателей сигнала —
            # различаются только userId и datetime. Поэтому она собирается один
            # раз и хранится один раз; получателям достаётся лишь запись
            # {user_id: datetime}. Порядок ключей сохранён прежний: потребитель
            # достраивает из неё словарь ровно того же вида, что и раньше.
            payload = {
                'symbol': symbol,
                'min_exchange': min_exchange,
                'summMin': str(result_dict['summMin']),
                'summasksVol': str(summasksVol),
                'countOrderAsks': str(result_dict['countOrderAsks']),
                'max_exchange': max_exchange,
                'summMax': str(result_dict['summMax']),
                'summbidsVol': str(summbidsVol),
                'countOrderBids': str(result_dict['countOrderBids']),
                'summProfit': str(summProfit),
                'SummDollarSpread': str(summDollarSpread),
                'networkSumm': str(networkSumm),
                'feeVal': str(float(feeVal) * result_dict['summMin']),
                'volumeSellSumm': str(volumeSellSumm),
                'countOrderPrice': str(countOrderPrice),
                'alltokensSell': str(result_dict['alltokensSell']),
                'alltokensBuy': str(result_dict['alltokensBuy']),
                'firstBuyPrice': str(result_dict['firstBuyPrice']),
                'lastBuyPrice': str(result_dict['lastBuyPrice']),
                'firstSellPrice': str(result_dict['firstSellPrice']),
                'lastSellPrice': str(result_dict['lastSellPrice']),
                'withdraw': withdraw,
                'deposit': deposit,
                'contract': contract
            }

            base_key = f'signal_{min_exchange}_{max_exchange}_{symbol}_{networkSumm}'
            now = datetime.now()

            # Один захват блокировки на всю подгруппу вместо захвата на каждого
            # пользователя (раньше — тысячи операций с блокировкой за цикл).
            async with self.signals_lock:
                self.signal_payloads[base_key] = payload
                known = self.signal_recipients.setdefault(base_key, {})
                for user in users:
                    user_id = user["id"]
                    # datetime — момент, когда ИМЕННО ЭТОТ пользователь впервые
                    # увидел сигнал (из него считается «время жизни»), поэтому
                    # хранится по получателю. Но это 8 байт, а не копия всех
                    # 25 полей, как было раньше.
                    if user_id not in known:
                        known[user_id] = now

        except Exception as e:
            print(f"Error in load_signal_every_user: {e}")


    async def _drop_recipients(self, min_exchange, max_exchange, symbol, network, users) -> None:
        """Снимает пользователей с сигнала; сигнал без получателей удаляется целиком."""
        base_key = f'signal_{min_exchange}_{max_exchange}_{symbol}_{network}'
        async with self.signals_lock:
            known = self.signal_recipients.get(base_key)
            if not known:
                return
            for user in users:
                known.pop(user["id"], None)
            if not known:
                self.signal_recipients.pop(base_key, None)
                self.signal_payloads.pop(base_key, None)

    async def get_spread_for_group(self, isbest, users, symbol: str, volume: float, symbolOrderBookMin: Dict, symbolOrderBookMax: Dict, min_exchange: str, max_exchange: str, feeVal: float, globalforSymb: Dict, network: str, ex_networks_asks: Dict, ex_networks_bids: Dict) -> List[Dict]:
        try:
            if not users:
                return None
                
            result = None
            withdraw = ex_networks_asks.get(network, {}).get('withdraw', 0)
            deposit = ex_networks_bids.get(network, {}).get('deposit', 0)
            if isbest:
                result = globalforSymb['best_result']
            elif not isbest and withdraw and deposit:
                math_bot = self.math_bot
                result = await math_bot.orderbookMath(symbolOrderBookMin, symbolOrderBookMax, volume, feeVal, symbol, min_exchange, max_exchange, math_bot.spotFees, self.tiers)
            else: 
                result = globalforSymb['best_result']
                                                                                                
            # Early validation to avoid unnecessary processing
            if not result or not isinstance(result, list) or len(result) <= 2 or result[1] <= 1:
                # Сигнал перестал быть актуальным — снимаем этих получателей
                await self._drop_recipients(min_exchange, max_exchange, symbol, network, users)
                return None

            result_dict = result[2]
            networkSumm = network

            summasksVol = result_dict['summasksVol']
            summbidsVol = result_dict['summbidsVol']
            summDollarSpread = result_dict['summDollarSpread']
            
            if summasksVol == 0 or result_dict['countOrderAsks'] < 1 or result_dict['countOrderBids'] < 1 or summDollarSpread < 1:
                # Условия не выполнены — снимаем этих получателей
                await self._drop_recipients(min_exchange, max_exchange, symbol, network, users)
                return None

            summProfit = (summbidsVol / summasksVol - 1) * 100
            contract = ex_networks_asks.get(network, {}).get('contract', '') if ex_networks_asks.get(network, {}).get('contract', '') == ex_networks_bids.get(network, {}).get('contract', '') else ''

            await self.load_signal_every_user(users, min_exchange, max_exchange, contract, deposit, withdraw, result_dict['countOrderPrice'], result_dict['volumeSellSumm'], symbol, feeVal, networkSumm, result_dict, summasksVol, summbidsVol, summProfit, summDollarSpread)
            return None
        
        except Exception as e:
            print(f'Error in get_spread_for_group: {e}')
            return None


    async def process_symbol(self, data: dict, index: UserVolumeIndex):
        try:
            if not index.total_users:
                return None

            # Get orderbooks
            symbolOrderBookMin = await self.get_socket_orderbook(data['symbol'], data['min_exchange'])
            symbolOrderBookMax = await self.get_socket_orderbook(data['symbol'], data['max_exchange'])
            
            if not symbolOrderBookMin or not symbolOrderBookMax:
                return None
                

            lastUpdateMin = datetime.fromisoformat(symbolOrderBookMin['lastUpdate'])
            lastUpdateMax = datetime.fromisoformat(symbolOrderBookMax['lastUpdate'])


            if (abs((lastUpdateMax - lastUpdateMin).total_seconds()) > 10 or any(not 0 <= (datetime.now() - t).total_seconds() <= 30 for t in (lastUpdateMin, lastUpdateMax))):
                return None

            min_boundary = data['min_boundary']
            max_boundary = data['max_boundary']
            best_volume = data['best_volume']

            # Выборка групп бинарным поиском по индексу вместо трёх полных
            # проходов по всем пользователям.
            group1_values, group1_nodes = index.group1_nodes(min_boundary, best_volume)
            group3_values, group3_nodes = index.group3_nodes(best_volume, max_boundary)
            group2 = index.group2(best_volume)

            # Skip if all groups are empty
            if not group1_nodes and not group2 and not group3_nodes:
                return None

            group1_subgroups = _bucket(group1_values, group1_nodes, group1_volume_key)
            group3_subgroups = _bucket(group3_values, group3_nodes, group3_volume_key)

            # Process all groups with proper task management
            tasks = []

            # Process group 1 subgroups
            for subgroup in group1_subgroups:
                avg_volume = sum(user['maxVolume'] for user in subgroup) / len(subgroup)
                tasks.append(self.get_spread_for_group(
                    False, subgroup, data['symbol'], avg_volume, 
                    symbolOrderBookMin, symbolOrderBookMax,
                    data['min_exchange'], data['max_exchange'], data['feeVal'],
                    {**data, 'best_volume': avg_volume}, data['networkSumm'],
                    data['ex_networks_asks'], data['ex_networks_bids']
                ))
            
            # Process group 2
            if group2:
                tasks.append(self.get_spread_for_group(
                    True, group2, data['symbol'], 0, 
                    symbolOrderBookMin, symbolOrderBookMax,
                    data['min_exchange'], data['max_exchange'], data['feeVal'],
                    data, data['networkSumm'], 
                    data['ex_networks_asks'], data['ex_networks_bids']
                ))
            
            # Process group 3 subgroups
            for subgroup in group3_subgroups:
                avg_volume = sum(user['minVolume'] for user in subgroup) / len(subgroup)
                tasks.append(self.get_spread_for_group(
                    False, subgroup, data['symbol'], avg_volume, 
                    symbolOrderBookMin, symbolOrderBookMax,
                    data['min_exchange'], data['max_exchange'], data['feeVal'],
                    {**data, 'best_volume': avg_volume}, data['networkSumm'],
                    data['ex_networks_asks'], data['ex_networks_bids']
                ))
            
            # Execute all tasks concurrently with semaphore to avoid too many concurrent tasks
            await asyncio.gather(*tasks)
            return None

        except Exception as e:
            print(f"Error in process_symbol: {e}")
            return None

    async def process_all_symbols(self, allSymbolsVolumes: List[List], users: List[Dict]):
        try:

            if not allSymbolsVolumes or not users:
                return []

            # Индекс строится один раз на цикл и переиспользуется всеми сигналами
            index = UserVolumeIndex(users)

            # Use a semaphore to limit concurrent processing
            semaphore = asyncio.Semaphore(10)  # Limit concurrent symbol processing

            async def process_with_semaphore(data):
                async with semaphore:
                    return await self.process_symbol(data[1], index)
            
            tasks = [process_with_semaphore(data) for data in allSymbolsVolumes]
            return await asyncio.gather(*tasks)
            
        except Exception as e:
            print(f"Error in process_all_symbols: {e}")
            return []
    

    async def get_socket_orderbook(self, symbol: str, exchange: str):
        try:

            # Format symbol according to exchange requirements
            format_map = {
                'OKX': lambda s: s.replace('/', '-'),
                'Kucoin': lambda s: s.replace('/', '-'),
                'Gate': lambda s: s.replace('/', '_'),
                'MEXC Global': lambda s: s.replace('/', ''),
                'Bitget': lambda s: s.replace('/', ''),
                'Bybit': lambda s: s.replace('/', ''),
                'BingX': lambda s: s.replace('/', '-'),
                'HTX': lambda s: s.replace('/', '').lower(),
                'CoinEx': lambda s: s.replace('/', ''),
                'Bitmart': lambda s: s.replace('/', '_')
            }
            
            if exchange not in format_map:
                return None
                
            symbol_formatted = format_map[exchange](symbol)
            
            # Get orderbook from local cache
            exchange_books = self.local_orderbooks.get(exchange, {})
            return exchange_books.get(symbol_formatted)
            
        except Exception as e:
            print(f"Error in get_socket_orderbook: {e}")
            return None
    
    async def update_local_orderbooks(self):
        # Create a connection pool
        pool = MemcachedConnectionPool()
        
        while True:
            try:
                start = datetime.now()
                
                # Function to get orderbook data for an exchange with better error handling
                async def get_exchange_orderbook(exchange):
                    try:
                        key = f'orderbook_{exchange}_full_snapshot'
                        # Try up to 3 times to get the data
                        for attempt in range(3):
                            connection = None
                            try:
                                # Get a connection from the pool
                                connection = await pool.get_connection()
                                
                                # Get the data
                                value = await asyncio.to_thread(connection.get, key)
                                
                                # Process successful response
                                if value and isinstance(value, bytes):
                                    try:
                                        # Safely decompress and unpickle
                                        data = pickle.loads(gzip.decompress(value))
                                        
                                        # Validate unpickled data
                                        if isinstance(data, dict):
                                            print(f'{exchange}: {len(data)}')

                                            # Handle MEXC exchange name difference
                                            exchange_name = 'MEXC Global' if exchange == 'MEXC' else exchange
                                            self.local_orderbooks[exchange_name] = data
                                            
                                            # Release the connection back to the pool
                                            if connection:
                                                await pool.release_connection(connection)
                                            return
                                        else:
                                            print(f"Invalid data structure for {exchange}: {type(data)}")
                                    except (gzip.BadGzipFile, pickle.UnpicklingError, ValueError) as e:
                                        print(f"Data corruption for {exchange} (attempt {attempt+1}): {e}")
                                else:
                                    print(f"No data for {exchange} (attempt {attempt+1})")
                                
                                # Release the connection back to the pool
                                if connection:
                                    await pool.release_connection(connection)
                                    
                                # Wait before retry with exponential backoff
                                await asyncio.sleep(0.5 * (attempt + 1))
                                
                            except Exception as e:
                                print(f"Error getting {exchange} orderbook (attempt {attempt+1}): {e}")
                                # Don't try to release a failed connection
                                await asyncio.sleep(0.5 * (attempt + 1))
                        
                        # If we reach here, all attempts failed
                        print(f"Failed to get orderbook for {exchange} after 3 attempts")
                    
                    except Exception as e:
                        print(f"Error in get_exchange_orderbook for {exchange}: {e}")
                
                # Get all orderbooks concurrently, but with a limit on concurrency
                semaphore = asyncio.Semaphore(3)  # Further limit concurrent memcached requests
                
                async def get_with_semaphore(exchange):
                    async with semaphore:
                        return await get_exchange_orderbook(exchange)
                
                # Process exchanges in smaller batches to reduce connection pressure
                for i in range(0, len(self.allExchanges), 3):
                    batch = self.allExchanges[i:i+3]
                    tasks = [get_with_semaphore(exchange) for exchange in batch]
                    await asyncio.gather(*tasks)
                    # Add a small delay between batches
                    await asyncio.sleep(0.5)

                end = datetime.now()
                print(f'Get orderbooks time: {end-start}')
                
                # Wait before refreshing orderbooks again
                await asyncio.sleep(3)  # Increased delay to reduce connection pressure
                
            except Exception as e:
                print(f"Error updating local order books: {e}")
                logging.error(f"Error updating local order books: {e}")
                await asyncio.sleep(3)  # Increased delay before retry



# Create a global connection pool
memcached_pool = MemcachedConnectionPool()

async def get_signal_volumes() -> Optional[List[Dict]]:
    try:
        connection = None
        # Try to get volumes with retry logic
        for attempt in range(3):
            try:
                # Get a connection from the pool
                connection = await memcached_pool.get_connection()
                
                volumes = await asyncio.to_thread(connection.get, 'volumes')
                
                # Release the connection back to the pool
                await memcached_pool.release_connection(connection)
                connection = None
                
                if not volumes:
                    print(f"No volumes data found in cache (attempt {attempt+1})")
                    await asyncio.sleep(0.5)
                    continue
                
                # Safely decompress and unpickle
                try:
                    data = pickle.loads(gzip.decompress(volumes))
                    return [volume for volume in data if volume]
                except (gzip.BadGzipFile, pickle.UnpicklingError, ValueError) as e:
                    print(f"Data corruption in volumes (attempt {attempt+1}): {e}")
                    await asyncio.sleep(0.5)
                    continue
            except Exception as e:
                print(f"Error getting volumes (attempt {attempt+1}): {e}")
                # Don't try to release a failed connection
                await asyncio.sleep(0.5)
                continue
            finally:
                # Ensure connection is released if something went wrong
                if connection:
                    try:
                        await memcached_pool.release_connection(connection)
                    except:
                        pass
        
        print("Failed to get volumes after 3 attempts")
        return None
    except Exception as e:
        print(f"Critical error in get_signal_volumes: {e}")
        return None


async def main():
    global last_keys_opened
    
    try:
        bot = UsersignalsBot()
        # Start the orderbook update task and store the task object
        orderbook_task = asyncio.create_task(bot.update_local_orderbooks())

        # Add a health check for memcached
        async def check_memcached_health():
            while True:
                try:
                    # Create a coroutine from the Future returned by to_thread
                    conn = base.Client(('memcached', 11211), connect_timeout=1, timeout=1)
                    stats_future = asyncio.to_thread(conn.stats)
                    stats = await stats_future  # Await the Future properly
                    
                    if stats:
                        print("Memcached health check: OK")
                    else:
                        print("Memcached health check: No stats returned")
                except Exception as e:
                    print(f"Memcached health check: FAILED - {e}")
                await asyncio.sleep(30)  # Check every 30 seconds
        
        health_check_task = asyncio.create_task(check_memcached_health())

        while True:  
            try:
                start = datetime.now()

                # Get users and volumes with timeout protection
                try:
                    users_future = client.userdata.user.find().to_list(length=None)
                    volumes_future = get_signal_volumes()
                    # Тиры Bitmart тянем один раз на цикл, а не при каждом
                    # расчёте спреда, как было раньше.
                    tiers_future = client.renderBase.bitmart_tier.find().to_list(length=None)

                    # Wait for all futures with a timeout
                    users, allSymbolsVolumes, tiers = await asyncio.wait_for(
                        asyncio.gather(users_future, volumes_future, tiers_future),
                        timeout=10  # 10 second timeout
                    )
                    bot.tiers = tiers
                except asyncio.TimeoutError:
                    print("Timeout getting users or volumes data")
                    await asyncio.sleep(1)
                    continue
                
                if allSymbolsVolumes:
                    # Track which symbols are active
                    current_symbols = set(f'volume_{data[1]["min_exchange"]}_{data[1]["max_exchange"]}_{data[1]["symbol"]}_{data[1]["networkSumm"]}' for data in allSymbolsVolumes)
                    
                    # Find symbols that were active before but are no longer active
                    removed_symbols = last_keys_opened - current_symbols
                    
                    # Clean up signals for removed symbols
                    if removed_symbols:
                        # Ключ сигнала теперь СОВПАДАЕТ с этим префиксом (раньше
                        # к нему дописывался '_ID_{user}'), поэтому вместо
                        # перебора всех ключей достаточно прямого удаления.
                        delete_keys = [res.replace('volume_', 'signal_') for res in removed_symbols]

                        async with bot.signals_lock:
                            for key in delete_keys:
                                bot.signal_payloads.pop(key, None)
                                bot.signal_recipients.pop(key, None)
                    
                    # Update last seen symbols
                    last_keys_opened = current_symbols
                    
                    # Process all symbols with timeout protection
                    try:
                        process_future = bot.process_all_symbols(allSymbolsVolumes, users)
                        await asyncio.wait_for(process_future, timeout=30)  # 30 second timeout
                    except asyncio.TimeoutError:
                        print("Timeout processing symbols")
                else:
                    # Clear all signals if no volumes available
                    async with bot.signals_lock:
                        bot.signal_payloads.clear()
                        bot.signal_recipients.clear()
                    last_keys_opened = set()

                # Update signals in cache with timeout protection
                try:
                    signals_future = bot.load_all_signals()
                    await asyncio.wait_for(signals_future, timeout=10)  # 10 second timeout
                except asyncio.TimeoutError:
                    print("Timeout loading signals")

                end = datetime.now()
                processing_time = end - start
                
                # Store processing time in cache
                try:
                    time_str = str(processing_time)
                    # Create a proper lambda that returns the compressed data
                    compress_func = lambda: gzip.compress(pickle.dumps(time_str))
                    compressed_time = await asyncio.to_thread(compress_func)
                    
                    # Use a separate await for the cache.set operation
                    await asyncio.to_thread(lambda: cache.set('time_signals', compressed_time, 60))
                except Exception as e:
                    print(f"Error storing processing time: {e}")
                
                print(f'Time circle: {processing_time} seconds for all {len(users)} users')
                
                # Prevent CPU overload by adding a small delay
                await asyncio.sleep(0.5)  # Increased delay
                
            except Exception as e:
                print(f"Error in main loop: {e}")
                logging.error(f"Error in main loop: {e}")
                await asyncio.sleep(2)
    
    except Exception as e:
        print(f"Critical error: {e}")
        logging.critical(f"Critical error: {e}")
    
    finally:
        # Ensure we cancel all tasks if main exits
        for task in [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass



if __name__ == '__main__':
    asyncio.run(main())
