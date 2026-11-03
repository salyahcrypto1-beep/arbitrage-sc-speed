import asyncio
from datetime import datetime
import logging
from typing import Dict, List, Optional
import gzip
import pickle
import diskcache as dc
import sys
import os
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient
import numpy as np
import concurrent.futures
from decimal import Decimal, getcontext
import math
from redis import asyncio as aioredis
import functools
from pymemcache. client import base
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from collections import OrderedDict


cache = base.Client(('memcached', 11211))

sys.path.append(os.path.join(os.getcwd(),'..'))
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.makedirs('logs', exist_ok=True)
from logging.handlers import RotatingFileHandler
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[RotatingFileHandler('logs/process_main.log', maxBytes=10_000_000, backupCount=3)])

getcontext().prec = 28

class MathSignalsBot:
    allExchanges: List[str] = ['OKX', 'Kucoin', 'Gate', 'MEXC', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx','Bitmart']
    allExchangesReverse: List[str] = ['Bitmart','CoinEx', 'HTX', 'BingX', 'Bybit', 'Bitget', 'MEXC Global', 'Gate', 'Kucoin', 'OKX']

    def __init__(self):
        self.allSymbolsData: Dict[str, Dict] = {}
        self.globalBlackList: List[str] = ['FROG/USDT', 'ARC/USDT','BOX/USDT','AD/USDT','FREE/USDT','LION/USDT','SHI/USDT','SQUAD/USDT','WUKING/USDT','LAND/USDT','AISC/USDT','GDT/USDT','MAGA/USDT','PUMP/USDT','TORN/USDT','FRONT/USDT','WX/USDT','FOR/USDT','CAT/USDT','CWIF/USDT','NLS/USDT','AGI/USDT','ROUTE/USDT','NLC/USDT','CSPR/USDT','LUNC/USDT','DEGEN/USDT','ISLM/USDT','MSI/USDT','MEMAI/USDT','TABOO/USDT','PLANETS/USDT','GOAL/USDT','FUTURE/USDT', 'PI/USDT', 'WOLF/USDT','RNDR/USDT','GAL/USDT','FLOKICEO/USDT','GINUX/USDT','TRIAS/USDT','SPIKE/USDT','QUACK/USDT', 'PIG/USDT', 'HERO/USDT', 'LOOP/USDT', 'REVO/USDT', 'ASS/USDT','OMNI/USDT', 'BIFI/USDT', 'AXL/USDT', 'GTC/USDT', 'NIM/USDT', 'BILLY/USDT', 'ASTRO/USDT','USDT/USDT', '10SET/USDT', 'BABYDOGE/USDT', 'BSV/USDT', 'CAPO/USDT', 'BRISE/USDT', 'IMPT/USDT', 'MAX/USDT', 'DERP/USDT', 'PIT/USDT', 'NGL/USDT', 'GEM/USDT', 'LOOT/USDT', 'RED/USDT', 'SOLS/USDT', 'KT/USDT', 'WALLET/USDT', 'ALT/USDT', 'PAW/USDT', 'VT/USDT', 'STC/USDT', 'SLN/USDT', 'GPT/USDT', 'LSD/USDT', 'NANO/USDT', 'GMT/USDT', 'VELO/USDT', 'GST/USDT', 'MDX/USDT', 'ORT/USDT', 'ZEN/USDT', 'SAGA/USDT','0X0/USDT']
        self.spotFees={'Bitmart':{'maker':0.6,'taker':0.6},'BingX':{'maker':0.1,'taker':0.1},'Bybit':{'maker':0.1,'taker':0.1},'Bitget':{'maker':0.08,'taker':0.08},'HTX':{'maker':0.2,'taker':0.2},'CoinEx':{'maker':0.2,'taker':0.2},'OKX':{'maker':0.14,'taker':0.23},'Gate':{'maker':0.1,'taker':0.1},'MEXC Global':{'maker':0,'taker':0.1},'Kucoin':{'maker':0.1,'taker':0.1}}
        self.client = pymongo.MongoClient('mongo', 27017)
        self.db_arbitrage = self.client.arbitrage
        self.opened_signal_combinations = []
        self.snapshots = []
        self.orderbooks = []
        self.orderbooks_snap = {}
        self.local_orderbooks = {}
        self.local_block1_snapshot = {}
        self.tiers = None
        self.tiers_version = 0
        self.orderbook_raw = {}
        self.orderbook_version = {}
        self.symbol_version = {}   # биржа → символ (в формате сокета) → версия; растёт только при изменении bids/asks
        self.blocks_raw = None
        self.combo_cache = {}
        self.inputs_changed = False

    
    async def init_redis(self):
        self.redis = await self.create_redis_pool()
    
    async def create_redis_pool(self) -> aioredis.Redis:
        return await aioredis.Redis(connection_pool=aioredis.ConnectionPool.from_url('redis://redis'))

    def orderbookMath(self, symbolOrderBookMin: Dict, symbolOrderBookMax: Dict, volume: float, feeVal: float, symbol: str, min_exchange: str, max_exchange: str, spotFees:Dict, tiers:Dict) -> Optional[List]:
        
        try:
            # Преобразование OrderedDict в список для единообразия
            asks_min = list(symbolOrderBookMin['asks'].items()) if isinstance(symbolOrderBookMin['asks'], OrderedDict) else symbolOrderBookMin['asks']
            bids_max = list(symbolOrderBookMax['bids'].items()) if isinstance(symbolOrderBookMax['bids'], OrderedDict) else symbolOrderBookMax['bids']

            summAsksVolContainerDollars = 0
            countOrderAsksCont = 0
            ordersArr = []
            
            while summAsksVolContainerDollars < volume and countOrderAsksCont < len(asks_min):
                price, amount = asks_min[countOrderAsksCont]
                summAsksVolContainerDollars += float(price) * float(amount)
                ordersArr.append([float(price), float(amount)])
                countOrderAsksCont += 1
            

            if summAsksVolContainerDollars < volume or volume <= 0:
                return None  # Never extrapolate liquidity beyond the available book.

            delOrdersVol = 0
            delSSum = 0
            for x in range(0, len(ordersArr)):
                if x != len(ordersArr) - 1:
                    delOrdersVol = delOrdersVol + ordersArr[x][1]
                    delSSum = delSSum + ordersArr[x][1] * ordersArr[x][0]
                else:
                    delOrdersVol = delOrdersVol + ((volume - delSSum) / ordersArr[x][0])

            srpriceBuy = 0
            if delOrdersVol != 0:
                srpriceBuy = volume / delOrdersVol
      
            summbidsVolContainer = 0
            countOrderBidsCont = 0
            ordersArrSell = []
            takerBuyFee = 0

            
            if min_exchange == 'Bitmart':
                try:
                    takerBuy = 0
                    
                    if symbol in tiers[0]['A']:                                         
                        takerBuy = 0.1
                    elif symbol in tiers[0]['B']:
                        takerBuy = 0.25
                    elif symbol in tiers[0]['C']:
                        takerBuy = 0.4
                    elif symbol in tiers[0]['D']:
                        takerBuy = 0.6
                    else:
                        takerBuy = 0.25

                    takerBuyFee = float(delOrdersVol) * takerBuy/100
                except Exception as e:
                    # Раньше при пустых tiers обращение к tiers[0] падало, и
                    # takerBuyFee оставалась НЕприсвоенной — следующая строка
                    # роняла весь orderbookMath, из-за чего переставали
                    # считаться объёмы и пропадали ВСЕ сигналы, а не только
                    # по Bitmart. Тиры недоступны (www.bitmart.com отдаёт 403
                    # с дата-центра), поэтому берём ту же ставку по умолчанию,
                    # что и ветка else выше.
                    print("tiers 1", tiers, e)
                    takerBuyFee = float(delOrdersVol) * 0.25/100
            else:
                takerBuyFee = float(delOrdersVol)*spotFees[min_exchange]['taker']/100

            summSellVolTokens = float(delOrdersVol) - float(feeVal) - float(takerBuyFee)
        

            if summSellVolTokens <= 0:
                return None

            while summbidsVolContainer < summSellVolTokens and countOrderBidsCont < len(bids_max):
    
                price, amount = bids_max[countOrderBidsCont]
                summbidsVolContainer += float(amount)
                ordersArrSell.append([float(price), float(amount)])
                countOrderBidsCont += 1
        
            if summbidsVolContainer < summSellVolTokens:
                return None

            chisOrdersVolBeforeFee = 0
            chisTSum = 0

            for x in range(0, len(ordersArrSell)):
                if x != len(ordersArrSell) - 1:
                    chisOrdersVolBeforeFee = chisOrdersVolBeforeFee + ordersArrSell[x][1] * ordersArrSell[x][0]
                    chisTSum = chisTSum + ordersArrSell[x][1]
                else:
                    chisOrdersVolBeforeFee = chisOrdersVolBeforeFee + ((summSellVolTokens - chisTSum) * ordersArrSell[x][0])

            takerSellFee = 0

            if max_exchange == 'Bitmart':
                try:
                    takerSell = 0
                    if symbol in tiers[0]['A']:
                        takerSell = 0.1
                    elif symbol in tiers[0]['B']:
                        takerSell = 0.25
                    elif symbol in tiers[0]['C']:
                        takerSell = 0.4
                    elif symbol in tiers[0]['D']:
                        takerSell = 0.6
                    else:
                        takerSell = 0.25
                    takerSellFee = float(chisOrdersVolBeforeFee) * takerSell/100
                except Exception as e:
                    # При пустых tiers комиссия молча оставалась нулевой, из-за
                    # чего спред по Bitmart завышался. Берём ту же ставку по
                    # умолчанию, что и ветка else выше.
                    print("tiers 2", e)
                    takerSellFee = float(chisOrdersVolBeforeFee) * 0.25/100
            else:
                takerSellFee = float(chisOrdersVolBeforeFee)*spotFees[max_exchange]['taker']/100

            chisOrdersVol = float(chisOrdersVolBeforeFee) - float(takerSellFee)
            
            srpriceSell = 0
            if summSellVolTokens != 0:
                srpriceSell = chisOrdersVol / summSellVolTokens
            
            volumeSellSumm = 0
            countOrderPrice = 0

            if len(bids_max) > 0:
                for order in bids_max:
                    if summSellVolTokens * order[0] > volume:
                        volumeSellSumm = volumeSellSumm + order[0] * order[1]
                        countOrderPrice = countOrderPrice + 1

            dollarsSpread = float(chisOrdersVol) - float(volume)
            symbollocalResult = [volume, dollarsSpread] 
            if len(ordersArr)>0 and len(ordersArrSell)>0:
                symbollocalResult.append({
                    'summDollarSpread': dollarsSpread,
                    'summasksVol': volume,
                    'summbidsVol': chisOrdersVol,
                    'summMin': srpriceBuy,
                    'summMax': srpriceSell,
                    'countOrderAsks': countOrderAsksCont,
                    'countOrderBids': countOrderBidsCont,
                    'alltokensSell': summSellVolTokens,
                    'alltokensBuy': delOrdersVol,
                    'firstBuyPrice': ordersArr[0][0],
                    'lastBuyPrice': ordersArr[-1][0],
                    'firstSellPrice': ordersArrSell[0][0],
                    'lastSellPrice': ordersArrSell[-1][0],
                    'volumeSellSumm': volumeSellSumm,
                    'countOrderPrice': countOrderPrice
                })
            else:
                symbollocalResult.append({
                    'summDollarSpread': dollarsSpread,
                    'summasksVol': volume,
                    'summbidsVol': chisOrdersVol,
                    'summMin': srpriceBuy,
                    'summMax': srpriceSell,
                    'countOrderAsks': countOrderAsksCont,
                    'countOrderBids': countOrderBidsCont,
                    'alltokensSell': summSellVolTokens,
                    'alltokensBuy': delOrdersVol,
                    'volumeSellSumm': volumeSellSumm,
                    'countOrderPrice': countOrderPrice
                })
            
            return symbollocalResult
        
        except Exception as e:
            print("orderbookMath", e, min_exchange, max_exchange, symbolOrderBookMin, symbolOrderBookMax)
            return None



    def optimized_linear_search_volume(self, symbolOrderBookMin: Dict, symbolOrderBookMax: Dict, feeVal: float, symbol: str, min_exchange: str, max_exchange: str, networkSumm: str, ex_networks_asks: Dict, ex_networks_bids: Dict) -> Optional[Dict]:
        try:
            left, right = 1, 15000  # Assumed volume range
            spotFees, tiers = self.spotFees, self.tiers
            memo = {}

            def calculate_spread(volume):
                # один и тот же объём внутри связки считается один раз (функция детерминирована)
                if volume not in memo:
                    result = self.orderbookMath(symbolOrderBookMin, symbolOrderBookMax, volume, feeVal, symbol, min_exchange, max_exchange, spotFees, tiers)
                    memo[volume] = result[1] if result else -float('inf')
                return memo[volume]

            def binary_search(start, end, target_point):
                while (end - start >= 5):
                    mid = (start + end) // 2

                    if target_point == 'max_profit':
                        spread_left = calculate_spread(mid - 1)
                        spread_right = calculate_spread(mid + 1)

                        if spread_left > spread_right:
                            end = mid
                        elif spread_right > spread_left:
                            start = mid
                        else:
                            # 02.10: при равных спредах (например, оба -inf) старый цикл крутился вечно
                            # и весь signalrender переставал отдавать сигналы — теперь плато считаем концом поиска
                            end = mid

                    elif target_point == 'spread_appearance':
                        spread_mid = calculate_spread(mid)

                        if spread_mid < 0:
                            end = mid
                        elif spread_mid > 0:
                            start = mid
                        else:
                            end = mid

                    elif target_point == 'spread_disappearance':
                        spread_mid = calculate_spread(mid)

                        if spread_mid < 0:
                            start = mid
                        elif spread_mid > 0:
                            end = mid
                        else:
                            end = mid

                return [start, end]

            best_volume_range = binary_search(left, right, 'max_profit')

            min_boundary = binary_search(1, best_volume_range[1], 'spread_appearance')
            max_boundary = binary_search(best_volume_range[0], 15000, 'spread_disappearance')

            best_volume_start, best_volume_end = best_volume_range
            best_results = []

            for volume in range(best_volume_start, best_volume_end + 1):
                result = self.orderbookMath(symbolOrderBookMin, symbolOrderBookMax, volume, feeVal, symbol, min_exchange, max_exchange, spotFees, tiers)
                if result:
                    best_results.append(result)

            if best_results:
                best_result = max(best_results, key=lambda x: x[1])
                best_volume = best_result[0]
            else:
                best_result = None
                best_volume = None

            if best_result is not None and float(best_result[1]) > 0.5:
                volume_key = f'volume_{min_exchange}_{max_exchange}_{symbol}_{networkSumm}'

                data = {
                    'min_boundary': float(min_boundary[0]),  # Приведем к числу
                    'max_boundary': float(max_boundary[1]),  # Приведем к числу
                    'best_volume': best_volume,
                    'best_result': best_result,
                    'min_exchange': min_exchange,
                    'max_exchange': max_exchange,
                    'networkSumm': networkSumm,
                    'symbol': symbol,
                    'feeVal': feeVal,
                    'ex_networks_asks': ex_networks_asks,
                    'ex_networks_bids': ex_networks_bids
                }
                return [volume_key, data]
            else:
                return None

        except Exception as e:
            print(f"Error in optimized_linear_search_volume: {e}")
            return None



    _SYMBOL_FMT = {
        'OKX': lambda s: s.replace('/', '-'),
        'Kucoin': lambda s: s.replace('/', '-'),
        'Gate': lambda s: s.replace('/', '_'),
        'MEXC Global': lambda s: s.replace('/', ''),
        'Bitget': lambda s: s.replace('/', ''),
        'Bybit': lambda s: s.replace('/', ''),
        'BingX': lambda s: s.replace('/', '-'),
        'HTX': lambda s: s.replace('/', '').lower(),
        'CoinEx': lambda s: s.replace('/', ''),
        'Bitmart': lambda s: s.replace('/', '_'),
    }

    def _combo_signature(self, symbol, min_exchange, max_exchange):
        """(версия стакана min, версия стакана max, свежесть). Свежесть False ⇔ signalLoops вернул бы None
        ещё до расчёта (нет стакана, неизвестная биржа, разница lastUpdate > 60 с или ошибка разбора)."""
        try:
            fmin = self._SYMBOL_FMT[min_exchange](symbol)
            fmax = self._SYMBOL_FMT[max_exchange](symbol)
            ob_min = self.local_orderbooks.get(min_exchange, {}).get(fmin, None)
            ob_max = self.local_orderbooks.get(max_exchange, {}).get(fmax, None)
            if not ob_min or not ob_max:
                return (None, None, False)
            fresh = all(0 <= (datetime.now() - datetime.fromisoformat(ob['lastUpdate'])).total_seconds() <= 30 for ob in (ob_min, ob_max))
            return (self.symbol_version.get(min_exchange, {}).get(fmin), self.symbol_version.get(max_exchange, {}).get(fmax), fresh)
        except Exception:
            return (None, None, False)

    def get_socket_orderbook(self, symbol: str, exchange: str, pipe=None):
        try:
            symbol_formatted = {
                'OKX': symbol.replace('/', '-'),
                'Kucoin': symbol.replace('/', '-'),
                'Gate': symbol.replace('/', '_'),
                'MEXC Global': symbol.replace('/', ''),
                'Bitget': symbol.replace('/', ''),
                'Bybit': symbol.replace('/', ''),
                'BingX': symbol.replace('/', '-'),
                'HTX': symbol.replace('/', '').lower(),
                'CoinEx': symbol.replace('/', ''),
                'Bitmart': symbol.replace('/', '_')
            }[exchange]
            
            orderbook = self.local_orderbooks.get(exchange, {}).get(symbol_formatted, None)
            if orderbook:
                # Преобразование данных стакана в единый формат
                return {
                    'bids': list(orderbook['bids'].items()) if isinstance(orderbook['bids'], OrderedDict) else orderbook['bids'],
                    'asks': list(orderbook['asks'].items()) if isinstance(orderbook['asks'], OrderedDict) else orderbook['asks'],
                    'lastUpdate': orderbook['lastUpdate']
                }
            return None
        except Exception as e:
            print(f"Error in get_socket_orderbook: {e}")
            return None


    def signalLoops(self, symbol: str, network: str, ex_networks_asks: Dict, ex_networks_bids: Dict, min_exchange: str, max_exchange: str):
        try:
            symbolOrderBookMin = self.get_socket_orderbook(symbol, min_exchange)
            symbolOrderBookMax = self.get_socket_orderbook(symbol, max_exchange)

            if symbolOrderBookMin and symbolOrderBookMax:
                lastUpdateMin = datetime.fromisoformat(symbolOrderBookMin['lastUpdate'])
                lastUpdateMax = datetime.fromisoformat(symbolOrderBookMax['lastUpdate'])

                if all(0 <= (datetime.now() - t).total_seconds() <= 30 for t in (lastUpdateMin, lastUpdateMax)):
                    feeVal = ex_networks_asks[network]['fee']
                    return self.optimized_linear_search_volume(symbolOrderBookMin, symbolOrderBookMax, feeVal, symbol, min_exchange, max_exchange, network, ex_networks_asks, ex_networks_bids)
            return None

        except Exception as e:
            print(f"Error in signalLoops: {e}, {symbol}, {min_exchange} {max_exchange}, {ex_networks_asks}")
            return None

    def compute_all(self):
        """Один проход по всем связкам. Результат для связки зависит только от стаканов её двух бирж,
        комиссии сети и тиров — поэтому, пока их версии не менялись, берём прошлый результат (тот же объект)."""
        snapshots = []
        new_cache = {}
        ver = self.orderbook_version
        tiers_ver = self.tiers_version
        for key, data in self.local_block1_snapshot.items():
            symbol, min_exchange, max_exchange, network, ex_asks, ex_bids = data[0], data[1], data[2], data[3], data[4], data[5]
            vmin, vmax, fresh = self._combo_signature(symbol, min_exchange, max_exchange)
            try:
                fee = ex_asks[network]['fee']
            except Exception:
                fee = None
            sig = (vmin, vmax, tiers_ver, fee, fresh)
            prev = self.combo_cache.get(key)
            if not fresh:
                res = None
            elif prev is not None and prev[0] == sig:
                res = prev[1]
            else:
                res = self.signalLoops(symbol, network, ex_asks, ex_bids, min_exchange, max_exchange)
            new_cache[key] = (sig, res)
            if res is not None:
                snapshots.append(res)
        self.combo_cache = new_cache
        return snapshots

    async def update_local_block1_snapshot(self):
        while True:
            try:
                raw = cache.get('blocks')
                if not raw:
                    self.local_block1_snapshot = {}
                    self.blocks_raw = None
                elif raw != self.blocks_raw:
                    data = pickle.loads(gzip.decompress(raw))
                    new_snapshot = {}
                    for key in data:
                        new_snapshot[key[0]] = key[1]
                    self.local_block1_snapshot = new_snapshot
                    self.blocks_raw = raw
                    self.inputs_changed = True
            except Exception as e:
                logging.error(f"Ошибка при обновлении локального снепшота block1: {e}", exc_info=True)
            finally:
                await asyncio.sleep(1)  # Пауза перед следующей попыткой

    async def update_local_orderbooks(self):
        while True:
            try:
                for exchange in self.allExchanges:
                    try:
                        key = f'orderbook_{exchange}_full_snapshot'
                        value = cache.get(key)
                        if not value or value == self.orderbook_raw.get(exchange):
                            continue
                        data = pickle.loads(gzip.decompress(value))
                        name = 'MEXC Global' if exchange == 'MEXC' else exchange
                        prev = self.local_orderbooks.get(name) or {}
                        symver = self.symbol_version.setdefault(name, {})
                        for sym, ob in data.items():
                            pv = prev.get(sym)
                            if pv is None or pv.get('bids') != ob.get('bids') or pv.get('asks') != ob.get('asks'):
                                symver[sym] = symver.get(sym, 0) + 1
                        self.local_orderbooks[name] = data
                        self.orderbook_raw[exchange] = value
                        self.orderbook_version[name] = self.orderbook_version.get(name, 0) + 1
                        self.inputs_changed = True
                    except Exception as e:
                        logging.error(f"Ошибка при получении данных для {exchange}: {e}", exc_info=True)
            except Exception as e:
                logging.error(f"Ошибка при обновлении локальных ордербуков: {e}", exc_info=True)
            await asyncio.sleep(1)

    async def load_all_signals(self, tasks, id,start):
        try:
            compressed_snap = gzip.compress(pickle.dumps(self.snapshots), 1)
            cache.set('volumes', compressed_snap, 60)

            len_snap = len(self.snapshots)
            
            end = datetime.now()
            cache.set('time_volumes', gzip.compress(pickle.dumps(str(end-start))), 60)

        except Exception as e:
            print(e)
            logging.error(f'Ошибка в _check_combinations: {e}')


    async def checking_exist_combinations(self):
        await asyncio.sleep(10)
        while True:
            try:
                start = datetime.now()
                if self.inputs_changed or not self.combo_cache:
                    self.inputs_changed = False
                    self.snapshots = await asyncio.to_thread(self.compute_all)
                await self.load_all_signals(None, len(self.snapshots), start)

            except Exception as e:
                logging.error(f'Ошибка в _check_combinations: {e}')
                pass

            await asyncio.sleep(0.5)

    async def update_tiers(self):
        while True:
            try:
                tiers = list(self.client.renderBase.bitmart_tier.find())
                if tiers != self.tiers:
                    self.tiers = tiers
                    self.tiers_version += 1
                    self.inputs_changed = True
                await asyncio.sleep(10)  # Обновление каждые 60 секунд
            except Exception as e:
                logging.error(f"Ошибка при обновлении tiers: {e}")
                await asyncio.sleep(1)


async def main():
    bot = MathSignalsBot()    
    await bot.init_redis()  # Инициализация Redis

    tasks = [
        asyncio.create_task(bot.checking_exist_combinations()),
        asyncio.create_task(bot.update_local_orderbooks()),
        asyncio.create_task(bot.update_local_block1_snapshot()),
        asyncio.create_task(bot.update_tiers())  
    ]

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())