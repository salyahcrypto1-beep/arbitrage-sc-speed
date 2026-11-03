import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
import asyncio
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import logging
import json
from manage.dbloader import Mongoloader
from manage.constants import Constants
from pymongo import MongoClient
from deepdiff import DeepDiff
import yarl

class Bitmart(Constants.Bitmart):
    def __init__(self) -> None:
        super().__init__()

    mongoload = Mongoloader()
    globalCurrency = {}
    alltickers = {}

    async def symbols(self):
        try:
            path = "/spot/v1/symbols"
            url = self.APIURL + path
            res = {}
            async with aiohttp.ClientSession() as session:
                res = await session.get(url)
                match res.status:
                    case 429:
                        return await self.symbols()
                    case 200:
                        # Позволяем декодировать JSON без проверки MIME‑типа
                        res = await res.json(content_type=None)
                        return res
        except Exception as e:
            print("Bitmart symbols req Error:", e)

    async def loadMarkets(self, sem=None):
        try:
            markets = await self.symbols()
            usdtPairs = []
            levels = []
            # Обновление тарифов комиссий — вспомогательный шаг: он ходит на
            # www.bitmart.com, который отдаёт 403 с IP дата-центра (в отличие от
            # api-cloud.bitmart.com, который доступен). Раньше при не-200 здесь
            # стоял return ДО сохранения символов, поэтому вся биржа выпадала из
            # рынка из-за необязательных данных. Теперь сбой тарифов не мешает
            # загрузке пар: в bitmart_tier просто остаются прежние значения,
            # то есть расчёт сигналов не меняется.
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=aiohttp_socks.ProxyConnector.from_url(
                        None)) as session:

                    url = yarl.URL("https://www.bitmart.com/gw-api/ds/search/symbol-list/fee-rate", encoded=True)
                    res = await session.request("GET", url)
                    if res.status == 200:
                        levels = await res.json()
                        doc = {}
                        for tier in levels['data']['feeRateList']:
                            doc[tier['feeRateName']] = []
                            for pair in tier['classList']:
                                if pair['symbolName'].split('/')[1] == 'USDT':
                                    doc[tier['feeRateName']].append(pair['symbolName'])
                        mongo = MongoClient("mongodb://mongo:27017")
                        check = list(mongo.renderBase.bitmart_tier.find())

                        if len(check) > 0:
                            diff = DeepDiff(check[0], doc).to_dict()
                            if len(diff.keys()) > 0:
                                mongo.renderBase.bitmart_tier.update_one({'_id': check[0]['_id']},
                                                                           {"$set": {'A': doc['A'], 'B': doc['B'], 'C': doc['C'], 'D': doc['D']}})
                        else:
                            mongo.renderBase.bitmart_tier.insert_one(doc)
                    else:
                        logging.error(f'Bitmart tiers {res.status} — пропускаем обновление тарифов, пары грузим дальше')
            except Exception as tier_err:
                logging.error(f'Bitmart tiers недоступны ({tier_err}) — пропускаем обновление тарифов, пары грузим дальше')

            for pair in markets['data']['symbols']:
                if pair.split("_")[1] == "USDT":
                    usdtPairs.append(pair)
            
            await self.mongoload.loadInToMongo(exName=self.name, symbols=usdtPairs)
        except Exception as e:
            print("Bitmart load_markets err:", e)

    async def reformTickers(self, ticker):
        try:
            if ticker[0][-4:] == "USDT":
                self.alltickers[ticker[0]] = {'ask': '', 'bid': ''}
                self.alltickers[ticker[0]]['ask'] = ticker[10]
                self.alltickers[ticker[0]]['bid'] = ticker[8]
        except Exception as e:
            logging.error(f"Reform tickers error BingX:{e}")

    async def tickers(self):
        try:
            path = '/spot/quotation/v3/tickers'
            url = self.APIURL + path
            res = {}
            async with aiohttp.ClientSession() as session:
                res = await session.get(url)
                match res.status:
                    case 429:
                        return await self.tickers()
                    case 200:
                        res = await res.json(content_type=None)
                        return res
        except Exception as e:
            print("Bitmart tickers req err:", e)

    async def fetchTickers(self, sem=None):
        tickers = await self.tickers()
        await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['data']])
        return self.alltickers
    
    def mathOrderBooks(self, ob: dict):
        symbolsBook = {'bids': [], 'asks': []}
        symbolOrderBook = ob["data"]
        
        for z in symbolOrderBook["bids"]:
            bidsContainer = []
            price = float(z[0])
            amount = float(z[1])
            bidsContainer.append(price)
            bidsContainer.append(amount)
            symbolsBook['bids'].append(bidsContainer)
        for z in symbolOrderBook["asks"]:
            asksContainer = []
            price = float(z[0])
            amount = float(z[1])
            asksContainer.append(price)
            asksContainer.append(amount)
            symbolsBook['asks'].append(asksContainer)
        
        return symbolsBook

    async def fetchOrderBookBot(self, symbol):
        async with aiohttp.ClientSession() as session:
            return await self.fetchOrderBook(symbol, session)

    async def fetchOrderBook(self, symbol, session: aiohttp.ClientSession):
        try:
            path = "/spot/quotation/v3/books"
            param = "?symbol=" + symbol
            url = self.APIURL + path + param
            r = await session.request('GET', url)
            match r.status:
                case 200:
                    ob = await r.json(content_type=None)
                    if ob["data"]:
                        return self.mathOrderBooks(ob)
                case 429:
                    await session.close()
                    session = aiohttp.ClientSession(
                        connector=aiohttp_socks.ProxyConnector.from_url(random.choice(Constants.proxies['books20']))
                    )
                    return await self.fetchOrderBook(symbol, session)
        except aiohttp.ClientConnectorError:
            await session.close()
            session = aiohttp.ClientSession(
                connector=aiohttp_socks.ProxyConnector.from_url(random.choice(Constants.proxies['books20']))
            )
            return await self.fetchOrderBook(symbol, session)
        except Exception as e:
            print(f"Fetch order book Bitmart unExpected err:{e}")

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair, value in copied_dict.items():
            if value['networks'] == {}:
                del self.globalCurrency[pair]

    async def currency(self, symbols, sem=None):
        print('start Bitmart currenct')
        try:
            path = "/account/v1/currencies"
            url = self.APIURL + path
            res = {}
            async with aiohttp.ClientSession() as session:
                res = await session.get(url)
                match res.status:
                    case 200:
                        # Используем content_type=None для обхода проверки MIME‑типа
                        res = await res.json(content_type=None)
                    case 429:
                        print('Bitmart currency 429', res)
                        return None
            await asyncio.gather(*[self.forEverySymbolCurr(symbol, res['data']) for symbol in symbols])
            self.check_null_nets()
            print("self.mongoload.loadInToMongoCurrency Bitmart")
            await self.mongoload.loadInToMongoCurrency(exName=self.name, currency=self.globalCurrency)
        except Exception as e:
            print("Bitmart currency error:", e)

    async def forNetworkList(self, n, symbol):
        try:
            if symbol[:-5] == n['currency'] or symbol[:-5] == n['currency'].split("-")[0] or symbol[:-5] == n['currency'].split("_")[0]:
                self.globalCurrency[symbol]['networks'][n['network'].upper()] = {}
                net = {
                    'withdraw': n['withdraw_enabled'],
                    'deposit': n['deposit_enabled'],
                    'fee': float(n['withdraw_fee']),
                    'contract': n['contract_address'],
                    'adminFee': 0
                }
                self.globalCurrency[symbol]['networks'][n['network'].upper()] = net    
        except Exception as e:
            print("forNetworkList:", e)

    async def forEverySymbolCurr(self, symbol, allSymbolsCurrency):
        try:
            self.globalCurrency[symbol] = {
                'networks': {}
            }
            await asyncio.gather(*[self.forNetworkList(i, symbol) for i in allSymbolsCurrency['currencies']])
        except Exception as e:
            print("forEverySymbolCurr:", e)

