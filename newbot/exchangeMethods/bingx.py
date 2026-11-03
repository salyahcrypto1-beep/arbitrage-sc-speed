import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
from datetime import datetime as datetime
from asyncio import gather
import asyncio
import time
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import json
import logging
from manage.dbloader import Mongoloader
from manage.constants import Constants

class BingX(Constants.BingX):
    def __init__(self) -> None:
        super().__init__()

    mongoload = Mongoloader()
    globalCurrency = {}

    logging.basicConfig(level=logging.INFO, filename='bingx_log.txt')

    async def symbols(self,sem):
        payload = {}
        path = '/openApi/spot/v1/common/symbols'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method, path, paramsStr, payload,sem)

    async def tickers(self,sem):
        payload = {}
        path = '/openApi/spot/v1/ticker/24hr'
        method = "GET"
        paramsStr ='timestamp='+str(int(time.time() * 1000))
        return await self.send_request(method, path, paramsStr, payload,sem)

    alltickers = {}

    async def reformTickers(self,ticker):
        try:
            
            self.alltickers[ticker['symbol']]={'ask':'','bid':''}
            
            self.alltickers[ticker['symbol']]['ask']=ticker['askPrice']
            self.alltickers[ticker['symbol']]['bid']=ticker['bidPrice']
        except Exception as e:
            #print(f"Reform tickers error BingX:{e}")
            pass

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
            await gather(*[self.reformTickers(ticker) for ticker in tickers['data']])
            
            return self.alltickers
        except Exception as e:
            
            logging.error(f"Fetch tickers BingX error:{e}")

    async def loadMarkets(self,sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            if loadMarkets_["data"]:           
                for i in loadMarkets_["data"]["symbols"]:     
                    if i["symbol"].split('-')[1] == "USDT":                
                        tradePair=i['symbol']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo(exName=self.name,symbols=allSymbolsData)
            # return collectedPair
        except Exception as e:
            logging.error('bingx format error', e)

    def mathOrderBooks(self,ob:dict):
        symbolsBook = {'bids': [], 'asks': []}
        symbolOrderBook = ob["data"]
        
        for z in symbolOrderBook["bids"]:
            bidsContainer = []
            symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][0] = float(symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][0])
            bidsContainer.append(symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][0])
            symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][1] = float(symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][1])
            bidsContainer.append(symbolOrderBook["bids"][symbolOrderBook["bids"].index(z)][1])
            symbolsBook['bids'].append(bidsContainer)
        for z in symbolOrderBook["asks"]:
            asksContainer=[]
            symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][0] = float(symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][0])
            asksContainer.append(symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][0])
            symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][1] = float(symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][1])
            asksContainer.append(symbolOrderBook["asks"][symbolOrderBook["asks"].index(z)][1])
            symbolsBook['asks'].append(asksContainer)
        
        return symbolsBook

    async def fetchOrderBookBot(self,symbol):
        session = aiohttp.ClientSession()
        return await self.fetchOrderBook(symbol,session)

    async def fetchOrderBook(self,symbol,session):
        try:
            ob = {}
            path = '/openApi/spot/v1/market/depth'
            method = "GET"
            paramsMap = {
                'symbol': symbol,
                'limit': 100
            }
            paramsStr = self.parseParam(paramsMap)
            url = f"{self.APIURL}{path}?{paramsStr}"
            r = await session.request('GET', url)
            match r.status:
                case 200:
                    try:
                            ob = await r.json()
                    except Exception as e:
                            resText = await r.text()
                            ob = json.loads(resText)
                    if ob["data"]:
                        return self.mathOrderBooks(ob)
                case 429:
                    print('Too many req BingX',symbol)
                    await session.close()
                    session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                    return await self.fetchOrderBook(symbol,session)
        except aiohttp.ClientConnectorError:
            print(f'BingX: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)
        
        
        except Exception as e:
            print(f"Fetch order book BingX unExpected err:{e}")

    
    
        
    async def demo(self,symbol):
        payload = {}
        path = '/openApi/spot/v1/market/depth'
        method = "GET"
        paramsMap = {
            'symbol': str(symbol[:-5])+'-USDT',
            'limit': 100
        }
        sem = asyncio.Semaphore(10000)
        paramsStr = self.parseParam(paramsMap)
        return await self.send_request(method, path, paramsStr, payload,sem,'books')

    async def forNetworkList(self, n, symbol):
        self.globalCurrency[symbol]['networks'][n['network'].upper()] = {}
        net ={
            'withdraw':n['withdrawEnable'],
            'deposit':n['depositEnable'],
            'fee':float(n['withdrawFee']) if 'withdrawFee' in n.keys() else '',
            'contract':n['contractAddress'] if 'contractAddress' in n.keys() else '',
            'adminFee': 0
            }
        self.globalCurrency[symbol]['networks'][n['network'].upper()]=net   
       
    async def forAllSymbolsCurr(self, i, symbol):
        if symbol[:-5] == i['coin']: 
            await asyncio.gather(*[self.forNetworkList(n, symbol) for n in i["networkList"]])

    async def forEverySymbolCurr(self,symbol, allSymbolsCurrency):
        self.globalCurrency[symbol] = {
            'networks':{}
        }

        await asyncio.gather(*[self.forAllSymbolsCurr(i, symbol) for i in allSymbolsCurrency])

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def currency(self,symbols, sem):     
        try:
            self.globalCurrency = {}
            allSymbolsCurrency = []
            payload = {}
            path = '/openApi/wallets/v1/capital/config/getall'
            method = "GET"
            paramsMap = {}

            paramsStr = self.parseParam(paramsMap)
            currency_ =  await self.send_request(method, path, paramsStr, payload, sem,'cur')

            if currency_["data"]:
                allSymbolsCurrency = currency_["data"]

            await gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
        except Exception as e:
            logging.error(f'BingX currency error:{e}, try again...')
            await self.currency(symbols,sem)




    def get_sign(self,api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        return signature
    

    async def send_request(self,method, path, urlpa, payload, sem,key=None):
        res = {}
        try:
            url = f"{self.APIURL}{path}?{urlpa}&signature={self.get_sign(self.SECRETKEY, urlpa)}"
            
            headers = {'X-BX-APIKEY': self.APIKEY}
            session = None
            connector = None
            match key:
                case 'cur':
                    connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur'])-1)])
                    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
                case 'books':
                    connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)])
                    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
                case None:
                    session = aiohttp.ClientSession()


            try:
                r = await session.request('GET', url, headers=headers, data=payload)
                try:
                    res = await r.json()
                except Exception as e:
                    resText = await r.text()
                    res = json.loads(resText)
                        
            except Exception as e:
                logging.warning(e)
                print(f'BingX send_request 1: {e}')
                if session is not None:
                    await session.close()
                if connector is not None:
                    await connector.close()
                pass
            finally:
                if session is not None:
                    await session.close()
                if connector is not None:
                    await connector.close()
            
                

            return res
        except Exception as e:
            print(f'BingX send_request 2: {e}')
            return res

    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))
    def parseParam(self, paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join([f"{x}={paramsMap[x]}" for x in sortedKeys])
        if paramsStr != "":
            return f"{paramsStr}&timestamp={int(time.time() * 1000)}"
        else:
            return f"{paramsStr}timestamp={int(time.time() * 1000)}"



         
 