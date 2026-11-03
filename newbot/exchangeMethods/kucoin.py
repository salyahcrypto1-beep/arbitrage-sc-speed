import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
from asyncio import gather
import asyncio
import time
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import logging
from manage.dbloader import Mongoloader
from manage.constants import Constants

class KuCoin(Constants.KuCoin):
    def __init__(self) -> None:
        super().__init__()

    logging.basicConfig(level=logging.INFO, filename='kucoin_log.txt')
    async def curencyWrapper(self, symbol, sem):
        await self.currency(symbol, sem)

    async def symbols(self,sem):
        payload = {}
        path = '/api/v2/symbols'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method, path, paramsStr, payload,sem)
    mongoload = Mongoloader()
    async def loadMarkets(self,sem):
        try:
            
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            if loadMarkets_["data"]:           
                for i in loadMarkets_["data"]:     
                    if i["quoteCurrency"] == "USDT":                
                        tradePair=i['symbol']
                        allSymbolsData.append(tradePair)

            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)
            # return collectedPair
        except Exception as e:
            logging.exception('kucoin format error')

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
                path = '/api/v1/market/orderbook/level2_100'
                paramsMap = {
                    "symbol": symbol
                }
                paramsStr = self.parseParam(paramsMap)
                url = self.APIURL+path+'?'+paramsStr
                res =  await session.get(url)
                match res.status:
                    case 200:
                        ob = await res.json()
                        if ob["data"]:
                            return self.mathOrderBooks(ob)
                    case 429:
                      print('Too many req Kucoin',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session)
                        
        except aiohttp.ClientConnectorError:
            print(f'Kucoin: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)            
        except Exception as e:
            logging.exception(f"Fetch order book Kucoin err:{e}")

    

    async def send_request(self,method, path, urlpa, payload,sem,key=None):
        url = f"{self.APIURL}{path}?{urlpa}&signature={self.get_sign(self.SECRETKEY, urlpa)}"
        connector = None
        session=None
        answ = {}
        headers = {'X-BX-APIKEY': self.APIKEY, 'Accept': 'application/json', 'Content-Type': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
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
            async with sem:
                r = await session.request('GET', url=url, headers=headers)
                answ = await r.json()
                
        except Exception as e:
            if session is not None:
                await session.close()
            if connector is not None:
                await connector.close() 
        finally:
            if session is not None:
                await session.close()
            if connector is not None:
                await connector.close() 
        return answ    
            
    
    globalCurrency = {}

    async def tickers(self,sem):
        path = '/api/v1/market/allTickers'
        method = 'GET'
        urlpa = ''
        payload = ''

        return await self.send_request(method, path, urlpa, payload,sem)

    alltickers = {}
    def get_sign(self, api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        return signature
    
    async def reformTickers(self,ticker):
        try:
            if ticker['symbol'][-4:] == "USDT":
                self.alltickers[ticker['symbol']]={'ask':'','bid':''}
                self.alltickers[ticker['symbol']]['ask']=ticker['sell']
                self.alltickers[ticker['symbol']]['bid']=ticker['buy']
        except Exception as e:
            logging.exception(f"Reform tickers error Kucoin :{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
        
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['data']['ticker']])
            return self.alltickers
        except Exception as e:
            logging.exception(f"Fetch tickers Kucoin error:{e}")

    async def currencyCheker(self,curr,symbol):
        if symbol[:-5] == curr['currency']:
                    for n in curr["chains"]:
                        self.globalCurrency[symbol]['networks'][n['chainName'].upper()]={}
                        try:
                            net ={
                                'withdraw':n['isWithdrawEnabled'],
                                'deposit':n['isDepositEnabled'],
                                'fee':float(n['withdrawalMinFee']),
                                'contract':n['contractAddress'],
                                'adminFee':0
                            }
                        except Exception:
                            net ={
                                'withdraw':n['withdrawEnable'],
                                'deposit':n['depositEnable'],
                                'fee':float(n['withdrawFee']),
                                'contract':''
                            }
                        self.globalCurrency[symbol]['networks'][n['chainName'].upper()]=net

    async def forEverySymbolCurr(self,symbol, allSymbolsCurrency):
        try:
            self.globalCurrency[symbol]= {
                            'networks':{}  
                            }
            await gather(*[self.currencyCheker(curr,symbol) for curr in allSymbolsCurrency])    
        except Exception as e:
            logging.exception(f'KuCoinForeach error:{e}')

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def currency(self,symbols,sem):
        try:
            payload = {}
            path = '/api/v3/currencies'
            method = "GET"
            paramsMap = {}
            paramsStr = self.parseParam(paramsMap)
            currency_ = await self.send_request(method, path, paramsStr, payload,sem,'cur')
            if currency_["data"]:
                allSymbolsCurrency = currency_["data"]
            await asyncio.gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency) 
        except Exception as e:
            logging.exception(f'Raised currency error Kucoin:{e}.Try again...')
            await self.currency(symbols,sem)

    def parseParam(self,paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join([f"{x}={paramsMap[x]}" for x in sortedKeys])
        if paramsStr != "":
            return f"{paramsStr}&timestamp={int(time.time() * 1000)}"
        else:
            return f"{paramsStr}timestamp={int(time.time() * 1000)}"


 