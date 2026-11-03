import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import asyncio
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import logging
from manage.dbloader import Mongoloader
from manage.constants import Constants

class Bitget(Constants.Bitget):
    def __init__(self) -> None:
        super().__init__()

    globalCurrency = {}

    def mathOrderBooks(self,ob:dict):
        
        symbolsBook={'bids':[], 'asks':[]}
        symbolOrderBook = ob['data']
        for z in symbolOrderBook['bids']:
            bidsContainer = []
            symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][0] = float(symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][0])
            symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][1] = float(symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][1])
            bidsContainer.append(symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][0])
            bidsContainer.append(symbolOrderBook['bids'][symbolOrderBook['bids'].index(z)][1])
            symbolsBook['bids'].append(bidsContainer)
        for z in symbolOrderBook['asks']:
            asksContainer = []
            symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][0] = float(symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][0])
            symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][1] = float(symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][1])
            asksContainer.append(symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][0])
            asksContainer.append(symbolOrderBook['asks'][symbolOrderBook['asks'].index(z)][1])
            symbolsBook['asks'].append(asksContainer)
        return symbolsBook

    async def fetchOrderBook(self, symbol,session):
        try:
                ob = {}
                path = '/v2/spot/market/orderbook'
                method = "GET"
            
                paramsStr = 'symbol='+str(symbol)+'&type=step0&limit=150'
                url = self.APIURL+path+'?'+paramsStr
                res = await session.get(url)
                match res.status:
                  case 200:
                       try:
                               ob = await res.json()
                       except:
                               ob =await  res.text()
                       if ob['data']:
                           return self.mathOrderBooks(ob)
                  case 429:
                    print('Too many req Bitget',symbol)
                    await session.close()
                    session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                    return await self.fetchOrderBook(symbol,session)
        except aiohttp.ClientConnectorError:
            print(f'Bitget: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)
            
        except Exception as e:
            print(f"Fetch order book Bitget unExpected error:{e}")

    logging.basicConfig(level=logging.INFO, filename='bitget_log.txt') 
                    
    async def tickers(self, sem):
        payload = {}
        path = '/v2/spot/market/tickers'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method, path, paramsStr, payload, sem)


    alltickers = {}
    
    async def fetchOrderBookBot(self,symbol):
        session = aiohttp.ClientSession()
        return await self.fetchOrderBook(symbol,session)



    logging.basicConfig(level=logging.INFO, filename='bitget_log.txt') 
    
    async def reformTickers(self,ticker):
        try:
            if ticker['symbol'][-4:] == "USDT":
                self.alltickers[ticker['symbol']]={'ask':'','bid':''}
                self.alltickers[ticker['symbol']]['ask']=ticker['askPr']
                self.alltickers[ticker['symbol']]['bid']=ticker['bidPr']
        except Exception as e:
            logging.exception(f"Reform tickers error Bitget:{e}")
    
    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
            
            print(len(tickers['data']))
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['data']])
            return self.alltickers
        except Exception as e:
            logging.exception(f"Fetch tickers Bitget error:{e}")

   
    mongoload = Mongoloader()

    async def symbols(self,sem):
        payload = {}
        path = '/v2/spot/public/symbols'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method,path,paramsStr,payload,sem)

    async def loadMarkets(self,sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem=sem)
            if loadMarkets_["data"]:           
                for i in loadMarkets_["data"]:     
                    if i["quoteCoin"] == "USDT":                
                        tradePair=i['symbol']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)
            # return collectedPair
        except Exception as e:
            logging.exception('bitget format error', e)

    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))

    async def demo(self, symbol):
        symbol = symbol
    
        path = '/v2/spot/market/orderbook'
        method = "GET"
        
        paramsStr = 'symbol='+str(symbol[:-5])+'USDT'+'&type=step0&limit=150'
        sem = asyncio.Semaphore(10000)
        return await self.send_request(method, path, paramsStr,'',sem,'books20')

    def get_sign(self, api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        
        return signature

    async def send_request_books(self, method, path, urlpa):
        url = "%s%s?%s" % (self.APIURL, path, urlpa)
        res = {}

        try:
            async with aiohttp.ClientSession() as session:
                r = await session.request(method, url=url)
                res = await r.json()
        except Exception as e:
            pass

        return res
        

    async def send_request_for_tikers(self):
        url = "https://api.bitget.com/api/spot/v1/market/tickers"
        res = {}
        try:
            async with aiohttp.ClientSession() as session:
                r = await session.get(url)
                try:
                    res = await r.json()
                except Exception:
                    res = await r.text()
        except Exception as e:
            logging.exception(f"Error in send_request_for_tikers: {e}")
        return res

    async def send_request(self, method, path, urlpa, payload, sem,key=None):
        if not urlpa == "":
            url = "%s%s?%s" % (self.APIURL, path, urlpa)
            
        else:
            url = "%s%s" % (self.APIURL, path)
        res = {}
        session = None
        connector = None

        match key:
            case 'cur':
                connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur'])-1)])
                session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
            case 'books20':
                connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)])
                session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
            case None:
                session = aiohttp.ClientSession()
        try:
            r = await session.request('GET', url=url)
            try:
                res = await r.json()
            except:
                res =await  r.text()
                    
                
 
        except Exception as e:
            print('Bitget Error', e)
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

    async def forNetworkList(self, n, symbol):
        self.globalCurrency[symbol]['networks'][n['chain'].upper()] = {}
        net ={
            'withdraw':False if n['withdrawable']=='false' else True,
            'deposit':False if n['rechargeable']=='false' else True ,
            'fee':float(n['withdrawFee']),
            'contract':n['contractAddress'] if n['contractAddress'] is not None else '',
            'adminFee': 0
        }
        self.globalCurrency[symbol]['networks'][n['chain'].upper()]=net  
       
    async def forAllSymbolsCurr(self, i, symbol):
        if symbol[:-4] == i['coin']:
            await asyncio.gather(*[self.forNetworkList(n, symbol) for n in i['chains']])

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

    async def currency(self, symbols,sem):
        try:
            allSymbolsCurrency = []
            payload = {}
            path = '/v2/spot/public/coins'
            method = "GET"
            paramsMap = {}
            paramsStr = ""
            currency_ = await self.send_request(method, path, paramsStr,self,sem,'cur')
            if currency_["data"]:
                allSymbolsCurrency = currency_["data"]

            await asyncio.gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
    

        except Exception as e:
            logging.exception(f'Error raised while currency was called Bitget:{e}. Try again...')
            await self.currency(symbols,sem)
