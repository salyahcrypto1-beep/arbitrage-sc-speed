import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from asyncio import gather
import hmac
from hashlib import sha256
import aiohttp
import aiohttp_socks
import asyncio
import logging
import asyncio
from manage.dbloader import Mongoloader
from manage.constants import Constants
import random

class CoinEx(Constants.CoinEx):

    def __init__(self) -> None:
        super().__init__()

    def mathOrderBooks(self,ob:dict):
        symbolsBook={'bids':[], 'asks':[]}
        
        symbolOrderBook = ob['data']['depth']
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

    async def fetchOrderBook(self,symbol,session):
        try:
                ob = {}
                path = '/v2/spot/depth'
                paramsStr = 'market='+str(symbol)+'&limit=50&interval=0.01'
                url = self.APIURL+path+'?'+paramsStr
                res = await session.get(url)
                match res.status:
                    case 200:
                        ob = await res.json()
                        if ob['data']['depth']:
                            return self.mathOrderBooks(ob)
                    case 429:
                      print('Too many req CoinEx',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session) 
        except aiohttp.ClientConnectorError:
            print(f'CoinEx: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)         
        except Exception as e:
           print(f"Fetch order book CoinEx err:{e}")
    
    
    
    async def fetchOrderBookBot(self,symbol):
        session = aiohttp.ClientSession()
        return await self.fetchOrderBook(symbol,session)
        
        
    async def demo(self,symbol):
        symbol = symbol
        payload = {}
        path = '/v2/spot/depth'
        method = "GET"
        sem = asyncio.Semaphore(10000)
        paramsStr = 'market='+str(symbol[:-5])+'USDT'+'&limit=50&interval=0.01'
        
        return await self.send_request(method, path, paramsStr, payload,sem,key='books')

    def get_sign(self,api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        
        return signature

    async def symbols(self,sem):
        payload = {}
        path = '/v2/spot/market'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method, path, paramsStr, payload,sem)

    async def tickers(self,sem):
        payload = {}
        path = '/v1/market/ticker/all'
        method = "GET"
        paramsStr = ""
        return await self.send_request(method, path, paramsStr, payload,sem)

    alltickers = {}
    
    async def reformTickers(self,ticker,symbol):
        try:
            if symbol[-4:] == "USDT":
                self.alltickers[symbol]={'ask':'','bid':''}
                self.alltickers[symbol]['ask']=ticker['sell']
                self.alltickers[symbol]['bid']=ticker['buy']
        except Exception as e:
            logging.error(f"Reform tickers error CoinEX:{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
            await asyncio.gather(*[self.reformTickers(ticker,symbol) for symbol,ticker in tickers['data']['ticker'].items()])
            return self.alltickers
        except Exception as e:
            print(e)
            logging.error(f"Fetch tickers CoinEX error:{e}")

    mongoload = Mongoloader()

    async def loadMarkets(self,sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            if loadMarkets_["data"]:           
                for i in loadMarkets_["data"]:     
                    if i["quote_ccy"] == "USDT":                
                        tradePair=i['market']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)

            # return collectedPair
        except Exception as e:
            logging.error('coinex format error', e)
    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))

    async def send_request_books(self, method, path, urlpa, payload):
        url = "%s%s?%s" % (self.APIURL, path, urlpa)
        res = {}

        try:
            async with aiohttp.ClientSession() as session:
                r = await session.request('GET', url=url, data=payload)
                res = await r.json()
        except Exception as e:
            pass

        return res
  

    async def send_request(self,method, path, urlpa, payload, sem,key=None):
        url = "%s%s?%s" % (self.APIURL, path, urlpa)
        res = {}
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
            
            r = await session.request('GET', url=url, data=payload)
            res = await r.json()
                
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

        return res

    globalCurrency = {}
    async def currency(self,symbols,sem):
        
        try:
            self.globalCurrency = {}
            await gather(*[self.currencyLoader(symbol,sem) for symbol in symbols])
            self.check_null_nets()
            await self.find_contract()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)

        except Exception as e:
            logging.error(f'Error raised currency CoinEx:{e}. Try again...')
            await self.currency(symbols,sem)


    async def forNetworkList(self, n, symbol):
        self.globalCurrency[symbol] = {
            'networks': {}
        }
        self.globalCurrency[symbol]['networks'][n['chain'].upper()] = {}
        net ={
            'withdraw':bool(n['withdraw_enabled']),
            'deposit':bool(n['deposit_enabled']),
            'fee':float(n['withdrawal_fee']),
            'contract':'',
            'adminFee':0
        }

        self.globalCurrency[symbol]['networks'][n['chain'].upper()]=net    

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def match_contract(self,pair:str,dict,cmc):
        try:
            formated_pair = pair.replace('USDT','')
            cmc_obj = {}
            for token in cmc:
                if token['symbol']==formated_pair:
                    cmc_obj = token
                    break
            
            for net in dict['networks'].keys():
                match net:
                    case 'ERC20':
                        if cmc_obj['chain_token']=='ETH' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
                    case 'BSC':
                        if cmc_obj['chain_token']=='BNB' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
                    case 'ARBITRUM':
                        if cmc_obj['chain_token']=='ARBITRUM' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
                    case 'SOL':
                        if cmc_obj['chain_token']=='SOL' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
                    case 'MATIC':
                        if cmc_obj['chain_token']=='MATIC' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
        except Exception:
            return
                
        
            

    async def find_contract(self):
        copied_dict = self.globalCurrency.copy()
        from motor.motor_asyncio import AsyncIOMotorClient as mongoclient
        cmc = mongoclient("mongodb://mongo:27017")
        cmc_data = await cmc.renderBase.cmc.find().to_list(length=None)
        await asyncio.gather(*[self.match_contract(pair,net_dict,cmc_data) for pair,net_dict in copied_dict.items()])   

    async def currencyLoader(self,symbol,sem):
        try:
            payload = {}
            path = '/v2/assets/deposit-withdraw-config'
            method = "GET"

            paramsStr = 'ccy='+symbol[:-4]
            currency_ = await self.send_request(method, path, paramsStr, payload, sem=sem)


            if currency_['code']==0:
                await asyncio.gather(*[self.forNetworkList(n, symbol) for n in currency_['data']['chains']])
                
        except Exception as e:
            logging.error(f'CoinEx foreach error:{e}')
