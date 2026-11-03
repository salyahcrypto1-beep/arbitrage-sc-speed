import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import asyncio
import time
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import base64
import logging
from manage.dbloader import Mongoloader
from manage.constants import Constants


# 02.10: Bybit блокирует IP дата-центра (403), поэтому REST идёт через прокси платформы (socks5 из /root/all_prx.json).
# ProxyConnector создаётся напрямую — from_url в прямом режиме подменён на обычное соединение.
import json as _json
from urllib.parse import urlparse as _urlparse
_PLATFORM_PROXIES = None


def _platform_connector():
    global _PLATFORM_PROXIES
    if _PLATFORM_PROXIES is None:
        try:
            raw = _json.load(open("/root/all_prx.json"))
            urls = sorted({(x.get("proxy") if isinstance(x, dict) else str(x)) for x in raw})
            _PLATFORM_PROXIES = [u for u in urls if u and "://" in u]
        except Exception:
            _PLATFORM_PROXIES = []
    if not _PLATFORM_PROXIES:
        return None
    u = _urlparse(random.choice(_PLATFORM_PROXIES))
    ptype = aiohttp_socks.ProxyType.SOCKS5 if u.scheme.startswith("socks5") else (
        aiohttp_socks.ProxyType.SOCKS4 if u.scheme.startswith("socks4") else aiohttp_socks.ProxyType.HTTP)
    return aiohttp_socks.ProxyConnector(proxy_type=ptype, host=u.hostname, port=u.port, username=u.username, password=u.password, rdns=True)

class Bybit(Constants.Bybit):
    def __init__(self) -> None:
        super().__init__()
    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))

    def mathOrderBooks(self,ob:dict):
        
        symbolsBook={'bids':[], 'asks':[]}
        symbolOrderBook = ob['result']
        for z in symbolOrderBook['b']:
            bidsContainer = []
            symbolOrderBook['b'][symbolOrderBook['b'].index(z)][0] = float(symbolOrderBook['b'][symbolOrderBook['b'].index(z)][0])
            bidsContainer.append(symbolOrderBook['b'][symbolOrderBook['b'].index(z)][0])
            symbolOrderBook['b'][symbolOrderBook['b'].index(z)][1] = float(symbolOrderBook['b'][symbolOrderBook['b'].index(z)][1])
            bidsContainer.append(symbolOrderBook['b'][symbolOrderBook['b'].index(z)][1])
            symbolsBook['bids'].append(bidsContainer)
        for z in symbolOrderBook['a']:
            asksContainer = []
            symbolOrderBook['a'][symbolOrderBook['a'].index(z)][0] = float(symbolOrderBook['a'][symbolOrderBook['a'].index(z)][0])
            asksContainer.append(symbolOrderBook['a'][symbolOrderBook['a'].index(z)][0])
            symbolOrderBook['a'][symbolOrderBook['a'].index(z)][1] = float(symbolOrderBook['a'][symbolOrderBook['a'].index(z)][1])
            asksContainer.append(symbolOrderBook['a'][symbolOrderBook['a'].index(z)][1])
            symbolsBook['asks'].append(asksContainer)

        return symbolsBook

    async def fetchOrderBookBot(self,symbol):
        session = aiohttp.ClientSession()
        return await self.fetchOrderBook(symbol,session)

    async def fetchOrderBook(self,symbol,session):
        try:
            path = '/v5/market/orderbook'
            orderbook = {}
            paramsMap = { 
            'symbol': symbol,
            'category':'spot'
             }
            paramsStr = self.parseParam(paramsMap)
            url = self.APIURL+path+'?'+paramsStr
            r = await session.request('GET', url=url)
            match r.status:
                  case 200:
                      orderbook = await r.json()
                      if orderbook['result']:
                          return self.mathOrderBooks(orderbook)
                  case 429:
                      print('Too many req Bybit',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session)
        
        
        except aiohttp.ClientConnectorError:
            print(f'Bybit: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)       
        except Exception as e:
            print(f"Fetch order book ByBit err:{e}")


    logging.basicConfig(level=logging.INFO, filename='bybitlog.txt')

    async def tickers(self,sem):
        payload = {}
        path = '/v5/market/tickers'
        method = "GET"
        paramsStr = "category=spot"
        return await self.send_request(method, path, paramsStr, payload,sem)

    alltickers = {}
    
    async def reformTickers(self,ticker):
        try:
            if ticker['symbol'][-4:] == "USDT":
                self.alltickers[ticker['symbol']]={'ask':'','bid':''}
                self.alltickers[ticker['symbol']]['ask']=ticker['ask1Price']
                self.alltickers[ticker['symbol']]['bid']=ticker['bid1Price']
        except Exception as e:
            logging.error(f"Reform tickers error Bybit:{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['result']['list']])
            return self.alltickers
        except Exception as e:
            logging.error(f"Fetch tickers Bybit error:{e}")

    async def symbols(self,sem):
        payload = {}
        path = '/v5/market/instruments-info'
        method = "GET"
        paramsStr = "category=spot"
        return await self.send_request(method, path, paramsStr, payload,sem)

    mongoload = Mongoloader()

    async def loadMarkets(self,sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            if loadMarkets_["result"]:           
                for i in loadMarkets_["result"]["list"]:     
                    if i["quoteCoin"] == "USDT":                
                        tradePair=i['symbol']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)
    
            # return collectedPair
        except Exception as e:
            logging.error('bybit format error', e)


    async def demo(self,symbol):
        payload = {}
        path = '/v5/market/orderbook'
        method = "GET"
        sem = asyncio.Semaphore(10000)
        paramsMap = { 
            'symbol': str(symbol[:-5])+'USDT',
            'category':'spot'
        }
        paramsStr = self.parseParam(paramsMap)
        return await self.send_request(method, path, paramsStr, payload,sem,'books')

    def get_sign(self,api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        
        return signature


    async def send_request(self,method, path, urlpa, payload, sem,key=None ):
        url = "%s%s?%s&signature=%s" % (self.APIURL, path, urlpa, self.sign(secret=self.SECRETKEY, message=urlpa))
        session = None
        connector = None
        res = {}
        # 02.10: любой запрос Bybit — через прокси платформы (403 напрямую); без прокси — как раньше
        connector = _platform_connector()
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector) if connector is not None else aiohttp.ClientSession()
        try:
            if key=='cur':
                headers = {'Accept': 'application/json', 
                'Content-Type': 'application/json', 
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
        
                
                url = "%s%s" % (self.APIURL, path)
            
                
                timestamp = int(time.time() * 1000)
                # Формирование строки запроса
                query_string = f'{timestamp}{self.APIKEY}50000'

                # Создание подписи с использованием HMAC SHA256
                headersReq={
                    "X-BAPI-SIGN": hmac.new(
                    bytes(self.SECRETKEY, 'utf-8'), query_string.encode('utf-8'),sha256).hexdigest(),
                    "X-BAPI-API-KEY": self.APIKEY,
                    "X-BAPI-TIMESTAMP": str(timestamp),
                    'X-BAPI-RECV-WINDOW': '50000'
                }
                headers.update(headersReq)
                response = await session.request(method=method,url=url, headers=headers)
                if response.status == 200:
                    res = await response.json()
                else:
                    logging.warning(f'Request Bybit was failed: {e}. Try again...')
                    await self.send_request(self,method, path, urlpa, payload, sem,key)
            else:
                r = await session.request('GET', url=url)
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

    def parseParam(self,paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
        if paramsStr != "":
            returnable =paramsStr+"&limit=100"
            
            return returnable
        else:
            returnable =paramsStr+"limit=100"
            
            return returnable

    def sign(self,message, secret):
        mac = hmac.new(bytes(secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d).decode()

    globalCurrency = {}

    networksById= {
                        'ETH': 'ERC20',
                        'TRX': 'TRC20',
                        'BSC': 'BEP20',
                        'OMNI': 'OMNI',
                        'SPL': 'SOL'}
    

    async def forNetworkList(self, n, symbol):
        try:
            chain = ''
            try:
                chain = self.networksById[n['chain']]
            except:
                chain = n['chain']
            self.globalCurrency[symbol]['networks'][chain.upper()]={}
            net ={
                'withdraw':True if (n['chainWithdraw'])=='1' else False,
                'deposit':True if  (n['chainDeposit'])=='1' else False,
                'fee':float(n['withdrawFee']),
                'contract':'',
                'adminFee':0
            }

            self.globalCurrency[symbol]['networks'][chain.upper()]=net
        except Exception:
            self.globalCurrency[symbol]['networks']={}  
       
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
                    case 'BEP20':
                        if cmc_obj['chain_token']=='BNB' and not cmc_obj['token_address'] == '':
                            self.globalCurrency[pair]['networks'][net]['contract']=cmc_obj['token_address']
                    case 'ARBI':
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


    async def currency(self,symbols, sem):
        try:    
            payload = {}
            path = '/v5/asset/coin/query-info'
            method = "GET"
            
            params = ''
            currency_ =  await self.send_request(method, path, params, payload,key='cur',sem=sem)
            if currency_:
                allSymbolsCurrency = currency_['result']['rows']

            await asyncio.gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.find_contract()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
            self._currency_attempt = 0
        except Exception as e:
            # 02.10: раньше — рекурсивный ретрай без ограничений (RecursionError). Теперь до 3 попыток, потом пропуск цикла.
            attempt = getattr(self, '_currency_attempt', 0) + 1
            self._currency_attempt = attempt
            if attempt < 3:
                logging.error(f'Error raised in currency Bybit: {e}. Try again ({attempt}/3)...')
                await asyncio.sleep(2)
                await self.currency(symbols, sem)
            else:
                self._currency_attempt = 0
                logging.error(f'Error raised in currency Bybit: {e} (пропускаем биржу в этом цикле)')
            return
