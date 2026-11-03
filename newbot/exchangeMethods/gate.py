import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
from asyncio import gather
import time
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import hashlib
import logging
import asyncio
import threading
from manage.dbloader import Mongoloader
from manage.constants import Constants

class Gate(Constants.Gate):

    def mathOrderBooks(self,ob:dict):
        symbolsBook={'bids':[], 'asks':[]}
        symbolOrderBook = ob
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

    async def fetchOrderBookBot(self,symbol):
        session = aiohttp.ClientSession()
        return await self.fetchOrderBook(symbol,session)

    async def fetchOrderBook(self,symbol,session):  
        try:
            ob = {}
            prefix = "/api/v4"
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

            url = '/spot/order_book'
            query_param = 'currency_pair=' + symbol + '&limit=100'
            link = self.APIURL + prefix + url + "?" + query_param
            res = await session.get(link,headers=headers)
            match res.status:
                case 200:
                    ob = await res.json()
                    return self.mathOrderBooks(ob)
                case 429:
                      print('Too many req Gate',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session)
            
        except aiohttp.ClientConnectorError:
            print(f'Gate: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)                       
        except Exception as e:
            print(f"Fetch order book Gate err:{e}")



    logging.basicConfig(level=logging.INFO, filename='gate_log.txt')


    mongoload = Mongoloader()

    async def tickers(self,sem):
        path = '/spot/tickers'
        link = self.APIURL + "/api/v4" + path
        return await self.send_request(sem,link)

    alltickers = {}
    
    async def reformTickers(self,ticker):
        try:
            if ticker['currency_pair'][-4:] == "USDT":
                self.alltickers[ticker['currency_pair']]={'ask':'','bid':''}
                self.alltickers[ticker['currency_pair']]['ask']=ticker['lowest_ask']
                self.alltickers[ticker['currency_pair']]['bid']=ticker['highest_bid']
        except Exception as e:
            logging.error(f"Reform tickers error Gate :{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
        
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers])
            return self.alltickers
        except Exception as e:
            logging.error(f"Fetch tickers Gate error:{e}")

    async def loadMarkets(self,sem):
        try:
            allSymbolsData = []
            url = '/spot/currency_pairs'
            link = self.APIURL + "/api/v4" + url
            loadMarkets_ = await self.send_request(sem=sem,url=link)
            if loadMarkets_:           
                for i in loadMarkets_:     
                    if i["quote"] == "USDT":                
                        tradePair=i['id']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)
            # return collectedPair
        except Exception as e:
            logging.error('gate format error', e)
    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))

    async def send_req_books(self,symbol):
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        url = '/spot/order_book'
        query_param = 'currency_pair=' + str(symbol[:-5]) + '_USDT' + '&limit=100'
        res = {}
        link = self.APIURL + prefix + url + "?" + query_param
        sem = asyncio.Semaphore(10000)
        return await self.send_request(sem,link,'books')
    
    
    async def send_request(self, sem,url,key=None):
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        timeout = aiohttp.ClientTimeout(
            total=5,connect=5,sock_read=5,sock_connect=5
        )
        res = {}
        session = None
        connector = None
        match key:
            case 'cur':
                connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur'])-1)])
                session = aiohttp.ClientSession(connector=connector,timeout=timeout)
            case 'books':
                connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)])
                session = aiohttp.ClientSession(connector=connector,timeout=timeout)
            case None:
                session = aiohttp.ClientSession()
        try:  
            r = await session.request('GET', url, headers=headers)
            res = await r.json()
        except Exception as e:
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


    networks= {
        'AVAX_C': 'AVAXC',
        'BSC': 'BEP20',
        'EOS': 'EOS',
        'ETH': 'ERC20',
        'GTEVM': 'GATECHAIN',
        'HT': 'HRC20',
        'KSMSM': 'KUSAMA',
        'NEAR': 'NEAR',
        'OKT': 'OKC',
        'OPETH': 'OPTIMISM',
        'DOTSM': 'POLKADOT',
        'TRX': 'TRC20'}

    async def currencyLoader(self,base,key):
        try:
            host = "https://api.gateio.ws"
            prefix = "/api/v4"
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

            url = '/wallet/currency_chains'
            query_param = 'currency='+base
            res = {}
            timeout = aiohttp.ClientTimeout(
            total=10
        )
            # Раньше при не-200 метод вызывал сам себя без ограничений: при
            # недоступности это уходило в бесконечную рекурсию и валило
            # загрузку валют ЦЕЛИКОМ («maximum recursion depth exceeded»),
            # из-за чего Gate вообще не попадал в рынок. Теперь — ограниченное
            # число повторов, и при неудаче просто возвращаем пусто.
            for attempt in range(3):
                async with aiohttp.ClientSession(timeout=timeout,connector=aiohttp_socks.ProxyConnector.from_url(random.choice(Constants.proxies[key]))) as session:
                    r = await session.get(host + prefix + url + "?" + query_param, headers=headers)
                    if r.status == 200:
                        return await r.json()
                await asyncio.sleep(0.5 * (attempt + 1))

            return res
        except Exception as e:
            return None

    globalCurrency = {}
    async def currency(self,symbols,sem):
       
        try:
            self.globalCurrency = {}
            admin = await self.getAdminFee()
            if admin is None:
                return 
            await gather(*[self.reformCurrency(symbol, admin,sem) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
        except Exception as e:
                # Здесь стоял self.currency(...) — ретрай через рекурсию без
                # ограничений, упиравшийся в предел стека. Теперь ошибка
                # только логируется и не мешает остальным биржам.
                logging.error(f'Error raised Gate currency:{e} (пропускаем биржу в этом цикле)')


    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def getAdminFee(self):
        try:
            host = "https://api.gateio.ws"
            prefix = "/api/v4"
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

            url = '/wallet/withdraw_status'
            query_param = ''
            sign_headers = self.gen_sign('GET', prefix + url, query_param)
            headers.update(sign_headers)
            connector=aiohttp_socks.ProxyConnector.from_url(random.choice(Constants.proxies['cur']))
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
            r =  await session.request('GET', host + prefix + url + "?" + query_param, headers=headers)
            if r.status == 200:
                res = await r.json()
                await connector.close()
                await session.close()
            else:
                await connector.close()
                await session.close()
                return None

            return res
        except Exception as e:
            print('currency erroed: ',e)

    async def currencyCheker(self,curr,symbol,admin):
        try:
                net = {}
                chainName = ''
                try:
                    chainName= self.networks[curr['chain']]
                except:
                    chainName = curr['chain']
                    
                self.globalCurrency[symbol]['networks'][chainName.upper()]={}
                deposit = bool(curr['is_deposit_disabled'])
                withdraw = bool(curr['is_withdraw_disabled'])
                getAdmin = {}
    
                for cur in admin:
                    if cur['currency'] == symbol.split('_')[0]:
                        getAdmin = cur
                        break
                fee= 0
                admin_fee = 0 
                if getAdmin and 'withdraw_fix_on_chains' in getAdmin.keys():
                    if curr['chain'] in getAdmin['withdraw_fix_on_chains'].keys():
                        fee = float(getAdmin['withdraw_fix_on_chains'][curr['chain']])
                if getAdmin and 'withdraw_percent_on_chains' in getAdmin.keys():
                    if curr['chain'] in getAdmin['withdraw_percent_on_chains'].keys():
                        admin_fee = float(getAdmin['withdraw_percent_on_chains'][curr['chain']].split('%')[0])/100
                try:
                    net ={
                        'withdraw':not withdraw,
                        'deposit':not deposit,
                        'fee':fee,
                        'contract':curr['contract_address'],
                        'adminFee':admin_fee
                    }
                except Exception as e:
                    logging.error(f'GATE Curr foreach error:{e}')
                self.globalCurrency[symbol]['networks'][chainName.upper()]=net
                return
        except Exception as e:
              print('Checker Gate errored:',e)
                    

    async def reformCurrency(self,symbol, admin,sem):
        try:
            self.globalCurrency[symbol]= {
                'networks':{}  
            }
            try:
                currency_ = await self.currencyLoader(symbol.split('_')[0],'cur')
                if not currency_ is None:
                        await asyncio.gather(*[self.currencyCheker(n,symbol,admin) for n in currency_])
                return
            except Exception as e:
                print(f'Reform foreach Gate error handled:{e}')
                        
            
        except Exception as e:
            logging.error(f'Gate foreach error:{e}')

    def gen_sign(self,method, url, query_string=None, payload_string=None):
        try:
            key = self.APIKEY        # api_key
            secret = self.APISECRET     # api_secret

            t = time.time()
            m = hashlib.sha512()
            m.update((payload_string or "").encode('utf-8'))
            hashed_payload = m.hexdigest()
            s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
            sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
            return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}
        except:
            print('errored')

# async def main():
#     bingx = Gate()
#     import manage.dbloader as loader
#     loader_ = loader.Mongoloader()
#     symbols = await loader_.loadFromMongo(key='Markets')
#     await bingx.currency(symbols['Gate'],asyncio.Semaphore(500))

# if __name__ == '__main__':
#     asyncio.run(main())  