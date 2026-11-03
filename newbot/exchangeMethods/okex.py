import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from datetime import datetime, timezone
import asyncio
import json
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import base64
import base64
from urllib.parse import urlencode
import logging
from manage.dbloader import Mongoloader
from manage.constants import Constants

class OKX(Constants.OKX):
    def __init__(self) -> None:
        super().__init__()


    logging.basicConfig(level=logging.INFO, filename='okx_log.txt')


    mongoload = Mongoloader()
    async def loadMarkets(self,sem):
        try:
            
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            if loadMarkets_["data"]:           
                for i in loadMarkets_["data"]:     
                    if i["quoteCcy"] == "USDT":                
                        tradePair=i['instId']
                        allSymbolsData.append(tradePair)
            await self.mongoload.loadInToMongo( exName=self.name,symbols=allSymbolsData)

            # return collectedPair
        except Exception as e:
            logging.error('kucoin format error', e)



    alltickers = {}
    
    async def reformTickers(self,ticker):
        try:
            if ticker['instId'][-4:] == "USDT":
                self.alltickers[ticker['instId']]={'ask':'','bid':''}
                self.alltickers[ticker['instId']]['ask']=ticker['askPx']
                self.alltickers[ticker['instId']]['bid']=ticker['bidPx']
        except Exception as e:
            logging.error(f"Reform tickers error OKX :{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.tickers(sem)
        
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['data']])
            return self.alltickers
        except Exception as e:
            logging.error(f"Fetch tickers OKX error:{e}")


    async def tickers(self,sem):     
        session2 = None
        connector = None
        res = {}
        try:
            flag = "0" 
            domain = 'https://www.okx.com'
            url = '/api/v5/market/tickers?instType=SPOT'
            req = domain+url

            session2 = aiohttp.ClientSession()
            r = await session2.request('GET', url=req)
            res = await r.json()
            await session2.close()
        except Exception as e:
            if session2 is not None:
                await session2.close()
            pass
        finally:
            if session2 is not None:
                await session2.close()

        return res    

    async def symbols(self,sem):     
        session2 = None
        connector = None
        res = {}
        try:
            async with sem:
                flag = "0" 
                domain = 'https://www.okx.com'
                url = '/api/v5/public/instruments?instType=SPOT'
                req = domain+url
                session2 = aiohttp.ClientSession()
                r = await session2.request('GET', url=req)
                res = await r.json()
                await session2.close()
        except Exception as e:
            if session2 is not None:
                await session2.close()
            if connector is not None:
                await connector.close()
        finally:
            if session2 is not None:
                await session2.close()
            if connector is not None:
                await connector.close()

        return res    

    async def send_req(self,symbol):     
        session2 = None
        connector = None
        res = {}
        try:
                flag = "0" 
                url = '/api/v5/market/books?instId='+symbol+'&sz=100'
                req = self.APIURL+url
                connector = aiohttp_socks.ProxyConnector.from_url(Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)])
                session2 = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
                r = await session2.request('GET', url=req)
                res = await r.json()
                await session2.close()
                await connector.close() 
        except Exception as e:
            if session2 is not None:
                await session2.close()
            if connector is not None:
                await connector.close()
            pass
        finally:
            if session2 is not None:
                await session2.close()
            if connector is not None:
                await connector.close()

        return res    

    async def send_req_books(self, symbol):
        res = {}
        try:
            flag = "0"
            url = '/api/v5/market/books?instId='+symbol+'&sz=100'
            req = self.APIURL+url
            async with aiohttp.ClientSession() as session:
                r = await session.request('GET', url=req)
                res = await r.json()
        except Exception as e:
            pass

        return res


    def mathOrderBooks(self,ob:dict):
        symbolsBook={'bids':[], 'asks':[]}
        symbolOrderBook = ob['data'][0]
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
        try:
            session = aiohttp.ClientSession()
            return await self.fetchOrderBook(symbol,session)
                  

        except Exception as e:
            print(f"Fetch order book OKX err:{e}")

    async def fetchOrderBook(self,symbol,session):  
        try:
            sendSymb = symbol
            url = '/api/v5/market/books?instId='+sendSymb+'&sz=100'
            link = self.APIURL+url
            ob = {}
            res = await session.get(link)
            match res.status:
                  case 200:
                      ob = await res.json()
                      if ob['data'][0]:
                          return self.mathOrderBooks(ob)
                  case 429:
                      print('Too many req OKX',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session)
                  
        except aiohttp.ClientConnectorError:
            print(f'OKX: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)    
        except Exception as e:
            print(f"Fetch order book OKX err:{e}")



    currencyPool={
        'Bitcoin': 'BTC',
    'Lightning': 'BTCLN',
    'BSC': 'BEP20',
    'ERC20': 'ERC20',
    'TRC20': 'TRC20',
    'Crypto': 'CRC20',
    'Acala': 'ACA',
    'Algorand': 'ALGO',
    'BHP': 'BHP',
    'Aptos': 'APT',
    'Arbitrum One': 'ARBONE',
    'Avalanche C': 'AVAXC',
    'Avalanche X': 'AVAXX',
    'ARK': 'ARK',
    'Arweave': 'AR',
    'Astar': 'ASTR',
    'BitcoinCash': 'BCH',
    'Bitcoin SV': 'BSV',
    'Bytom': 'BTM',
    'Cardano': 'ADA',
    'Casper': 'CSPR',
    'CELO': 'CELO',
    'Chia': 'XCH',
    'Chiliz': 'CHZ',
    'Cosmos': 'ATOM',
    'TrueChain': 'TRUE',
    'Decred': 'DCR',
    'Digibyte': 'DGB',
    'Dogecoin': 'DOGE',
    'XEC': 'XEC',
    'Elrond': 'EGLD',
    'EOS': 'EOS',
    'Ethereum Classic': 'ETC',
    'EthereumPow': 'ETHW',
    'Fantom': 'FTM',
    'Filecoin': 'FIL',
    'FLOW': 'FLOW',
    'Fusion': 'FSN',
    'Harmony': 'ONE',
    'Hedera': 'HBAR',
    'Helium': 'HNT',
    'Horizen': 'ZEN',
    'ICON': 'ICX',
    'Dfinity': 'ICP',
    'IOST': 'IOST',
    'MIOTA': 'IOTA',
    'Kadena': 'KDA',
    'KAR': 'KAR',
    'Klaytn': 'KLAY',
    'Kusama': 'KSM',
    'Lisk': 'LSK',
    'Litecoin': 'LTC',
    'Metis': 'METIS',
    'Mina': 'MINA',
    'Monero': 'XMR',
    'Moonbeam': 'GLRM',
    'Moonriver': 'MOVR',
    'Nano': 'NANO',
    'NEAR': 'NEAR',
    'Nebulas': 'NAS',
    'New Economy Movement': 'NEM',
    'NULS': 'NULS',
    'OASYS': 'OASYS',
    'OKC': 'OKC',
    'Ontology': 'ONT',
    'Optimism': 'OPTIMISM',
    'PlatON': 'LAT',
    'Polkadot': 'DOT',
    'Polygon': 'MATIC',
    'Ravencoin': 'RVN',
    'Ripple': 'XRP',
    'Siacoin': 'SC',
    'Solana': 'SOL',
    'l-Stacks': 'STX',
    'Stellar Lumens': 'XLM',
    'Tezos': 'XTZ',
    'TON': 'TON',
    'Theta': 'THETA',
    'VSYSTEMS': 'VSYS',
    'WAVES': 'WAVES',
    'Wax': 'WAX',
    'Zcash': 'ZEC',
    'Zilliqa': 'ZIL',
    'ZKSYNC': 'ZKSYNC'
    }

    globalCurrency = {}

    def sign(self,message, secret):
        mac = hmac.new(bytes(secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d).decode()

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def match_contract(self,pair:str,dict,cmc):
        try:
            formated_pair = pair.replace('-USDT','')
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
                    case 'ARBONE':
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

    async def currency(self,symbols,sem):
        
        try:
            self.globalCurrency = {}
            allSymbolsCurrency = []
            currency_ = await self.getContract()
            currency_ = json.loads(currency_)
            if currency_['data']:
                allSymbolsCurrency = currency_['data']

            await asyncio.gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.find_contract()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
        except Exception as e:
            # Здесь стоял self.currency(...) — ретрай через рекурсию без
            # ограничений. Любая устойчивая ошибка упирала стек и давала
            # RecursionError, который всплывал в CurrencyLoader и мешал
            # остальным биржам. Теперь ошибка только логируется.
            logging.error(f'Raised error currency OKX:{e} (пропускаем биржу в этом цикле)')

    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))


    async def forEverySymbolCurr(self,symbol,currency):
        try:
            pairInfo = { 'networks':{}}
            for i in currency:    
                if symbol[:-5] == i['ccy']:
                    chain = i['chain'].split('-')[1]
                    chainName =''
                    try:
                        chainName= self.currencyPool[chain]
                    except:
                        chainName = chain

                    pairInfo['networks'][chainName.upper()]={} 

                    try:
                        net ={
                            'withdraw':bool(i['canWd']),
                            'deposit':bool(i['canDep']),
                            'fee':float(i['minFee']),
                            'contract':'',
                            'adminFee': 0

                        }
                    except Exception as e:
                        print(e)
                    pairInfo['networks'][chainName.upper()]=net
            self.globalCurrency[symbol] = pairInfo
        except Exception as e:
            print('error reform', e)
            return

    async def getContract(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=aiohttp_socks.ProxyConnector.from_url(Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur'])-1)])) as session:
            flag = "0" 
            domain = 'https://www.okx.com'
            
            # Retrieve order book of the instrument
            url = '/api/v5/asset/currencies'
            current_time = datetime.now(timezone.utc)
            formatted_time = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            req = domain+url
            params= {}
            body = '' if not params else urlencode(params)
            message = '{}{}{}{}{}'.format(formatted_time, 'GET', url, '', body)
            secret = self.sign(secret=self.APISECRET, message=message)

            auth = {
                "OK-ACCESS-KEY" :self.APIKEY,
                "OK-ACCESS-SIGN":secret,
                "OK-ACCESS-PASSPHRASE":self.PASSPHRASE,
                'OK-ACCESS-TIMESTAMP':formatted_time
            }
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
            headers.update(auth)
            res =  await session.request('GET',url=req, headers=headers) 
            result  =await res.text()
            return result
