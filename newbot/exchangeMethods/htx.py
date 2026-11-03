import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
from asyncio import gather
import random
import hmac
from hashlib import sha256
import aiohttp_socks
import aiohttp
import logging
import asyncio
from manage.dbloader import Mongoloader
from manage.constants import Constants


class HTX(Constants.HTX):
    def __init__(self) -> None:
        super().__init__()

    def mathOrderBooks(self,ob:dict):
        
        symbolsBook={'bids':[], 'asks':[]}
        symbolOrderBook = ob['tick']
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
            symbol = symbol.lower()
            path = '/market/depth?'
            paramsMap = { 
                'symbol': symbol,
                'depth': 100
            }
            paramsStr = self.parseParam(paramsMap)
            url = "%s%s?%s" % (self.APIURL,path,paramsStr)
            res = await session.get(url)
            match res.status:
                case 200:
                    ob = await res.json()
                    if ob['tick']:
                        return self.mathOrderBooks(ob)
                case 429:
                      print('Too many req HTX',symbol)
                      await session.close()
                      session = aiohttp.ClientSession(
                                                     connector=aiohttp_socks.ProxyConnector.from_url(
                                                     Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
                                                     
                      return await self.fetchOrderBook(symbol,session)
        except aiohttp.ClientConnectorError:
            print(f'HTX: proxy error:{symbol}')
            await session.close()
            session = aiohttp.ClientSession(
                                            connector=aiohttp_socks.ProxyConnector.from_url(
                                            Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20'])-1)]))
            return await self.fetchOrderBook(symbol,session)       
        except Exception as e:
            print(f"Fetch order book HTX err:{e}")

    async def demo(self,symbol):
        symbol = symbol.lower()
        payload = {}
        path = '/market/depth?'
        method = "GET"
        paramsMap = {
            
            
            'symbol': str(symbol[:-5])+'usdt',
            'depth': 100
        }
        paramsStr = self.parseParam(paramsMap)
        return await self.send_request(method, path, paramsStr, payload,sem=asyncio.Semaphore(10000),key='books20')

    def get_sign(self, api_secret, payload):
        signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
        
        return signature

    logging.basicConfig(level=logging.INFO, filename='htx_log.txt')

    async def symbols(self,sem):
            payload = {}
            path = '/market/tickers'
            method = "GET"
            paramsStr = ""
            return await self.send_request(method, path, paramsStr, payload,sem)

    mongoload = Mongoloader()
    async def loadMarkets(self, sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem)
            # Проверяем, что в ответе есть ключ "data" и он не пустой
            if "data" in loadMarkets_ and loadMarkets_["data"]:
                for i in loadMarkets_["data"]:
                    if i["symbol"][-4:] == "usdt":
                        tradePair = i['symbol']
                        allSymbolsData.append(tradePair)
            else:
                logging.exception("HTX loadMarkets error: 'data' key missing or empty in response: %s", loadMarkets_)
            await self.mongoload.loadInToMongo(exName=self.name, symbols=allSymbolsData)
        except Exception as e:
            logging.exception("htx format error: %s", e)


    def curencyWrapper(self,symbol,sem):
        asyncio.run(self.currency(symbol,sem))


    async def send_request(self,method, path, urlpa, payload, sem,key=None):
        url = "%s%s?%s&signature=%s" % (self.APIURL, path, urlpa, self.get_sign(self.SECRETKEY, urlpa))

        session = None
        connector = None
        res = {}
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
            async with sem: 
                r = await session.request('GET', url=url, data=payload)
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

    def parseParam(self,paramsMap):
        sortedKeys = sorted(paramsMap)
        paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
        if paramsStr != "":
            
            return paramsStr+"&type=step0"
        else:
            
            return paramsStr+"type=step0"



    async def getChainFee(self,sem):
        payload = {}
        path = '/v2/reference/currencies'
        method = "GET"
        paramsMap = {}
        paramsStr = ''
        return await self.send_request(method, path, paramsStr, payload,sem,'cur')

    

    alltickers = {}
    
    async def reformTickers(self,ticker):
        try:
            if ticker['symbol'][-4:] == "usdt":
                self.alltickers[ticker['symbol']]={'ask':'','bid':''}
                self.alltickers[ticker['symbol']]['ask']=ticker['ask']
                self.alltickers[ticker['symbol']]['bid']=ticker['bid']
        except Exception as e:
            logging.exception(f"Reform tickers error CoinEX:{e}")

    async def fetchTickers(self,sem):
        try:
            tickers = await self.symbols(sem)
            
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers['data']])
            return self.alltickers
        except Exception as e:
            logging.exception(f"Fetch tickers CoinEX error:{e}")

    
       

    async def getContracts(self,sem):
        payload = {}
        path = '/v1/settings/common/chains'
        method = "GET"
        paramsMap = {
        
    }
        paramsStr = ''
        return await self.send_request(method, path, paramsStr, payload,sem,'cur')

    globalCurrency = {}

    async def reformForeach(self,symbol, contractFee, contracts):
        try:
            pairInfo = {
                        'networks':{},
                        }
            cur = symbol[:-4].lower()

            contractAdr = {}
            for cont in contracts:
                if cont['currency']==cur:
                    contractAdr[cont['chain']] =  cont['ca']


            for z in contractFee:
                if z['currency']==cur:
                    for c in z['chains']:
                        pairInfo['networks'][c['displayName'].upper()]={}
                        try:
                            wd = True
                            dep = True
                            if c['withdrawStatus'] != 'allowed':
                                wd=False
                            if c['depositStatus'] != 'allowed':
                                dep=False
                            
                            fee =0
                            if c['withdrawFeeType']=="fixed":
                                fee = float(c['transactFeeWithdraw'])
                            if c['withdrawFeeType']=='ratio' or c['withdrawFeeType']=='circulated':
                                try:
                                    fee = float(c['minTransactFeeWithdraw'])
                                except:
                                    try:
                                        fee = float(c['maxTransactFeeWithdraw'])
                                    except Exception:
                                        fee =0  
                            net ={
                                'withdraw':wd,
                                'deposit':dep,
                                'fee':fee,
                                'contract':contractAdr[c['chain']],
                                'adminFee':0
                            }
                        except Exception as e:
                            print(e,c)
                        pairInfo['networks'][c['displayName'].upper()]=net
            self.globalCurrency[symbol] = pairInfo
        except Exception as e:
            logging.exception(f'HTX foreach error:{e}')
            return

    networksForms= {
                        # by displaynames
                        'TRC20': 'TRX',  # TRON for mainnet
                        'BTC': 'BTC',
                        'ERC20': 'ETH',  # ETH for mainnet
                        'SOL': 'SOLANA',
                        'HRC20': 'HECO',
                        'BEP20': 'BSC',
                        'XMR': 'XMR',
                        'LTC': 'LTC',
                        'XRP': 'XRP',
                        'XLM': 'XLM',
                        'CRONOS': 'CRO',
                        'CRO': 'CRO',
                        'GLMR': 'GLMR',
                        'POLYGON': 'MATIC',
                        'MATIC': 'MATIC',
                        'BTT': 'BTT',
                        'CUBE': 'CUBE',
                        'IOST': 'IOST',
                        'NEO': 'NEO',
                        'KLAY': 'KLAY',
                        'EOS': 'EOS',
                        'THETA': 'THETA',
                        'NAS': 'NAS',
                        'NULS': 'NULS',
                        'QTUM': 'QTUM',
                        'FTM': 'FTM',
                        'CELO': 'CELO',
                        'DOGE': 'DOGE',
                        'DOGECHAIN': 'DOGECHAIN',
                        'NEAR': 'NEAR',
                        'STEP': 'STEP',
                        'BITCI': 'BITCI',
                        'CARDANO': 'ADA',
                        'ADA': 'ADA',
                        'ETC': 'ETC',
                        'LUK': 'LUK',
                        'MINEPLEX': 'MINEPLEX',
                        'DASH': 'DASH',
                        'ZEC': 'ZEC',
                        'IOTA': 'IOTA',
                        'NEON3': 'NEON3',
                        'XEM': 'XEM',
                        'HC': 'HC',
                        'LSK': 'LSK',
                        'DCR': 'DCR',
                        'BTG': 'BTG',
                        'STEEM': 'STEEM',
                        'BTS': 'BTS',
                        'ICX': 'ICX',
                        'WAVES': 'WAVES',
                        'CMT': 'CMT',
                        'BTM': 'BTM',
                        'VET': 'VET',
                        'XZC': 'XZC',
                        'ACT': 'ACT',
                        'SMT': 'SMT',
                        'BCD': 'BCD',
                        'WAX': 'WAX1',
                        'WICC': 'WICC',
                        'ELF': 'ELF',
                        'ZIL': 'ZIL',
                        'ELA': 'ELA',
                        'BCX': 'BCX',
                        'SBTC': 'SBTC',
                        'BIFI': 'BIFI',
                        'CTXC': 'CTXC',
                        'WAN': 'WAN',
                        'POLYX': 'POLYX',
                        'PAI': 'PAI',
                        'WTC': 'WTC',
                        'DGB': 'DGB',
                        'XVG': 'XVG',
                        'AAC': 'AAC',
                        'AE': 'AE',
                        'SEELE': 'SEELE',
                        'BCV': 'BCV',
                        'GRS': 'GRS',
                        'ARDR': 'ARDR',
                        'NANO': 'NANO',
                        'ZEN': 'ZEN',
                        'RBTC': 'RBTC',
                        'BSV': 'BSV',
                        'GAS': 'GAS',
                        'XTZ': 'XTZ',
                        'LAMB': 'LAMB',
                        'CVNT1': 'CVNT1',
                        'DOCK': 'DOCK',
                        'SC': 'SC',
                        'KMD': 'KMD',
                        'ETN': 'ETN',
                        'TOP': 'TOP',
                        'IRIS': 'IRIS',
                        'UGAS': 'UGAS',
                        'TT': 'TT',
                        'NEWTON': 'NEWTON',
                        'VSYS': 'VSYS',
                        'FSN': 'FSN',
                        'BHD': 'BHD',
                        'ONE': 'ONE',
                        'EM': 'EM',
                        'CKB': 'CKB',
                        'EOSS': 'EOSS',
                        'HIVE': 'HIVE',
                        'RVN': 'RVN',
                        'DOT': 'DOT',
                        'KSM': 'KSM',
                        'BAND': 'BAND',
                        'OEP4': 'OEP4',
                        'NBS': 'NBS',
                        'FIS': 'FIS',
                        'AR': 'AR',
                        'HBAR': 'HBAR',
                        'FIL': 'FIL',
                        'MASS': 'MASS',
                        'KAVA': 'KAVA',
                        'XYM': 'XYM',
                        'ENJ': 'ENJ',
                        'CRUST': 'CRUST',
                        'ICP': 'ICP',
                        'CSPR': 'CSPR',
                        'FLOW': 'FLOW',
                        'IOTX': 'IOTX',
                        'LAT': 'LAT',
                        'APT': 'APT',
                        'XCH': 'XCH',
                        'MINA': 'MINA',
                        'XEC': 'ECASH',
                        'XPRT': 'XPRT',
                        'CCA': 'ACA',
                        'AOTI': 'COTI',
                        'AKT': 'AKT',
                        'ARS': 'ARS',
                        'ASTR': 'ASTR',
                        'AZERO': 'AZERO',
                        'BLD': 'BLD',
                        'BRISE': 'BRISE',
                        'CORE': 'CORE',
                        'DESO': 'DESO',
                        'DFI': 'DFI',
                        'EGLD': 'EGLD',
                        'ERG': 'ERG',
                        'ETHF': 'ETHFAIR',
                        'ETHW': 'ETHW',
                        'EVMOS': 'EVMOS',
                        'FIO': 'FIO',
                        'FLR': 'FLR',
                        'FINSCHIA': 'FINSCHIA',
                        'KMA': 'KMA',
                        'KYVE': 'KYVE',
                        'MEV': 'MEV',
                        'MOVR': 'MOVR',
                        'NODL': 'NODL',
                        'OAS': 'OAS',
                        'OSMO': 'OSMO',
                        'PAYCOIN': 'PAYCOIN',
                        'POKT': 'POKT',
                        'PYG': 'PYG',
                        'REI': 'REI',
                        'SCRT': 'SCRT',
                        'SDN': 'SDN',
                        'SEI': 'SEI',
                        'SGB': 'SGB',
                        'SUI': 'SUI',
                        'SXP': 'SOLAR',
                        'SYS': 'SYS',
                        'TENET': 'TENET',
                        'TON': 'TON',
                        'UNQ': 'UNQ',
                        'UYU': 'UYU',
                        'WEMIX': 'WEMIX',
                        'XDC': 'XDC',
                        'XPLA': 'XPLA',

    }


    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair,value in copied_dict.items():
            if value['networks']=={}:
                del self.globalCurrency[pair]

    async def currency(self,symbols,sem):
        
        try:
            self.globalCurrency = {}
            allChainsFee = []
            allContracts = []

            chainsFee = await self.getChainFee(sem)
            contracts = await self.getContracts(sem)
            if chainsFee['data']:
                    chainsFee = chainsFee['data']
                    for i in chainsFee:
                        allChainsFee.append(i)
            if contracts['data']:
                    contracts = contracts['data']
                    for i in contracts:
                        allContracts.append(i)

            await gather(*[self.reformForeach(symbol, chainsFee,contracts) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name,currency=self.globalCurrency)
        except Exception as e:
            logging.exception(f'HTX currency raised error:{e}. Try again...')
            await self.currency(symbols,sem)
        
