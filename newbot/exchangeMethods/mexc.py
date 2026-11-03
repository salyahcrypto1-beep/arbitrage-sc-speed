import sys 
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append('D:/Projects/scArbitGit/scArbit/scArbit/serverVersion/stepback/new/exchangeMethods/manage')
import asyncio
import time
import hmac
from hashlib import sha256
import aiohttp
import urllib
import logging
import aiohttp_socks
import random
from manage.dbloader import Mongoloader
from manage.constants import Constants

# Настройка логирования
logging.basicConfig(level=logging.INFO, filename='mexc_log.txt')

class MEXC(Constants.Mexc):
    # Восстанавливаем словари netWorksId и networksForm
    netWorksId = {
        'Aleph Zero(AZERO)': 'AZERO',
        'Alephium(ALPH)': 'ALPH',
        'Algorand(ALGO)': 'ALGO',
        'APTOS(APT)': 'APT',
        'Arbitrum One(ARB)': 'ARB',
        'Avalanche C Chain(AVAX CCHAIN)': 'AVAXC',
        'Avalanche X Chain(AVAX XCHAIN)': 'AVAXX',
        'BEP20(BSC)': 'BSC',
        'Bitcoin Cash(BCH)': 'BCH',
        'Bitcoin SV(BSV)': 'BSV',
        'Bitcoin(BTC)': 'BTC',
        'Bittensor(TAO)': 'TAO',
        'BNB Beacon Chain(BEP2)': 'BEP2',
        'BNB Smart Chain(BEP20-RACAV1)': 'BSC',
        'BNB Smart Chain(BEP20-RACAV2)': 'BSC',
        'BNB Smart Chain(BEP20)': 'BSC',
        'Cardano(ADA)': 'ADA',
        'Celestia(TIA)': 'TIA',
        'Chia(XCH)': 'XCH',
        'Chiliz Chain(CHZ2)': 'CHZ2',
        'Chiliz Legacy Chain(CHZ)': 'CHZ',
        'Clore.ai(CLORE)': 'CLORE',
        'Cosmos(ATOM)': 'ATOM',
        'Dogechain(DC)': 'DC',
        'Dogecoin(DOGE)': 'DOGE',
        'Dymension(DYM)': 'DYM',
        'Dynex(DNX)': 'DNX',
        'Elysium(LAVA)': 'LAVA',
        'Ethereum Classic(ETC)': 'ETC',
        'Ethereum(ERC20)': 'ERC20',
        'Fantom(FTM)': 'FTM',
        'Hedera(HBAR)': 'HBAR',
        'Index Chain': 'INDEX',
        'Internet Computer(ICP)': 'ICP',
        'Kaspa(KAS)': 'KAS',
        'Klaytn(KLAY)': 'KLAY',
        'Litecoin(LTC)': 'LTC',
        'Mantle(MNT)': 'MNT',
        'Mcoin Network': 'MCOIN',
        'Meter(MTRG)': 'MTRG',
        'Monero(XMR)': 'XMR',
        'NEAR Protocol(NEAR)': 'NEAR',
        'Neoxa Network': 'NEOXA',
        'Neurai(XNA)': 'XNA',
        'Omega Network(OMN)': 'OMN',
        'Optimism(OP)': 'OPTIMISM',
        'Polkadot(DOT)': 'DOT',
        'Polygon(MATIC)': 'MATIC',
        'RAP20(Rangers Mainnet)': 'RAP20',
        'Ravencoin(RVN)': 'RVN',
        'Ripple(XRP)': 'XRP',
        'Satoxcoin(SATOX)': 'SATOX',
        'Solana(SOL)': 'SOL',
        'Starknet(STARK)': 'STARK',
        'Stellar(XLM)': 'XLM',
        'Terra(LUNA)': 'LUNA',
        'Toncoin(TON)': 'TON',
        'Tron(TRC20)': 'TRC20',
        'UGAS(Ultrain)': 'UGAS',
        'VeChain(VET)': 'VET',
        'Vexanium(VEX)': 'VEX',
        'XPR Network': 'XPR',
        'Zilliqa(ZIL)': 'ZIL',
    }

    networksForm = {
        'ABBC': 'ABBC',
        'ACA': 'ACALA',
        'ADA': 'Cardano(ADA)',
        'AE': 'AE',
        'ALGO': 'Algorand(ALGO)',
        'ALPH': 'Alephium(ALPH)',
        'AME': 'AME',
        'AOK': 'AOK',
        'APT': 'APTOS(APT)',
        'AR': 'AR',
        'ARB': 'Arbitrum One(ARB)',
        'ARBNOVA': 'ARBNOVA',
        'ARBONE': 'ArbitrumOne(ARB)',
        'ARK': 'ARK',
        'ASTR': 'ASTAR',
        'ATOM': 'Cosmos(ATOM)',
        'AVAXC': 'Avalanche C Chain(AVAX CCHAIN)',
        'AVAXX': 'Avalanche X Chain(AVAX XCHAIN)',
        'AZERO': 'Aleph Zero(AZERO)',
        'BCH': 'Bitcoin Cash(BCH)',
        'BDX': 'BDX',
        'BEAM': 'BEAM',
        'BEP2': 'BNB Beacon Chain(BEP2)',
        'BEP20': 'BNB Smart Chain(BEP20)',
        'BITCI': 'BITCI',
        'BNC': 'BNC',
        'BNCDOT': 'BNCPOLKA',
        'BOBA': 'BOBA',
        'BSC': 'BEP20(BSC)',
        'BSV': 'Bitcoin SV(BSV)',
        'BTC': 'Bitcoin(BTC)',
        'BTM': 'BTM2',
        'CELO': 'CELO',
        'CFX': 'CFX',
        'CHZ': 'Chiliz Legacy Chain(CHZ)',
        'CHZ2': 'Chiliz Chain(CHZ2)',
        'CKB': 'CKB',
        'CLORE': 'Clore.ai(CLORE)',
        'CRC20': 'CRONOS',
        'CSPR': 'CSPR',
        'DASH': 'DASH',
        'DC': 'Dogechain(DC)',
        'DCR': 'DCR',
        'DNX': 'Dynex(DNX)',
        'DOGE': 'Dogecoin(DOGE)',
        'DOT': 'Polkadot(DOT)',
        'DYM': 'Dymension(DYM)',
        'EDG': 'EDG',
        'EGLD': 'EGLD',
        'EOS': 'EOS',
        'ERC20': 'Ethereum(ERC20)',
        'ETC': 'Ethereum Classic(ETC)',
        'ETHF': 'ETF',
        'ETHW': 'ETHW',
        'EVER': 'EVER',
        'FET': 'FET',
        'FIL': 'FIL',
        'FIO': 'FIO',
        'FLOW': 'FLOW',
        'FSN': 'FSN',
        'FTM': 'Fantom(FTM)',
        'FUSE': 'FUSE',
        'GLMR': 'GLMR',
        'GRIN': 'GRIN',
        'HBAR': 'Hedera(HBAR)',
        'HIVE': 'HIVE',
        'HRC20': 'HECO',
        'HYDRA': 'HYDRA',
        'ICP': 'Internet Computer(ICP)',
        'INDEX': 'Index Chain',
        'IOST': 'IOST',
        'IOTA': 'IOTA',
        'IOTX': 'IOTX',
        'IRIS': 'IRIS',
        'KAR': 'KAR',
        'KAS': 'Kaspa(KAS)',
        'KAVA': 'KAVA',
        'KDA': 'KDA',
        'KILT': 'KILT',
        'KLAY': 'Klaytn(KLAY)',
        'KMA': 'KMA',
        'KSM': 'KSM',
        'LAT': 'LAT',
        'LAVA': 'Elysium(LAVA)',
        'LTC': 'Litecoin(LTC)',
        'LUNA': 'Terra(LUNA)',
        'MASS': 'MASS',
        'MATIC': 'Polygon(MATIC)',
        'MCOIN': 'Mcoin Network',
        'METIS': 'METIS',
        'MINA': 'MINA',
        'MNT': 'Mantle(MNT)',
        'MOVR': 'MOVR',
        'MTRG': 'Meter(MTRG)',
        'NAS': 'NAS',
        'NEAR': 'NEAR Protocol(NEAR)',
        'NEBL': 'NEBL',
        'NEM': 'NEM',
        'NEO': 'NEO',
        'NEO3': 'NEO3',
        'NEOXA': 'Neoxa Network',
        'NULS': 'NULS',
        'OASIS': 'ROSE',
        'OASYS': 'OASYS',
        'OKC': 'OKT',
        'OMN': 'Omega Network(OMN)',
        'OMNI': 'OMNI',
        'ONE': 'ONE',
        'ONT': 'ONT',
        'OPTIMISM': 'Optimism(OP)',
        'OSMO': 'OSMO',
        'PLCU': 'PLCU',
        'POKT': 'POKT',
        'QKC': 'QKC',
        'QTUM': 'QTUM',
        'RAP20': 'RAP20(Rangers Mainnet)',
        'REI': 'REI',
        'RSK': 'RBTC',
        'RVN': 'Ravencoin(RVN)',
        'SATOX': 'Satoxcoin(SATOX)',
        'SC': 'SC',
        'SCRT': 'SCRT',
        'SDN': 'SDN',
        'SGB': 'SGB',
        'SOL': 'Solana(SOL)',
        'STAR': 'STAR',
        'STARK': 'Starknet(STARK)',
        'STEEM': 'STEEM',
        'SYS': 'SYS',
        'TAO': 'Bittensor(TAO)',
        'TIA': 'Celestia(TIA)',
        'TOMO': 'TOMO',
        'TON': 'Toncoin(TON)',
        'TRC10': 'TRC10',
        'TRC20': 'Tron(TRC20)',
        'UGAS': 'UGAS(Ultrain)',
        'VET': 'VeChain(VET)',
        'VEX': 'Vexanium(VEX)',
        'VSYS': 'VSYS',
        'WAVES': 'WAVES',
        'WAX': 'WAX',
        'WEMIX': 'WEMIX',
        'XCH': 'Chia(XCH)',
        'XDC': 'XDC',
        'XEC': 'XEC',
        'XLM': 'Stellar(XLM)',
        'XMR': 'Monero(XMR)',
        'XNA': 'Neurai(XNA)',
        'XPR': 'XPR Network',
        'XRD': 'XRD',
        'XRP': 'Ripple(XRP)',
        'XTZ': 'XTZ',
        'XVG': 'XVG',
        'XYM': 'XYM',
        'ZEC': 'ZEC',
        'ZEN': 'ZEN',
        'ZIL': 'Zilliqa(ZIL)',
        'ZTG': 'ZTG',
        'ALAYA': 'ATP',
        'ANDUSCHAIN': 'DEB',
        'ASSETMANTLE': 'MNTL',
        'AXE': 'AXE',
        'BITCOINHD': 'BHD',
        'BITCOINVAULT': 'BTCV',
        'BITKUB': 'KUB',
        'BITSHARES_OLD': 'BTS',
        'BITSHARES': 'NBS',
        'BYTZ': 'BYTZ',
        'CANTO': 'CANTO',  # CANTOEVM
        'CENNZ': 'CENNZ',
        'CHAINX': 'PCX',
        'CONCODRIUM': 'CCD',
        'CONTENTVALUENETWORK': 'CVNT',
        'CORTEX': 'CTXC',
        'CYPHERIUM': 'CPH',
        'DANGNN': 'DGC',
        'DARWINIASMARTCHAIN': 'Darwinia Smart Chain',
        'DHEALTH': 'DHP',
        'DOGECOIN': 'DOGE',  # todo after unification
        'DRAC': 'DRAC',
        'DRAKEN': 'DRK',
        'ECOCHAIN': 'ECOC',
        'ELECTRAPROTOCOL': 'XEP',
        'EMERALD': 'EMERALD',  # sits on top of OASIS
        'EVMOS': 'EVMOS',
        'EXOSAMA': 'SAMA',
        'FIBOS': 'FO',
        'FILECASH': 'FIC',
        'FIRMACHAIN': 'FCT',
        'FIRO': 'XZC',
        'FNCY': 'FNCY',
        'FRUITS': 'FRTS',
        'GLEEC': 'GLEEC',
        'GXCHAIN': 'GXC',
        'HANDSHAKE': 'HNS',
        'HPB': 'HPB',
        'HSHARE': 'HC',
        'HUAHUA': 'HUAHUA',
        'HUPAYX': 'HPX',
        'INDEXCHAIN': 'IDX',
        'INTCHAIN': 'INT',
        'INTEGRITEE': 'TEER',
        'INTERLAY': 'INTR',
        'IOEX': 'IOEX',
        'JUNO': 'JUNO',
        'KASPA': 'KASPA',
        'KEKCHAIN': 'KEKCHAIN',
        'KINTSUGI': 'KINT',
        'KOINOS': 'KOINOS',
        'KONSTELLATION': 'DARC',
        'KUJIRA': 'KUJI',
        'KULUPU': 'KLP',
        'LBRY': 'LBC',
        'LEDGIS': 'LED',
        'LIGHTNINGBITCOIN': 'LBTC',
        'LINE': 'LINE',
        'MDNA': 'DNA',
        'MDUKEY': 'MDU',
        'METAMUI': 'MMUI',
        'METAVERSE_ETP': 'ETP',
        'METER': 'MTRG',
        'MEVERSE': 'MEVerse',
        'NEWTON': 'NEW',
        'NODLE': 'NODLE',
        'ORIGYN': 'OGY',
        'PAC': 'PAC',
        'PASTEL': 'PSL',
        'PHALA': 'Khala',
        'PLEX': 'PLEX',
        'PMG': 'PMG',
        'POINT': 'POINT',
        'PROOFOFMEMES': 'POM',
        'PROXIMAX': 'XPX',
        'RCHAIN': 'REV',
        'REBUS': 'REBUS',
        'RIZON': 'ATOLO',
        'SENTINEL': 'DVPN',
        'SERO': 'SERO',
        'TECHPAY': 'TPC',
        'TELOSCOIN': 'TLOS',
        'TERRA': 'LUNA2',
        'TERRACLASSIC': 'LUNC',
        'TLOS': 'TELOS',
        'TOMAINFO': 'TON',
        'TONGTONG': 'TTC',
        'TURTLECOIN': 'TRTL',
        'ULORD': 'UT',
        'ULTRAIN': 'UGAS',
        'UMEE': 'UMEE',
        'VDIMENSION': 'VOLLAR',
        'VEXANIUM': 'VEX',
        'VNT': 'VNT',
        'WAYKICHAIN': 'WICC',
        'WHITECOIN': 'XWC',
        'WITNET': 'WIT',
        'XDAI': 'XDAI',
        'XX': 'XX',
        'YAS': 'YAS',
        'ZENITH': 'ZENITH',
        'ZKSYNC': 'ZKSYNC',
    }

    def __init__(self) -> None:
        super().__init__()
        self.mongoload = Mongoloader()
        self.globalCurrency = {}
        self.alltickers = {}

    async def loadMarkets(self, sem):
        try:
            allSymbolsData = []
            loadMarkets_ = await self.symbols(sem=sem)
            if "symbols" in loadMarkets_ and loadMarkets_["symbols"]:
                for i in loadMarkets_["symbols"]:
                    if i.get("quoteAsset") == "USDT":
                        tradePair = i.get('symbol')
                        allSymbolsData.append(tradePair)
            else:
                logging.error('MEXC loadMarkets error: "symbols" key missing in response: %s', loadMarkets_)
            await self.mongoload.loadInToMongo(exName=self.name, symbols=allSymbolsData)
        except Exception as e:
            logging.error('mexc format error: %s', e)

    def mathOrbderBook(self, ob: dict):
        symbolsBook = {'bids': [], 'asks': []}
        symbolOrderBook = ob  
        for z in symbolOrderBook.get('bids', []):
            bidsContainer = []
            price = float(z[0])
            amount = float(z[1])
            bidsContainer.append(price)
            bidsContainer.append(amount)
            symbolsBook['bids'].append(bidsContainer)
        for z in symbolOrderBook.get('asks', []):
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

    async def fetchOrderBook(self, symbol, session):
        try:
            ob = {}
            self.APIURL = 'https://api.mexc.com'
            prefix = '/api/v3/depth'
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            query_param = 'symbol=' + symbol + '&limit=100'
            url = self.APIURL + prefix + "?" + query_param
            r = await session.request('GET', url=url, headers=headers)
            if r.status == 200:
                try:
                    ob = await r.json()
                except Exception as e:
                    ob = await r.json(content_type='text/html')
                return self.mathOrbderBook(ob)
            elif r.status == 429:
                logging.info('Too many requests for symbol %s', symbol)
                await session.close()
                session = aiohttp.ClientSession(
                    connector=aiohttp_socks.ProxyConnector.from_url(
                        Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur']) - 1)]
                    )
                )
                return await self.fetchOrderBook(symbol, session)
        except aiohttp.ClientConnectorError:
            logging.error('MEXC: proxy error for symbol %s', symbol)
            await session.close()
            session = aiohttp.ClientSession(
                connector=aiohttp_socks.ProxyConnector.from_url(
                    Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur']) - 1)]
                )
            )
            return await self.fetchOrderBook(symbol, session)
        except Exception as e:
            logging.error("Fetch order book MEXC err: %s", e)

    async def tickers(self, sem):
        res = {}
        session = aiohttp.ClientSession(
                connector=aiohttp_socks.ProxyConnector.from_url(
                    Constants.proxies['cur'][random.randint(0, len(Constants.proxies['cur']) - 1)]
                )
            )
        try:
            prefix = '/api/v3/ticker/bookTicker'
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            query_param = ''

            url = self.APIURL + prefix + "?" + query_param
            
            r = await session.request('GET', url=url, headers=headers)
            try:
                res = await r.json()
            except Exception as e:
                print('Error res Mexc:', r)
                res = await r.json(content_type='text/html')
        except Exception as e:
            print('MEXC tickers error: %s', e)
        finally:
            if session is not None:
                await session.close()
        return res

    async def reformTickers(self, ticker):
        try:
            if ticker.get('symbol', '')[-4:] == "USDT":
                self.alltickers[ticker['symbol']] = {'ask': '', 'bid': ''}
                self.alltickers[ticker['symbol']]['ask'] = ticker.get('askPrice', '')
                self.alltickers[ticker['symbol']]['bid'] = ticker.get('bidPrice', '')
        except Exception as e:
            print("Reform tickers error MEXC: %s", e)

    async def fetchTickers(self, sem):
        try:
            tickers = await self.tickers(sem)
            await asyncio.gather(*[self.reformTickers(ticker) for ticker in tickers])
            return self.alltickers
        except Exception as e:
            print("Fetch tickers MEXC error: %s", e)

    async def symbols(self, sem):
        res = {}
        session2 = None
        try:
            prefix = '/api/v3/exchangeInfo'
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            query_param = ''
            async with sem:
                connector = aiohttp_socks.ProxyConnector.from_url(
                    Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20']) - 1)]
                )
                session2 = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
                url = self.APIURL + prefix + "?" + query_param
                r = await session2.request('GET', url=url, headers=headers)
                try:
                    res = await r.json()
                except Exception as e:
                    res = await r.json(content_type='text/html')
        except Exception as e:
            logging.error('MEXC symbols error: %s', e)
        finally:
            if session2 is not None:
                await session2.close()
        return res

    async def send_req_books(self, symbol):
        res = {}
        session = None
        connector = None
        try:
            host = 'https://api.mexc.com'
            prefix = '/api/v3/depth'
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            query_param = 'symbol=' + str(symbol[:-5]) + 'USDT' + '&limit=100'
            connector = aiohttp_socks.ProxyConnector.from_url(
                Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20']) - 1)]
            )
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=connector)
            url = host + prefix + "?" + query_param
            r = await session.request('GET', url=url, headers=headers)
            try:
                res = await r.json()
            except Exception as e:
                res = await r.json(content_type='text/html')
            print(res, symbol)
        except Exception as e:
            print(f'Mexc send_req: {e}')
        finally:
            if session is not None:
                await session.close()
            if connector is not None:
                await connector.close()
        return res
    
    async def send_req(self, symbol, sem):
        res = {}
        session2 = None
        try:
            host = 'https://api.mexc.com'
            prefix = '/api/v3/depth'
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            query_param = 'symbol=' + str(symbol[:-5]) + 'USDT' + '&limit=100'
            async with sem:
                session2 = aiohttp.ClientSession()
                url = host + prefix + "?" + query_param
                r = await session2.request('GET', url=url, headers=headers)
                try:
                    res = await r.json()
                except Exception as e:
                    res = await r.json(content_type='text/html')
        except Exception as e:
            logging.error('MEXC send_req error: %s', e)
        finally:
            if session2 is not None:
                await session2.close()
        return res

    async def forEverySymbolCurr(self, symbol, allSymbolsCurrency):
        try:
            pairInfo = {'networks': {}}
            for i in allSymbolsCurrency:
                if symbol[:-4] == i.get('coin', ''):
                    for n in i.get("networkList", []):
                        netDict = self.netWorksId.get(n.get('network'), n.get('network'))
                        pairInfo['networks'][netDict.upper()] = {
                            'withdraw': n.get('withdrawEnable'),
                            'deposit': n.get('depositEnable'),
                            'fee': float(n.get('withdrawFee', 0)),
                            'contract': n.get('contract'),
                            'adminFee': 0
                        }
            self.globalCurrency[symbol] = pairInfo
        except Exception as e:
            logging.warning('MEXC forEverySymbolCurr error: %s', e)

    async def currency(self, symbols, sem):
        try:
            params = {
                'timestamp': int(time.time() * 1000),
                'recvWindow': '50000'
            }
            query_string = urllib.parse.urlencode(sorted(params.items()))
            signature = hmac.new(self.APISECRET.encode('utf-8'), query_string.encode('utf-8'), sha256).hexdigest()
            query_string += "&signature=" + signature
            headers = {
                'X-MEXC-APIKEY': self.APIKEY,
            }
            allSymbolsCurrency = []
            currency_ = await self.send_request(query_string, '/api/v3/capital/config/getall', api=headers)
            if currency_ is not None:
                allSymbolsCurrency = currency_
            else:
                return
            await asyncio.gather(*[self.forEverySymbolCurr(symbol, allSymbolsCurrency) for symbol in symbols])
            self.check_null_nets()
            await self.mongoload.loadInToMongoCurrency(exName=self.name, currency=self.globalCurrency)
        except Exception as e:
            # Раньше здесь стоял self.currency(...) — ретрай через рекурсию без
            # ограничений. Эндпоинт /capital/config/getall у MEXC ПРИВАТНЫЙ и
            # требует APIKEY/APISECRET, а они в коде не заданы вовсе, поэтому
            # каждая попытка падала и рекурсия упиралась в предел стека
            # («maximum recursion depth exceeded»). Это исключение всплывало из
            # CurrencyLoader.currency() и рвало загрузку валют ДЛЯ ВСЕХ бирж.
            # Теперь ошибка просто логируется и не мешает остальным.
            logging.error('Raised error MEXC currency: %s (пропускаем биржу в этом цикле)', e)

    def check_null_nets(self):
        copied_dict = self.globalCurrency.copy()
        for pair, value in copied_dict.items():
            if value.get('networks', {}) == {}:
                del self.globalCurrency[pair]

    async def send_request(self, query_param, prefix, api=""):
        try:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/86.0.4240.75 Safari/537.36'
            }
            if api is not None:
                headers.update(api)
            connector = aiohttp_socks.ProxyConnector.from_url(
                Constants.proxies['books20'][random.randint(0, len(Constants.proxies['books20']) - 1)]
            )
            async with aiohttp.ClientSession() as session:
                url = self.APIURL + prefix + "?" + query_param
                r = await session.request('GET', url=url, headers=headers)
                if r.status == 200:
                    try:
                        res = await r.json()
                    except Exception as e:
                        res = await r.json(content_type='text/html')
                else:
                    logging.error('MEXC send_request error: %s', await r.text())
                    return None
        except Exception as e:
            logging.error('MEXC send_request error: %s', e)
            return None
        return res
