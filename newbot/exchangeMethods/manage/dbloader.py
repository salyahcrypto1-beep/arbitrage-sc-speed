from asyncio import run, gather
import asyncio
import logging
import motor.motor_asyncio
import diskcache as dc

cache = dc.Cache('../exchange/cache')

CONNECTION_STRING = "mongodb://mongo:27017"
client = motor.motor_asyncio.AsyncIOMotorClient(CONNECTION_STRING)


from logging.handlers import RotatingFileHandler
logging.basicConfig(level=logging.WARNING, handlers=[RotatingFileHandler('dbloader.log', maxBytes=10_000_000, backupCount=2)])

class Mongoloader:
    def __init__(self):
        self.token = ''
        self.tokensFolder = {'Gate':[],
                    'MEXC Global':[],
                    'Bitget':[],
                    'Bybit':[],
                    'BingX':[],
                    'Kucoin':[],
                    'OKX':[],
                    'HTX':[],
                    'CoinEx':[],
                    'Bitmart':[]}
        self.loaderMarkets = client.renderBase.tradePairs
        self.loaderCurrency = client.renderBase.pairsCurrency
        self.AllSymbolsData = client.renderBase.AllSymbolsData
        self.allSymbolsData = {}
        self.new_asd_count = []
        self.edited_count = []
    
    chainPool = {
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'CC': ['$CC'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'MOVA': ['MOVA MAINNET'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'DATA': ['DATA NETWORK'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'APE': ['APECHAIN'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'ASSETHUBKUSAMA': ['ASSET HUB KUSAMA'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'ABCORE': ['AB CORE'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'PHAROS': ['PHAROS MAINNET'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'ZIG': ['ZIGCHAIN'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'WORLDCHAIN': ['WORLD CHAIN'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'XLAYER': ['X_LAYER'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'MEGAETH': ['MEGA-ETH'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'CANTON': ['CANTON NETWORK'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'STABLE': ['STABLECHAIN'],
        # добавлено: одна и та же сеть называлась по-разному на разных биржах
        'BERA': ['BERACHAIN'],
    'ERC20':['ETRC20','ERC20TPY','ETH'],
    'AMB':['AMBROSUS'],
    'SMART':['SMART BLOCKCHAIN'],
    'MTR':['MTRG', 'METER'],
    'CRO':['CRO_CHAIN'],
'XPR':['PROTON'],
'XRD':['RADIX'],
'WAXL':['AXELAR'],
'IOTX':['IOTEX'],
'FRA':['FRAEVM'],
'GT':['GATECHAIN'],
'OPTIMISM':['OPTIMISM(BRIDGED)','OP'],
'LIGHTNING':['LIGHTNING NETWORK','LIGHTNING_BTC','BTCLN'],

'DFI':['DEFI'],
'ZEN':['HORIZEN'],
'ROUTE':['ROUTERPROTOCOL'],
'METAL':['METAL L2','METALDAOL2','MTL'],
'CFX':['CFX ESPACE'],
'DOG20':['DC'],

    'AR':['ARWEAVE','AREA','ARK'],
    'KDA':['KDA0'],
    'JOY':['JOYSTREAM'],
    'MEV':['MEVERSE'],
    'POLYGON': ['MATIC','POLYGON(BRIDGED)','POLYGON(CIRCLE)', 'POLYGON POS'],
    'BEP20': ['BSC','BSC_BNB'],
    'ARBITRUM': ['ARBI','ARB','ARBITRUMONE','ARBEVM','ARBONE','ARBITRUMONE(BRIDGED)','ARBITRUMONE(CIRCLE)'],
    'AVAX-X': ['XAVAX','AVAXX','AVAX','X-CHAIN','AVA','AVAXX-CHAIN'],
    'CRO':['CRONOSCHAIN','CRONOS'],
    'AVAX-C': ['AVAX CCHAIN','AVAX_CCHAIN','CAVAX','AVAX C-CHAIN','AVAXC','C-CHAIN','AVAXCCHAIN','AVA_C','CCHAIN', 'AVAXC-CHAIN'],
    'XRP': ['XRPTOKEN'],
    'ABBC': ['ABBCCOIN'],
    'TRC20': ['TRX'],
    'POLKADOT': ['DOT'],
    'ICX': ['ICON'],
    'CTXC':['CORTEX'],
'ENJ':['ENJIN RELAY CHAIN','ENJIN'],
'FTN':['BAHAMUT'],
'KCALP':['PHANTASMA'],
'ISLM':['HAQQ','ISLMEVM'],
    'BEP2': ['BNB'],
    'ONE': ['HARMONY'],
    'KLAY': ['KLAYTN'],
    'CSPR': ['CASPER'],
    'CLV':['CLVEVM'],
    'TELOS':['TELOSZERO'],
    'BDX':['BELDEX'],
    'HMND':['EHMND'],
    'BNC' : ['BNCDOT','BNCPOLKA','BNCKSM','BIFROST'],
    'LUNA(TERRA)':['LUNANEW','TERRA','LUNA','LUNC','TERRA_CLASSIC','TERRA CLASSIC'],
    'RON':['RONIN'],
    'METIS': ['METIS (TOKEN TRANSFER)','METISTOKEN'],
    'WEMIX':['WEMIXMAINNET'],
    'ETHW':['ETHEREUMPOW','ETHPOW','ETHEREUM POW'],
    'ZKFAIR':['ZKFUSDC'],
    'VIC':['VICTION'],
    'SOL':['SOLANA'],
    'ROOT':['THE ROOT NETWORK'],
    'CHILIZ':['CHZ','CHILIZ CHAIN','CHILIZ_CHZ'],
    'CHZ2':['CHILIZ 2.0 CHAIN'],
    'NEAR':['NEARPROTOCOL'],
    'IOST':['IOSTNEW'],
    'FIL':['FILEVM','FEVM','FIL-0X'],
    'DC':['DOGEEVM','DOGECHAIN'],
    'INJ':['INJECTIVE'],
    'SXP':['SOLAR'],
    'LYX':['LUKSO'],
    'OSMO':['OSMOSIS'],
    'OAS':['OASYS'],
    'NEOX':['NEOXA'],
    'ETN':['ETN_SC'],
    'BLD':['AGORIC'],
    'AKT':['AKASH'],
    'ERG':['ERGO'],
    'IRON':['IRONFISH'],
    'BASE':['BASEEVM','BASE-ETH'],
    'XETA':['XENA','XANA'],
    'ARC20':['ALGOOPUL','ARC20OPUL','OPUL'],
    'LAI':['LAYERAI'],
    'NEX':['NEXA'],
    'VANAR':['VANRY','VANARCHAIN'],
    'ZKFAIR':['ZKFAIR-USDC'],
    'INTR':['INTERLAY'],
    'NGL':['NIGELLA'],
    'ZK LINK':['ZKLINK','ZKLINK NOVA','ZKLETH','ZKLINK-ETH'],
    'ZETA':['ZETACHAIN','ZETAEVM'],
    'KAVA':['KAVAEVM','KAVA EVM CO-CHAIN','KAVAEVMTOKEN'],
    'SIS':['ARC20SIS','BEP20SIS'],
    'ONT':['ONG','ONTOLOGY'],
    'NEW':['NEWTON'],
    'BAJU':['BAJUN'],
    'MODE':['MODEETH'],
    'ARCH':['ARCHWAY'],
    'ML':['MINTLAYER'],
    'TIA':['CELESTIA'],
    'STEP':['STEP NETWORK'],
    'KUJI':['KUJIRA'],
    'MERLIN':['MERLBTC','MERLIN NETWORK','MERLIN-BTC'],
    'EVER':['EVER'],
    'HMND':['HUMANODE'],
    'MANTLE':['MNT'],
    'POLYX':['POLYMESH'],
    'ZKSYNC':['ZKSYNCERA','ZKSYNC ERA','ZKSERA', 'ZKSYNCERA_ETH'],
    'EVMOS':['EVMOSETH'],
    'XEM':['NEM'],
    'MANTA':['MANTANETWORK','MANTA NETWORK','MANTAATLANTIC','MANTAETH','MANTADOT'],
    'KAS':['KASPA'],
    'ZKLINK NOVA':['ZKLINKNOVA'],
'BLAST':['BLASTETH','BLAST-ETH'],
'BB':['BOUNCEBIT'],
'AVAIL':['AVL'],
'SLF':['SELF'],
'SMH':['SPACEMESH'],
'STEEM':['STEEMOLD'],
'CORE':['CORE DAO'],
'OMAX':['OMAX CHAIN'],
    'NOM':['ONOMY'],
    'NEO3':['NEON3','N3','NEO','GAS'],
    'SC':['SCRT'],
    'NODL':['NODLE'],
    'DYM':['DYMENSION','DYMEVM'],
    'POKT':['POCKETNETWORK'],
    'EOS':['EOSEVM','EOSIO'],
    'ADA':['CARDANO'],
    'STRAX':['STRATISEVM'],
    'EGLD':['ELROND'],
    'ELA':['ESC','ELAEVM','HRC20ELA'],
    'VLX':['VLX(EVM)'],
    'NTRN':['NEUTRON'],
    'NIBI':['NIBIRU'],
    'APT':['APTOS'],
    'XLM':['XLMVELO'],
    'TENET':['TENETEVM'],
    'LYX':['LUKSO'],
    'BOBA':['BOBAEVM'],
    'SYS':['SYSCOINNEVM','SYSCOIN UTXO','SYSNEVM','SYSCOIN ROLLUX'],
    'SDN':['SDNEVM'],
    'KSM':['KUSAMA'],
    'ARBNOVA':['ARBITRUM_NOVA','ARBINOVA'],
    'CANTO':['CANTOEVM','CANTO-EVM','CANTO-COSMOS'],
    'FTM':['FANTOM'],
    'ACA':['ACALA','ACALATOKEN'],
    'FLR':['FLARE'],
    'GLMR':['MOONBEAM','GLRM'],
    'RUNE':['THORCHAIN'],
    'STX':['STACKS'],
    'ROSE':['OASIS'],
    'TAIKO':['TAIKOETH','TAIKO-ETH'],
    'AURORA':['AURORAEVM'],
    'VET':['VECHAIN'],
    'XLM':['STELLA','STELLARLUMENS'],
    'APT':['APTOS'],
    'EWT':['ENERGY WEB CHAIN(EWC)','EWC'],
    'CFX':['CONFLUX','CFXCORE','CFXEVM','CFX_EVM','CFXESPACE','CFXE'],
    'TELOS':['TLOS','TELOS(EVM)','TLOSEVM'],
    'STARKNET':['STARK','STRKETH', 'STARK-ETH'],
    'RSK':['RBTC'],
    'BTC':['BTCBRC'],
    'RUNES':['RUNES-BTC','BTCRUNES'],
    'THETA':['THETATOKEN'],
    'QTUM':['QUANTUM'],
    'SEI':['SEIEVM'],
    'BCH':['BCHN','BCHA'],
    'XDC':['XDCNETWORKXDC'],
    'WAX':['WAXP','WAX1'],
    'NULS':['NULSEVM'],
    'ATOM':['COSMOS','ATOM1'],
    'LINEA':['LINEAETH','LINEA-ETH'],
    'ASTR':['ASTREVM','ASTAR'],
    'OMN':['OMEGA CHAIN','OMEGA'],
    'FNSA':['FINSCHIA'],
    '3ULL':['PLAYA3ULL'],
    'VSC':['VYVOCOIN'],
    'MTC':['METATIME','METABIT'],
    'DIS':['DISCHAIN'],
    'GMMT':['GIANTMAMMOTH'],
    'ONT':['ONG','ONTOLOGY'],
    'CORE':['COREDAO','COREUM'],
    'LSK':['LISK'],
'PHA':['PHAKSM','KHALA'],
'ETHF':['ETHFAIR'],
'MPC':['PARTISIA'],
'NEER':['PIONEER'],
'PSL':['PASTEL'],
'ACT':['ACT(EVM)'],
'PDEX':['POLKADEX'],
'XDB':['XDB CHAIN'],
'CRU':['CRUST'],
'NIM':['NIMIQ'],
'TAO':['BITTENSOR'],
'KLV':['KLEVER'],
'XEC':['ECASH'],
'XYM':['SYMBOL'],
}

    currencyFolder = {'Gate':[],
                    'MEXC Global':[],
                    'Bitget':[],
                    'Bybit':[],
                    'BingX':[],
                    'Kucoin':[],
                    'OKX':[],
                    'HTX':[],
                    'CoinEx':[],
                    'Bitmart':[]
                    }
    async def replaceNet(self,exCurr,exName):
        keys = list(exCurr[1]['networks'].keys())
        for net in keys:
            for key in self.chainPool.keys():
                if net in self.chainPool[key]:
                    exCurr[1]['networks'][key] = exCurr[1]['networks'][net].copy()
                    exCurr[1]['networks'].pop(net)
        self.currencyFolder[exName][exCurr[0]]=exCurr[1]

    async def loadFromMongo(self,key):
        self.tokensFolder = {'Gate':[],
                    'MEXC Global':[],
                    'Bitget':[],
                    'Bybit':[],
                    'BingX':[],
                    'Kucoin':[],
                    'OKX':[],
                    'HTX':[],
                    'CoinEx':[],
                    'Bitmart':[]}
        try:
            match key:
                case 'Markets':
                    tokens = await self.loaderMarkets.find().to_list(length=None)
                    for obj in tokens:
                        match obj['id']:
                            case 'MEXC Global':
                                self.tokensFolder['MEXC Global'] = obj['tokens']
                            case 'Gate':
                                self.tokensFolder['Gate'] = obj['tokens']
                            case 'Bitget':
                                self.tokensFolder['Bitget'] = obj['tokens']
                            case 'BingX':
                                self.tokensFolder['BingX'] = obj['tokens']
                            case 'Bybit':
                                self.tokensFolder['Bybit'] = obj['tokens']
                            case 'Kucoin':
                                self.tokensFolder['Kucoin'] = obj['tokens']
                            case 'OKX':
                                self.tokensFolder['OKX'] = obj['tokens']
                            case 'HTX':
                                self.tokensFolder['HTX'] = obj['tokens']
                            case 'CoinEx':
                                self.tokensFolder['CoinEx'] = obj['tokens']
                            case 'Bitmart':
                                self.tokensFolder['Bitmart'] = obj['tokens']
                    returnable = self.tokensFolder.copy()        
                case 'Currency':
                    currency = await self.loaderCurrency.find().to_list(length=None)
                    for obj in currency:
                        match obj['id']:
                            case 'MEXC Global':
                                self.tokensFolder['MEXC Global'] = obj['currency']
                            case 'Gate':
                                self.tokensFolder['Gate'] = obj['currency']
                            case 'Bitget':
                                self.tokensFolder['Bitget'] = obj['currency']
                            case 'BingX':
                                self.tokensFolder['BingX'] = obj['currency']
                            case 'Bybit':
                                self.tokensFolder['Bybit'] = obj['currency']
                            case 'Kucoin':
                                self.tokensFolder['Kucoin'] = obj['currency']
                            case 'OKX':
                                self.tokensFolder['OKX'] = obj['currency']
                            case 'HTX':
                                self.tokensFolder['HTX'] = obj['currency']
                            case 'CoinEx':
                                self.tokensFolder['CoinEx'] = obj['currency']
                            case 'Bitmart':
                                self.tokensFolder['Bitmart'] = obj['currency']
                    returnable = self.tokensFolder.copy()        
        except:
            logging.info('base is empty')
            returnable = self.tokensFolder
        return returnable
    
    #Загрузка пар
    async def loadInToMongo(self,exName, symbols):
              tokensAvailable = await self.loadFromMongo(key='Markets')
              if len(tokensAvailable[exName])==0:
                check = await self.loaderMarkets.find({'id':exName}).to_list(length=None)
                if len(check)>0:    
                    await self.loaderMarkets.update_one({'id':exName},
                            {'$set':{'tokens':symbols}})
                else:
                    obj= {'id':exName,
                        'tokens':symbols}
                    await self.loaderMarkets.insert_one(obj)
              else:
                known = set(tokensAvailable[exName])
                for pair in symbols:
                     if pair not in known:
                         self.loaderMarkets.update_one({"id":exName},{"$push":{"tokens":pair}})

    # async def loadAllSymbolsData(self, token, exName, currencies):
    #     try:
    #         quote = ""
    #         base = ""
    #         if '/' in token:
    #             base, quote = token.upper().split('/')
    #         elif '-' in token:
    #             base, quote = token.upper().split('-')
    #         elif '_' in token:
    #             base, quote = token.upper().split('_')
    #         else:
    #             # Обработка случая, когда нет разделителя
    #             quote = token[-4:].upper()
    #             base = token[:-4].upper()
    #         existing_data = await self.AllSymbolsData.find_one({'symbol': base+'/'+quote})
    #         if existing_data:
    #             await self.AllSymbolsData.update_one(
    #                 {'symbol': base+'/'+quote},
    #                 {'$set': {f'currency.{exName}': {'networks': currencies[token]['networks']}}}
    #             )
    #             logging.info(f'AllSymbolsData was updated for {token}')
    #         else:
    #             # Если документ не существует, создаем новый
    #             await self.AllSymbolsData.insert_one({
    #                 'symbol': base+'/'+quote,
    #                 'currency': {
    #                     exName: {
    #                         'networks': currencies[token]['networks']
    #                     }
    #                 }
    #             })
    #             logging.info(f'Added new AllSymbolsData for {token}')
    #     except Exception as e:
    #         logging.error(f'Error raised while AllSymbolsData was updated with {token} {exName}:{e}')

    async def target_update_asd(self,snapshot,new):
        if isinstance(snapshot, dict):
            pair_to_upd = snapshot.get(new['symbol'], {})
        else:
            pair_to_upd = {}
            for snap_asd in snapshot:
                if snap_asd['symbol']==new['symbol']:
                    pair_to_upd=snap_asd
                    break

        if pair_to_upd=={}:
            await self.AllSymbolsData.insert_one(new)
            self.new_asd_count.append(new['symbol'])
        else:
            if pair_to_upd.get('currency') == new['currency']:
                self.unchanged_count += 1
                return
            await self.AllSymbolsData.update_one(
                    {'symbol': new['symbol']},{'$set': {'currency':new['currency']}}
                )
            self.edited_count.append(new['symbol'])



    async def collect_all_symbols_data(self):
        # Получаем текущие данные и валюты
        snapshot = await self.AllSymbolsData.find().to_list(length=None)
        currencies = await self.loaderCurrency.find().to_list(length=None)
        # Если валюты пустые, не обновляем данные
        if not currencies:
            logging.info(f'AllSymbolsData no needs to update. Currencies is None')
            return
        
        all_symbols_data = {}
        end_obj_arr = []

        # Обработка валют асинхронно
        async def process_currency(ex):
            nonlocal all_symbols_data  # Для доступа к переменной из внешней функции
            for pair, values in ex['currency'].items():
                symbol_formatted = {
                    'OKX': pair.replace('-', '/'),
                    'Kucoin': pair.replace('-', '/'),
                    'Gate': pair.replace('_', '/'),
                    'MEXC Global': pair[:-4] + '/USDT',
                    'Bitget': pair[:-4] + '/USDT',
                    'Bitmart': pair.replace('_', '/'),
                    'Bybit': pair[:-4] + '/USDT',
                    'BingX': pair.replace('-', '/'),
                    'HTX': str(pair[:-4] + '/USDT').upper(),
                    'CoinEx': pair[:-4] + '/USDT'
                }.get(ex['id'], pair)
                
                # Создаем новый символ, если он не существует
                if symbol_formatted not in all_symbols_data:
                    all_symbols_data[symbol_formatted] = {'currency': {}}

                all_symbols_data[symbol_formatted]['currency'][ex['id']] = {'networks': {}}
                
                # Копируем значения сети
                for net, value in values.items():
                    all_symbols_data[symbol_formatted]['currency'][ex['id']][net] = value.copy()
            
        

        # Обработка всех валют параллельно
        await asyncio.gather(*[process_currency(ex) for ex in currencies])

        # Формирование списка для вставки в базу
        for pair, data in all_symbols_data.items():
            add_dict = data.copy()
            add_dict['symbol'] = pair
            end_obj_arr.append(add_dict)

        if not snapshot:
            # Вставка новых данных
            await self.AllSymbolsData.insert_many(end_obj_arr)
            logging.info(f'AllSymbolsData was created with {len(end_obj_arr)} symbols')
        else:
            self.edited_count = []
            self.new_asd_count = []
            self.unchanged_count = 0

            # 02.10: индекс «символ → первый документ» (как и раньше брался первый найденный);
            # у каждого символа одна операция, порядок между символами на результат не влияет
            index = {}
            for snap_asd in snapshot:
                index.setdefault(snap_asd['symbol'], snap_asd)
            for new_data in end_obj_arr:
                await self.target_update_asd(index, new_data)

            logging.info(f'AllSymbolsData was updated.\nNew added {len(self.new_asd_count)}: {self.new_asd_count}\nUpdated {len(self.edited_count)}')
  
    """ async def collect_all_symbols_data(self):
        # Получаем текущие данные и валюты
        snapshot = await self.AllSymbolsData.find().to_list(length=None)
        currencies = await self.loaderCurrency.find().to_list(length=None)
        
        # Если валюты пустые, не обновляем данные
        if not currencies:
            logging.info('AllSymbolsData no needs to update. Currencies is None')
            return
        
        all_symbols_data = {}
        end_obj_arr = []

        # Обработка валют асинхронно
        async def process_currency(ex):
            for pair, values in ex['currency'].items():
                symbol_formatted = {
                    'OKX': pair.replace('-', '/'),
                    'Kucoin': pair.replace('-', '/'),
                    'Gate': pair.replace('_', '/'),
                    'MEXC Global': pair[:-4] + '/USDT',
                    'Bitget': pair[:-4] + '/USDT',
                    'Bitmart': pair.replace('_', '/'),
                    'Bybit': pair[:-4] + '/USDT',
                    'BingX': pair.replace('-', '/'),
                    'HTX': str(pair[:-4] + '/USDT').upper(),
                    'CoinEx': pair[:-4] + '/USDT'
                }.get(ex['id'], pair)

                if symbol_formatted not in all_symbols_data:
                    all_symbols_data[symbol_formatted] = {'currency': {}}

                all_symbols_data[symbol_formatted]['currency'][ex['id']] = {'networks': {}}

                for net, value in values.items():
                    all_symbols_data[symbol_formatted]['currency'][ex['id']][net] = value.copy()

        # Обработка всех валют параллельно
        await asyncio.gather(*[process_currency(ex) for ex in currencies])

        # Формирование списка для вставки в базу
        for pair, data in all_symbols_data.items():
            add_dict = data.copy()
            add_dict['symbol'] = pair
            end_obj_arr.append(add_dict)

        # Подсчет количества символов в данных
        snapshot_size = sum(len(str(item)) for item in snapshot)
        new_data_size = sum(len(str(item)) for item in end_obj_arr)

        # Проверка на наличие CARV/USDT в базе данных
        carv_data = next((item for item in snapshot if item.get('symbol') == 'CARV/USDT'), None)
        if carv_data:
            print('Existing CARV/USDT data:', carv_data)

        # Если новый список не меньше старого по количеству символов, заменяем все данные
        if new_data_size >= snapshot_size:
            # Удаление дублированных записей перед вставкой
            for entry in end_obj_arr:
                # Для каждой записи проверяем, есть ли такая запись в базе
                existing_record = await self.AllSymbolsData.find_one({"symbol": entry['symbol']})
                if existing_record:
                    # Если запись уже существует, обновляем ее, а не добавляем новую
                    await self.AllSymbolsData.update_one(
                        {"symbol": entry['symbol']},
                        {"$set": entry}
                    )
                    logging.info(f"Updated existing symbol {entry['symbol']}")
                else:
                    # Вставляем новую запись
                    await self.AllSymbolsData.insert_one(entry)
                    logging.info(f"Inserted new symbol {entry['symbol']}")

            logging.info(f'AllSymbolsData was processed with {len(end_obj_arr)} symbols')
        else:
            logging.info('AllSymbolsData update skipped: new dataset is smaller than the existing one.') """


    #Загрузка currency               
    async def currencyUpdater(self,exName,currency,snap,token):
        if token in snap.keys():
            res = all((currency[token].get(k) == v for k, v in snap[token].items()))
            if not res and not currency[token]['networks']=={}:
                formated = 'currency.'+token
                curr = currency[token]
                await self.loaderCurrency.update_one(
                    {'id': exName},
                    {'$set': {formated: curr}}
                )
        else:
            formated = 'currency.'+token
            curr = currency[token]
            await self.loaderCurrency.update_one(
                {'id': exName},
                {'$set': {formated: curr}}
            )          


    async def snapChecker(self,exName, snap,currency):
        workerDoc = {}
        for finder in snap:
            if finder['id']==exName:
                workerDoc = finder['currency']
                break
        await gather(*[self.currencyUpdater(exName=exName,currency=currency,snap=workerDoc,token=token) for token in currency])
        


    #Загрузка currency               
    async def loadInToMongoCurrency(self, exName, currency):
        print('start loadInToMongoCurrency')
        if currency=={}:
            print('loadInToMongoCurrency is Empty')
            return
        try:
            # Попытка найти существующий документ по exName
            try:
                self.currencyFolder[exName] = currency
                await gather(*[self.replaceNet(currencySingle,exName) for currencySingle in currency.items()])
                currency = self.currencyFolder[exName]
            except Exception:
                currency = currency
                
            existing_doc = await self.loaderCurrency.find_one({"id": exName})
            print(exName)
            if existing_doc:
                if exName == 'Bitmart':
                    print('loadInToMongoCurrency Bitmart Exist',currency)
                # Если документ уже существует, обновляем его
                if currency:
                    snap = await self.loaderCurrency.find().to_list(length=None)
                    await self.snapChecker(exName,snap,currency)   #work   
                    # await asyncio.gather(*[self.loadAllSymbolsData(token, exName, currency) for token in currency])        
                    logging.info(f"Currency updated success: {exName}")
                else:
                    logging.info(f"Currency not updated: {exName}, no new currencies")
            else:
                # Если документ не существует, создаем новый
                if exName == 'Bitmart':
                    print('loadInToMongoCurrency Bitmart not Exist',currency)
                obj = {"id": exName, "currency": currency}
                await self.loaderCurrency.insert_one(obj)
                logging.info(f"Currency added success: {exName}")
        except Exception as e:
            logging.warning(f"Currency operation error: {exName}, {e}")
