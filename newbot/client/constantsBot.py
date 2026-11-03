import motor.motor_asyncio

class Constants:

    def __init__(self) -> None:
        pass

    links={
        'Gate':'https://www.gate.io/ru/trade/',
        'Kucoin':'https://www.kucoin.com/ru/trade/',
        'MEXC Global':'https://www.mexc.com/ru-RU/exchange/',
        'Bybit':'https://www.bybit.com/ru-RU/trade/spot/',
        'OKX':'https://www.okx.com/ru/trade-spot/',
        'Bitget':'https://www.bitget.com/ru/spot/',
        'BingX':'https://bingx.com/ru-RU/spot/',
        'HTX': 'https://www.htx.com/trade/', 
        'CoinEx': 'https://www.coinex.com/ru/exchange/',
        'Bitmart':'https://www.bitmart.com/trade/ru-RU?symbol='
    }
    
    chains_scan = {'ERC20':'https://etherscan.io/token/',
                   'BEP20':'https://bscscan.com/token/',
                   'TRC20':'https://tronscan.org/#/token20/',
                   'POLYGON':'https://polygonscan.com/token/',
                   'TON':'https://tonscan.org/jetton/',
                   'BASE':'https://basescan.org/token/',
                   'SOL':'https://solscan.io/token/'}
    cmc_link = 'https://coinmarketcap.com/ru/currencies/'
    CONNECTION_STRING = "mongodb://mongo:27017"
    client = motor.motor_asyncio.AsyncIOMotorClient(CONNECTION_STRING, serverSelectionTimeoutMS=10000)
    dbOrderBooks = client.orderbooks.orderbooks
    dbMathPairs = client.arbitrage.data
    dbYellow = client.arbitrage.yellowList
    dbBlack = client.arbitrage.blackList
    db = client.userdata.user
    cmc = client.renderBase.cmc
    yellowTokens = ['XETA/USDT', 'ZKPEPE/USDT', 'ZK/USDT', 'NOT/USDT', 'TON/USDT', 'TRUMP/USDT', 'TAMA/USDT', 'PBR/USDT', 'ARRR/USDT', 'AURA/USDT', 'VMPX/USDT', 'AGI/USDT', 'MAN/USDT', 'MERL/USDT', 'LSK/USDT', 'LUNC/USDT', 'KMD/USDT', 'MAS/USDT', 'BDX/USDT']
    blackTokens = []
    spotFees={'BingX':{'maker':0.1,'taker':0.1},'Bitmart':{'maker':0.6,'taker':0.6},'Bybit':{'maker':0.1,'taker':0.1},'Bitget':{'maker':0.08,'taker':0.08},'HTX':{'maker':0.2,'taker':0.2},'CoinEx':{'maker':0.2,'taker':0.2},'OKX':{'maker':0.14,'taker':0.23},'Gate':{'maker':0.1,'taker':0.1},'MEXC Global':{'maker':0,'taker':0.1},'Kucoin':{'maker':0.1,'taker':0.1}}

    