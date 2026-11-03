

class Exchange:
    def __init__(self) -> None:
        pass

    name = ''
    APIKEY = ''
    SECRETKEY = ''
    APIURL = ''


class Constants:

    def __init__(self) -> None:
        pass
    
    proxies ={'books20': [None], 'cur': [None]}

    class BingX(Exchange):
        
        def __init__(self) -> None:
            pass

        name = 'BingX'
        APIURL = "https://open-api.bingx.com"
        APIKEY = ""
        SECRETKEY = ""

    class Bitget(Exchange):
        
        def __init__(self) -> None:
            pass

        name = 'Bitget'
        APIURL = 'https://api.bitget.com/api'
        APIKEY = ""
        SECRETKEY = ""
    
    class Bybit(Exchange):
        
        def __init__(self) -> None:
            pass

        name = 'Bybit'

        APIURL = "https://api.bybit.com"
        APIKEY = ""
        SECRETKEY = ""

    class CoinEx(Exchange):
        
        def __init__(self) -> None:
            pass

        name = 'CoinEx'

        APIURL = "https://api.coinex.com"
        APIKEY = ""
        SECRETKEY = ""

    class Gate(Exchange):
        
        def __init__(self) -> None:
            pass
        
        name = 'Gate'

        APIURL = "https://api.gateio.ws"
        APIKEY = ''
        APISECRET = ''

    class HTX(Exchange):
        
        def __init__(self) -> None:
            pass
        
        name = 'HTX'

        APIURL = "https://api.huobi.pro"
        APIKEY = ""
        SECRETKEY = ""

    class KuCoin(Exchange):
        
        def __init__(self) -> None:
            pass
        
        name = 'Kucoin'

        APIURL = "https://api.kucoin.com"
        APIKEY = ""
        SECRETKEY = ""

    class Mexc(Exchange):
        
        def __init__(self) -> None:
            pass
        
        name = 'MEXC Global'

        APIURL = 'https://api.mexc.com'
        APIKEY = ''
        APISECRET = ''

    class OKX(Exchange):
        
        def __init__(self) -> None:
            pass
        
        name = 'OKX'

        APIURL = 'https://www.okx.com'
        APIKEY=''
        APISECRET =''
        PASSPHRASE = ''

    class Bitmart(Exchange):
        
        def __init__(self) -> None:
            pass

        name = 'Bitmart'
        APIURL = 'https://api-cloud.bitmart.com'
        APIKEY = ""
        SECRETKEY = ""




    
