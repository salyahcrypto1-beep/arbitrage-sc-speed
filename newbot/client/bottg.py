from portable.access import is_owner, pair_user
from datetime import datetime,timedelta
from telebot import asyncio_filters
import telebot.async_telebot as telebot
from telebot.async_telebot import types
from telebot.asyncio_storage import StateMemoryStorage
from telebot.asyncio_handler_backends import State, StatesGroup
import sys
import os
import aiohttp
sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
from constantsBot import Constants
#REMOVED_TELEGRAM_TOKEN -- scamBot для теста
#REMOVED_TELEGRAM_TOKEN -- основной новый 
import re
import jsons
import asyncio
from asyncio import gather
import random
import math
import pymongo

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exchanges import exchanges
import gzip
import pickle
import multiprocessing
from pymemcache. client import base

cache = base.Client(
    ('memcached', 11211),
    connect_timeout=5,
    timeout=10,
    no_delay=True,
    ignore_exc=True
)

clientTiers = pymongo.MongoClient('mongo', 27017)



event = asyncio.new_event_loop()
allExchanges = ['OKX', 'Kucoin', 'Gate', 'MEXC Global', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx','Bitmart']
allExchangesReverse = ['Bitmart','CoinEx', 'HTX', 'BingX', 'Bybit', 'Bitget', 'MEXC Global', 'Gate', 'Kucoin', 'OKX']
bot = telebot.AsyncTeleBot(os.environ.get("TELEGRAM_BOT_TOKEN", ""),state_storage=StateMemoryStorage())

time_detect_signals = {}
linkAsk = ''
linkBid = ''
data = list()
 
tgId = []
baseTokens = []

sem = asyncio.Semaphore(1000)
exSessions = {}
cmc_list = []

class MyStates(StatesGroup):
    tokenStateBlack = State()
    netBlack = State()
    tokenStateWhite = State()
    netWhite = State()
    minVolUser = State() 
    maxVolUser = State()
    minSpreadUser = State()
    minProcentUser = State()
    feesUser = State()
    addUser = State()
    delUser = State()
    yellowList = State()
    globalBlackList = State()




async def formatLink(ex,symbol):
    if ex=='Gate' or ex=='MEXC Global' or ex=='Bitmart':   
        linkEnd = ""
        if symbol[-4:] == "USDT":
            linkEnd = symbol.replace('/USDT','_'+'USDT') 
        elif symbol[-4:] == "USDC":
            linkEnd = symbol.replace('/USDC','_'+'USDC')
        return '<a href="'+Constants.links[ex]+linkEnd+'">'+ex+'</a>'
        
    elif ex=='Kucoin' or ex=='OKX' or ex=='CoinEx':
        linkEnd = ""
        if symbol[-4:] == "USDT":
            linkEnd = symbol.replace('/USDT','-'+'USDT')
        elif symbol[-4:] == "USDC":
            linkEnd = symbol.replace('/USDC','-'+'USDC')
        return '<a href="'+Constants.links[ex]+linkEnd+'">'+ex+'</a>'
        
    elif ex=='Bitget' or ex=='BingX':
        linkEnd = ""
        if symbol[-4:] == "USDT":
            linkEnd = symbol.replace('/USDT','USDT')
        elif symbol[-4:] == "USDC":
            linkEnd = symbol.replace('/USDC','USDC')
        return '<a href="'+Constants.links[ex]+linkEnd+'">'+ex+'</a>'
        
    elif ex=='Bybit':
        return '<a href="'+Constants.links[ex]+symbol+'">'+ex+'</a>'

    elif ex=='HTX':
        linkEnd = ""
        if symbol[-4:] == "USDT":
            linkEnd = symbol.replace('/USDT','_usdt').lower()
        elif symbol[-4:] == "USDC":
            linkEnd = symbol.replace('/USDC','_usdc').lower()
        return '<a href="'+Constants.links[ex]+linkEnd+'">'+ex+'</a>'
    

async def formatTokenCount(tokens):
    if len(str(int(float(tokens)))) >= 4 and len(str(int(float(tokens)))) < 7:    
        return str(round((int(float(tokens))/1000), 2)) + 'K'     
    elif len(str(int(float(tokens)))) >= 7 and len(str(int(float(tokens)))) < 10:    
        return str(round((int(float(tokens))/1000000), 2)) + 'M'     
    elif len(str(int(float(tokens)))) >= 10:      
        return str(round((int(float(tokens))/1000000000), 2)) + 'B' 
    else:    
        return float(tokens)

async def updateSignalData(min_exchange, max_exchange, symbol, feeDoll, userId):
    try:
      minExchangeObj = {}
      maxExchangeObj = {}
      for minEx in exchanges:
          if minEx.name == min_exchange:
              minExchangeObj = minEx
              break
      for maxEx in exchanges:
          if maxEx.name == max_exchange:
              maxExchangeObj = maxEx
              break    
              
      symbol_formatted_min = {
          'OKX': symbol.replace('/', '-'),
          'Kucoin': symbol.replace('/', '-'),
          'Gate': symbol.replace('/', '_'),
          'Bitmart': symbol.replace('/', '_'),
          'MEXC Global': symbol.replace('/', ''),
          'Bitget': symbol.replace('/', ''),
          'Bybit': symbol.replace('/', ''),
          'BingX': symbol.replace('/', '-'),
          'HTX': symbol.replace('/', '').lower(),
          'CoinEx': symbol.replace('/', '')
      }[min_exchange]
      
      symbol_formatted_max = {
          'OKX': symbol.replace('/', '-'),
          'Kucoin': symbol.replace('/', '-'),
          'Gate': symbol.replace('/', '_'),
          'Bitmart': symbol.replace('/', '_'),
          'MEXC Global': symbol.replace('/', ''),
          'Bitget': symbol.replace('/', ''),
          'Bybit': symbol.replace('/', ''),
          'BingX': symbol.replace('/', '-'),
          'HTX': symbol.replace('/', '').lower(),
          'CoinEx': symbol.replace('/', '')
      }[max_exchange]
      
      symbolOrderBookMin = {}
      symbolOrderBookMax = {}
      try:   
        async with aiohttp.ClientSession() as session:
          symbolOrderBookMin = await minExchangeObj.fetchOrderBook(symbol_formatted_min, session)
          if symbolOrderBookMin is None or len(symbolOrderBookMin['asks']) == 0:
              print('symbolOrderBookMin None:', min_exchange)
              return None
      except Exception as e:
            print('Orderbook min error:',e)
            return None
      try:
        async with aiohttp.ClientSession() as session:
          symbolOrderBookMax = await maxExchangeObj.fetchOrderBook(symbol_formatted_max, session)
          if symbolOrderBookMax is None or len(symbolOrderBookMax['bids']) == 0:
              print('symbolOrderBookMax None:', max_exchange)
              return None
      except Exception as e:
            print('Orderbook max error:',e)
            return None
      
      summasksVol = 0
      summbidsVol = 0
      summMin = 0
      summMax = 0
  
  
      countOrderAsks = 0
      countOrderBids = 0
      
      finallBuyOrderPrice = 0
      alltokensSell = 0
      alltokensBuy = 0
      firstBuyPrice = 0
      lastBuyPrice = 0
      firstSellPrice = 0
      lastSellPrice = 0
                                             
      user = await Constants.db.find_one({'id':userId})
  
      maxUserVol = user['maxVolume']
      summDollarSpread = 0
      volume = user['minVolume']
      if maxUserVol > 0 and volume > 0:
          while volume <= maxUserVol:
              summAsksVolContainerDollars = 0
              countOrderAsksCont = 0
              ordersArr = []
              while summAsksVolContainerDollars < volume  and countOrderAsksCont < len(symbolOrderBookMin['asks']):
                  summAsksVolContainerDollars = summAsksVolContainerDollars + symbolOrderBookMin['asks'][countOrderAsksCont][0]*symbolOrderBookMin['asks'][countOrderAsksCont][1]
                  ordersArr.append(symbolOrderBookMin['asks'][countOrderAsksCont])
                  countOrderAsksCont = countOrderAsksCont + 1
  
              delOrdersVol = 0 
              delSSum = 0 
              for x in range(0, len(ordersArr)):
                  
                  if x != len(ordersArr)-1:
                      delOrdersVol = delOrdersVol + ordersArr[x][1]
                      delSSum = delSSum + ordersArr[x][1]*ordersArr[x][0]
                  else:    
                      delOrdersVol = delOrdersVol + ((volume-delSSum)/ordersArr[x][0])
              
              srpriceBuy = 0
              if delOrdersVol != 0:
                  srpriceBuy = volume / delOrdersVol 
              
              summbidsVolContainer = 0
              countOrderBidsCont = 0
              ordersArrSell = []
              fee = feeDoll/srpriceBuy
              feeVal = fee if (not fee==0) and not fee is None else 0
              takerBuyFee = 0
              if min_exchange == 'Bitmart':
                tiers = list(clientTiers.renderBase.bitmart_tier.find())
                try:
                    takerBuy = 0
                    
                    if symbol in tiers[0]['A']:                                         
                        takerBuy = 0.1
                    elif symbol in tiers[0]['B']:
                        takerBuy = 0.25
                    elif symbol in tiers[0]['C']:
                        takerBuy = 0.4
                    elif symbol in tiers[0]['D']:
                        takerBuy = 0.6
                    else:
                        takerBuy = 0.25
                        print('Bitmart Else', symbol)

                    takerBuyFee = float(delOrdersVol) * takerBuy/100
                except Exception as e:
                    print("tiers 1", tiers)
              else:
                takerBuyFee = float(delOrdersVol)*Constants.spotFees[min_exchange]['taker']/100

              summSellVolTokens = float(delOrdersVol) - float(feeVal) - float(takerBuyFee)    
  
              while summbidsVolContainer < summSellVolTokens and countOrderBidsCont < len(symbolOrderBookMax['bids']):
                  summbidsVolContainer = summbidsVolContainer + symbolOrderBookMax['bids'][countOrderBidsCont][1]
                  ordersArrSell.append(symbolOrderBookMax['bids'][countOrderBidsCont])
                  countOrderBidsCont = countOrderBidsCont + 1
              
              chisOrdersVolBeforeFee = 0
              chisTSum = 0
              for x in range(0, len(ordersArrSell)):
                  if x != len(ordersArrSell) - 1:
                      chisOrdersVolBeforeFee = chisOrdersVolBeforeFee + ordersArrSell[x][1] * ordersArrSell[x][0]
                      chisTSum = chisTSum + ordersArrSell[x][1]
                  else:
                      chisOrdersVolBeforeFee = chisOrdersVolBeforeFee + ((summSellVolTokens - chisTSum) * ordersArrSell[x][0])
  
              if len(ordersArrSell) > 0: 
                  if not (ordersArrSell[-1][1] >= summSellVolTokens-chisTSum): 
                      break    
              
              takerSellFee = 0
              if max_exchange == 'Bitmart':
                    tiers = list(clientTiers.renderBase.bitmart_tier.find())
                    try:
                        takerSell = 0
                        if symbol in tiers[0]['A']:
                            takerSell = 0.1
                        elif symbol in tiers[0]['B']:
                            takerSell = 0.25
                        elif symbol in tiers[0]['C']:
                            takerSell = 0.4
                        elif symbol in tiers[0]['D']:
                            takerSell = 0.6
                        else:
                            takerSell = 0.25
                            print('Bitmart Else', symbol)
                        takerSellFee = float(chisOrdersVolBeforeFee) * takerSell/100
                    except Exception as e:
                        print("tiers 2")
              else:
                    takerSellFee = float(chisOrdersVolBeforeFee)*Constants.spotFees[max_exchange]['taker']/100

              chisOrdersVol = float(chisOrdersVolBeforeFee) - float(takerSellFee)
            
              srpriceSell = 0
              if summSellVolTokens != 0:
                  srpriceSell = chisOrdersVol / summSellVolTokens     
  
              dollarsSpread = float(chisOrdersVol) - float(volume)
              
              if summDollarSpread < dollarsSpread:
                  summDollarSpread = dollarsSpread
                  summasksVol = volume
                  summbidsVol = chisOrdersVol
                  summMin = srpriceBuy
                  summMax = srpriceSell
                  countOrderAsks = countOrderAsksCont
                  countOrderBids = countOrderBidsCont
                  alltokensSell = summSellVolTokens
                  alltokensBuy = delOrdersVol
                  finallBuyOrderPrice = ordersArr[-1][0]
                  firstBuyPrice = ordersArr[0][0]
                  lastBuyPrice = ordersArr[-1][0]
                  firstSellPrice = ordersArrSell[0][0]
                  lastSellPrice = ordersArrSell[-1][0]
              volume = volume + 1    
              
          summProfit = 0
          if summasksVol != 0:
              summProfit = (summbidsVol/summasksVol - 1) * 100
  
          volumeSellSumm = 0
          countOrderPrice = 0
          if len(symbolOrderBookMax['bids']) > 0: 
            for order in symbolOrderBookMax['bids']:
                if alltokensSell*order[0] > summasksVol:
                    volumeSellSumm = volumeSellSumm + order[0]*order[1]   
                    countOrderPrice = countOrderPrice + 1   
                    
          
          return [summProfit, summasksVol, summDollarSpread, summMin, countOrderAsks, summMax, countOrderBids, summbidsVol, alltokensSell, alltokensBuy, firstBuyPrice, lastBuyPrice, firstSellPrice, lastSellPrice, volumeSellSumm, countOrderPrice] 
    except Exception as e:
        print(f"Error updateSignalData: {e}")
        
        return None

async def sendNewSignal(call, min_exchange, max_exchange, symbol, feeDoll, userId, networkSumm, lastText):
    global globalMsgMenuLink
    try:
        newData = await  getDataFromMongo()

        # Раньше здесь шёл перебор ВСЕГО списка сигналов ради одной записи —
        # O(сигналы × пользователи) на каждое нажатие кнопки. Теперь нужная
        # запись достаётся прямым доступом по ключу, а цикл оставлен, чтобы
        # тело обработки осталось нетронутым.
        _found = findUserSignal(newData, userId, symbol, min_exchange, max_exchange, networkSumm)
        for signal in ([_found] if _found is not None else []):
            sqlUserId = str(signal['userId'])
            symbolSql = str(signal['symbol'])
            min_exchangeSql = str(signal['min_exchange'])
            max_exchangeSql = str(signal['max_exchange'])

            if sqlUserId == str(userId) and symbolSql == str(symbol) and min_exchangeSql == str(min_exchange) and max_exchangeSql == str(max_exchange):
                if networkSumm == str(signal['networkSumm']):
                    withdraw = float(signal['withdraw'])
                    deposit = float(signal['deposit'])
                    lastTime = signal['datetime']
                    summProfit = float(signal['summProfit'])
                    
                    if summProfit > 0:
                        answMSG = await formatMessage(float(signal['SummDollarSpread']),
                                                        float(signal['summasksVol']),
                                                        float(signal['summMin']),
                                                        int(signal['countOrderAsks']),
                                                        float(signal['summMax']),
                                                        int(signal['countOrderBids']),
                                                        float(signal['summbidsVol']),
                                                        float(signal['alltokensSell']),
                                                        float(signal['alltokensBuy']),
                                                        float(signal['firstBuyPrice']),
                                                        float(signal['lastBuyPrice']),
                                                        float(signal['firstSellPrice']),
                                                        float(signal['lastSellPrice']),
                                                        float(signal['volumeSellSumm']),
                                                        int(signal['countOrderPrice']),
                                                        min_exchange, max_exchange, 
                                                        symbol, feeDoll,lastTime,userId,str(signal['contract']),networkSumm,deposit,withdraw,summProfit)
                        
                        buttMSG = f'@|{min_exchange}!{max_exchange}${symbol}%{feeDoll}?{networkSumm}*'
                        up = types.InlineKeyboardMarkup(row_width=1)
                        update = types.InlineKeyboardButton(text='Обновить', callback_data=jsons.dump(buttMSG))
                        up.add(update)
                        
                        try:
                            await bot.send_message(call.message.chat.id, answMSG, reply_markup=up, parse_mode="HTML",disable_web_page_preview=True)
                        except telebot.asyncio_helper.ApiException as e:
                            if "bot was blocked by the user" in str(e):
                                print(f"Не удалось отправить сообщение, так как пользователь {call.message.chat.id} заблокировал бота")
                                await Constants.db.find_one_and_delete({'id': int(call.message.chat.id)})
                            pass    

                        
                    else:  
                        buttMSG = f'@|{min_exchange}!{max_exchange}${symbol}%{feeDoll}?{networkSumm}*'
                        up = types.InlineKeyboardMarkup(row_width=1)
                        update = types.InlineKeyboardButton(text='Обновить', callback_data=jsons.dump(buttMSG))
                        up.add(update)
                       
                        try:
                            await bot.send_message(call.message.chat.id, f'Пара {symbol} неактуальна ❌\n\nОбновите список активных спредов', reply_markup=up, parse_mode="HTML",disable_web_page_preview=True) 
                            #await Constants.dbMathPairs.find_one_and_delete({'symbol':symbol, 'max_exchange': max_exchange, 'min_exchange':min_exchange, 'userId': int(userId), 'networkSumm': networkSumm})
                        except telebot.asyncio_helper.ApiException as e:
                            if "bot was blocked by the user" in str(e):
                                print(f"Не удалось отправить сообщение, так как пользователь {call.message.chat.id} заблокировал бота")
                                await Constants.db.find_one_and_delete({'id': int(call.message.chat.id)})
                            pass 
                            
    except Exception as e:
        print(f"Error sendNewSignal: {e}")
        pass

async def formatMessage(summDollarSpread,
                        summasksVol,
                        summMin,countOrderAsks,
                        summMax,countOrderBids,
                        summbidsVol,
                        alltokensSell,
                        alltokensBuy,
                        firstBuyPrice,
                        lastBuyPrice,
                        firstSellPrice,
                        lastSellPrice,
                        volumeSellSumm,
                        countOrderPrice,
                        min_exchange,max_exchange,symbol,feeDoll,lastTime,userId,
                        contract,networkSumm,deposit,withdraw,
                        summProfit):

    cmc_list = await Constants.cmc.find().to_list(length=None)
    linkAsk = await formatLink(min_exchange,symbol)
    linkBid = await formatLink(max_exchange,symbol)
    
    buyTokensString = await formatTokenCount(alltokensBuy)
    sellTokensString = await formatTokenCount(alltokensSell)
   
    allFeeTokens = feeDoll/summMin
    feeTokens = await formatTokenCount(allFeeTokens)
    now = datetime.now()
    signalLive = str(now - lastTime).split('.')[0]
    
    userData = await Constants.db.find_one({'id': userId})
    contractsData = '🔋 Контракты совпадают' if len(contract)>0 else '🪫 Контракт неизвестен'
    baseSymb = symbol.replace('/USDT', '') if symbol[-4:] == 'USDT' else symbol.replace('/USDC', '')
    risk = '<b>❗️Осторожно</b>' if symbol in Constants.yellowTokens else ''
    explorer = ''
    cmc_link = ''
    for cmc in cmc_list:
        if cmc.get('symbol')==symbol.split('/')[0]:
            cmc_link=f'- <a href="{Constants.cmc_link}{cmc['slug']}">CMC</a>'
            break
    
    for network in Constants.chains_scan.keys():
        if network == networkSumm and len(contract)>0:
            explorer = f'<a href="{Constants.chains_scan[network]}{contract}">Сканер</a>'
            break
    if f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']:
        if deposit == 0 and withdraw == 0:
            answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif withdraw == 0:
            answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif deposit == 0:
            answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        else:
            answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
    elif baseSymb.upper() in userData['whiteListTokens'] or networkSumm.upper() in userData['whiteListNet']:
        if deposit == 0 and withdraw == 0:
            answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif withdraw == 0:
            answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif deposit == 0:
            answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        else:
            answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
    else:

        if deposit == 0 and withdraw == 0:
            answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif withdraw == 0:
            answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        elif deposit == 0:
            answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
        else:
            answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDoll,2)}$, {feeTokens} | Сеть: {networkSumm}"
    return answMSG


async def updateExistSignal(call, min_exchange, max_exchange, symbol, feeDoll, userId, networkSumm, lastText):
    global globalMsgMenuLink
    try:
        signal = {}
        founded = False
        signals = await getDataFromMongo()
        # Был линейный перебор всего снапшота, теперь прямой доступ по ключу.
        _found = findUserSignal(signals, userId, symbol, min_exchange, max_exchange, networkSumm)
        if _found is not None:
            signal = _found
            founded = True
                
        if not founded:
                print('not founded')
                answText = str(lastText)+"\n\n❌Неактуальный спред" if lastText.find('❌') == -1 else str(lastText)
                try:
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=answText, parse_mode="HTML",disable_web_page_preview=True)  
                except:
                    print('errorUpdate')
                    pass
                return
                
        
        contract = ''
        lastTime = ''
        withdraw= ''
        deposit = ''
        contract = str(signal['contract'])
        lastTime = signal['datetime']
        withdraw = float(signal['withdraw'])
        deposit = float(signal['deposit'])
        getUpdatedData = await  updateSignalData(min_exchange, max_exchange, symbol, feeDoll, userId)
        if not getUpdatedData is None:
                    summProfit = getUpdatedData[0]
                    
                    if summProfit > 0:

                        answMSG = await formatMessage(getUpdatedData[2],
                                                        getUpdatedData[1],
                                                        getUpdatedData[3],
                                                        getUpdatedData[4], 
                                                        getUpdatedData[5], 
                                                        getUpdatedData[6], 
                                                        getUpdatedData[7], 
                                                        getUpdatedData[8], 
                                                        getUpdatedData[9], 
                                                        getUpdatedData[10], 
                                                        getUpdatedData[11], 
                                                        getUpdatedData[12], 
                                                        getUpdatedData[13], 
                                                        getUpdatedData[14],
                                                        getUpdatedData[15],
                                                        min_exchange, max_exchange, 
                                                        symbol, feeDoll,lastTime,userId,contract,networkSumm,deposit,withdraw,summProfit)

                        buttMSG = f'@|{min_exchange}!{max_exchange}${symbol}%{feeDoll}?{networkSumm}*'
                        up = types.InlineKeyboardMarkup(row_width=1)
                        update = types.InlineKeyboardButton(text='Обновить', callback_data=jsons.dump(buttMSG))
                        up.add(update)
                        await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=answMSG, reply_markup=up, parse_mode="HTML",disable_web_page_preview=True)
                        
                    else:  
                        print('getUpdatedData spread < 0')
                        answMSGFormat = await formatMessage(getUpdatedData[2],
                                                        getUpdatedData[1],
                                                        getUpdatedData[3],
                                                        getUpdatedData[4], 
                                                        getUpdatedData[5], 
                                                        getUpdatedData[6], 
                                                        getUpdatedData[7], 
                                                        getUpdatedData[8], 
                                                        getUpdatedData[9], 
                                                        getUpdatedData[10], 
                                                        getUpdatedData[11], 
                                                        getUpdatedData[12], 
                                                        getUpdatedData[13], 
                                                        getUpdatedData[14],
                                                        getUpdatedData[15],
                                                        min_exchange, max_exchange, 
                                                        symbol, feeDoll,lastTime,userId,contract,networkSumm,deposit,withdraw,summProfit)

                        buttMSG = f'@|{min_exchange}!{max_exchange}${symbol}%{feeDoll}?{networkSumm}*'
                        up = types.InlineKeyboardMarkup(row_width=1)
                        update = types.InlineKeyboardButton(text='Обновить', callback_data=jsons.dump(buttMSG))
                        up.add(update)
                        answMSG = answMSGFormat + "\n\n❌Неактуальный спред"
                        #answText = str(lastText)+"\n\n❌Неактуальный спред" if lastText.find('❌') == -1 else str(lastText)
                        try:
                            await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=answText, reply_markup=up, parse_mode="HTML",disable_web_page_preview=True)  
                            
                        except:
                            pass 
        else:  
            print('getUpdatedData None')
            answText = str(lastText)+"\n\n❌Неактуальный спред" if lastText.find('❌') == -1 else str(lastText)
            try:
                await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=answText, parse_mode="HTML",disable_web_page_preview=True)  
                
                
            except Exception as e:
                print('Edit message update error:',e)
                pass
    except Exception as e:
        print(f"Error updateExistSignal: {e}")
        pass

async def getNewSignalData(call, min_exchange, max_exchange, symbol, feeDoll, userId, networkSumm, lastText, newMsg):
    match newMsg:
        case True:
            await sendNewSignal(call, min_exchange, max_exchange, symbol, feeDoll, userId, networkSumm, lastText)
        case False:
            await updateExistSignal(call, min_exchange, max_exchange, symbol, feeDoll, userId, networkSumm, lastText)


@bot.message_handler(state="*", commands=['cancel'])
async def any_state(message):
    await bot.delete_state(message.from_user.id, message.chat.id)



unicSignals = {}

user_dict = {}

async def forEveryUserSend(signal):
    global time_detect_signals,user_dict,cmc_list
    try:
        if signal['volumeSellSumm']<signal['summbidsVol']:
          return
        userData = user_dict[signal['userId']]

        summProfit = float(signal['summProfit'])
        summDollarSpread = float(signal['SummDollarSpread'])
        feeDollars = float(signal['feeVal'])
        id = signal['userId']
        symbol = signal['symbol']
        min_exchange = signal['min_exchange']
        max_exchange = signal['max_exchange']
                    
        lastTime = signal['datetime']
        networkSumm = signal['networkSumm']
        withdraw = signal['withdraw'] 
        deposit = signal['deposit'] 
        contract = signal['contract']

        minProcentSpread = userData['minProcentSpread']
        maxFeeVal = userData['maxFeeVal']
        minDollarSpread = userData['minDollarSpread']

        if summDollarSpread > minDollarSpread and summProfit > minProcentSpread and feeDollars <= maxFeeVal: 
                    check_key = f'{symbol}{min_exchange}{max_exchange}{networkSumm}{id}'
                    if check_key not in time_detect_signals.keys():
                        time_detect_signals[check_key]={'time':'','spread':''}
                        time_detect_signals[check_key]['time']=lastTime
                        time_detect_signals[check_key]['spread']=summDollarSpread
                    else:
                        difference = abs(time_detect_signals[check_key]['time'] - lastTime)
                        if difference > timedelta(minutes=2):
                            time_detect_signals[check_key]['time']=lastTime
                            time_detect_signals[check_key]['spread']=summDollarSpread
                        else:
                            return

        
                    
                    
                    summasksVol = float(signal['summasksVol'] )
                    summMin = float(signal['summMin'] )
                    countOrderAsks = float(signal['countOrderAsks'] )
                    summMax = float(signal['summMax'] )
                    countOrderBids = float(signal['countOrderBids'] )
                    summbidsVol = float(signal['summbidsVol'] )
                    alltokensSell = float(signal['alltokensSell'] )
                    alltokensBuy = float(signal['alltokensBuy'] )
                    firstBuyPrice = float(signal['firstBuyPrice'] )
                    lastBuyPrice = float(signal['lastBuyPrice'] )
                    firstSellPrice = float(signal['firstSellPrice'] )
                    lastSellPrice = float(signal['lastSellPrice'] )
                    volumeSellSumm = float(signal['volumeSellSumm'])
                    countOrderPrice = float(signal['countOrderPrice'])

                    minEx = userData['minEx']
                    maxEx = userData['maxEx']
                    blackListNet = userData['blackListNet']
                    blackListTokens = userData['blackListTokens']
                    
                    linkAsk = await formatLink(min_exchange,symbol)
                    linkBid = await formatLink(max_exchange,symbol)
                    
                    buyTokensString = await formatTokenCount(alltokensBuy)
                    sellTokensString = await formatTokenCount(alltokensSell)     

                    allFeeTokens = float(feeDollars)/float(summMin)
                    feeTokens = await formatTokenCount(allFeeTokens)

                    
                    if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                           
                            explorer = ''
                            cmc_link = ''
                            for cmc in cmc_list:
                                
                                if cmc.get('symbol')==symbol.split('/')[0]:
                                    cmc_link=f'- <a href="{Constants.cmc_link}{cmc['slug']}">CMC</a>'
                                    break
                            
                            for network in Constants.chains_scan.keys():
                                if network == networkSumm and len(contract)>0:
                                    explorer = f'<a href="{Constants.chains_scan[network]}{contract}">Сканер</a>'
                                    break
                            """ lastTimeData = datetime.strptime(lastTime, '%Y-%m-%d %H:%M:%S') """
                            now = datetime.now()
                            signalLive = str(now - lastTime).split('.')[0]
                                
                            baseSymb = symbol.replace('/USDT', '') if symbol[-4:] == 'USDT' else symbol.replace('/USDC', '')
                            contractsData = '🔋 Контракты совпадают' if len(contract)>0 else '🪫 Контракт неизвестен'
                            
                            answMSG = ""
                            risk = '<b>❗️Осторожно</b>' if symbol in Constants.yellowTokens else ''
                            if f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']:
                                
                                if (userData['typeSignal'] == 'False' and (not deposit or not withdraw)) or (userData['typeSignal'] == 'onlyTop'  and (not deposit or not withdraw) ):  
                                    if deposit == 0 and withdraw == 0:
                                        answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif withdraw == 0:
                                        answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif deposit == 0:
                                        answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    else:
                                        answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                elif (userData['typeSignal'] == 'True'  and deposit and withdraw) or (userData['typeSignal'] == 'onlyTop'  and deposit and withdraw ):
                                    answMSG = f"<b>Ⓜ️Ⓜ️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                else:
                                    return None
                            elif baseSymb.upper() in userData['whiteListTokens'] or networkSumm.upper() in userData['whiteListNet']:
                                
                                if (userData['typeSignal'] == 'False' and (not deposit or not withdraw)) or (userData['typeSignal'] == 'onlyTop'  and (not deposit or not withdraw) ):  
                                    if deposit == 0 and withdraw == 0:
                                        answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif withdraw == 0:
                                        answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif deposit == 0:
                                        answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    else:
                                        answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                elif (userData['typeSignal'] == 'True'  and deposit and withdraw) or (userData['typeSignal'] == 'onlyTop'  and deposit and withdraw ):
                                    answMSG = f"<b>⭐️Избранный лист</b>\n\n{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                else:
                                    return None

                            else:
                                if (userData['typeSignal'] == 'False' and (not deposit or not withdraw)) or (userData['typeSignal'] == 'onlyTop'  and (not deposit or not withdraw) ): 
                                    if deposit == 0 and withdraw == 0:
                                        answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif withdraw == 0:
                                        answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n🔒Вывод закрыт\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    elif deposit == 0:
                                        answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n🔒Ввод закрыт\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                    else:
                                        answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                elif (userData['typeSignal'] == 'True'  and deposit and withdraw) or (userData['typeSignal'] == 'onlyTop'  and deposit and withdraw ):
                                    answMSG = f"{baseSymb}: {min_exchange}→{max_exchange} {round(summasksVol)} +{round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n\nПара: <code>{baseSymb}</code>{'/USDT' if symbol[-4:] == 'USDT' else '/USDC'}\t{explorer}{risk}\n\n📕 Покупаем: {linkAsk}\nЦена: <code>{summMin}</code>$ [{firstBuyPrice}$-{lastBuyPrice}$]\nСумма покупки: <b>{summasksVol}$</b>, {buyTokensString} | {countOrderAsks} ордеров\n\n📗Продаем: {linkBid}\nЦена: <code>{summMax}</code>$  [{firstSellPrice}$-{lastSellPrice}$]\nСумма продажи: <b>{round(summbidsVol,2)}$</b>, {sellTokensString} | {countOrderBids} ордеров\nДоступный объем: <b>{round(volumeSellSumm,2)}$</b> | {countOrderPrice} ордеров\n\n{contractsData}\t{cmc_link}\n\n💰Чистый спред: {round(summDollarSpread,2)}$ ({round(summProfit,2)}%)\n🕐Время жизни: {signalLive}\n\nКомиссия: {round(feeDollars,2)}$, {feeTokens} | Сеть: {networkSumm}"
                                else:
                                    return None       
                                
                            buttMSG = f'@|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,2)}?{networkSumm}*'
                            up = types.InlineKeyboardMarkup(row_width=1)
                            update = types.InlineKeyboardButton(text='Обновить', callback_data=jsons.dump(buttMSG))
                            up.add(update)
                             
                            try:
                                if (userData['monitoring'] and userData['monitoringTop']) or (userData['monitoring'] and not(userData['monitoringTop'])): 
                                        
                                        await bot.send_message(id, answMSG, reply_markup=up, parse_mode="HTML",disable_web_page_preview=True)     
                                if (not(userData['monitoring']) and userData['monitoringTop']):
                                    if baseSymb.upper() in userData['whiteListTokens'] or networkSumm.upper() in userData['whiteListNet']:
                                            
                                            await bot.send_message(id, answMSG, reply_markup=up, parse_mode="HTML",disable_web_page_preview=True)   
                                else:
                                    
                                    return None 
                                        
                            except telebot.asyncio_helper.ApiException as e:
                                if "bot was blocked by the user" in str(e):
                                    print(f"Не удалось отправить сообщение, так как пользователь {id} заблокировал бота")
                                    await Constants.db.find_one_and_delete({'id': int(id)})
                                pass
                                               
    except Exception as e:
        # Раньше печаталось голое сообщение исключения — по нему нельзя было
        # понять ни тип, ни строку, где оно возникло. Для KeyError текст
        # выглядел просто как 'symbol', что ничего не давало.
        import traceback
        print(f"forEveryUserSend {type(e).__name__}: {e}")
        print(traceback.format_exc())
        try:
            print("ключи сигнала:", sorted(signal.keys()) if isinstance(signal, dict) else type(signal).__name__)
        except Exception:
            pass

async def format_user_settings(userdata):
    global user_dict
    user_dict[userdata['id']] = {'maxFeeVal':userdata['maxFeeVal'],
                                 'minDollarSpread':userdata['minDollarSpread'],
                                 'minProcentSpread':userdata['minProcentSpread'],
                                 'minEx':userdata['minEx'],
                                 'maxEx':userdata['maxEx'],
                                 'blackListNet':userdata['blackListNet'],
                                 'blackListTokens':userdata['blackListTokens'],
                                 'typeSignal':userdata['typeSignal'],
                                 'monitoring':userdata['monitoring'],
                                 'monitoringTop':userdata['monitoringTop'],
                                 'whiteListTokens':userdata['whiteListTokens'],
                                 'whiteListNet':userdata['whiteListNet'],
                                 'save_mm_signals': userdata['save_mm_signals']}

async def collect_users_settings():
        global user_dict
        user_dict.clear()
        users = await Constants.db.find().to_list(length=None)
        [asyncio.create_task(format_user_settings(user)) for user in users]

async def collect_cmc_data():
    global cmc_list
    cmc_list = await Constants.cmc.find().to_list(length=None)

async def startSignals(type):
    global data, unicSignals

    try:
        await collect_users_settings()
        await collect_cmc_data()
        newData = await getDataFromMongo()
        # len() больше не применим напрямую: в новом формате снапшот — словарь
        # с payloads/recipients, а не плоский список. countSignals считает
        # именно число пар «сигнал × получатель», как раньше делал len().
        if not (data == newData) and countSignals(newData) != 0:

            loops = [asyncio.create_task(forEveryUserSend(signal)) for signal in expandSignals(newData)]
            await gather(*loops)

        data = newData
        unicSignals.clear()


    except Exception as e:
        print('startSignals', e)

    
globalMsgMenuLink = {}  

@bot.message_handler(commands=['start'])
async def start_message(message:types.Message):
    if not await pair_user(message, bot):
        return
    try:
        mm = types.ReplyKeyboardMarkup(row_width=2)
        button2 = types.KeyboardButton("⚡️Биржи")
        button3 = types.KeyboardButton("📜Активные связки") 
        button10 = types.KeyboardButton("💻Мониторинг")  
        button11 = types.KeyboardButton("👤Профиль") 
        button1 = types.KeyboardButton("⚙️Настройки")
        button9 = types.KeyboardButton("☁️Сохраненные токены/сети")
        
        
        if is_owner(message.chat.id):
            button12 = types.KeyboardButton("🔐Добавить пользователя") 
            button13 = types.KeyboardButton("🔪Удалить пользователя")   
            #button14 = types.KeyboardButton("❗️ Осторожно список") 
            #button15 = types.KeyboardButton("⛔️ Глобальный ЧС") 
            button16 = types.KeyboardButton("Статистика DB")
            mm.add(button11, button3, button2,  button10, button1, button9, button12, button13, button16)
            value = {'id':message.chat.id}
            enter = await Constants.db.find_one(value)
            if not (enter is None): 
                try:
                    tryGetWhiteList = enter['whiteListTokens']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'whiteListTokens': []}})  
                    pass

                try:
                    tryGetWhiteList = enter['whiteListNet']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'whiteListNet': []}}) 
                    pass  

                try:
                    tryGetMonitoringTop = enter['monitoringTop']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'monitoringTop': False}}) 
                    pass        

                try:
                    await bot.send_message(message.chat.id, 'Идет расчет связок', reply_markup=mm)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

                if not (message.chat.id in tgId):
                    tgId.append(message.chat.id)
                
            else:
                try:
                    await bot.send_message(message.chat.id, 'У вас нет доступа\n\nОбратитесь https://t.me/BotFather')       
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 
        else:
            mm.add(button11, button3, button2, button10, button1, button9 )
            value = {'id':message.chat.id}
            enter = await Constants.db.find_one(value)
            if not (enter is None):
                try:
                    tryGetWhiteList = enter['whiteListTokens']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'whiteListTokens': []}})  
                    pass

                try:
                    tryGetWhiteList = enter['whiteListNet']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'whiteListNet': []}})  
                    pass

                try:
                    tryGetMonitoringTop = enter['monitoringTop']
                except Exception as e:
                    await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'monitoringTop': False}}) 
                    pass   


                try:
                    await bot.send_message(message.chat.id, 'Идет расчет связок', reply_markup=mm)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

                if not (message.chat.id in tgId):
                    tgId.append(message.chat.id)
                
            else:
                try:
                    await bot.send_message(message.chat.id, 'Для получения доступа обратитесь по ссылке ниже 👇\n\nhttps://t.me/BotFather')      
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 
    except Exception as e:
            print(f"Error messageHandler: {e}")
            pass

async def generateMenu(userexchangesMin, userexchangesMax):
    markup_inline = types.InlineKeyboardMarkup(row_width=2)
    bueEx = types.InlineKeyboardButton(text='ПОКУПКА', callback_data='Покупка')
    sellEx = types.InlineKeyboardButton(text='ПРОДАЖА', callback_data='ПРОДАЖА')
    okxMin = types.InlineKeyboardButton(text=userexchangesMin[0], callback_data=userexchangesMin[0])
    okxMax = types.InlineKeyboardButton(text=userexchangesMax[0], callback_data=userexchangesMax[0])
    kukoinMin = types.InlineKeyboardButton(text=userexchangesMin[1], callback_data=userexchangesMin[1])
    kukoinMax = types.InlineKeyboardButton(text=userexchangesMax[1], callback_data=userexchangesMax[1])
    gateMin = types.InlineKeyboardButton(text=userexchangesMin[2], callback_data=userexchangesMin[2])
    gateMax = types.InlineKeyboardButton(text=userexchangesMax[2], callback_data=userexchangesMax[2])
    mexcMin = types.InlineKeyboardButton(text=userexchangesMin[3], callback_data=userexchangesMin[3])
    mexcMax = types.InlineKeyboardButton(text=userexchangesMax[3], callback_data=userexchangesMax[3])
    bitgetMin = types.InlineKeyboardButton(text=userexchangesMin[4], callback_data=userexchangesMin[4])
    bitgetMax = types.InlineKeyboardButton(text=userexchangesMax[4], callback_data=userexchangesMax[4])
    bingxMin = types.InlineKeyboardButton(text=userexchangesMin[5], callback_data=userexchangesMin[5])
    bingxMax = types.InlineKeyboardButton(text=userexchangesMax[5], callback_data=userexchangesMax[5])
    bybitMin = types.InlineKeyboardButton(text=userexchangesMin[6], callback_data=userexchangesMin[6])
    bybitMax = types.InlineKeyboardButton(text=userexchangesMax[6], callback_data=userexchangesMax[6])
    CoinExMin = types.InlineKeyboardButton(text=userexchangesMin[8], callback_data=userexchangesMin[8])
    CoinExMax = types.InlineKeyboardButton(text=userexchangesMax[8], callback_data=userexchangesMax[8])
    HTXMin = types.InlineKeyboardButton(text=userexchangesMin[7], callback_data=userexchangesMin[7])
    HTXMax = types.InlineKeyboardButton(text=userexchangesMax[7], callback_data=userexchangesMax[7])
    BitmartMin = types.InlineKeyboardButton(text=userexchangesMin[9], callback_data=userexchangesMin[9])
    BitmartMax = types.InlineKeyboardButton(text=userexchangesMax[9], callback_data=userexchangesMax[9])
    markup_inline.add(bueEx, sellEx, okxMin, okxMax, kukoinMin, kukoinMax, gateMin, gateMax, mexcMin, mexcMax, bitgetMin, bitgetMax, bingxMin, bingxMax, bybitMin, bybitMax, CoinExMin, CoinExMax,HTXMin, HTXMax,BitmartMin,BitmartMax)

    return markup_inline

globalProcessActList = {}

async def asyncUpdatingActiveList(mongoDataSignal, message, processMsgAns):
    global globalProcessActList
    try:
        if str(mongoDataSignal['userId']) == str(message.chat.id):     
            symbol = mongoDataSignal['symbol']
            min_exchange = mongoDataSignal['min_exchange']
            max_exchange = mongoDataSignal['max_exchange']
            networkSumm = mongoDataSignal['networkSumm']
            feeDollars = float(mongoDataSignal['feeVal'])
            contract = mongoDataSignal['contract']

            userData = await Constants.db.find_one({'id': int(message.chat.id)})

            maxFeeVal = float(userData['maxFeeVal'])
            minDollarSpread = float(userData['minDollarSpread'])
            minProcentSpread = float(userData['minProcentSpread'])
            minEx = userData['minEx']
            maxEx = userData['maxEx']
            blackListNet = userData['blackListNet']
            blackListTokens = userData['blackListTokens']

            contractsData = '🔋' if len(contract)>0 else '🪫'        
            baseSymb = symbol.replace('/USDT', '') if symbol[-4:] == 'USDT' else symbol.replace('/USDC', '')
            isTop = baseSymb.upper() in userData['whiteListTokens'] or networkSumm.upper() in userData['whiteListNet']
            isMM = f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or  f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']
            save_icon = 'Ⓜ️Ⓜ️' if (f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or  f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']) else '⭐️'
            if symbol in Constants.yellowTokens:
                if (userData['typeSignal'] == 'False') or (userData['typeSignal'] == 'True' and int(mongoDataSignal['withdraw']) == 1 and int(mongoDataSignal['deposit']) == 1):   
                    if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                        if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                            btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {'🔒 ❗️' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and not((isTop or isMM))  else f'🔒 {save_icon} ❗️' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and (isTop or isMM) else f'✅ {save_icon} ❗️' if (isTop or isMM) else '✅ ❗️'} {contractsData}"
                            btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                            res = {'spread':'', 'btn':''}
                            res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                            res['spread']=float(mongoDataSignal['SummDollarSpread'])
                            globalProcessActList[message.chat.id] = globalProcessActList[message.chat.id]+1
                            #await update_progress_message(message.chat.id, processMsgAns, globalProcessActList[message.chat.id])
                            return res

                elif userData['typeSignal'] == 'onlyTop' and (isTop or isMM):
                    if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                        if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                            btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {f'🔒 {save_icon} ❗️' if (int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0) else f'✅ {save_icon} ❗️'} {contractsData}"
                            btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                            res = {'spread':'', 'btn':''}
                            res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                            res['spread']=float(mongoDataSignal['SummDollarSpread'])
                            globalProcessActList[message.chat.id] = globalProcessActList[message.chat.id]+1
                            #await update_progress_message(message.chat.id, processMsgAns, globalProcessActList[message.chat.id])
                            return res

                else:
                    pass
            else:
                if (userData['typeSignal'] == 'False') or (userData['typeSignal'] == 'True' and int(mongoDataSignal['withdraw']) == 1 and int(mongoDataSignal['deposit']) == 1):   
                    if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                        if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                            btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {'🔒' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and not((isTop or isMM))  else f'🔒 {save_icon}' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and (isTop or isMM) else f'✅ {save_icon}' if (isTop or isMM) else '✅'} {contractsData}"
                            btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                            res = {'spread':'', 'btn':''}
                            res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                            res['spread']=float(mongoDataSignal['SummDollarSpread'])
                            globalProcessActList[message.chat.id] = globalProcessActList[message.chat.id]+1
                            #await update_progress_message(message.chat.id, processMsgAns, globalProcessActList[message.chat.id])
                            return res

                elif userData['typeSignal'] == 'onlyTop' and (isTop or isMM):
                    if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                        if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                            btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {f'🔒 {save_icon}' if (int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0) else f'✅ {save_icon}'} {contractsData}"
                            btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                            res = {'spread':'', 'btn':''}
                            res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                            res['spread']=float(mongoDataSignal['SummDollarSpread'])
                            globalProcessActList[message.chat.id] = globalProcessActList[message.chat.id]+1
                            #await update_progress_message(message.chat.id, processMsgAns, globalProcessActList[message.chat.id])
                            return res

                else:
                    pass    
        else:
            return None    
    except Exception as e:
        print('AsyncErr', e)        
        return None


async def callUpdatingActiveList(mongoDataSignal, call:types.CallbackQuery):
    global globalProcessActList
    if str(mongoDataSignal['userId']) == str(call.message.chat.id):
        
        symbol = mongoDataSignal['symbol']
        min_exchange = mongoDataSignal['min_exchange']
        max_exchange = mongoDataSignal['max_exchange']
        networkSumm = mongoDataSignal['networkSumm']
        feeDollars = float(mongoDataSignal['feeVal']) 
        contract = mongoDataSignal['contract']
        
        userData = await Constants.db.find_one({'id': int(call.message.chat.id)})
        
        maxFeeVal = float(userData['maxFeeVal'])
        minDollarSpread = float(userData['minDollarSpread'])
        minProcentSpread = float(userData['minProcentSpread'])
        minEx = userData['minEx']
        maxEx = userData['maxEx']
        blackListNet = userData['blackListNet']
        blackListTokens = userData['blackListTokens']
        
        contractsData = '🔋' if len(contract)>0 else '🪫'        
        baseSymb = symbol.replace('/USDT', '') if symbol[-4:] == 'USDT' else symbol.replace('/USDC', '')
        isTop = baseSymb.upper() in userData['whiteListTokens'] or networkSumm.upper() in userData['whiteListNet']
        isMM = f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or  f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']
        save_icon = 'Ⓜ️Ⓜ️' if (f'{symbol.upper()}|{min_exchange}' in userData['save_mm_signals'] or  f'{symbol.upper()}|{max_exchange}' in userData['save_mm_signals']) else '⭐️'
        if symbol in Constants.yellowTokens:
            if (userData['typeSignal'] == 'False') or (userData['typeSignal'] == 'True' and int(mongoDataSignal['withdraw']) == 1 and int(mongoDataSignal['deposit']) == 1):   
                if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                    if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                        btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {'🔒 ❗️' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and not((isTop or isMM))  else f'🔒 {save_icon} ❗️' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and (isTop or isMM) else f'✅ {save_icon} ❗️' if (isTop or isMM) else '✅ ❗️'} {contractsData}"
                        btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                        res = {'spread':'', 'btn':''}
                        res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                        res['spread']=float(mongoDataSignal['SummDollarSpread'])
                        globalProcessActList[call.message.chat.id] = globalProcessActList[call.message.chat.id]+1
                        #await update_progress_message(call.message.chat.id, processMsgAns, globalProcessActList[call.message.chat.id])
                        return res

            elif userData['typeSignal'] == 'onlyTop' and (isTop or isMM):
                if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                    if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                        btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {f'🔒 {save_icon} ❗️' if (int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0) else f'✅ {save_icon} ❗️'} {contractsData}"
                        btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                        res = {'spread':'', 'btn':''}
                        res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                        res['spread']=float(mongoDataSignal['SummDollarSpread'])
                        globalProcessActList[call.message.chat.id] = globalProcessActList[call.message.chat.id]+1
                        #await update_progress_message(call.message.chat.id, processMsgAns, globalProcessActList[call.message.chat.id])
                        return res

            else:
                pass
        else:
            if (userData['typeSignal'] == 'False') or (userData['typeSignal'] == 'True' and int(mongoDataSignal['withdraw']) == 1 and int(mongoDataSignal['deposit']) == 1):   
                if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                    if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                        btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {'🔒' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and not((isTop or isMM))  else f'🔒 {save_icon}' if ((int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0)) and (isTop or isMM) else f'✅ {save_icon}' if (isTop or isMM) else '✅'} {contractsData}"
                        btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                        res = {'spread':'', 'btn':''}
                        res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                        res['spread']=float(mongoDataSignal['SummDollarSpread'])
                        globalProcessActList[call.message.chat.id] = globalProcessActList[call.message.chat.id]+1
                        #await update_progress_message(call.message.chat.id, processMsgAns, globalProcessActList[call.message.chat.id])
                        return res

            elif userData['typeSignal'] == 'onlyTop' and (isTop or isMM):
                if str(min_exchange)+' ✅' in minEx and str(max_exchange)+'✅' in maxEx and not (symbol.replace('/USDT', '').upper() in blackListTokens) and not (networkSumm.upper() in blackListNet):
                    if float(mongoDataSignal['SummDollarSpread']) > minDollarSpread and float(mongoDataSignal['summProfit']) > minProcentSpread and feeDollars <= maxFeeVal:
                        btn = f"{symbol.replace('/USDT', '')}: {round(float(mongoDataSignal['summasksVol']))} +{round(float(mongoDataSignal['SummDollarSpread']),2)}$ ({round(float(mongoDataSignal['summProfit']),2)}%) {f'🔒 {save_icon}' if (int(mongoDataSignal['withdraw']) == 0) or (int(mongoDataSignal['deposit']) == 0) else f'✅ {save_icon}'} {contractsData}"
                        btnUpdate = f'№|{min_exchange}!{max_exchange}${symbol}%{round(feeDollars,9)}?{networkSumm}*'
                        res = {'spread':'', 'btn':''}
                        res['btn']=types.InlineKeyboardButton(text=btn, callback_data=btnUpdate)
                        res['spread']=float(mongoDataSignal['SummDollarSpread'])
                        globalProcessActList[call.message.chat.id] = globalProcessActList[call.message.chat.id]+1
                        #await update_progress_message(call.message.chat.id, processMsgAns, globalProcessActList[call.message.chat.id])
                        return res
            else:
                pass    
    else:
        return None


async def cointainerActiveList(message, newData, type, processMsgAns):
    userDataArr = []
    summDataArr = []
    sortedSpread = []

    if type:
        userDataArr.append(await asyncio.gather(*[asyncUpdatingActiveList(mongolist, message, processMsgAns) for mongolist in newData]))
    else:
        userDataArr.append(await asyncio.gather(*[callUpdatingActiveList(mongolist, message) for mongolist in newData]))

    try:
        for i in userDataArr[0]:
            if not i is None:
                summDataArr.append(i)
        sortedSpread = sorted([item['spread'] for item in summDataArr], reverse=True)
        userDataArr = []
        for spread in sortedSpread:
            for item in summDataArr:
                if item['spread'] == spread:
                    userDataArr.append(item['btn'])
        
        listCount = (len(userDataArr) + 7) // 8
        
        summDataArr = [userDataArr[i:i+8] for i in range(0, len(userDataArr), 8)]

        if type:
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'lastActiveList': jsons.dumps(userDataArr)}})
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'currentPage':0}})   
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'countPages': listCount}})
            globalProcessActList[message.chat.id] = 0
            return [summDataArr, userDataArr]
        else:
            await Constants.db.find_one_and_update({'id':message.message.chat.id},{'$set':{'lastActiveList': jsons.dumps(userDataArr)}})
            await Constants.db.find_one_and_update({'id':message.message.chat.id},{'$set':{'currentPage':0}})   
            await Constants.db.find_one_and_update({'id':message.message.chat.id},{'$set':{'countPages': listCount}})
            globalProcessActList[message.message.chat.id] = 0
            return [summDataArr, userDataArr]
            
        
    except Exception as e:
        print('ContError', e)
        globalProcessActList[message.chat.id] = 0
        return None


async def send_progress_message(chat_id, progress):
    try:
        message =  await bot.send_message(chat_id, f'Рассчитываем активные связки 🕑 {progress}')
        return message
    except telebot.asyncio_helper.ApiException as e:
        if "bot was blocked by the user" in str(e):
            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
            await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
        pass 
    

async def update_progress_message(chat_id, message, progress):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message.message_id, text=f'Рассчитываем активные связки 🕑 {progress}')
    except telebot.asyncio_helper.ApiException as e:
        if "bot was blocked by the user" in str(e):
            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
            await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
        pass 

            
@bot.message_handler(state=MyStates.minVolUser)
async def minVolGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['minVolUser'] = message.text

        text = message.text
        answ = 0
        try:
            answ = int(text)
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'minVolume':int(answ)}})
            await bot.reply_to(message, f'Минимальный объем {answ}USDT успешно сохранен')
            await bot.delete_state(message.from_user.id, message.chat.id)
        except:    
            pass 

@bot.message_handler(state=MyStates.maxVolUser)
async def maxVolGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['maxVolUser'] = message.text

        text = message.text
        answ = 0
        try:
            answ = int(text)
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'maxVolume':int(answ)}})
            await bot.reply_to(message, f'Максимальный объем {answ}USDT успешно сохранен')
            await bot.delete_state(message.from_user.id, message.chat.id)
        except:    
            pass 

@bot.message_handler(state=MyStates.minSpreadUser)
async def minSpreadGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['minSpreadUser'] = message.text

        text = message.text
        answ = 0
        try:
            answ = float(text)
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'minDollarSpread':float(answ)}})
            await bot.reply_to(message, f'Минимальный спред {answ}$ успешно сохранен')
            await bot.delete_state(message.from_user.id, message.chat.id)
        except:    
            pass    

@bot.message_handler(state=MyStates.minProcentUser)
async def minSpreadGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['minProcentUser'] = message.text

        text = message.text
        answ = 0
        try:
            answ = float(text)
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'minProcentSpread':float(answ)}})
            await bot.reply_to(message, f'Минимальный спред {answ}% успешно сохранен')
            await bot.delete_state(message.from_user.id, message.chat.id)
        except:    
            pass         


@bot.message_handler(state=MyStates.feesUser)
async def maxSpreadGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['feesUser'] = message.text

        text = message.text
        answ = 0
        try:
            answ = float(text)
            await Constants.db.find_one_and_update({'id':message.chat.id},{'$set':{'maxFeeVal':float(answ)}})
            await bot.reply_to(message, f'Максимальная комиссия {answ}$ успешно сохранена')
            await bot.delete_state(message.from_user.id, message.chat.id)
        except:    
            pass     

@bot.message_handler(state=MyStates.addUser)
async def addUserGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        if is_owner(message.chat.id):
            data['addUser'] = message.text

            value = {'id':message.text}
            enter = await Constants.db.find_one(value)
            if enter is None:
                try:
                    await Constants.db.insert_one({'id':int(message.text), 'whiteListNet': [], 'whiteListTokens': [], 'lastActiveList': "", 'countPages': 0, 'currentPage': 0,'typeSignal': 'True', 'monitoring': True, 'monitoringTop': False,'blackListTokens': [], 'blackListNet': [], 'maxFeeVal': 20, 'minDollarSpread':0, 'minProcentSpread':0, 'minVolume': 300, 'maxVolume': 2000, 'minEx':['OKX ❌', 'Kucoin ❌', 'Gate ❌', 'MEXC Global ❌', 'Bitget ❌', 'BingX ❌', 'Bybit ❌', 'HTX ❌','CoinEx ❌','Bitmart ❌'],'maxEx':['OKX❌', 'Kucoin❌', 'Gate❌', 'MEXC Global❌', 'Bitget❌', 'BingX❌', 'Bybit❌', 'HTX❌','CoinEx❌','Bitmart❌'],'save_mm_signals':[]})
                    await bot.send_message(message.chat.id, 'Пользователь успешно добавлен!')
                    await bot.delete_state(message.from_user.id, message.chat.id)
                except: 
                    await bot.send_message(message.chat.id, 'Error') 
                    pass

@bot.message_handler(state=MyStates.delUser)
async def delUserGet(message):      
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        if is_owner(message.chat.id):
            data['delUser'] = message.text

            try:
                await Constants.db.find_one_and_delete({'id': int(message.text)})
            except: 
                await bot.send_message(message.chat.id, 'Error') 
                pass

            value = {'id': message.text}
            enter = await Constants.db.find_one(value)
            if enter is None:
                await bot.send_message(message.chat.id, 'Пользователь успешно удален 🔪')
                await bot.delete_state(message.from_user.id, message.chat.id)

tokensBtnsArrGlobalBlack = {}
netBtnsArrGlobalBlack = {}
tokensBtnsArrGlobalWhite = {}
netBtnsArrGlobalWhite = {}
userEditMsgId = {}
tokensBtnsArrYellow = {}
tokensBtnsArrAllGlobalBlackList = {}

# Глобальные словари для хранения id сообщений для каждого типа списка
edit_msg_ids = {
    'black_tokens': {},
    'white_tokens': {},
    'white_nets': {},
    'black_nets': {}
}

# Глобальные переменные для хранения текущей страницы для каждого списка
global_black_token_page = {}   # для чёрных токенов
global_white_token_page = {}   # для белых токенов
global_white_net_page = {}     # для белых сетей
global_black_net_page = {}     # для чёрных сетей



def generate_paginated_markup(items, current_page, items_per_page, list_id):
    """
    items: список кортежей (display_text, delete_callback)
    current_page: текущая страница (начиная с 1)
    list_id: идентификатор списка (например, "black_tokens", "white_nets" и т.д.)
    """
    total_items = len(items)
    total_pages = math.ceil(total_items / items_per_page) if total_items > 0 else 1
    current_page = max(1, min(current_page, total_pages))
    start = (current_page - 1) * items_per_page
    end = start + items_per_page
    page_items = items[start:end]
    markup = types.InlineKeyboardMarkup(row_width=2)
    for display, del_callback in page_items:
        btn1 = types.InlineKeyboardButton(text=display, callback_data="noop")
        btn2 = types.InlineKeyboardButton(text="❌", callback_data=del_callback)
        markup.add(btn1, btn2)
    nav_buttons = []
    if current_page > 1:
        nav_buttons.append(types.InlineKeyboardButton("⬅️", callback_data=f"page|{list_id}|prev|{current_page-1}"))

    nav_buttons.append(types.InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="noop"))
    if current_page < total_pages:
        nav_buttons.append(types.InlineKeyboardButton("➡️", callback_data=f"page|{list_id}|next|{current_page+1}"))
    markup.row(*nav_buttons)
    return markup

@bot.message_handler(state=MyStates.tokenStateBlack)
async def tokenGet_black(message: types.Message):
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        # Если текст не содержит управляющих символов
        if all(x not in message.text.upper() for x in ['⬅️', '⚪️', '⚫️', '⚡️', '💵', '💰', '📜', '💻', '👤', '☁️', '⚙️']):
            userData = await Constants.db.find_one({'id': message.chat.id})
            blackListTokens = userData.get('blackListTokens', [])
            token_to_add = message.text.upper()
            blackListTokens.append(token_to_add)
            await Constants.db.find_one_and_update({'id': message.chat.id}, {'$set': {'blackListTokens': blackListTokens}})
            
            # Формируем список элементов для постраничного отображения
            items = [(token, f'remove_black_{token}') for token in blackListTokens]
            items_per_page = 8
            global global_black_token_page
            if message.chat.id not in global_black_token_page:
                global_black_token_page[message.chat.id] = 1
            current_page = global_black_token_page[message.chat.id]
            
            markup_inline = generate_paginated_markup(items, current_page, items_per_page, "black_tokens")
            text_message = ("Чтобы добавить монету в чёрный список, напишите её название.\n"
                            "Пример: ETH или eth\n\nВаш чёрный список:")
            # Если уже было выведено предыдущее сообщение – удаляем его
            if message.chat.id in userEditMsgId:
                try:
                    await bot.delete_message(message.chat.id, userEditMsgId[message.chat.id])
                except Exception:
                    pass
            sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
            userEditMsgId[message.chat.id] = sent_msg.message_id
            
            await bot.reply_to(message, f'Токен {token_to_add} сохранён в чёрный список!')
            await bot.delete_state(message.from_user.id, message.chat.id)


@bot.message_handler(state=MyStates.tokenStateWhite)
async def tokenGet_white(message: types.Message):
    if all(x not in message.text.upper() for x in ['⬅️', '⚪️', '⚫️', '⚡️', '💵', '💰', '📜', '💻', '👤', '☁️', '⚙️']):
        userData = await Constants.db.find_one({'id': message.chat.id})
        whiteListTokens = userData.get('whiteListTokens', [])
        token_to_add = message.text.upper()
        whiteListTokens.append(token_to_add)
        await Constants.db.find_one_and_update({'id': message.chat.id}, {'$set': {'whiteListTokens': whiteListTokens}})
        
        items = [(token, f'remove_white_{token}') for token in whiteListTokens]
        items_per_page = 8
        global global_white_token_page
        if message.chat.id not in global_white_token_page:
            global_white_token_page[message.chat.id] = 1
        current_page = global_white_token_page[message.chat.id]
        
        markup_inline = generate_paginated_markup(items, current_page, items_per_page, "white_tokens")
        text_message = ("Чтобы добавить монету в белый список, напишите её название.\n"
                        "Пример: ETH или eth\n\nВаш белый список:")
        # Удаляем старое сообщение (если есть) и отправляем новое
        if message.chat.id in userEditMsgId:
            try:
                await bot.delete_message(message.chat.id, userEditMsgId[message.chat.id])
            except Exception:
                pass
        sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
        userEditMsgId[message.chat.id] = sent_msg.message_id
        
        await bot.reply_to(message, f'Токен {token_to_add} сохранён в белый список!')
        await bot.delete_state(message.from_user.id, message.chat.id)


@bot.message_handler(state=MyStates.netWhite)
async def netGet_white(message: types.Message):
    if all(x not in message.text.upper() for x in ['⬅️', '⚪️', '⚫️', '⚡️', '💵', '💰', '📜', '💻', '👤', '☁️', '⚙️']):
        userData = await Constants.db.find_one({'id': message.chat.id})
        whiteListNet = userData.get('whiteListNet', [])
        net_to_add = message.text.upper()
        whiteListNet.append(net_to_add)
        await Constants.db.find_one_and_update({'id': message.chat.id}, {'$set': {'whiteListNet': whiteListNet}})
        
        items = [(net, f'remove_net_white_{net}') for net in whiteListNet]
        items_per_page = 8
        global global_white_net_page
        if message.chat.id not in global_white_net_page:
            global_white_net_page[message.chat.id] = 1
        current_page = global_white_net_page[message.chat.id]
        
        markup_inline = generate_paginated_markup(items, current_page, items_per_page, "white_nets")
        text_message = ("Чтобы добавить сеть в белый список, напишите её название.\n"
                        "Пример: BEP20 или bep20\n\nВаш белый список:")
        if message.chat.id in userEditMsgId:
            try:
                await bot.delete_message(message.chat.id, userEditMsgId[message.chat.id])
            except Exception:
                pass
        sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
        userEditMsgId[message.chat.id] = sent_msg.message_id
        
        await bot.reply_to(message, f'Сеть {net_to_add} сохранена в белый список!')
        await bot.delete_state(message.from_user.id, message.chat.id)


@bot.message_handler(state=MyStates.netBlack)
async def netGet_black(message: types.Message):
    if all(x not in message.text.upper() for x in ['⬅️', '⚪️', '⚫️', '⚡️', '💵', '💰', '📜', '💻', '👤', '☁️', '⚙️']):
        userData = await Constants.db.find_one({'id': message.chat.id})
        blackListNet = userData.get('blackListNet', [])
        net_to_add = message.text.upper()
        blackListNet.append(net_to_add)
        await Constants.db.find_one_and_update({'id': message.chat.id}, {'$set': {'blackListNet': blackListNet}})
        
        items = [(net, f'remove_net_black_{net}') for net in blackListNet]
        items_per_page = 8
        global global_black_net_page
        if message.chat.id not in global_black_net_page:
            global_black_net_page[message.chat.id] = 1
        current_page = global_black_net_page[message.chat.id]
        
        markup_inline = generate_paginated_markup(items, current_page, items_per_page, "black_nets")
        text_message = ("Чтобы добавить сеть в чёрный список, напишите её название.\n"
                        "Пример: BEP20 или bep20\n\nВаш чёрный список:")
        if message.chat.id in userEditMsgId:
            try:
                await bot.delete_message(message.chat.id, userEditMsgId[message.chat.id])
            except Exception:
                pass
        sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
        userEditMsgId[message.chat.id] = sent_msg.message_id
        
        await bot.reply_to(message, f'Сеть {net_to_add} сохранена в чёрный список!')
        await bot.delete_state(message.from_user.id, message.chat.id)



@bot.callback_query_handler(func=lambda call: call.data.startswith("page|"))
async def handle_pagination(call):
    """
    Обрабатывает callback данные вида:
      "page_{list_id}_prev_{new_page}" или "page_{list_id}_next_{new_page}"
    """
    try:
        parts = call.data.split("|")
        if len(parts) != 4:
            return
        _, list_id, direction, new_page_str = parts

        new_page = int(new_page_str)
        chat_id = call.message.chat.id
        
        if list_id == "black_tokens":
            userData = await Constants.db.find_one({'id': chat_id})
            tokens = userData.get('blackListTokens', [])
            items = [(token, f'remove_black_{token}') for token in tokens]
            items_per_page = 8
            global global_black_token_page
            global_black_token_page[chat_id] = new_page
            markup = generate_paginated_markup(items, new_page, items_per_page, "black_tokens")
            try:
                await bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            await bot.send_message(chat_id, "Новый список:", reply_markup=markup)

        elif list_id == "white_tokens":
            userData = await Constants.db.find_one({'id': chat_id})
            tokens = userData.get('whiteListTokens', [])
            items = [(token, f'remove_white_{token}') for token in tokens]
            items_per_page = 8
            global global_white_token_page
            global_white_token_page[chat_id] = new_page
            markup = generate_paginated_markup(items, new_page, items_per_page, "white_tokens")
            try:
                await bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            await bot.send_message(chat_id, "Новый список:", reply_markup=markup)

        elif list_id == "white_nets":
            userData = await Constants.db.find_one({'id': chat_id})
            nets = userData.get('whiteListNet', [])
            items = [(net, f'remove_net_white_{net}') for net in nets]
            items_per_page = 8
            global global_white_net_page
            global_white_net_page[chat_id] = new_page
            markup = generate_paginated_markup(items, new_page, items_per_page, "white_nets")
            try:
                await bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            await bot.send_message(chat_id, "Новый список:", reply_markup=markup)
        elif list_id == "black_nets":
            userData = await Constants.db.find_one({'id': chat_id})
            nets = userData.get('blackListNet', [])
            items = [(net, f'remove_net_black_{net}') for net in nets]
            items_per_page = 8
            global global_black_net_page
            global_black_net_page[chat_id] = new_page
            markup = generate_paginated_markup(items, new_page, items_per_page, "black_nets")
            try:
                await bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            await bot.send_message(chat_id, "Новый список:", reply_markup=markup)
        await bot.answer_callback_query(call.id)
    except Exception as e:
        print("Pagination error:", e)
        await bot.answer_callback_query(call.id, "Ошибка пагинации")

@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_black_"))
async def handle_remove_black(call):
    token = call.data[len("remove_black_"):]  # Получаем токен для удаления
    chat_id = call.message.chat.id
    # Получаем текущий список из БД
    userData = await Constants.db.find_one({'id': chat_id})
    blackListTokens = userData.get('blackListTokens', [])
    if token in blackListTokens:
        blackListTokens.remove(token)
        await Constants.db.find_one_and_update({'id': chat_id}, {'$set': {'blackListTokens': blackListTokens}})
    # Обновляем инлайн-разметку
    items = [(t, f"remove_black_{t}") for t in blackListTokens]
    items_per_page = 8
    global global_black_token_page
    current_page = global_black_token_page.get(chat_id, 1)
    markup = generate_paginated_markup(items, current_page, items_per_page, "black_tokens")
    await bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    await bot.answer_callback_query(call.id, text=f"Токен {token} удален")

@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_white_"))
async def handle_remove_white(call):
    token = call.data[len("remove_white_"):]  # Получаем токен для удаления
    chat_id = call.message.chat.id
    userData = await Constants.db.find_one({'id': chat_id})
    whiteListTokens = userData.get('whiteListTokens', [])
    if token in whiteListTokens:
        whiteListTokens.remove(token)
        await Constants.db.find_one_and_update({'id': chat_id}, {'$set': {'whiteListTokens': whiteListTokens}})
    # Обновляем инлайн-разметку для белого списка токенов
    items = [(t, f"remove_white_{t}") for t in whiteListTokens]
    items_per_page = 8
    global global_white_token_page
    current_page = global_white_token_page.get(chat_id, 1)
    markup = generate_paginated_markup(items, current_page, items_per_page, "white_tokens")
    await bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    await bot.answer_callback_query(call.id, text=f"Токен {token} удален из белого списка")

@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_net_black_"))
async def handle_remove_net_black(call):
    net = call.data[len("remove_net_black_"):]  # Получаем сеть для удаления
    chat_id = call.message.chat.id
    userData = await Constants.db.find_one({'id': chat_id})
    blackListNet = userData.get('blackListNet', [])
    if net in blackListNet:
        blackListNet.remove(net)
        await Constants.db.find_one_and_update({'id': chat_id}, {'$set': {'blackListNet': blackListNet}})
    # Обновляем инлайн-разметку для черного списка сетей
    items = [(n, f"remove_net_black_{n}") for n in blackListNet]
    items_per_page = 8
    global global_black_net_page
    current_page = global_black_net_page.get(chat_id, 1)
    markup = generate_paginated_markup(items, current_page, items_per_page, "black_nets")
    await bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    await bot.answer_callback_query(call.id, text=f"Сеть {net} удалена из черного списка")

@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_net_white_"))
async def handle_remove_net_white(call):
    net = call.data[len("remove_net_white_"):]  # Получаем сеть для удаления
    chat_id = call.message.chat.id
    userData = await Constants.db.find_one({'id': chat_id})
    whiteListNet = userData.get('whiteListNet', [])
    if net in whiteListNet:
        whiteListNet.remove(net)
        await Constants.db.find_one_and_update({'id': chat_id}, {'$set': {'whiteListNet': whiteListNet}})
    # Обновляем инлайн-разметку для белого списка сетей
    items = [(n, f"remove_net_white_{n}") for n in whiteListNet]
    items_per_page = 8
    global global_white_net_page
    current_page = global_white_net_page.get(chat_id, 1)
    markup = generate_paginated_markup(items, current_page, items_per_page, "white_nets")
    await bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    await bot.answer_callback_query(call.id, text=f"Сеть {net} удалена из белого списка")



@bot.message_handler(state=MyStates.yellowList)
async def yellowListnGet(message):    
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:  
        if message.text.upper().find('⬅️') == -1 and message.text.upper().find('⚪️') == -1 and message.text.upper().find('⚫️') == -1 and  message.text.upper().find('⚡️') == -1 and message.text.upper().find('💵') == -1 and message.text.upper().find('💰') == -1 and message.text.upper().find('📜') == -1 and message.text.upper().find('💻') == -1 and message.text.upper().find('⚫️') == -1 and message.text.upper().find('👤') == -1:
            markup_inline_edit = types.InlineKeyboardMarkup(row_width=2)
            userDataUpdate = await Constants.dbYellow.find().to_list(length=None)
            whiteListTokens = userDataUpdate[0]['tokens'] 
            whiteListTokens.append(message.text.upper())  
            await Constants.dbYellow.find_one_and_update({'updater':  str(1)},{'$set':{'tokens': whiteListTokens}})
            tokensBtnsArrYellow[message.chat.id].clear()
            if len(whiteListTokens) > 0:
                if len(whiteListTokens) > 15 and len(whiteListTokens)<30:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=4)
                elif len(whiteListTokens) >= 30:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=6)
                elif len(whiteListTokens) <= 15:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=2)
                for token in whiteListTokens:
                    btnUpdate = f'УбратьМонетуYellow|{token}'
                    tokensBtnsArrYellow[message.chat.id].append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                    tokensBtnsArrYellow[message.chat.id].append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                markup_inline_edit.add(*tokensBtnsArrYellow[message.chat.id])
            blacklistTokensAnsw = 'Чтобы добавить монету в список "Осторожно", напишите её название.\nПример: ETH или eth\n\nВаш белый список:'
            await bot.edit_message_reply_markup(message.chat.id, userEditMsgId[message.chat.id], blacklistTokensAnsw, reply_markup=markup_inline_edit)
            await bot.reply_to(message, f'Токен {message.text} сохранен в белый список!')
            await bot.delete_state(message.from_user.id, message.chat.id) 




@bot.message_handler(state=MyStates.globalBlackList)
async def globalBlackListGet(message):    
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:  
        if message.text.upper().find('⬅️') == -1 and message.text.upper().find('⚪️') == -1 and message.text.upper().find('⚫️') == -1 and  message.text.upper().find('⚡️') == -1 and message.text.upper().find('💵') == -1 and message.text.upper().find('💰') == -1 and message.text.upper().find('📜') == -1 and message.text.upper().find('💻') == -1 and message.text.upper().find('⚫️') == -1 and message.text.upper().find('👤') == -1:
            markup_inline_edit = types.InlineKeyboardMarkup(row_width=2)
            userDataUpdate = await Constants.dbBlack.find().to_list(length=None)
            whiteListTokens = userDataUpdate[0]['tokens']
            whiteListTokens.append(message.text.upper())  
            await Constants.dbBlack.find_one_and_update({'updater':  str(1)},{'$set':{'tokens': whiteListTokens}})
            tokensBtnsArrAllGlobalBlackList[message.chat.id].clear()
            if len(whiteListTokens) > 0:
                if len(whiteListTokens) > 15 and len(whiteListTokens)<30:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=4)
                elif len(whiteListTokens) >= 30:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=6)
                elif len(whiteListTokens) <= 15:
                    markup_inline_edit = types.InlineKeyboardMarkup(row_width=2)
                for token in whiteListTokens:
                    btnUpdate = f'УбратьМонетуGlobalBlack|{token}'
                    tokensBtnsArrAllGlobalBlackList[message.chat.id].append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                    tokensBtnsArrAllGlobalBlackList[message.chat.id].append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                markup_inline_edit.add(*tokensBtnsArrAllGlobalBlackList[message.chat.id])
            blacklistTokensAnsw = 'Чтобы добавить монету в ГЛОБАЛЬНЫЙ черный список, напишите её название.\nПример: ETH или eth\n\nВаш белый список:'
            await bot.edit_message_reply_markup(message.chat.id, userEditMsgId[message.chat.id], blacklistTokensAnsw, reply_markup=markup_inline_edit)
            await bot.reply_to(message, f'Токен {message.text} сохранен в белый список!')
            await bot.delete_state(message.from_user.id, message.chat.id) 



async def getDataFromMongo():
    try:
        signals = cache.get('signals')
        return pickle.loads(gzip.decompress(signals))
    except Exception as e:
        print(f"error get_signal_volumes: {e}")
        return None


# --- Работа с дедуплицированным снапшотом сигналов ---
#
# Раньше в кэше лежал плоский список, где ОДИН И ТОТ ЖЕ сигнал повторялся для
# каждого получателя со всеми 25 полями. При 5000 пользователей это 210 000
# словарей и ~143 МБ. Теперь снапшот выглядит так:
#
#   {'version': 2,
#    'payloads':   {base_key: {25 полей, без userId и datetime}},
#    'recipients': {base_key: {user_id: datetime первого показа}}}
#
# Функции ниже отдают ровно такие же словари сигналов, как прежний формат,
# поэтому остальная логика бота осталась без изменений. Старый плоский список
# тоже поддерживается — на случай, если в кэше ещё лежит он.

def _is_packed(data):
    return isinstance(data, dict) and data.get('version') == 2


def _build_signal(payload, user_id, moment):
    """Достраивает полный словарь сигнала — в том же виде, что был раньше."""
    signal = {'datetime': moment, 'userId': user_id}
    signal.update(payload)
    return signal


def expandSignals(data):
    """Отдаёт сигналы по одному, не материализуя весь список в памяти."""
    if not data:
        return
    if not _is_packed(data):
        for signal in data:
            yield signal
        return
    payloads = data['payloads']
    for base_key, recipients in data['recipients'].items():
        payload = payloads.get(base_key)
        if payload is None:
            continue
        for user_id, moment in recipients.items():
            yield _build_signal(payload, user_id, moment)


def countSignals(data):
    if not data:
        return 0
    if not _is_packed(data):
        return len(data)
    return sum(len(users) for users in data['recipients'].values())


def signalsForUser(data, userId):
    """Только сигналы одного пользователя.

    Раньше «Активные связки» перебирали сигналы ВСЕХ пользователей и отсеивали
    чужие уже внутри обработчика — то есть на каждое открытие меню создавалось
    столько задач, сколько всего пар «сигнал × получатель». Результат тот же,
    но работы на порядки меньше.
    """
    if not data:
        return []
    target = str(userId)
    if not _is_packed(data):
        return [s for s in data if str(s['userId']) == target]

    payloads = data['payloads']
    result = []
    for base_key, recipients in data['recipients'].items():
        payload = payloads.get(base_key)
        if payload is None:
            continue
        for user_id, moment in recipients.items():
            if str(user_id) == target:
                result.append(_build_signal(payload, user_id, moment))
                break
    return result


def findUserSignal(data, userId, symbol, min_exchange, max_exchange, networkSumm):
    """Ищет конкретный сигнал пользователя.

    В новом формате это прямой доступ по ключу вместо перебора всего списка:
    было O(сигналы × пользователи), стало O(1).
    """
    if not data:
        return None
    if _is_packed(data):
        base_key = f'signal_{min_exchange}_{max_exchange}_{symbol}_{networkSumm}'
        payload = data['payloads'].get(base_key)
        if payload is None:
            return None
        recipients = data['recipients'].get(base_key, {})
        # userId приходит из callback-данных то числом, то строкой, а ключи
        # словаря получателей — числа. Прямой .get() на этом молча промахивался
        # и сигнал «не разворачивался». Прежний код сравнивал через str(), то
        # есть тип не имел значения — восстанавливаем это поведение.
        moment = recipients.get(userId)
        if moment is None:
            target = str(userId)
            for stored_id, stored_moment in recipients.items():
                if str(stored_id) == target:
                    return _build_signal(payload, stored_id, stored_moment)
            return None
        return _build_signal(payload, userId, moment)

    for signal in data:
        if (str(signal['userId']) == str(userId)
                and str(signal['symbol']) == str(symbol)
                and str(signal['min_exchange']) == str(min_exchange)
                and str(signal['max_exchange']) == str(max_exchange)
                and str(signal['networkSumm']) == str(networkSumm)):
            return signal
    return None


@bot.message_handler(content_types=['text'])
async def handler(message:types.Message):
    ex = await Constants.db.find_one({'id':message.chat.id})
    userexchangesMin = ex['minEx']
    userexchangesMax = ex['maxEx'] 
    blackListTokens = ex['blackListTokens']
    whiteListTokens = ex['blackListTokens']
    blackListNet = ex['blackListNet']
    whiteListNet = ex['whiteListNet']
    blacklistTokensAnsw = 'Чтобы добавить монету в черный список, напишите её название.\nПример: ETH или eth\n\nВаш черный список:'
    blacklistNetAnsw = 'Чтобы добавить сеть в черный список, напишите её название.\nПример: BEP20 или bep20\n\nВаш черный список:'
    blacklistTokensAnswWhite = 'Чтобы добавить монету в белый список, напишите её название.\nПример: ETH или eth\n\nВаш белый список:'
    blacklistNetAnswWhite = 'Чтобы добавить сеть в белый список, напишите её название.\nПример: BEP20 или bep20\n\nВаш белый список:'

    global netBtnsArrGlobalBlack
    global tokensBtnsArrGlobalBlack
    global netBtnsArrGlobalWhite
    global tokensBtnsArrGlobalWhite
    global globalMsgMenuLink
    try:
        if "УДАЛИТЬ|" in message.text.upper():
                if is_owner(message.chat.id):
                    pair = message.text.upper().split('|')[1]
                    try:
                        pairs = await Constants.client.renderBase.black_list.find().to_list(length=None)
                        await Constants.client.renderBase.black_list.update_one({"_id":pairs[0]["_id"]},{"$pull":{"list":pair}})
                        await bot.send_message(message.chat.id,f"Токен {pair} удален")
                    except Exception as e:
                        print(f'Error in delete pair:{e}')
                        await bot.send_message(message.chat.id,"Ошибка при удалении токена")
            
        elif "ДОБАВИТЬ|" in message.text.upper():
                if is_owner(message.chat.id):
                    pair = message.text.upper().split('|')[1]
                    try:
                        pairs = await Constants.client.renderBase.black_list.find().to_list(length=None)
                        if pair in pairs[0]['list']:
                            await bot.send_message(message.chat.id,f"Токен {pair} уже числится в базе")
                            return

                        await Constants.client.renderBase.black_list.update_one({"_id":pairs[0]["_id"]},{"$push":{"list":pair}})
                        await bot.send_message(message.chat.id,f"Токен {pair} добавлен в базу")
                    except Exception as e:
                        print(f'Error in add pair:{e}')
                        await bot.send_message(message.chat.id,"Ошибка при добавлении токена")
                        
        match message.text:
            case  "🔐Добавить пользователя":
                if is_owner(message.chat.id):
                    try:
                        await bot.set_state(message.from_user.id, MyStates.addUser, message.chat.id)
                        await bot.send_message(message.chat.id, 'Введите ID пользователя:')
                    except telebot.asyncio_helper.ApiException as e:
                        if "bot was blocked by the user" in str(e):
                            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                            await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                        pass 
                
            case "🔪Удалить пользователя":
                if is_owner(message.chat.id):
                    try:
                        await bot.set_state(message.from_user.id, MyStates.delUser, message.chat.id)
                        await bot.send_message(message.chat.id, 'Введите ID пользователя:')
                    except telebot.asyncio_helper.ApiException as e:
                        if "bot was blocked by the user" in str(e):
                            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                            await Constants.db.find_one_and_delete({'id':int(message.chat.id)})
                        pass 
            
            case "Глобальный ЧС":
                if is_owner(message.chat.id):
                    pairs = await Constants.client.renderBase.black_list.find().to_list(length=None)
                    await bot.send_message(message.chat.id,f"Глобальный черный список:\n\n{pairs[0]['list']}")

            


            case "❗️ Осторожно список":
                if is_owner(message.chat.id):
                    try:
                        await bot.set_state(message.from_user.id, MyStates.yellowList, message.chat.id)
                        
                        dbYellowList = await Constants.dbYellow.find().to_list(length=None)
                        blackListTokens = dbYellowList[0]['tokens']
                        tokensBtnsArrBlack = []
                        
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        if len(blackListTokens) > 0:
                            if len(blackListTokens) > 15 and len(blackListTokens)<30:
                                markup_inline = types.InlineKeyboardMarkup(row_width=4)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуYellow|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                            elif len(blackListTokens) > 30:
                                markup_inline = types.InlineKeyboardMarkup(row_width=6)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуYellow|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                            elif len(blackListTokens) < 15:
                                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуYellow|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))

                            markup_inline.add(*tokensBtnsArrBlack)
                            willEditMsg =  await bot.send_message(chat_id= message.chat.id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                            userEditMsgId[message.chat.id] = willEditMsg.message_id
                        else:
                            markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                            markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных монет', callback_data='Нет добавленных монет'))
                            willEditMsg =  await bot.send_message(chat_id= message.chat.id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                            userEditMsgId[message.chat.id] = willEditMsg.message_id

                        
                        tokensBtnsArrYellow[message.chat.id] = tokensBtnsArrBlack
                        
                    except Exception as e:
                        print('QueryTokenError', e) 

            case "⛔️ Глобальный ЧС":
                if is_owner(message.chat.id):
                    try:
                        await bot.set_state(message.from_user.id, MyStates.globalBlackList, message.chat.id)
                        
                        dbBlackList = await Constants.dbBlack.find().to_list(length=None)
                        blackListTokens = dbBlackList[0]['tokens']
                        tokensBtnsArrBlack = []
                        
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        if len(blackListTokens) > 0:
                            if len(blackListTokens) > 8 and len(blackListTokens)<16:
                                markup_inline = types.InlineKeyboardMarkup(row_width=4)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуGlobalBlack|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                            elif len(blackListTokens) > 16:
                                markup_inline = types.InlineKeyboardMarkup(row_width=6)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуGlobalBlack|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                            elif len(blackListTokens) < 8:
                                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                                for token in blackListTokens:
                                    btnUpdate = f'УбратьМонетуGlobalBlack|{token}'
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                                    tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))

                            markup_inline.add(*tokensBtnsArrBlack)
                            willEditMsg =  await bot.send_message(chat_id= message.chat.id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                            userEditMsgId[message.chat.id] = willEditMsg.message_id
                        else:
                            markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                            markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных монет', callback_data='Нет добавленных монет'))
                            willEditMsg =  await bot.send_message(chat_id= message.chat.id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                            userEditMsgId[message.chat.id] = willEditMsg.message_id

                        
                        tokensBtnsArrAllGlobalBlackList[message.chat.id] = tokensBtnsArrBlack
                        
                    except Exception as e:
                        print('QueryTokenError', e)        

            case "Статистика DB":
                if is_owner(message.chat.id):
                    try:
                        # Получение списка уникальных бирж из ключей кэша
                        def get_completed_data():
                            cache_keys = ['blocks','signals','volumes']
                            time_keys = ['time_blocks','time_signals','time_volumes']
                            blocks,signals,volumes = [len(pickle.loads(gzip.decompress(cache.get(key)))) for key in cache_keys]
                            time_b,time_s,time_v = [pickle.loads(gzip.decompress(cache.get(key))) for key in time_keys]
                            return blocks,signals,volumes,time_b,time_s,time_v
                        
                        blocks,signals,volumes,time_b,time_s,time_v = get_completed_data()
                        print(blocks,signals,volumes,time_b,time_s,time_v)
                        exes = {}
                        books = {}
                        keys=[]
                        for exchange in allExchanges:
                            try:
                              key = ''
                              if exchange == 'MEXC Global':
                                
                                  key = f'orderbook_MEXC_full_snapshot'
                              else:
                                  key = f'orderbook_{exchange}_full_snapshot'
                            
                              value = cache.get(key)

                              if value is not None:
                                  value = pickle.loads(gzip.decompress(value))
                                  exes[exchange]=len(value.keys())
                                  books[exchange]=value.copy()
                              else:
                                  pass
                            except Exception as e:
                                  print(e)
                                  
                        print(blocks,signals,volumes)
                                

                        # Получение количества стаканов для каждой биржи
                        

                            
                        # Получение lastUpdate, первого ордера asks и первого ордера bids для токена BTC на каждой бирже
                        btc_data = {}
                        """ for exchange in exes.keys():
                            try:
                                symbol_formatted = {
                                'OKX': 'BTC-USDT',
                                'Kucoin': 'STRK-USDT',
                                'Gate': 'FLOW_USDT',
                                'MEXC Global': 'SUSHIUSDT',
                                'Bitget': 'XAIUSDT',
                                'Bitmart': 'BTC_USDT',
                                'Bybit': 'MAKUSDT',
                                'BingX': 'BTC-USDT',
                                'HTX': 'sparkletusdt',
                                'CoinEx': 'FUDUSDT'
                                }.get(exchange, 'BTC/USDT')
                                cache_key = f'orderbook_{exchange}_{symbol_formatted}'
                                order_book_data = books[exchange][symbol_formatted]
                                if order_book_data:
                                    btc_data[exchange] = {
                                        'lastUpdate': order_book_data['lastUpdate'],
                                        'asks': order_book_data['asks'][2] if order_book_data['asks'] else None,
                                        'bids': order_book_data['bids'][2] if order_book_data['bids'] else None
                                    }
                            except Exception as e:
                                 print(e)  
 """
                        # Получение количества пользователей в базе данных
                        user_count = await Constants.db.count_documents({})

                        # Формирование сообщения со статистикой
                        stats_message = f"Статистика базы данных:\n\nLast lap stats:\nCombinations: {blocks}  Time: {time_b}\nVolumes by orderbooks: {volumes} Time: {time_v}\nSummary signals for all users: {signals} Time: {time_s}\n\n"
                        
                        stats_message += "Количество стаканов по биржам:\n"
                        for exchange, count in exes.items():
                            stats_message += f"{exchange}: {count}\n"
                            
                        """ stats_message += "\nLastUpdate для BTC по биржам:\n"
                        for exchange, data in btc_data.items():
                            stats_message += f"{exchange}: {data['lastUpdate']}\n" """
                        
                        stats_message += f"\nКоличество пользователей: {user_count}"

                        await bot.send_message(message.chat.id, stats_message)

                    except Exception as e:
                        print(f"Error getting database statistics: {e}")
                        await bot.send_message(message.chat.id, "Произошла ошибка при получении статистики базы данных.")




       

            case "⚙️Настройки":
                markup_inline_edit = types.ReplyKeyboardMarkup(row_width=2)
                button4 = types.KeyboardButton("💵Минимальный спред $")
                button5 = types.KeyboardButton("💵Минимальный спред %")  
                button6 = types.KeyboardButton("💰Минимальный объем")
                button7 = types.KeyboardButton("💰Максимальный объем")
                button9 = types.KeyboardButton("🪝Комиссии")
                button10 = types.KeyboardButton("🚨Настройка риска")
                button11 = types.KeyboardButton("⬅️Главное меню")
                markup_inline_edit.add(button4, button5, button6, button7, button9, button10, button11)
                try:
                    await bot.send_message(message.chat.id, 'Настройки:', reply_markup=markup_inline_edit)  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass  
            case "🚨Настройка риска":  
                try:
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    markup_inline = types.InlineKeyboardMarkup(row_width=1)
                    btn1Text = 'Рискованный✅' if ex['typeSignal'] == 'False' else 'Рискованный'
                    btn2Text = 'Безопасный✅' if ex['typeSignal'] == 'True' else 'Безопасный'
                    btn3Text = 'Только избранное✅' if ex['typeSignal'] == 'onlyTop' else 'Только избранное'
                    btn1 = types.InlineKeyboardButton(text=btn1Text, callback_data='typeSignalFalse') 
                    btn2 = types.InlineKeyboardButton(text=btn2Text, callback_data='typeSignalTrue') 
                    btn3 = types.InlineKeyboardButton(text=btn3Text, callback_data='typeSignalTop') 
                    markup_inline.add(btn1, btn2, btn3)
                    
                    await bot.send_message(message.chat.id, 'Выберите режим торговли\n\n❗️В рискованном режиме отображаются сигналы с закрытыми вводами и выводами.', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass          
            case "⬅️Главное меню":  
                mm = types.ReplyKeyboardMarkup(row_width=2)
                button2 = types.KeyboardButton("⚡️Биржи")
                button3 = types.KeyboardButton("📜Активные связки") 
                button10 = types.KeyboardButton("💻Мониторинг")  
                button11 = types.KeyboardButton("👤Профиль") 
                button1 = types.KeyboardButton("⚙️Настройки")
                button9 = types.KeyboardButton("☁️Сохраненные токены/сети")
                
                if is_owner(message.chat.id):
                    button12 = types.KeyboardButton("🔐Добавить пользователя") 
                    button13 = types.KeyboardButton("🔪Удалить пользователя")
                    #button14 = types.KeyboardButton("❗️ Осторожно список") 
                    #button15 = types.KeyboardButton("⛔️ Глобальный ЧС") 
                    button16 = types.KeyboardButton("Статистика DB")
                    mm.add(button11, button3, button2,  button10, button1, button9, button12, button13, button16)
                    try:
                        await bot.send_message(message.chat.id, 'Главное меню', reply_markup=mm)  
                    except telebot.asyncio_helper.ApiException as e:
                        if "bot was blocked by the user" in str(e):
                            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                            await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                        pass 
                else:   
                    mm.add(button11, button3, button2, button1, button10, button9)
                    try:
                        await bot.send_message(message.chat.id, 'Главное меню', reply_markup=mm)  
                    except telebot.asyncio_helper.ApiException as e:
                        if "bot was blocked by the user" in str(e):
                            print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                            await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                        pass 

            case "👤Профиль":
                try:
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    monitoringText = ""
                    if ex['monitoring'] and ex['monitoringTop']:
                        monitoringText = 'Включен ✅'
                    elif ex['monitoring'] and not(ex['monitoringTop']):     
                        monitoringText = 'Включен ✅'
                    elif not(ex['monitoring']) and ex['monitoringTop']:  
                        monitoringText = 'Только избранные ✅'
                    else:
                        monitoringText = 'Выключен ❌' 
                                

                    riskText = ""
                    if ex['typeSignal'] == 'False':
                        riskText = 'Рискованный ❗️'
                    elif ex['typeSignal'] == 'True':
                        riskText = 'Безопасный ❇️'
                    else:
                        riskText = 'Только избранное ⭐️'  

                    answMsg = f"<b>Профиль:</b>\n\nМониторинг: {monitoringText}\n\nРежим: {riskText}\n\nМинимальный спред: {ex['minDollarSpread']}$ / {ex['minProcentSpread']}%\n\nРассматриваемый объем: {ex['minVolume']}$ -> {ex['maxVolume']}$\n\nМаксимальная комиссия: {ex['maxFeeVal']}$"
                    await bot.send_message(message.chat.id, answMsg, parse_mode="HTML",)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 
            case "💻Мониторинг":
                try:
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    markup_inline = types.InlineKeyboardMarkup(row_width=3)
                    btn1Text = 'Вкл✅' if ex['monitoring'] else 'Вкл'
                    btn2Text = 'Выкл✅' if not ex['monitoring'] else 'Выкл'
                    btn3Text = 'Вкл✅' if ex['monitoringTop'] else 'Вкл'
                    btn4Text = 'Выкл✅' if not ex['monitoringTop'] else 'Выкл'
                    btn1 = types.InlineKeyboardButton(text='Все связки', callback_data='Все связки') 
                    btn2 = types.InlineKeyboardButton(text=btn1Text, callback_data='Мониторинг') 
                    btn3 = types.InlineKeyboardButton(text=btn2Text, callback_data='Мониторинг') 
                    btn4 = types.InlineKeyboardButton(text='Избранные', callback_data='Избранные связки') 
                    btn5 = types.InlineKeyboardButton(text=btn3Text, callback_data='МониторингТоп') 
                    btn6 = types.InlineKeyboardButton(text=btn4Text, callback_data='МониторингТоп') 
                    markup_inline.add(btn1, btn2, btn3, btn4, btn5, btn6) 
                    
                    await bot.send_message(message.chat.id, 'Включить или выключить мониторинг сигналов:', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 


            case "☁️Сохраненные токены/сети":
                markup_inline = types.ReplyKeyboardMarkup(row_width=2)
                btn1 = types.KeyboardButton(text='⚪️Белый список') 
                btn2 = types.KeyboardButton(text='⚫️Черный список') 
                btn3 = types.KeyboardButton(text='⬅️Главное меню')
                markup_inline.add(btn1, btn2, btn3)
                try:
                    await bot.send_message(message.chat.id, 'Выберите режим сохранения:', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

            case "⚪️Белый список":
                markup_inline = types.ReplyKeyboardMarkup(row_width=2)
                btn1 = types.KeyboardButton(text='Монеты ⚪️') 
                btn2 = types.KeyboardButton(text='Сети ⚪️') 
                btn3 = types.KeyboardButton(text='⬅️Назад')
                markup_inline.add(btn1, btn2, btn3)
                try:
                    await bot.send_message(message.chat.id, 'Выберите, что хотите добавить в белый список:', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

            case "⚫️Черный список":
                markup_inline = types.ReplyKeyboardMarkup(row_width=2)
                btn1 = types.KeyboardButton(text='Монеты ⚫️') 
                btn2 = types.KeyboardButton(text='Сети ⚫️') 
                btn3 = types.KeyboardButton(text='⬅️Назад')
                markup_inline.add(btn1, btn2, btn3)
                try:
                    await bot.send_message(message.chat.id, 'Выберите, что хотите добавить в чёрный список:', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass   

            case "⬅️Назад":
                markup_inline = types.ReplyKeyboardMarkup(row_width=2)
                btn1 = types.KeyboardButton(text='⚪️Белый список') 
                btn2 = types.KeyboardButton(text='⚫️Черный список') 
                btn3 = types.KeyboardButton(text='⬅️Главное меню')
                markup_inline.add(btn1, btn2, btn3)
                try:
                    await bot.send_message(message.chat.id, 'Выберите режим сохранения:', reply_markup=markup_inline)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

            case "Монеты ⚪️":
                try:
                    # Переходим в состояние добавления токена в белый список
                    await bot.set_state(message.from_user.id, MyStates.tokenStateWhite, message.chat.id)
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    # Получаем список белых токенов (если ключ отсутствует – пустой список)
                    whiteListTokens = ex.get('whiteListTokens', [])
                    # Формируем список кортежей: (текст кнопки, callback для удаления)
                    items = [(token, f'remove_white_{token}') for token in whiteListTokens]
                    items_per_page = 8
                    global global_white_token_page
                    if message.chat.id not in global_white_token_page:
                        global_white_token_page[message.chat.id] = 1
                    current_page = global_white_token_page[message.chat.id]
                    
                    # Формируем постраничную клавиатуру
                    markup_inline = generate_paginated_markup(items, current_page, items_per_page, "white_tokens")
                    text_message = ('Чтобы добавить монету в белый список, напишите её название.\n'
                                    'Пример: ETH или eth\n\nВаш белый список:')
                    
                    # Если уже есть предыдущее сообщение для белого списка токенов – удаляем его
                    if message.chat.id in edit_msg_ids['white_tokens']:
                        try:
                            await bot.delete_message(message.chat.id, edit_msg_ids['white_tokens'][message.chat.id])
                        except Exception:
                            pass
                    sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
                    edit_msg_ids['white_tokens'][message.chat.id] = sent_msg.message_id

                    
                except Exception as e:
                    print('QueryTokenError (white tokens):', e)
   

            case "Монеты ⚫️":
                try:
                    await bot.set_state(message.from_user.id, MyStates.tokenStateBlack, message.chat.id)
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    blackListTokens = ex.get('blackListTokens', [])
                    items = [(token, f'remove_black_{token}') for token in blackListTokens]
                    items_per_page = 8
                    global global_black_token_page
                    if message.chat.id not in global_black_token_page:
                        global_black_token_page[message.chat.id] = 1
                    current_page = global_black_token_page[message.chat.id]
                    
                    markup_inline = generate_paginated_markup(items, current_page, items_per_page, "black_tokens")
                    text_message = ('Чтобы добавить монету в черный список, напишите её название.\n'
                                    'Пример: ETH или eth\n\nВаш черный список:')
                    
                    # Если уже есть предыдущее сообщение для белого списка токенов – удаляем его
                    if message.chat.id in edit_msg_ids['black_tokens']:
                        try:
                            await bot.delete_message(message.chat.id, edit_msg_ids['black_tokens'][message.chat.id])
                        except Exception:
                            pass
                    sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
                    edit_msg_ids['black_tokens'][message.chat.id] = sent_msg.message_id

                    
                except Exception as e:
                    print('QueryTokenError (black tokens):', e)


            case "Сети ⚫️":
                try:
                    await bot.set_state(message.from_user.id, MyStates.netBlack, message.chat.id)
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    blackListNet = ex.get('blackListNet', [])
                    items = [(net, f'remove_net_black_{net}') for net in blackListNet]
                    items_per_page = 8
                    global global_black_net_page
                    if message.chat.id not in global_black_net_page:
                        global_black_net_page[message.chat.id] = 1
                    current_page = global_black_net_page[message.chat.id]
                    
                    markup_inline = generate_paginated_markup(items, current_page, items_per_page, "black_nets")
                    text_message = ('Чтобы добавить сеть в черный список, напишите её название.\n'
                                    'Пример: BEP20 или bep20\n\nВаш черный список:')
                    
                    # Если уже есть предыдущее сообщение для белого списка токенов – удаляем его
                    if message.chat.id in edit_msg_ids['black_nets']:
                        try:
                            await bot.delete_message(message.chat.id, edit_msg_ids['black_nets'][message.chat.id])
                        except Exception:
                            pass
                    sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
                    edit_msg_ids['black_nets'][message.chat.id] = sent_msg.message_id

                    
                except Exception as e:
                    print('QueryNetError (black nets):', e)

            

            case "Сети ⚪️":
                try:
                    await bot.set_state(message.from_user.id, MyStates.netWhite, message.chat.id)
                    ex = await Constants.db.find_one({'id': message.chat.id})
                    whiteListNet = ex.get('whiteListNet', [])
                    items = [(net, f'remove_net_white_{net}') for net in whiteListNet]
                    items_per_page = 8
                    global global_white_net_page
                    if message.chat.id not in global_white_net_page:
                        global_white_net_page[message.chat.id] = 1
                    current_page = global_white_net_page[message.chat.id]
                    
                    markup_inline = generate_paginated_markup(items, current_page, items_per_page, "white_nets")
                    text_message = ('Чтобы добавить сеть в белый список, напишите её название.\n'
                                    'Пример: BEP20 или bep20\n\nВаш белый список:')
                    
                    # Если уже есть предыдущее сообщение для белого списка токенов – удаляем его
                    if message.chat.id in edit_msg_ids['white_nets']:
                        try:
                            await bot.delete_message(message.chat.id, edit_msg_ids['white_nets'][message.chat.id])
                        except Exception:
                            pass
                    sent_msg = await bot.send_message(message.chat.id, text_message, reply_markup=markup_inline)
                    edit_msg_ids['white_nets'][message.chat.id] = sent_msg.message_id

                    
                except Exception as e:
                    print('QueryNetError (white nets):', e)

                                

            case "📜Активные связки":
                global processMsgAns
                try:
                    
                    markup_inline = types.InlineKeyboardMarkup()
                    # Берём сразу только свои сигналы: обработчик всё равно
                    # отбрасывал чужие, но уже после создания задачи на каждый.
                    newData = signalsForUser(await getDataFromMongo(), message.chat.id)

                    globalProcessActList.setdefault(message.chat.id, 0)
                    globalProcessActList[message.chat.id] = 0
                    
                    processMsgAns = await send_progress_message(message.chat.id, globalProcessActList[message.chat.id]) 
                    
                    userDataArrFull = await cointainerActiveList(message, newData, True, processMsgAns)
                    print(userDataArrFull,message.chat.id)
                    userDataArr = userDataArrFull[0]
                    
                    if not (userDataArr is None):
                        userData = await Constants.db.find_one({'id':message.chat.id})
                        
                        for i in userDataArr[0]: 
                            markup_inline.add(i)
                        
                        listCount = divmod(len(userDataArrFull[1]),8)[0]
                        if divmod(len(userDataArrFull[1]),8)[1]!=0:
                            listCount+=1 
                                                                                                                                                       
                        nav = []
                        nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                        nav.append(types.InlineKeyboardButton(text=f'1/{listCount}', callback_data='.'))
                        nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                    
                        markup_inline.add(*nav)
                        markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                        try:
                            countFirstLists = (listCount-1)*8
                            await update_progress_message(message.chat.id, processMsgAns, (countFirstLists+(len(userDataArrFull[1])-countFirstLists)))
                            await bot.send_message(message.chat.id, 'Активные связки:', reply_markup=markup_inline)
                            
                        except telebot.asyncio_helper.ApiException as e:
                            if "bot was blocked by the user" in str(e):
                                print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                                await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                            pass
                        except Exception as e:
                            print('Err ', e)
                    else:
                        nav = []
                        nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                        nav.append(types.InlineKeyboardButton(text=f'0/0', callback_data='.'))
                        nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                    
                    
                        markup_inline.add(*nav)
                        markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                        try:
                            await bot.send_message(message.chat.id, 'Активные связки:', reply_markup=markup_inline)
                            
                        except telebot.asyncio_helper.ApiException as e:
                            if "bot was blocked by the user" in str(e):
                                print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                                await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                            pass
                        except Exception as e:
                            print('Err ', e)
                except Exception as e:
                    print('Sent active order err:', e,message.chat.id)  
                

            case "💰Минимальный объем":  
                try:
                    await bot.set_state(message.from_user.id, MyStates.minVolUser, message.chat.id)
                    await bot.send_message(message.chat.id, 'Введите минимальный объем в USDT:')  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

            case "💰Максимальный объем":  
                try:
                    max = await bot.set_state(message.from_user.id, MyStates.maxVolUser, message.chat.id)
                    
                    await bot.send_message(message.chat.id, 'Введите максимальный объем в USDT:')  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass  

            case "💵Минимальный спред $":  
                try:
                    await bot.set_state(message.from_user.id, MyStates.minSpreadUser, message.chat.id)
                    
                    await bot.send_message(message.chat.id, 'Введите значение минимального спреда в $:')  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass      

            case "💵Минимальный спред %":  
                try:
                    await bot.set_state(message.from_user.id, MyStates.minProcentUser, message.chat.id)
                    await bot.send_message(message.chat.id, 'Введите значение минимального спреда в %:')  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass 

            case "🪝Комиссии": 
                try: 
                    await bot.set_state(message.from_user.id, MyStates.feesUser, message.chat.id)
                    await bot.send_message(message.chat.id, 'Введите значение максимальной комиссии в $:')  
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass                              

            case "⚡️Биржи":
                try:
                    ex = await Constants.db.find_one({'id':message.chat.id})
                    if not (ex is None):
                        userexchangesMin = ex['minEx']
                        userexchangesMax = ex['maxEx']  
                        if not (userexchangesMin is None) and not (userexchangesMin is None):
                            menu = await generateMenu(userexchangesMin, userexchangesMax)

                        await bot.send_message(message.chat.id, 'Выберите биржи:', reply_markup=menu)
                except telebot.asyncio_helper.ApiException as e:
                    if "bot was blocked by the user" in str(e):
                        print(f"Не удалось отправить сообщение, так как пользователь {message.chat.id} заблокировал бота")
                        await Constants.db.find_one_and_delete({'id': int(message.chat.id)})
                    pass  
                
    except Exception as e:
            print(f"Error handler: {str(e)}")
            pass    




@bot.callback_query_handler(func=lambda call: True)
async def callback_inline(call):
    
    try:
        ex = await Constants.db.find_one({'id':call.message.chat.id})
        globalYellowList = await Constants.dbYellow.find().to_list(length=None)
        globalBlackList = await Constants.dbBlack.find().to_list(length=None)

        userexchangesMin = ex['minEx']
        userexchangesMax = ex['maxEx'] 

        blackListTokens = ex['blackListTokens']
        whiteListTokens = ex['whiteListTokens']
        blackListNet = ex['blackListNet']
        whiteListNet = ex['whiteListNet']
        blacklistTokensAnsw = 'Чтобы добавить монету в черный список, напишите её название.\nПример: ETH или eth\n\nВаш черный список:'
        whitelistTokensAnsw = 'Чтобы добавить монету в белый список, напишите её название.\nПример: ETH или eth\n\nВаш белый список:'
        blacklistNetAnsw = 'Чтобы добавить сеть в черный список, напишите её название.\nПример: BEP20 или bep20\n\nВаш черный список:'
        whitelistNetAnsw = 'Чтобы добавить сеть в белый список, напишите её название.\nПример: BEP20 или bep20\n\nВаш белый список:'
        tokensBtnsArrBlack = []
        tokensBtnsArrWhite = []
        netBtnsArrBlack = []
        netBtnsArrWhite = []
        globalBlacListNet = globalYellowList[0]['tokens']
        yellowListNet = globalBlackList[0]['tokens']
        yellowBtnsArr = []
        globalBalckBtnsArr = []

        try:
            if call.data == 'Мониторинг':
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'monitoring': not ex['monitoring']}})           

                markup_inline = types.InlineKeyboardMarkup(row_width=3)
                btn1Text = 'Вкл✅' if not ex['monitoring'] else 'Вкл'
                btn2Text = 'Выкл✅' if ex['monitoring'] else 'Выкл'
                btn3Text = 'Вкл✅' if ex['monitoringTop'] else 'Вкл'
                btn4Text = 'Выкл✅' if not ex['monitoringTop'] else 'Выкл'
                btn1 = types.InlineKeyboardButton(text='Все связки', callback_data='Все связки') 
                btn2 = types.InlineKeyboardButton(text=btn1Text, callback_data='Мониторинг') 
                btn3 = types.InlineKeyboardButton(text=btn2Text, callback_data='Мониторинг') 
                btn4 = types.InlineKeyboardButton(text='Избранные', callback_data='Избранные связки') 
                btn5 = types.InlineKeyboardButton(text=btn3Text, callback_data='МониторингТоп') 
                btn6 = types.InlineKeyboardButton(text=btn4Text, callback_data='МониторингТоп') 
                markup_inline.add(btn1, btn2, btn3, btn4, btn5, btn6) 

                await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Включить или выключить мониторинг сигналов:', reply_markup=markup_inline)      

            elif call.data == 'МониторингТоп':
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'monitoringTop': not ex['monitoringTop']}})           

                markup_inline = types.InlineKeyboardMarkup(row_width=3)
                btn1Text = 'Вкл✅' if ex['monitoring'] else 'Вкл'
                btn2Text = 'Выкл✅' if not ex['monitoring'] else 'Выкл'
                btn3Text = 'Вкл✅' if not ex['monitoringTop'] else 'Вкл'
                btn4Text = 'Выкл✅' if ex['monitoringTop'] else 'Выкл'
                btn1 = types.InlineKeyboardButton(text='Все связки', callback_data='Все связки') 
                btn2 = types.InlineKeyboardButton(text=btn1Text, callback_data='Мониторинг') 
                btn3 = types.InlineKeyboardButton(text=btn2Text, callback_data='Мониторинг') 
                btn4 = types.InlineKeyboardButton(text='Избранные', callback_data='Избранные связки') 
                btn5 = types.InlineKeyboardButton(text=btn3Text, callback_data='МониторингТоп') 
                btn6 = types.InlineKeyboardButton(text=btn4Text, callback_data='МониторингТоп') 
                markup_inline.add(btn1, btn2, btn3, btn4, btn5, btn6) 
                await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Включить или выключить мониторинг сигналов:', reply_markup=markup_inline)      


            elif call.data == 'typeSignalFalse':
                try:
                    print('Entered')
                    await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'typeSignal': 'False'}})           
                    ex['typeSignal'] = 'False'
                    markup_inline = types.InlineKeyboardMarkup(row_width=1)
                    btn1Text = 'Рискованный✅' if ex['typeSignal'] == 'False' else 'Рискованный'
                    btn2Text = 'Безопасный✅' if ex['typeSignal'] == 'True' else 'Безопасный'
                    btn3Text = 'Только избранное✅' if ex['typeSignal'] == 'onlyTop' else 'Только избранное'
                    btn1 = types.InlineKeyboardButton(text=btn1Text, callback_data='typeSignalFalse') 
                    btn2 = types.InlineKeyboardButton(text=btn2Text, callback_data='typeSignalTrue') 
                    btn3 = types.InlineKeyboardButton(text=btn3Text, callback_data='typeSignalTop') 
                    markup_inline.add(btn1, btn2, btn3)
                    await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите режим торговли\n\n❗️В рискованном режиме отображаются сигналы с закрытыми вводами и выводами.', reply_markup=markup_inline)
                except Exception as e:
                    print(e)

            elif call.data == 'typeSignalTrue':
                try:
                    print('Entered')
                    ex = await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'typeSignal': 'True'}})           
                    ex['typeSignal'] = 'True'
                    markup_inline = types.InlineKeyboardMarkup(row_width=1)
                    btn1Text = 'Рискованный✅' if ex['typeSignal'] == 'False' else 'Рискованный'
                    btn2Text = 'Безопасный✅' if ex['typeSignal'] == 'True' else 'Безопасный'
                    btn3Text = 'Только избранное✅' if ex['typeSignal'] == 'onlyTop' else 'Только избранное'
                    btn1 = types.InlineKeyboardButton(text=btn1Text, callback_data='typeSignalFalse') 
                    btn2 = types.InlineKeyboardButton(text=btn2Text, callback_data='typeSignalTrue') 
                    btn3 = types.InlineKeyboardButton(text=btn3Text, callback_data='typeSignalTop') 
                    markup_inline.add(btn1, btn2, btn3)
                    await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите режим торговли\n\n❗️В рискованном режиме отображаются сигналы с закрытыми вводами и выводами.', reply_markup=markup_inline)
                except Exception as e:
                    print(e)

            elif call.data == 'typeSignalTop':
                try:
                    print('Entered')
                    ex = await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'typeSignal': 'onlyTop'}})           
                    ex['typeSignal'] = 'onlyTop'
                    markup_inline = types.InlineKeyboardMarkup(row_width=1)
                    btn1Text = 'Рискованный✅' if ex['typeSignal'] == 'False' else 'Рискованный'
                    btn2Text = 'Безопасный✅' if ex['typeSignal'] == 'True' else 'Безопасный'
                    btn3Text = 'Только избранное✅' if ex['typeSignal'] == 'onlyTop' else 'Только избранное'
                    btn1 = types.InlineKeyboardButton(text=btn1Text, callback_data='typeSignalFalse') 
                    btn2 = types.InlineKeyboardButton(text=btn2Text, callback_data='typeSignalTrue') 
                    btn3 = types.InlineKeyboardButton(text=btn3Text, callback_data='typeSignalTop') 
                    markup_inline.add(btn1, btn2, btn3)
                    await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите режим торговли\n\n❗️В рискованном режиме отображаются сигналы с закрытыми вводами и выводами.', reply_markup=markup_inline)
                except Exception as e:
                    print(e)

            elif call.data == 'Выключить':
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'monitoring': False}})           

            
            elif call.data.find('УбратьМонетуBlack')!=-1:
                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                token = call.data.split('|')[1]

                blackListTokens.remove(token)
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'blackListTokens': blackListTokens}})

                if len(blackListTokens) > 0:
                    tokensBtnsArrBlack.clear()
                    if len(blackListTokens) > 15 and len(blackListTokens)<30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=4)
                        for token in blackListTokens:
                            btnUpdate = f'УбратьМонетуBlack|{token}'
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(blackListTokens) >= 30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=6)
                        for token in blackListTokens:
                            btnUpdate = f'УбратьМонетуBlack|{token}'
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(blackListTokens) <= 15:
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        for token in blackListTokens:
                            btnUpdate = f'УбратьМонетуBlack|{token}'
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))

                    markup_inline.add(*tokensBtnsArrBlack)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных монет', callback_data='Нет добавленных монет'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline_empty)
            
            elif call.data.find('УбратьМонетуWhite')!=-1:
                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                token = call.data.split('|')[1]

                whiteListTokens.remove(token)
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'whiteListTokens': whiteListTokens}})

                if len(whiteListTokens) > 0:
                    tokensBtnsArrWhite.clear()

                    if len(whiteListTokens) > 15 and len(whiteListTokens)<30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=4)
                        for token in whiteListTokens:
                            btnUpdate = f'УбратьМонетуWhite|{token}'
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(whiteListTokens) >= 30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=6)
                        for token in whiteListTokens:
                            btnUpdate = f'УбратьМонетуWhite|{token}'
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(whiteListTokens) <= 15:
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        for token in whiteListTokens:
                            btnUpdate = f'УбратьМонетуWhite|{token}'
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text=str(token), callback_data=str(token)))
                            tokensBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))

                    markup_inline.add(*tokensBtnsArrWhite)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=whitelistTokensAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных монет', callback_data='Нет добавленных монет'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=whitelistTokensAnsw, reply_markup=markup_inline_empty)

            elif call.data.find('УбратьСетьBlack') != -1:
                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                net = call.data.split('|')[1]

                blackListNet.remove(net)
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'blackListNet': blackListNet}})

                if len(blackListNet) > 0:
                    netBtnsArrBlack.clear()
                    

                    if len(blackListNet) > 15 and len(blackListNet)<30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=4)
                        for net in blackListNet:
                            btnUpdate = f'УбратьСетьBlack|{net}'
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(blackListNet) >= 30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=6)
                        for net in blackListNet:
                            btnUpdate = f'УбратьСетьBlack|{net}'
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(blackListNet) <= 15:
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        for net in blackListNet:
                            btnUpdate = f'УбратьСетьBlack|{net}'
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrBlack.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))

                    markup_inline.add(*netBtnsArrBlack)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistNetAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных сетей', callback_data='Нет добавленных сетей'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistNetAnsw, reply_markup=markup_inline_empty)

            elif call.data.find('УбратьСетьWhite') != -1:
                
                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                net = call.data.split('|')[1]
                whiteListNet.remove(net)
                await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'whiteListNet': whiteListNet}})
                if len(whiteListNet) > 0:
                    netBtnsArrWhite.clear()
                    

                    if len(whiteListNet) > 15 and len(whiteListNet)<30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=4)
                        for net in whiteListNet:
                            btnUpdate = f'УбратьСетьWhite|{net}'
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(whiteListNet) >= 30:
                        markup_inline = types.InlineKeyboardMarkup(row_width=6)
                        for net in whiteListNet:
                            btnUpdate = f'УбратьСетьWhite|{net}'
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    elif len(whiteListNet) <= 15:
                        markup_inline = types.InlineKeyboardMarkup(row_width=2)
                        for net in whiteListNet:
                            btnUpdate = f'УбратьСетьWhite|{net}'
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                            netBtnsArrWhite.append(types.InlineKeyboardButton(text='❌', callback_data=btnUpdate))
                    
                    markup_inline.add(*netBtnsArrWhite)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=whitelistNetAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных сетей', callback_data='Нет добавленных сетей'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=whitelistNetAnsw, reply_markup=markup_inline_empty)

            elif call.data.find('УбратьМонетуYellow') != -1:

                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                net = call.data.split('|')[1]
                yellowListNet.remove(net)
                await Constants.dbYellow.find_one_and_update({'updater':  str(1)},{'$set':{'tokens': yellowListNet}})
                if len(yellowListNet) > 0:
                    yellowBtnsArr.clear()
                    for net in yellowListNet:
                        btnUpdate = f'УбратьМонетуYellow|{net}'
                        yellowBtnsArr.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                        yellowBtnsArr.append(types.InlineKeyboardButton(text='Убрать', callback_data=btnUpdate))
                    
                    markup_inline.add(*yellowBtnsArr)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных сетей', callback_data='Нет добавленных сетей'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline_empty)

            elif call.data.find('УбратьМонетуGlobalBlack') != -1:
                
                markup_inline = types.InlineKeyboardMarkup(row_width=2)
                net = call.data.split('|')[1]
                globalBlacListNet.remove(net)
                await Constants.dbBlack.find_one_and_update({'updater':  str(1)},{'$set':{'tokens': globalBlacListNet}})
                if len(globalBlacListNet) > 0:
                    globalBalckBtnsArr.clear()
                    for net in globalBlacListNet:
                        btnUpdate = f'УбратьМонетуGlobalBlack|{net}'
                        globalBalckBtnsArr.append(types.InlineKeyboardButton(text=str(net), callback_data=str(net)))
                        globalBalckBtnsArr.append(types.InlineKeyboardButton(text='Убрать', callback_data=btnUpdate))
                    
                    markup_inline.add(*globalBalckBtnsArr)
                
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline)
                else:
                    markup_inline_empty = types.InlineKeyboardMarkup(row_width=1)
                    markup_inline_empty.add(types.InlineKeyboardButton(text='Нет добавленных сетей', callback_data='Нет добавленных сетей'))
                    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=blacklistTokensAnsw, reply_markup=markup_inline_empty)                



            elif call.data == 'Bitmart ❌' or call.data == 'OKX ❌' or call.data == 'Kucoin ❌' or call.data == 'Gate ❌' or call.data == 'MEXC Global ❌' or call.data == 'Bitget ❌' or call.data == 'BingX ❌' or call.data == 'Bybit ❌' or call.data == 'HTX ❌' or call.data == 'CoinEx ❌':
                for i in userexchangesMin:
                    if i == call.data:
                        call.data
                        userexchangesMin[userexchangesMin.index(i)] = call.data.replace('❌', '✅')
                        await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'minEx': userexchangesMin}})     
                
                changedMenu = await generateMenu(userexchangesMin, userexchangesMax)
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите биржи закупа:', reply_markup=changedMenu)
    
            elif call.data == 'Bitmart ✅' or call.data == 'OKX ✅' or call.data == 'Kucoin ✅' or call.data == 'Gate ✅' or call.data == 'MEXC Global ✅' or call.data == 'Bitget ✅' or call.data == 'BingX ✅' or call.data == 'Bybit ✅' or call.data == 'HTX ✅' or call.data == 'CoinEx ✅':
                for i in userexchangesMin:
                    if i == call.data:
                        call.data
                        userexchangesMin[userexchangesMin.index(i)] = call.data.replace('✅', '❌')
                        await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'minEx': userexchangesMin}})     
                
                changedMenu = await generateMenu(userexchangesMin, userexchangesMax)
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите биржи закупа:', reply_markup=changedMenu)
    
            elif call.data == 'Bitmart❌' or call.data == 'OKX❌' or call.data == 'Kucoin❌' or call.data == 'Gate❌' or call.data == 'MEXC Global❌' or call.data == 'Bitget❌' or call.data == 'BingX❌' or call.data == 'Bybit❌' or call.data == 'HTX❌' or call.data == 'CoinEx❌':
                for i in userexchangesMax:
                    if i == call.data:
                        call.data
                        userexchangesMax[userexchangesMax.index(i)] = call.data.replace('❌', '✅')
                        await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'maxEx': userexchangesMax}})     
                
                changedMenu = await generateMenu(userexchangesMin, userexchangesMax)
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите биржи закупа:', reply_markup=changedMenu)
    
            elif call.data == 'Bitmart✅' or call.data == 'OKX✅' or call.data == 'Kucoin✅' or call.data == 'Gate✅' or call.data == 'MEXC Global✅' or call.data == 'Bitget✅' or call.data == 'BingX✅' or call.data == 'Bybit✅' or call.data == 'HTX✅' or call.data == 'CoinEx✅':
                for i in userexchangesMax:
                    if i == call.data:
                        call.data
                        userexchangesMax[userexchangesMax.index(i)] = call.data.replace('✅', '❌')
                        await Constants.db.find_one_and_update({'id':call.message.chat.id},{'$set':{'maxEx': userexchangesMax}})     
                
                changedMenu = await generateMenu(userexchangesMin, userexchangesMax)
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Выберите биржи закупа:', reply_markup=changedMenu)  

            elif call.data == 'Обновить активные спреды':
                try:
                    
                    markup_inline = types.InlineKeyboardMarkup()
                    # Только свои сигналы — чужие обработчик всё равно отбрасывал.
                    newData = signalsForUser(await getDataFromMongo(), call.message.chat.id)

                    userDataArrFull = await cointainerActiveList(call, newData, False, "")
                    userDataArr = userDataArrFull[0]
                    if not (userDataArr is None):
                        counter = 1
                        for i in userDataArr[0]:
                            markup_inline.add(i)

                        userData = await Constants.db.find_one({'id': int(call.message.chat.id)})

                        nav = []
                        nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                        nav.append(types.InlineKeyboardButton(text=f'1/{userData["countPages"]}',callback_data='.'))
                        nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                        
                        
                        markup_inline.add(*nav)
                        markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                        await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Активные связки:', reply_markup=markup_inline)
                    else:
                        nav = []
                        nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                        nav.append(types.InlineKeyboardButton(text=f'0/0',callback_data='.'))
                        nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                        
                        
                        markup_inline.add(*nav)
                        markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                        await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='Активные связки:', reply_markup=markup_inline)
                except Exception as e:
                    print('Update err',e)
            elif call.data == '<--':
                try:
                    markup_inline = types.InlineKeyboardMarkup()
                    userDataCurr = await Constants.db.find_one({'id': call.message.chat.id})

                    userDataArrFirst = jsons.loads(userDataCurr['lastActiveList'])
                    userCurrPage = userDataCurr['currentPage']
                    userCurrPage-=1
                    if userCurrPage<0:
                        return None
                    userDataArr = []
                    
                    for i in userDataArrFirst:
                        userDataArr.append(types.InlineKeyboardButton(text=i['text'], callback_data=i['callback_data']))
                    if userDataCurr['currentPage']-1>=0:
                        counter = 8*(userDataCurr['currentPage']-1)
                
                    summDataArr = []
                    
                    for i in range(counter,counter+8):
                        try:
                            summDataArr.append(userDataArr[i])
                        except:
                            break
                    

                    if len(summDataArr)>0:
                        for i in summDataArr:
                            markup_inline.add(i)
                    else: 
                        return None
            
                    listCount = divmod(len(userDataArr),8)[0]

                    if divmod(len(userDataArr),8)[1]!=0:
                        listCount+=1
                    
                    nav = []
                    nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                    nav.append(types.InlineKeyboardButton(text=f'{userCurrPage+1}/{userDataCurr["countPages"]}',callback_data='.'))
                    nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                    
                
                    markup_inline.add(*nav)
                    markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                    try:
                        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Активные связки:', reply_markup=markup_inline)
                        await Constants.db.find_one_and_update({'id': call.message.chat.id},{'$set':{'currentPage': userCurrPage}})

                    except Exception as e:
                        print('Не получилось()',e)
                        # else:
                        #     pass        
                except Exception as e:
                    print(e, 'ОШибка')
                
            elif call.data == '-->':
                try:
                    markup_inline = types.InlineKeyboardMarkup()
                    userDataCurr = await Constants.db.find_one({'id': call.message.chat.id})
                    userDataArrFirst = jsons.loads(userDataCurr['lastActiveList'])
                    userDataArr = []
                    userCurrPage = userDataCurr['currentPage']
                    userCurrPage+=1

                    

                    for i in userDataArrFirst:
                        userDataArr.append(types.InlineKeyboardButton(text=i['text'], callback_data=i['callback_data']))
            
                    pages = []
                    summDataArr = []
                    if userDataCurr['currentPage']<=userDataCurr['countPages']:
                        counter = 8*(userDataCurr['currentPage']+1)
            
                    for i in range(counter,counter+8):
                        try:
                            summDataArr.append(userDataArr[i])
                        except:
                            break
                    
                    
                    if len(summDataArr)>0:
                        for i in summDataArr:
                            markup_inline.add(i)
                    else: 
                        return None
                    
                    nav = []
                    nav.append(types.InlineKeyboardButton(text='<--',callback_data='<--'))
                    nav.append(types.InlineKeyboardButton(text=f'{userCurrPage+1}/{userDataCurr["countPages"]}',callback_data='.'))
                    nav.append(types.InlineKeyboardButton(text='-->', callback_data='-->'))
                    
                
                    markup_inline.add(*nav)
                    markup_inline.add(types.InlineKeyboardButton(text='Обновить', callback_data='Обновить активные спреды'))
                    try:
                        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 'Активные связки:', reply_markup=markup_inline)
                        await Constants.db.find_one_and_update({'id': call.message.chat.id},{'$set':{'currentPage': userCurrPage}})


                    except Exception as e:
                        print('Не получилось()',e)
                    

                except Exception as e:
                    print(e, 'ОШибка')       

            elif call.data.find('№')!=-1:
                signal = call.message.text
    
                min_exchange = ''
                minex = re.search(r'\|(\w+.+)\!', call.data)
                if minex:
                    min_exchange = minex.group(1)
    
                max_exchange = ''
                maxex = re.search(r'\!(\w+.+)\$', call.data)
                if maxex:
                    max_exchange = maxex.group(1)  
    
                symbol = ''
                s = re.search(r'\$(\w+.+)\%', call.data)
                if s:
                    symbol = s.group(1)
    
                feeDoll = ''
                fee = re.search(r'\%(\w+.+)\?', call.data)
                if fee:
                    feeDoll = fee.group(1)  
    
                networkSumm = ''
                net = re.search(r'\?(\w+.+)\*', call.data)
                if net:
                    networkSumm = net.group(1)   
                
                await gather(*[getNewSignalData(call, min_exchange, max_exchange, symbol, float(feeDoll), call.message.chat.id, networkSumm, signal, True)])
            
            elif call.data.find('@')!=-1:
                signal = call.message.text
    
                min_exchange = ''
                minex = re.search(r'\|(\w+.+)\!', call.data)
                if minex:
                    min_exchange = minex.group(1)
    
                max_exchange = ''
                maxex = re.search(r'\!(\w+.+)\$', call.data)
                if maxex:
                    max_exchange = maxex.group(1)  
    
                symbol = ''
                s = re.search(r'\$(\w+.+)\%', call.data)
                if s:
                    symbol = s.group(1)
    
                feeDoll = ''
                fee = re.search(r'\%(\w+.+)\?', call.data)
                if fee:
                    feeDoll = fee.group(1)  
    
                networkSumm = ''
                net = re.search(r'\?(\w+.+)\*', call.data)
                if net:
                    networkSumm = net.group(1)   
                await gather(*[getNewSignalData(call, min_exchange, max_exchange, symbol, float(feeDoll), call.message.chat.id, networkSumm, signal, False)])
    
        except Exception as e:
            pass        
    except:
        pass

async def getMarkets(exchange):
    
    global baseTokens
    markets = {}
    try:
        exchange.aiohttp_proxy = Constants.proxies[random.randint(0, len(Constants.proxies)-1)]
        markets = await exchange.load_markets()
    except Exception as e:
        print('getMarkets', e)
        pass   
            
    for i in markets:
        if not markets[i]['base'].upper() in baseTokens:
            baseTokens.append(markets[i]['base'].upper()) 

async def startGetmatkets():
    loopsSignals = [getMarkets(exchange) for exchange in Constants.exchanges]
    await gather(*loopsSignals)   
        
""" async def startMarket(func):
    while True:
        await func()
        await asyncio.sleep(180) """

async def startSignal(func):
    while True:
        await func()
        await asyncio.sleep(2)
            
async def botStart():
    
    await bot.infinity_polling()

async def start_async_loop(func, call):
    await func(call)


async def startSignalsContain():
    await startSignals(False)        

async def checkList():
        dbBlack = await Constants.dbBlack.find().to_list(length=None)
        Constants.blackTokens = dbBlack[0]['tokens']
        dbYellow = await Constants.dbYellow.find().to_list(length=None)
        Constants.yellowTokens = dbYellow[0]['tokens']

async def checkTokensList(func):
    while True:
        await func()
        await asyncio.sleep(2)



async def check_tokens_list():
    await checkList()

async def start_signals():
    await startSignal(startSignalsContain)

async def start_bot():
    await botStart()

def run_check_tokens_list():
    asyncio.run(check_tokens_list())

def run_start_signals():
    asyncio.run(start_signals())

def run_start_bot():
    asyncio.run(start_bot())

def main():
    bot.add_custom_filter(asyncio_filters.StateFilter(bot))
    bot.add_custom_filter(asyncio_filters.IsDigitFilter())

    try:
        processes = [
            multiprocessing.Process(target=run_check_tokens_list),
            multiprocessing.Process(target=run_start_signals),
            multiprocessing.Process(target=run_start_bot)
        ]

        for process in processes:
            process.start()

        for process in processes:
            process.join()

    except Exception as e:
        print("Exception occurred:", e)
    except:
        pass

if __name__ == '__main__':
    main()





