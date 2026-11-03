import motor.motor_asyncio as motor
import asyncio

CONNECTION_STRING = "mongodb://mongo:27017"
client = motor.AsyncIOMotorClient(CONNECTION_STRING)

pairs = client.renderBase.AllSymbolsData

pairs_keys = {}

async def get_pairs():
    asd = await pairs.find().to_list(length=None)
    for symbol in asd:
        if symbol['symbol'] not in pairs_keys.keys():
            pairs_keys[symbol['symbol']]={'count':1,'_id':[],'len':[]}
            pairs_keys[symbol['symbol']]['_id'].append(symbol['_id'])
            pairs_keys[symbol['symbol']]['len'].append(len(symbol['currency']))
        else:
            pairs_keys[symbol['symbol']]['count']+=1
            pairs_keys[symbol['symbol']]['_id'].append(symbol['_id'])
            pairs_keys[symbol['symbol']]['len'].append(len(symbol['currency']))

    for key,dic in pairs_keys.items():
        if dic['count']>1:
            print(key,dic)
            await pairs.delete_one({'_id':dic['_id'][1]})
            

asyncio.run(get_pairs())
