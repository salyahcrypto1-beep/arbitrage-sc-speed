"""Explicit local owner pairing; no inherited paid-membership database or IDs."""
import hmac
import os
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

_client = None

def db():
    global _client
    if _client is None: _client = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=5000)
    return _client.userdata


def is_owner(chat_id):
    owner = db().local_owner.find_one({'_id': 'owner'})
    return bool(owner and owner['chat_id'] == int(chat_id))


def defaults(chat_id):
    names = ['OKX', 'Kucoin', 'Gate', 'MEXC Global', 'Bitget', 'BingX', 'Bybit', 'HTX', 'CoinEx', 'Bitmart']
    return {'id': int(chat_id), 'whiteListNet': [], 'whiteListTokens': [], 'lastActiveList': '',
            'countPages': 0, 'currentPage': 0, 'typeSignal': 'True', 'monitoring': False,
            'monitoringTop': False, 'blackListTokens': [], 'blackListNet': [], 'maxFeeVal': 20,
            'minDollarSpread': 0, 'minProcentSpread': 0, 'minVolume': 100, 'maxVolume': 1000,
            'minEx': [n + ' ✅' for n in names], 'maxEx': [n + '✅' for n in names], 'save_mm_signals': []}


async def pair_user(message, bot):
    if message.chat.type != 'private':
        await bot.send_message(message.chat.id, 'Откройте личный чат с ботом.')
        return False
    chat_id = int(message.chat.id)
    database = db()
    owner = database.local_owner.find_one({'_id': 'owner'})
    if owner is None:
        expected = os.getenv('PAIRING_CODE', '')
        supplied = (message.text or '').split(maxsplit=1)
        if not expected or len(supplied) != 2 or not hmac.compare_digest(expected, supplied[1]):
            await bot.send_message(chat_id, 'Для первого подключения отправьте /start КОД_ИЗ_ЛОКАЛЬНОЙ_УСТАНОВКИ.')
            return False
        try: database.local_owner.insert_one({'_id': 'owner', 'chat_id': chat_id})
        except DuplicateKeyError: pass
    if not is_owner(chat_id) and not database.user.find_one({'id': chat_id}):
        await bot.send_message(chat_id, 'Доступ выдаёт владелец этой локальной установки.')
        return False
    database.user.update_one({'id': chat_id}, {'$setOnInsert': defaults(chat_id)}, upsert=True)
    database.mm_user.update_one({'id': chat_id}, {'$setOnInsert': {
        'id': chat_id, 'monitoring': False, 'saveList': [], 'blackList': []}}, upsert=True)
    return True
