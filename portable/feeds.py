"""Bounded public WebSocket ingestion, public metadata, and legacy cache schema."""
import asyncio
import gzip
import logging
import math
import os
import pickle
import json
from pathlib import Path
NETWORK_ALIASES = json.loads(Path(__file__).with_name("networks.json").read_text())
import time
from datetime import datetime
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from pymemcache.client.base import Client
from portable.public_exchange import configured_exchanges, native

log = logging.getLogger(__name__)
MAX_SYMBOLS = int(os.getenv('MAX_SYMBOLS_PER_EXCHANGE', '40'))
BOOK_TTL = 30


def network_record(withdraw, deposit, fee, contract=''):
    # Missing metadata is unknown, not permission to transfer.
    def boolean(value):
        return value is True or (isinstance(value, str) and value.lower() in ('true', 'enabled', 'allowed'))
    try:
        fee = float(fee)
        if not math.isfinite(fee) or fee < 0: fee = None
    except (TypeError, ValueError):
        fee = None
    return {'withdraw': boolean(withdraw), 'deposit': boolean(deposit), 'fee': fee,
            'contract': contract or '', 'adminFee': 0}


def canonical_network(name):
    value = str(name).upper()
    for key, aliases in NETWORK_ALIASES.items():
        if value == key.upper() or value in (str(x).upper() for x in aliases): return key
    return value


async def public_currencies(exchange, session):
    """Only documented public endpoints. No signed wallet API fallback."""
    if exchange.id == 'bitget':
        async with session.get('https://api.bitget.com/api/v2/spot/public/coins') as r:
            r.raise_for_status(); payload = await r.json()
        return {coin['coin']: {canonical_network(n['chain']): network_record(
                    n.get('withdrawable'), n.get('rechargeable'), n.get('withdrawFee'), n.get('contractAddress'))
                    for n in (coin.get('chains') or [])} for coin in payload.get('data', [])}
    if exchange.id == 'kucoin':
        async with session.get('https://api.kucoin.com/api/v3/currencies') as r:
            r.raise_for_status(); payload = await r.json()
        return {coin['currency']: {canonical_network(n.get('chainName') or n.get('chainId')): network_record(
                    n.get('isWithdrawEnabled'), n.get('isDepositEnabled'), n.get('withdrawalMinFee'), n.get('contractAddress'))
                    for n in (coin.get('chains') or [])} for coin in payload.get('data', [])}
    if exchange.id == 'coinex':
        async with session.get('https://api.coinex.com/v2/assets/all-deposit-withdraw-config') as r:
            r.raise_for_status(); payload = await r.json()
        return {coin['asset']['ccy']: {canonical_network(n['chain']): network_record(
                    n.get('withdraw_enabled'), n.get('deposit_enabled'), n.get('withdrawal_fee'))
                    for n in (coin.get('chains') or [])} for coin in payload.get('data', [])}
    if exchange.id == 'htx':
        async with session.get('https://api.huobi.pro/v2/reference/currencies') as r:
            r.raise_for_status(); payload = await r.json()
        return {coin['currency'].upper(): {canonical_network(n.get('displayName') or n['chain']): network_record(
                    n.get('withdrawStatus'), n.get('depositStatus'),
                    n.get('transactFeeWithdraw') if n.get('withdrawFeeType') == 'fixed' else None)
                    for n in (coin.get('chains') or [])} for coin in payload.get('data', [])}
    # Gate exposes currency chain status publicly, but withdrawal fee is not
    # reliably available there. Preserve unknown fee instead of inventing zero.
    if exchange.id == 'gate':
        async with session.get('https://api.gateio.ws/api/v4/spot/currencies') as r:
            r.raise_for_status(); payload = await r.json()
        return {coin['currency']: {canonical_network(n.get('name')): network_record(
                    not n.get('withdraw_disabled', True), not n.get('deposit_disabled', True), None, n.get('addr'))
                    for n in (coin.get('chains') or [])} for coin in payload}
    return {}


async def metadata(exchange, symbols, mongo, session):
    networks = await public_currencies(exchange, session)
    for symbol in symbols:
        currency = {'networks': networks.get(symbol.split('/')[0], {})}
        await mongo.renderBase.AllSymbolsData.update_one({'symbol': symbol},
            {'$set': {f'currency.{exchange.name}': currency}}, upsert=True)
    await mongo.renderBase.tradePairs.update_one({'id': exchange.name},
        {'$set': {'tokens': [native(exchange.name, s) for s in symbols]}}, upsert=True)
    log.info('%s: %d markets; public network metadata for %d coins', exchange.name, len(symbols), len(networks))


async def watch(exchange, symbol, books):
    delay = 1
    while True:
        try:
            if exchange.client.has.get('watchOrderBook'):
                book = await asyncio.wait_for(exchange.client.watch_order_book(symbol, limit=20), timeout=45)
            else:
                book = await exchange.client.fetch_order_book(symbol, limit=20)
                await asyncio.sleep(5)
            bids, asks = book.get('bids', []), book.get('asks', [])
            if not bids or not asks: continue
            books[native(exchange.name, symbol)] = {
                'bids': [list(x[:2]) for x in bids[:20]], 'asks': [list(x[:2]) for x in asks[:20]],
                'lastUpdate': datetime.now().isoformat(), '_received': time.monotonic()}
            delay = 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            books.pop(native(exchange.name, symbol), None)
            log.warning('%s %s: %s; retry in %ss', exchange.name, symbol, type(exc).__name__, delay)
            await asyncio.sleep(delay)
            delay = min(60, delay * 2)


async def publish(exchange, books, cache):
    while True:
        fresh = {s: {k: v for k, v in b.items() if k != '_received'} for s, b in books.items()
                 if time.monotonic() - b['_received'] <= BOOK_TTL}
        blob = gzip.compress(pickle.dumps(fresh), compresslevel=1)
        # The original MM code calls MEXC "MEXC", arbitrage calls it "MEXC Global".
        keys = [f'orderbook_{exchange.name}_full_snapshot']
        if exchange.name == 'MEXC Global': keys.append('orderbook_MEXC_full_snapshot')
        for key in keys: cache.set(key, blob, expire=BOOK_TTL)
        await asyncio.sleep(1)


async def run_exchange(exchange, mongo):
    cache = Client(('memcached', 11211), connect_timeout=5, timeout=5)
    tasks = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
        try:
            markets = await exchange.markets()
            requested = [s.strip() for s in os.getenv('SYMBOLS', '').split(',') if s.strip()]
            symbols = sorted(s for s, m in markets.items() if m.get('spot') and m.get('quote') == 'USDT'
                             and m.get('active') is not False and (not requested or s in requested))
            # Prefer liquid pairs on small computers rather than arbitrary alphabetical tokens.
            if MAX_SYMBOLS and len(symbols) > MAX_SYMBOLS and not requested:
                try:
                    tickers = await exchange.client.fetch_tickers(params={'type': 'spot'})
                    symbols.sort(key=lambda s: float(tickers.get(s, {}).get('quoteVolume') or 0), reverse=True)
                except Exception:
                    log.warning('%s: volume ranking unavailable; using sorted symbols', exchange.name)
                symbols = symbols[:MAX_SYMBOLS]
            async def refresh_metadata():
                while True:
                    try: await metadata(exchange, symbols, mongo, session)
                    except Exception as exc:
                        # Clear old network status on failure; retain markets for MM analysis.
                        for symbol in symbols:
                            await mongo.renderBase.AllSymbolsData.update_one({'symbol': symbol},
                                {'$set': {f'currency.{exchange.name}': {'networks': {}}}}, upsert=True)
                        log.warning('%s metadata unavailable: %s', exchange.name, type(exc).__name__)
                    await asyncio.sleep(300)
            books = {}
            tasks = [asyncio.create_task(watch(exchange, s, books)) for s in symbols]
            tasks += [asyncio.create_task(publish(exchange, books, cache)), asyncio.create_task(refresh_metadata())]
            await asyncio.gather(*tasks)
        finally:
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            cache.close()
            await exchange.close()


async def main():
    mongo = AsyncIOMotorClient('mongodb://mongo:27017')
    async def resilient(exchange):
        while True:
            try: await run_exchange(exchange, mongo)
            except asyncio.CancelledError: raise
            except Exception as exc:
                log.warning('%s setup failed: %s; retry in 30s', exchange.name, type(exc).__name__)
                exchange._client = None
                await asyncio.sleep(30)
    try: await asyncio.gather(*(resilient(ex) for ex in configured_exchanges()))
    finally: mongo.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    asyncio.run(main())
