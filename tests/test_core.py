import ast
import asyncio
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import pytest
from portable.feeds import network_record
from portable.public_exchange import native, configured_exchanges

ROOT = Path(__file__).resolve().parents[1]


def isolated_class(path, name):
    """Load real pure methods without starting database clients or Telegram polling."""
    tree = ast.parse((ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
    node.body = [n for n in node.body if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) or n.name != '__init__']
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), node], type_ignores=[])
    namespace = dict(Dict=Dict, List=List, Optional=Optional, OrderedDict=OrderedDict,
                     defaultdict=defaultdict, datetime=datetime, asyncio=asyncio)
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return namespace[name]()


def math_bot():
    return isolated_class('newbot/render/signalrender.py', 'MathSignalsBot')


def calculate(buy_qty=100, sell_qty=100, fee=0, volume=100):
    fees = {'A': {'taker': .1}, 'B': {'taker': .1}}
    return math_bot().orderbookMath({'asks': [[10., buy_qty]]}, {'bids': [[11., sell_qty]]},
                                   volume, fee, 'TEST/USDT', 'A', 'B', fees, [])


def test_volume_and_fees():
    result = calculate()
    # 100 / 10 = 10 coins; buy fee 0.01; sell gross 109.89; sell fee 0.10989.
    assert result[1] == pytest.approx(9.78011)
    assert result[2]['alltokensSell'] == pytest.approx(9.99)


def test_network_fee_is_in_base_asset():
    assert calculate(fee=1)[1] == pytest.approx(-1.20889)


@pytest.mark.parametrize('kwargs', [{'buy_qty': 1}, {'sell_qty': 1}, {'fee': 100}, {'volume': 0}])
def test_no_imaginary_liquidity(kwargs):
    assert calculate(**kwargs) is None


def test_unknown_metadata_never_looks_transferable():
    record = network_record(None, None, None)
    assert record['withdraw'] is False and record['deposit'] is False and record['fee'] is None
    assert network_record('false', 'true', 'NaN')['fee'] is None


def test_native_symbols_and_no_credentials():
    assert native('Gate', 'BTC/USDT') == 'BTC_USDT'
    assert native('HTX', 'BTC/USDT') == 'btcusdt'
    assert native('Kucoin', 'BTC/USDT') == 'BTC-USDT'
    assert all(ex._client is None for ex in configured_exchanges())


def test_stale_books_with_equal_timestamps_are_rejected():
    bot = math_bot()
    stamp = (datetime.now() - timedelta(minutes=5)).isoformat()
    bot.local_orderbooks = {'Gate': {'BTC_USDT': {'lastUpdate': stamp}}, 'Kucoin': {'BTC-USDT': {'lastUpdate': stamp}}}
    bot.symbol_version = {}
    assert bot._combo_signature('BTC/USDT', 'Gate', 'Kucoin')[2] is False


def test_network_routes_require_permissions_and_fee():
    bot = isolated_class('newbot/render/combinations_render.py', 'ArbitrageBot')
    bot.local_block_array = []
    good = {'ERC20': network_record(True, True, .01, '0xabc')}
    bad = {'ERC20': network_record(True, True, None, '0xabc')}
    bot.collect_blocks('X/USDT', 'A', 'B', bad, good)
    assert not bot.local_block_array
    bot.collect_blocks('X/USDT', 'A', 'B', good, good)
    assert len(bot.local_block_array) == 1
    mismatch = {'ERC20': network_record(True, True, .01, '0xdef')}
    bot.collect_blocks('X/USDT', 'A', 'B', good, mismatch)
    assert len(bot.local_block_array) == 1


@pytest.mark.skipif(not (ROOT/'MarketMaker').exists(), reason='MM-specific algorithm')
def test_active_mm_keeps_asks_and_bids_separate():
    async def run():
        bot = isolated_class('MarketMaker/active_mm.py', 'OrderBookAnalyzer')
        bot.initial_filtered_orders = {}
        await bot.second_stage_comparison([[10, 7]], [[11, 7]], 'X', 'A', 'asks')
        await bot.second_stage_comparison([[9, 8]], [[8, 8]], 'X', 'A', 'bids')
        assert bot.initial_filtered_orders['X|A'] == {'asks': [[10, 7]], 'bids': [[9, 8]]}
    asyncio.run(run())


def test_book_freshness_removes_old_and_future_timestamps():
    from portable.freshness import fresh_books
    now = datetime.now()
    books = {'fresh': {'lastUpdate': now.isoformat()},
             'old': {'lastUpdate': (now-timedelta(minutes=5)).isoformat()},
             'future': {'lastUpdate': (now+timedelta(minutes=5)).isoformat()},
             'missing': {}}
    assert list(fresh_books(books)) == ['fresh']
