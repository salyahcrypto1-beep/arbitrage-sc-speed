"""Regression coverage for native feed semantics and personal-installation access."""
import ast
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import pytest
from portable import access
from telebot.asyncio_handler_backends import CancelUpdate

ROOT=Path(__file__).resolve().parents[1]

def methods(path, class_name, names):
    tree=ast.parse((ROOT/path).read_text())
    cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name==class_name)
    cls.body=[n for n in cls.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
    ns={'json':json,'datetime':datetime,'logging':logging,'logger':logging.getLogger('test'),'MAX_ORDERS_IN_DB':50}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[cls],type_ignores=[])),path,'exec'),ns)
    return ns[class_name]()

def test_complete_snapshots_coalesce_without_losing_latest_value():
    u=methods('newbot/webSockets/bitget.py','BitgetOrderBookUpdater',{'handle_message','_parse_pending'})
    u.temp_order_books={};u.raw_books={}
    stamp=datetime.now()
    for price in (10,11,12):
        u.raw_books['BTCUSDT']=(json.dumps({'data':[{'asks':[[str(price+1),'3']],'bids':[[str(price),'2']]}]}),stamp)
    u._parse_pending()
    assert u.temp_order_books['BTCUSDT']['bids']==[[12.,2.]]
    assert u.temp_order_books['BTCUSDT']['lastUpdate']==stamp
    assert not u.raw_books

@pytest.mark.asyncio
async def test_native_delta_updates_deletes_and_sorts_levels():
    u=methods('newbot/webSockets/bybit.py','BybitOrderBookUpdater',{'update_order_book'})
    book={'bids':[[10.,5.],[9.,4.]],'asks':[[11.,2.],[12.,3.]]}
    await u.update_order_book(book,[['10','0'],['9','7'],['9.5','2']],[['11','0'],['11.5','6']],'BTCUSDT')
    assert book=={'bids':[[9.5,2.],[9.,7.]],'asks':[[11.5,6.],[12.,3.]]}

@pytest.mark.asyncio
async def test_local_access_cannot_be_bypassed_with_callback_or_group(monkeypatch):
    monkeypatch.setattr(access,'is_owner',lambda i:i==123)
    m=access.LocalAccessMiddleware()
    own=SimpleNamespace(chat=SimpleNamespace(id=123,type='private'),text='settings')
    assert await m.pre_process(own,{}) is None
    stranger=SimpleNamespace(chat=SimpleNamespace(id=456,type='private'),text='settings')
    assert isinstance(await m.pre_process(stranger,{}),CancelUpdate)
    spoof=SimpleNamespace(message=own,from_user=SimpleNamespace(id=456))
    assert isinstance(await m.pre_process(spoof,{}),CancelUpdate)
    group=SimpleNamespace(chat=SimpleNamespace(id=123,type='group'),text='settings')
    assert isinstance(await m.pre_process(group,{}),CancelUpdate)
    start=SimpleNamespace(chat=SimpleNamespace(id=456,type='private'),text='/start local-code')
    assert await m.pre_process(start,{}) is None # pairing handler validates the code

def test_distribution_has_no_ccxt_import_or_old_admin_commands():
    for folder in ('portable','newbot','MarketMaker'):
        for path in (ROOT/folder).rglob('*.py'):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node,ast.Import): assert all(not a.name.startswith('ccxt') for a in node.names)
                if isinstance(node,ast.ImportFrom): assert not (node.module or '').startswith('ccxt')
    text=(ROOT/'newbot/client/bottg.py').read_text()
    for old in ('is_owner(', 'Добавить пользователя','Удалить пользователя','MyStates.addUser','MyStates.delUser','УбратьМонетуYellow','УбратьМонетуGlobalBlack','https://t.me/BotFather'):
        assert old not in text
