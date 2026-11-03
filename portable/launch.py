"""Foreground process supervisor; terminates the stack on an unexpected worker exit."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import pymongo

ROOT = Path(__file__).resolve().parents[1]

def main():
    token = os.getenv('TELEGRAM_BOT_TOKEN', '')
    if not token or ':' not in token:
        raise SystemExit('TELEGRAM_BOT_TOKEN is missing. Run python setup.py on your computer.')
    if not os.getenv('PAIRING_CODE'):
        raise SystemExit('PAIRING_CODE is missing. Run python setup.py.')
    client = pymongo.MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=3000)
    for _ in range(30):
        try: client.admin.command('ping'); break
        except pymongo.errors.PyMongoError: time.sleep(2)
    else: raise SystemExit('MongoDB did not become ready')
    client.renderBase.black_list.update_one({'_id': 'local'}, {'$setOnInsert': {'list': []}}, upsert=True)
    client.close()
    kind = os.getenv('BOT_KIND', 'arbitrage')
    jobs = [('public-feeds', ['-m', 'portable.feeds'], ROOT)]
    if kind == 'arbitrage':
        jobs += [(name, [str(ROOT / 'newbot/render' / (name + '.py'))], ROOT / 'newbot/render')
                 for name in ['combinations_render', 'signalrender', 'usersrendersignal']]
        jobs += [('telegram', [str(ROOT / 'newbot/client/bottg.py')], ROOT / 'newbot/client')]
    elif kind == 'market-maker':
        jobs += [(name, [str(ROOT / 'MarketMaker' / (name + '.py'))], ROOT / 'MarketMaker')
                 for name in ['marketM', 'active_mm', 'MM_signals']]
    else: raise SystemExit('Invalid BOT_KIND')
    env = dict(os.environ, PYTHONUNBUFFERED='1', PYTHONPATH=os.pathsep.join(map(str, [ROOT,
        ROOT/'newbot/exchangeMethods', ROOT/'newbot/exchangeMethods/manage', ROOT/'newbot/render', ROOT/'newbot/client'])))
    children = []
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for name, args, cwd in jobs:
            print('Starting:', name, flush=True)
            children.append((name, subprocess.Popen([sys.executable, '-u', *args], cwd=cwd, env=env, start_new_session=True)))
        while not stopped:
            for name, proc in children:
                if proc.poll() is not None:
                    raise RuntimeError(f'{name} exited with {proc.returncode}; restarting stack via Docker')
            time.sleep(1)
    finally:
        for _, proc in children:
            try: os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError: pass
        for _, proc in children:
            try: proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try: os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError: pass

if __name__ == '__main__': main()
