"""All production entry points must import without external API calls."""
import os
from pathlib import Path
import subprocess
import sys
import pytest
ROOT=Path(__file__).resolve().parents[1]
MODULES=['newbot/render/combinations_render.py','newbot/render/signalrender.py',
         'newbot/render/usersrendersignal.py','newbot/client/bottg.py']
if (ROOT/'MarketMaker').exists(): MODULES += ['MarketMaker/marketM.py','MarketMaker/active_mm.py','MarketMaker/MM_signals.py']
MODULES += ['newbot/webSockets/'+x+'.py' for x in ['kucoin','gate','bitget','mexc','bybit','bingx','htx','coinex','bitmart','okx']]
@pytest.mark.parametrize('module',MODULES)
def test_entrypoint_import(module):
    env=dict(os.environ, TELEGRAM_BOT_TOKEN='123456789:'+'x'*35,
        PYTHONPATH=os.pathsep.join(str(ROOT/p) for p in ['.','newbot/exchangeMethods','newbot/exchangeMethods/manage','newbot/render','newbot/client']))
    target=ROOT/module
    (target.parent/'logs').mkdir(exist_ok=True)
    code="import runpy; runpy.run_path("+repr(str(target))+", run_name='import_check')"
    result=subprocess.run([sys.executable,'-c',code],cwd=target.parent,env=env,capture_output=True,text=True,timeout=25)
    assert result.returncode==0, result.stderr[-3000:]
