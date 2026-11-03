import asyncio
import hmac
import sys
import time
import urllib.parse
from hashlib import sha256

sys.path.insert(0, ".")
sys.path.insert(0, "./manage")

import gate
import mexc
import okex


def size(value):
    if value is None:
        return "None"
    try:
        return f"OK, записей: {len(value)}"
    except TypeError:
        return f"OK: {str(value)[:60]}"


async def main():
    g = gate.Gate()
    print("Gate  APIKEY:", (g.APIKEY or "ПУСТО")[:12])
    try:
        r = await asyncio.wait_for(g.getAdminFee(), timeout=40)
        print("   getAdminFee ->", size(r))
    except Exception as exc:
        print("   getAdminFee ОШИБКА:", type(exc).__name__, str(exc)[:110])

    m = mexc.MEXC()
    print("MEXC  APIKEY:", (m.APIKEY or "ПУСТО")[:12])
    try:
        params = {"timestamp": int(time.time() * 1000), "recvWindow": "50000"}
        qs = urllib.parse.urlencode(sorted(params.items()))
        qs += "&signature=" + hmac.new(m.APISECRET.encode(), qs.encode(), sha256).hexdigest()
        r = await asyncio.wait_for(
            m.send_request(qs, "/api/v3/capital/config/getall", api={"X-MEXC-APIKEY": m.APIKEY}),
            timeout=40,
        )
        print("   capital/config ->", size(r))
    except Exception as exc:
        print("   MEXC ОШИБКА:", type(exc).__name__, str(exc)[:110])

    o = okex.OKX()
    print("OKX   APIKEY:", (o.APIKEY or "ПУСТО")[:12], "| PASSPHRASE:", "есть" if getattr(o, "PASSPHRASE", "") else "НЕТ")
    try:
        r = await asyncio.wait_for(o.getContract(), timeout=40)
        print("   getContract ->", str(r)[:120])
    except Exception as exc:
        print("   OKX ОШИБКА:", type(exc).__name__, str(exc)[:110])


asyncio.run(main())
