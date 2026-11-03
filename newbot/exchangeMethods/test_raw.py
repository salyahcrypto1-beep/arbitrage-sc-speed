import hashlib
import hmac
import json
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, ".")
sys.path.insert(0, "./manage")
import constants as C


def show(name, req):
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()[:200]
            print(f"{name}: HTTP {resp.status} | {body}")
    except urllib.error.HTTPError as exc:
        print(f"{name}: HTTP {exc.code} | {exc.read().decode()[:200]}")
    except Exception as exc:
        print(f"{name}: ОШИБКА {type(exc).__name__} {str(exc)[:120]}")


# ---- Gate: подпись HMAC-SHA512 ----
key, secret = C.Constants.Gate.APIKEY, C.Constants.Gate.APISECRET
path = "/api/v4/wallet/withdraw_status"
ts = str(int(time.time()))
payload_hash = hashlib.sha512(b"").hexdigest()
sign_str = f"GET\n{path}\n\n{payload_hash}\n{ts}"
sign = hmac.new(secret.encode(), sign_str.encode(), hashlib.sha512).hexdigest()
req = urllib.request.Request(
    "https://api.gateio.ws" + path,
    headers={"KEY": key, "Timestamp": ts, "SIGN": sign, "Accept": "application/json"},
)
show("GATE", req)

# ---- MEXC: подпись HMAC-SHA256 ----
key, secret = C.Constants.Mexc.APIKEY, C.Constants.Mexc.APISECRET
qs = urllib.parse.urlencode({"timestamp": int(time.time() * 1000), "recvWindow": "50000"})
qs += "&signature=" + hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
req = urllib.request.Request(
    f"https://api.mexc.com/api/v3/capital/config/getall?{qs}",
    headers={"X-MEXC-APIKEY": key},
)
show("MEXC", req)
