import re
import sys
from collections import defaultdict

sys.path.insert(0, "/app/newbot/exchangeMethods/manage")
from pymongo import MongoClient

src = open("/app/newbot/exchangeMethods/manage/dbloader.py", encoding="utf-8").read()
m = re.search(r"chainPool\s*=\s*\{", src)
i = m.end() - 1
depth = 0
for j in range(i, len(src)):
    if src[j] == "{":
        depth += 1
    elif src[j] == "}":
        depth -= 1
        if depth == 0:
            break
body = src[i:j + 1]
pool = {}
for k, v in re.findall(r"['\"]([^'\"]+)['\"]\s*:\s*\[([^\]]*)\]", body):
    pool[k] = [x.strip().strip("'\"") for x in v.split(",") if x.strip()]

known_aliases = {a for lst in pool.values() for a in lst}
print(f"канонов: {len(pool)}, алиасов: {len(known_aliases)}")

db = MongoClient('mongo', 27017).renderBase
net_by_ex = defaultdict(set)
for doc in db.AllSymbolsData.find({}, {"currency": 1}):
    for ex, payload in (doc.get("currency") or {}).items():
        for net in ((payload or {}).get("networks") or {}):
            net_by_ex[net].add(ex)

CANDIDATES = [
    ("AVAX-C", "AVAXC-CHAIN"), ("BERA", "BERACHAIN"), ("STABLE", "STABLECHAIN"),
    ("CANTON", "CANTON NETWORK"), ("MEGAETH", "MEGA-ETH"), ("XLAYER", "X_LAYER"),
    ("WORLDCHAIN", "WORLD CHAIN"), ("ZIG", "ZIGCHAIN"), ("PHAROS", "PHAROS MAINNET"),
    ("ABCORE", "AB CORE"), ("ASSETHUBKUSAMA", "ASSET HUB KUSAMA"), ("APE", "APECHAIN"),
    ("DATA", "DATA NETWORK"), ("MOVA", "MOVA MAINNET"), ("CC", "$CC"),
    ("ZKSYNC", "ZKSYNCERA_ETH"), ("STARKNET", "STARK-ETH"),
]

print("\nканон              алиас                 в chainPool?   биржи канона / биржи алиаса")
todo = []
for canon, alias in CANDIDATES:
    covered = alias in known_aliases or alias in pool
    ce = ",".join(sorted(net_by_ex.get(canon, []))) or "-"
    ae = ",".join(sorted(net_by_ex.get(alias, []))) or "-"
    mark = "ЕСТЬ" if covered else "НЕТ"
    print(f"{canon:18s} {alias:22s} {mark:12s}  {ce} / {ae}")
    if not covered and net_by_ex.get(alias):
        todo.append((canon, alias))

print(f"\nнужно добавить: {len(todo)}")
for c, a in todo:
    print(f"   '{c}' <- '{a}'")
