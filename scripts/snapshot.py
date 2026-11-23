#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""고래 모니터 1시간 스냅샷 — 하이퍼리퀴드 상위 고래 20곳의 BTC·ETH 롱/숏 노출을 기록합니다.

GitHub Actions(.github/workflows/snapshot.yml)가 매시 정각에 실행하고,
결과를 data/snapshots.json 에 덧붙여 커밋합니다. 대시보드(index.html)는 이 파일을
읽어 체결 복원이 닿지 않는 구간을 실측 스냅샷으로 채웁니다.

지갑 목록의 단일 출처(single source of truth)는 대시보드 index.html 의 DEFAULTS 배열입니다.
data/wallets.json 이 있으면 그 파일을 먼저 사용합니다(주소 배열 또는 {address} 객체 배열).

사용 예:
    python3 scripts/snapshot.py                 # 스냅샷 1회 기록
    python3 scripts/snapshot.py --dry-run       # API 조회·집계만, 파일 쓰기 없음
    python3 scripts/snapshot.py --force         # 30분 중복 방지 창 무시
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API_URL = "https://api.hyperliquid.xyz/info"
SCOPE_COINS = ("BTC", "ETH")
DEDUPE_SEC = 30 * 60          # 같은 30분 창 안의 중복 스냅샷은 건너뜀
MAX_ENTRIES = 10000           # 보관 상한 (1시간 간격 약 1년 1개월)
SPACING_SEC = 0.35            # 지갑 사이 간격(429 예방)
MAX_ATTEMPTS = 4              # 429/5xx 재시도 횟수
TIMEOUT_SEC = 20

# index.html 을 읽지 못했을 때만 쓰는 최후 기본값(2026-11-23 기준 대시보드 기본 20곳)
FALLBACK_WALLETS = [
    "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00",
    "0x7fdafde5cfb5465924316eced2d3715494c517d1",
    "0x64b766b07362405a9cc19c2226490f3015929da9",
    "0x45d26f28196d226497130c4bac709d808fed4029",
    "0xfc27136e42af1732ddc9ce2605ea9bff1b959d9d",
    "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05",
    "0x92cbb22a9b9fc01da9914c9b39ed904dd61b859c",
    "0x023a3d058020fb76cca98f01b3c48c8938a22355",
    "0xfc667adba8d4837586078f4fdcdc29804337ca06",
    "0xdf954bbe619273a56e0cfa90dad8382195dc7d72",
    "0x519c721de735f7c9e6146d167852e60d60496a47",
    "0x10e14ba2d77263543be1bdb2851b0933ddf4f4bd",
    "0x77eeda199553e33b246e4b4666849b9ad0972902",
    "0x58b27998b4ff896949d5f57466b259e50ee211f3",
    "0xfe7ce058edc7cfcde9ef8262ba51f8d4796ab7ae",
    "0xca4903c207a0df58bf37d4bc8bbc5210726f53c8",
    "0xc926ddba8b7617dbc65712f20cf8e1b58b8598d3",
    "0xe867fbdad3291530e41530301ecb77693850c78e",
    "0x960b6c2c4b1498681b8422eb572a8b3a6aa3f0fc",
    "0xec85bc052ec5b6d6cb2b40f5caa45e39b61a77fe",
]

ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def log(msg: str) -> None:
    print(msg, flush=True)


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_wallets(root: str) -> tuple[list[str], str]:
    """지갑 주소 10곳과 그 출처를 돌려줍니다."""
    wj = os.path.join(root, "data", "wallets.json")
    if os.path.isfile(wj):
        try:
            with open(wj, encoding="utf-8") as f:
                raw = json.load(f)
            items = raw.get("wallets") if isinstance(raw, dict) else raw
            out = []
            for it in items or []:
                addr = it.get("address") if isinstance(it, dict) else it
                if isinstance(addr, str) and ADDR_RE.match(addr.strip()):
                    out.append(addr.strip().lower())
            if out:
                return dedupe(out), "data/wallets.json"
        except Exception as e:  # noqa: BLE001
            log(f"[warn] data/wallets.json 읽기 실패({e}) — index.html 로 대체합니다")

    idx = os.path.join(root, "index.html")
    if os.path.isfile(idx):
        try:
            with open(idx, encoding="utf-8", errors="replace") as f:
                html = f.read()
            m = re.search(r"var\s+DEFAULTS\s*=\s*\[(.*?)\]\s*;", html, re.S)
            if m:
                out = [a.lower() for a in re.findall(r'address\s*:\s*"([^"]+)"', m.group(1))
                       if ADDR_RE.match(a.strip())]
                if out:
                    return dedupe(out), "index.html DEFAULTS"
        except Exception as e:  # noqa: BLE001
            log(f"[warn] index.html 파싱 실패({e})")

    return list(FALLBACK_WALLETS), "built-in fallback"


def dedupe(seq: list[str]) -> list[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def post_info(payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        API_URL, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "whale-monitor-snapshot/1.0 (+https://github.com/macd46035/whale-monitor)",
        },
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_state(addr: str) -> dict:
    """429/5xx/네트워크 오류는 지수 백오프로 재시도합니다."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            data = post_info({"type": "clearinghouseState", "user": addr})
            if not isinstance(data, dict):
                raise ValueError("응답이 객체가 아닙니다")
            return data
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429 or 500 <= e.code < 600:
                wait = (1.5 * (2 ** attempt)) + random.uniform(0, 0.5)
                log(f"  · {addr[:10]}… HTTP {e.code} — {wait:.1f}s 후 재시도")
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            last = e
            wait = (1.5 * (2 ** attempt)) + random.uniform(0, 0.5)
            log(f"  · {addr[:10]}… {type(e).__name__} — {wait:.1f}s 후 재시도")
            time.sleep(wait)
    raise RuntimeError(f"{addr} 조회 실패: {last}")


def num(v) -> float:
    try:
        f = float(v)
        return f if f == f and f not in (float("inf"), float("-inf")) else 0.0
    except (TypeError, ValueError):
        return 0.0


def exposures(state: dict) -> dict:
    """BTC·ETH 포지션만 평가액 기준으로 집계합니다(index.html parsePositions 와 같은 규칙)."""
    long_usd = short_usd = 0.0
    btc_long = btc_short = 0.0
    for item in state.get("assetPositions") or []:
        p = (item or {}).get("position") or item or {}
        coin = str(p.get("coin") or "").upper()
        if coin not in SCOPE_COINS:
            continue
        size = num(p.get("szi"))
        if size == 0:
            continue
        value = abs(num(p.get("positionValue")))
        if value == 0:
            value = abs(size) * num(p.get("entryPx"))
        if not value:
            continue
        if size > 0:
            long_usd += value
        else:
            short_usd += value
        if coin == "BTC":
            if size > 0:
                btc_long += value
            else:
                btc_short += value
    return {
        "long": long_usd, "short": short_usd, "net": long_usd - short_usd,
        "btcLong": btc_long, "btcShort": btc_short,
    }


def block(long_usd: float, short_usd: float, acc: dict, ok: int, total: int) -> dict:
    tot = long_usd + short_usd
    return {
        "netLongPct": round(long_usd / tot * 100, 2) if tot > 0 else None,
        "longUsd": round(long_usd, 2),
        "shortUsd": round(short_usd, 2),
        "netUsd": round(long_usd - short_usd, 2),
        "accounts": {"long": acc["long"], "short": acc["short"], "flat": acc["flat"]},
        "coverage": {"ok": ok, "total": total},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="하이퍼리퀴드 고래 BTC·ETH 노출 1시간 스냅샷")
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않고 결과만 출력")
    ap.add_argument("--force", action="store_true", help="30분 중복 방지 창 무시")
    ap.add_argument("--out", default=None, help="출력 파일 경로(기본 data/snapshots.json)")
    args = ap.parse_args()

    root = repo_root()
    out_path = args.out or os.path.join(root, "data", "snapshots.json")
    wallets, src = load_wallets(root)
    log(f"[wallets] {len(wallets)}곳 · 출처: {src}")

    long_usd = short_usd = 0.0
    btc_long = btc_short = 0.0
    ok = 0
    failed = []
    acc = {"long": 0, "short": 0, "flat": 0}

    for i, addr in enumerate(wallets):
        if i:
            time.sleep(SPACING_SEC)
        try:
            st = fetch_state(addr)
        except Exception as e:  # noqa: BLE001
            failed.append(addr)
            log(f"[fail] {addr} — {e}")
            continue
        ex = exposures(st)
        ok += 1
        long_usd += ex["long"]
        short_usd += ex["short"]
        btc_long += ex["btcLong"]
        btc_short += ex["btcShort"]
        if ex["net"] > 0:
            acc["long"] += 1
        elif ex["net"] < 0:
            acc["short"] += 1
        else:
            acc["flat"] += 1

    total = len(wallets)
    if ok == 0:
        log("[error] 모든 지갑 조회에 실패해 스냅샷을 만들지 않습니다.")
        return 1

    now = datetime.now(timezone.utc)
    ts = int(now.timestamp() * 1000)
    entry = {
        "ts": ts,
        "iso": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts % 1000:03d}Z",
        "btcEth": block(long_usd, short_usd, acc, ok, total),
        "btc": block(btc_long, btc_short, acc, ok, total),
    }

    log(f"[snapshot] {entry['iso']} · BTC·ETH 롱 {entry['btcEth']['netLongPct']}% "
        f"(롱 ${long_usd:,.0f} / 숏 ${short_usd:,.0f}) · 커버리지 {ok}/{total}"
        + (f" · 실패 {len(failed)}곳" if failed else ""))

    doc = {"version": 1, "updated": entry["iso"], "wallets": wallets,
           "scope": "BTC·ETH", "snapshots": []}
    if os.path.isfile(out_path):
        try:
            with open(out_path, encoding="utf-8") as f:
                old = json.load(f)
            if isinstance(old, list):
                doc["snapshots"] = old
            elif isinstance(old, dict):
                doc.update({k: v for k, v in old.items() if k not in ("snapshots",)})
                doc["snapshots"] = old.get("snapshots") or []
                doc["version"] = 1
                doc["scope"] = "BTC·ETH"
                # 이번 실행의 지갑 목록과 파일에 적힌 목록을 비교하려고 잠시 보관
                doc["prevWallets"] = old.get("wallets") or None
                doc["wallets"] = wallets          # 지갑 목록은 항상 현재 기준으로 갱신
        except Exception as e:  # noqa: BLE001
            log(f"[warn] 기존 {os.path.basename(out_path)} 을 읽지 못해 새로 씁니다: {e}")
    snapshots = [s for s in doc["snapshots"] if isinstance(s, dict) and isinstance(s.get("ts"), (int, float))]
    snapshots.sort(key=lambda s: s["ts"])

    # 지갑 목록이 바뀌면(예: 10 → 20곳 확장) 예전 목록으로 잰 값은 같은 지표가 아니므로 버리고
    # 새 목록 기준으로 다시 쌓습니다. 대시보드도 목록이 다르면 스냅샷을 합치지 않습니다.
    prev_wallets = doc.get("prevWallets")
    if prev_wallets is not None and prev_wallets != wallets and snapshots:
        log("[reset] 지갑 목록이 바뀌어(=%d곳 → %d곳) 기존 %d개 스냅샷을 비웁니다."
            % (len(prev_wallets), len(wallets), len(snapshots)))
        snapshots = []

    last = snapshots[-1] if snapshots else None
    if last and not args.force and (ts - last["ts"]) < DEDUPE_SEC * 1000:
        gap_min = (ts - last["ts"]) / 60000
        log(f"[skip] 마지막 스냅샷이 {gap_min:.1f}분 전입니다 — 30분 중복 방지 창 안이라 건너뜁니다."
            " (강제 기록: --force)")
        return 0

    # 같은 밀리초가 이미 있으면 교체
    snapshots = [s for s in snapshots if s["ts"] != ts]
    snapshots.append(entry)
    snapshots.sort(key=lambda s: s["ts"])
    if len(snapshots) > MAX_ENTRIES:
        snapshots = snapshots[-MAX_ENTRIES:]
    doc["snapshots"] = snapshots
    doc["updated"] = entry["iso"]
    doc["count"] = len(snapshots)
    doc.pop("prevWallets", None)

    if args.dry_run:
        log(f"[dry-run] {len(snapshots)}개 항목 · 파일을 쓰지 않았습니다.")
        return 0

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2, separators=(",", ": "))
        f.write("\n")
    os.replace(tmp, out_path)
    log(f"[write] {out_path} · {len(snapshots)}개 항목")
    return 0


if __name__ == "__main__":
    sys.exit(main())
