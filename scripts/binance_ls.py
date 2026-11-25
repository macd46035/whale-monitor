#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""바이낸스 고래 롱 비율 — BTCUSDT 상위 트레이더 롱/숏 비율 과거 데이터를 모읍니다.

바이낸스 선물 API(fapi.binance.com/futures/data/topLongShortPositionRatio)는 최근
30일치만 주고, 미국 IP에서는 451로 막힙니다. 대신 바이낸스 공식 아카이브
(data.binance.vision)의 일별 metrics 파일은 2021-12-01부터 모든 5분 값을 담고 있어
어디서든 받을 수 있습니다.

  https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-YYYY-MM-DD.zip
  create_time,sum_toptrader_long_short_ratio,count_toptrader_long_short_ratio,...
  · sum_toptrader_long_short_ratio   = 상위 20% 트레이더 '포지션 평가액' 기준 롱/숏 비율
  · count_toptrader_long_short_ratio = 상위 20% 트레이더 '계좌 수' 기준 롱/숏 비율

결과는 data/binance_ls.json 에 타임프레임별 롱 비율(%) 배열로 저장합니다.
  long% = ratio / (1 + ratio) * 100        (순비율 = 2 * long% - 100)

사용 예:
    python3 scripts/binance_ls.py --backfill      # 2021-12-01 ~ 어제 전체 수집(약 1,800일)
    python3 scripts/binance_ls.py                 # 새로 올라온 날짜만 덧붙임(매시간 실행용)
    python3 scripts/binance_ls.py --days 30       # 마지막 30일만 새로 받아 갱신
    python3 scripts/binance_ls.py --dry-run       # 파일을 쓰지 않고 결과만 확인
"""
from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

SYMBOL = "BTCUSDT"
BASE = ("https://data.binance.vision/data/futures/um/daily/metrics/"
        f"{SYMBOL}/{SYMBOL}-metrics-{{day}}.zip")
FIRST_DAY = "2021-12-01"          # 아카이브에 metrics 가 있는 첫 날
SAMPLE_SEC = 300                  # 5분 값
# 타임프레임별 (초, 보관 기간 일수 — None 이면 전체)
TFS = {
    "15m": (900, 60),
    "1h": (3600, None),
    "4h": (14400, None),
    "1d": (86400, None),
}
COL_POS = "sum_toptrader_long_short_ratio"
COL_ACC = "count_toptrader_long_short_ratio"
WORKERS = 16
RETRIES = 3
TIMEOUT = 30

_lock = threading.Lock()


def log(msg: str) -> None:
    print(msg, flush=True)


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def utc_yesterday() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def days_between(a: str, b: str) -> list[str]:
    d0 = datetime.strptime(a, "%Y-%m-%d").date()
    d1 = datetime.strptime(b, "%Y-%m-%d").date()
    out = []
    while d0 <= d1:
        out.append(d0.isoformat())
        d0 += timedelta(days=1)
    return out


def ratio_to_pct(ratio: float) -> float:
    """롱/숏 비율 → 롱 비율(%). 비율 1.0 이면 50%."""
    if ratio <= 0:
        return 50.0
    return ratio / (1.0 + ratio) * 100.0


def parse_row_pct(row: dict) -> tuple[float, float] | None:
    try:
        pos = float(row[COL_POS])
        acc = float(row[COL_ACC])
    except (KeyError, TypeError, ValueError):
        return None
    if pos <= 0 or acc <= 0:
        return None
    return round(ratio_to_pct(pos), 2), round(ratio_to_pct(acc), 2)


def fetch_day(day: str) -> tuple[str, list[tuple[int, float, float]] | None]:
    """하루치 zip 을 받아 [(epoch_sec, pos%, acc%), ...] (시간순) 로 돌려줍니다.
    아카이브에 그 날짜가 없으면(404) None."""
    url = BASE.format(day=day)
    last_err = None
    for attempt in range(RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "whale-monitor-binance-ls/1.0 (+https://github.com/macd46035/whale-monitor)",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                raw = r.read()
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                name = next((n for n in z.namelist() if n.endswith(".csv")), None)
                if name is None:
                    return day, []
                text = z.read(name).decode("utf-8", "replace")
            rows = []
            lines = text.strip().split("\n")
            if not lines:
                return day, []
            head = [h.strip() for h in lines[0].split(",")]
            idx = {h: i for i, h in enumerate(head)}
            has_head = "create_time" in idx
            # 헤더가 없는 옛 파일도 있어 컬럼 순서로 대비합니다.
            ip = idx.get(COL_POS, 5) if has_head else 5
            ia = idx.get(COL_ACC, 4) if has_head else 4
            it = idx.get("create_time", 0) if has_head else 0
            seen = {}
            for ln in lines[1:] if has_head else lines:
                cells = ln.split(",")
                if len(cells) <= max(ip, ia, it):
                    continue
                try:
                    ts = calendar.timegm(time.strptime(cells[it].strip(), "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    continue
                try:
                    pos = float(cells[ip]); acc = float(cells[ia])
                except ValueError:
                    continue
                if pos <= 0 or acc <= 0:
                    continue
                seen[ts] = (ts, round(ratio_to_pct(pos), 2), round(ratio_to_pct(acc), 2))
            return day, [seen[k] for k in sorted(seen)]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return day, None
            last_err = e
            time.sleep(1.0 + attempt * 1.5)
        except (urllib.error.URLError, TimeoutError, OSError, zipfile.BadZipFile) as e:
            last_err = e
            time.sleep(1.0 + attempt * 1.5)
    raise RuntimeError(f"{day} 내려받기 실패: {last_err}")


def fetch_days(days: list[str]) -> list[tuple[int, float, float]]:
    """여러 날짜를 병렬로 받아 한 줄기(시간순)로 합칩니다."""
    rows: list[tuple[int, float, float]] = []
    missing: list[str] = []
    failed: list[str] = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for day, got in ex.map(fetch_day, days):
            done += 1
            if got is None:
                missing.append(day)
            else:
                rows.extend(got)
            if done % 200 == 0 or done == len(days):
                log(f"  · {done}/{len(days)}일 · {time.time() - t0:.0f}s")
    if missing:
        log(f"[warn] 아카이브에 없는 날짜 {len(missing)}일: {missing[:5]}{' …' if len(missing) > 5 else ''}")
    rows.sort()
    # 날짜 파일이 겹쳐 같은 시각이 두 번 들어오면 뒤 값으로 교체
    dedup = {}
    for ts, pos, acc in rows:
        dedup[ts] = (ts, pos, acc)
    return [dedup[k] for k in sorted(dedup)]


def build_tf(rows: list[tuple[int, float, float]], step: int, keep_days: int | None) -> dict:
    """5분 값을 봉 마감 시점(그 봉의 마지막 5분 값)으로 집계해 배열로 만듭니다."""
    if not rows:
        return {"start": 0, "step": step, "pos": [], "acc": []}
    first_ts = rows[0][0]
    last_ts = rows[-1][0]
    start = (first_ts // step) * step
    if keep_days:
        cutoff = ((last_ts - keep_days * 86400) // step) * step
        start = max(start, cutoff)
    end = (last_ts // step) * step
    n = (end - start) // step + 1
    pos = [None] * n
    acc = [None] * n
    for ts, p, a in rows:
        i = (ts - start) // step
        if 0 <= i < n:
            pos[i] = p
            acc[i] = a
    return {"start": start, "step": step, "pos": pos, "acc": acc}


def expand_tf(block: dict) -> dict[int, tuple[float, float]]:
    out: dict[int, tuple[float, float]] = {}
    start = int(block.get("start") or 0)
    step = int(block.get("step") or SAMPLE_SEC)
    pos = block.get("pos") or []
    acc = block.get("acc") or []
    for i, p in enumerate(pos):
        a = acc[i] if i < len(acc) else None
        if p is None or a is None:
            continue
        out[start + i * step] = (p, a)
    return out


def iso(ts: int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_doc(bars: dict[str, dict[int, tuple[float, float]]], last_sample_ts: int) -> dict:
    tfs = {}
    for tf, (step, keep_days) in TFS.items():
        m = bars.get(tf) or {}
        if not m:
            tfs[tf] = {"start": 0, "step": step, "pos": [], "acc": []}
            continue
        keys = sorted(m)
        start = (keys[0] // step) * step
        end = (keys[-1] // step) * step
        if keep_days:
            start = max(start, ((end - keep_days * 86400) // step) * step)
        n = (end - start) // step + 1
        pos = [None] * n
        acc = [None] * n
        for ts, (p, a) in m.items():
            i = (ts - start) // step
            if 0 <= i < n:
                pos[i] = p
                acc[i] = a
        tfs[tf] = {"start": start, "step": step, "pos": pos, "acc": acc}
    spans = {tf: b["start"] for tf, b in tfs.items() if b["start"]}
    return {
        "v": 1,
        "symbol": SYMBOL,
        "source": "binance-vision",
        "sourceUrl": "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/",
        "metric": "top-20% trader long/short ratio",
        "note": ("pos = 상위 20% 트레이더 포지션 평가액 기준 롱 비율(%), "
                 "acc = 같은 트레이더 계좌 수 기준 롱 비율(%). 각 봉은 그 봉의 마지막 5분 값."),
        "updated": iso(last_sample_ts),
        "first": iso(min(spans.values())) if spans else None,
        "tfs": tfs,
    }


def load_doc(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict) and isinstance(doc.get("tfs"), dict):
            return doc
    except (OSError, ValueError):
        pass
    return None


def collect(root: str, out_path: str | None = None, backfill: bool = False,
            days: int | None = None, dry_run: bool = False) -> dict:
    """아카이브를 받아 data/binance_ls.json 을 갱신하고 요약을 돌려줍니다."""
    out_path = out_path or os.path.join(root, "data", "binance_ls.json")
    yesterday = utc_yesterday()
    old = None if backfill else load_doc(out_path)

    if old:
        # 기존 파일의 봉을 되살린 뒤, 마지막으로 받은 날의 하루 전부터 다시 받습니다
        # (그 날 파일이 아직 만들어지는 중이었을 수 있어 한 번 더 덮어씁니다).
        bars = {tf: expand_tf(old["tfs"].get(tf) or {}) for tf in TFS}
        last_ts = 0
        for m in bars.values():
            if m:
                last_ts = max(last_ts, max(m))
        last_day = datetime.fromtimestamp(last_ts, timezone.utc).strftime("%Y-%m-%d") if last_ts else FIRST_DAY
        from_day = (datetime.strptime(last_day, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
        if from_day < FIRST_DAY:
            from_day = FIRST_DAY
        log(f"[merge] 기존 {out_path} · 마지막 {iso(last_ts)} · {from_day} ~ {yesterday} 갱신")
    else:
        bars = {tf: {} for tf in TFS}
        last_ts = 0
        from_day = FIRST_DAY
        log(f"[backfill] {from_day} ~ {yesterday}")

    if days:
        cut = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        from_day = max(from_day, cut) if old else cut

    days_list = days_between(from_day, yesterday)
    if not days_list:
        log("[skip] 새로 받을 날짜가 없습니다.")
        return {"changed": False, "days": 0}
    log(f"[fetch] {len(days_list)}일 · 스레드 {WORKERS}")
    rows = fetch_days(days_list)
    log(f"[parse] 5분 값 {len(rows):,}개 · {iso(rows[0][0]) if rows else '—'} ~ {iso(rows[-1][0]) if rows else '—'}")
    if not rows:
        return {"changed": False, "days": len(days_list)}

    fetched_bars: dict[str, dict[int, tuple[float, float]]] = {}
    for tf, (step, _keep) in TFS.items():
        m = {}
        for ts, p, a in rows:
            m[(ts // step) * step] = (p, a)
        fetched_bars[tf] = m
        bars.setdefault(tf, {}).update(m)

    # 보관 기간(15m 60일) 밖은 버립니다
    for tf, (step, keep_days) in TFS.items():
        if keep_days:
            keys = sorted(bars[tf])
            if keys:
                cutoff = keys[-1] - keep_days * 86400
                bars[tf] = {k: v for k, v in bars[tf].items() if k >= cutoff}

    new_last = max([last_ts] + [max(m) for m in fetched_bars.values() if m]) if fetched_bars else last_ts
    doc = build_doc(bars, new_last)
    size = len(json.dumps(doc, separators=(",", ":")))
    log(f"[build] {size / 1024:,.0f} KB · " + " · ".join(
        f"{tf} {len(b['pos']):,}봉({iso(b['start'] or None)})" for tf, b in doc["tfs"].items()))

    if dry_run:
        log("[dry-run] 파일을 쓰지 않았습니다.")
        return {"changed": False, "days": len(days_list), "updated": doc["updated"]}

    # 내용이 그대로면 파일을 건드리지 않습니다(매시간 커밋 방지)
    if old and json.dumps(old.get("tfs"), sort_keys=True) == json.dumps(doc["tfs"], sort_keys=True):
        log("[skip] 새 자료 없음 — 파일 그대로")
        return {"changed": False, "days": len(days_list), "updated": doc["updated"]}

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, out_path)
    log(f"[write] {out_path} · {os.path.getsize(out_path) / 1024:,.0f} KB")
    return {"changed": True, "days": len(days_list), "updated": doc["updated"]}


def update_archive(root: str, dry_run: bool = False) -> dict:
    """snapshot.py(매시간)에서 부르는 증분 갱신 — 새 날짜만 덧붙입니다."""
    return collect(root, backfill=False, dry_run=dry_run)


def main() -> int:
    ap = argparse.ArgumentParser(description="바이낸스 BTCUSDT 상위 트레이더 롱/숏 비율 아카이브 수집")
    ap.add_argument("--backfill", action="store_true", help=f"{FIRST_DAY}부터 전체 수집")
    ap.add_argument("--days", type=int, default=None, help="마지막 N일만 새로 받아 갱신")
    ap.add_argument("--out", default=None, help="출력 경로(기본 data/binance_ls.json)")
    ap.add_argument("--dry-run", action="store_true", help="파일을 쓰지 않음")
    args = ap.parse_args()
    collect(repo_root(), out_path=args.out, backfill=args.backfill, days=args.days, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
