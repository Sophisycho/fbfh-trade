#!/usr/bin/env python3
"""
Uniform number generator + API checker with:
- 429 retry (infinite by default): retry the SAME BAN respecting Retry-After (or fallback) with exponential backoff.
- Fatal-stop on unexpected server responses (non-200, non-JSON, result!='success', verifySHidden mismatch, schema error):
  * Print details to console
  * Append full diagnostics into errors.log
  * Save state/hits/ok then exit
  * IMPORTANT: state.json now records the FAILED ban_no itself (no +1), to avoid skipping it next run.
- Keeps ok.json (normal rows) and hits.json (A-K) updated
- Progress line with RPS and last legal VAT
- Resumable state via state.json (integer position, not necessarily legal)

CLI examples:
    python3 runner.py --year 113 --sleep 0
    python3 runner.py --year 113 --sleep 0 --checkpoint-every 500 --progress-every 50
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import zlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

import requests

try:
    from verify_s_hidden_client import get_verify_s_hidden
except Exception:
    get_verify_s_hidden = None  # type: ignore
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Files ----------
STATE_PATH = Path("state.json")
HITS_PATH = Path("hits.json")
OK_PATH = Path("ok.json")
ERR_LOG_PATH = Path("errors.log")

# ---------- API ----------
API_URL = "https://fbfh.trade.gov.tw/fb/common/popGrade.action"
VERIFY_S_HIDDEN = "1DrSTL1zk6l5itRvaE4eGQ=="  # 固定值（若伺服器更換，會觸發致命錯誤並停止）

# ---------- VAT rule ----------
WEIGHTS: List[int] = [1, 2, 1, 2, 1, 2, 4, 1]


# ===== VAT helpers =====
def sum_digits(n: int) -> int:
    return n // 10 + n % 10


def explain_uniform_number(uniform_number: str) -> Tuple[List[int], List[int], int]:
    if len(uniform_number) != 8 or not uniform_number.isdigit():
        raise ValueError("統一編號必須是 8 碼數字字串。")
    digits = [int(ch) for ch in uniform_number]
    products = [d * w for d, w in zip(digits, WEIGHTS)]
    per_digit_sums = [sum_digits(p) for p in products]
    z_total = sum(per_digit_sums)
    return products, per_digit_sums, z_total


def is_valid_uniform_number(uniform_number: str) -> bool:
    _, _, z = explain_uniform_number(uniform_number)
    if z % 5 == 0:
        return True
    if uniform_number[6] == "7" and (z + 1) % 5 == 0:
        return True
    return False


def uniform_number_stream(start: str) -> Generator[str, None, None]:
    if len(start) != 8 or not start.isdigit():
        raise ValueError("start 必須是 8 碼數字字串。")
    n = int(start)
    while n <= 99_999_999:
        s = f"{n:08d}"
        if is_valid_uniform_number(s):
            yield s
        n += 1


# ===== State / JSON I/O =====
def load_state() -> int:
    if STATE_PATH.exists():
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return int(data.get("next_number", 0))
    return 0


def save_state(next_number: int) -> None:
    STATE_PATH.write_text(json.dumps({"next_number": next_number}, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> Dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_json(path: Path, obj: Dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def append_error_log(title: str, details: Dict) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"time": ts, "title": title, "details": details}
    with ERR_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ===== HTTP =====
def create_session(pool_size: int, retries: int, backoff: float) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    # 不主動要求 br，降低 brotli 解析的不確定性
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "uniform-runner/1.4",
        "Origin": "https://fbfh.trade.gov.tw",
        "Referer": "https://fbfh.trade.gov.tw/",
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


def try_brotli_decompress(raw: bytes) -> Optional[bytes]:
    try:
        import brotli  # type: ignore
        return brotli.decompress(raw)
    except Exception:
        try:
            import brotlicffi  # type: ignore
            return brotlicffi.decompress(raw)
        except Exception:
            return None


def decode_body(resp: requests.Response) -> Optional[str]:
    enc = (resp.headers.get("Content-Encoding") or "").lower().strip()
    raw = resp.content
    try:
        if enc == "gzip":
            return zlib.decompress(raw, zlib.MAX_WBITS | 16).decode(resp.encoding or "utf-8", errors="replace")
        if enc == "deflate":
            try:
                return zlib.decompress(raw).decode(resp.encoding or "utf-8", errors="replace")
            except zlib.error:
                return zlib.decompress(raw, -zlib.MAX_WBITS).decode(resp.encoding or "utf-8", errors="replace")
        if enc == "br":
            dec = try_brotli_decompress(raw)
            if dec is not None:
                return dec.decode(resp.encoding or "utf-8", errors="replace")
            return None
        return raw.decode(resp.encoding or resp.apparent_encoding or "utf-8", errors="replace")
    except Exception:
        return None


class FatalStop(Exception):
    """用於觸發致命停止的內部例外。"""


def fatal_stop_and_log(
    ban_no: str,
    reason: str,
    resp: Optional[requests.Response],
    extra: Dict,
    hits: Dict,
    ok_map: Dict,
    last_legal: Optional[str],
    start_int: int,
) -> None:
    """
    寫入 state/hits/ok、記錄 errors.log，印出錯誤，然後退出程式。
    重要：state 會記錄「出錯的 ban_no 本身」（不再 +1），避免下次略過。
    """
    try:
        next_number = int(ban_no)
    except Exception:
        # 極端 fallback
        next_number = int(last_legal) if last_legal is not None else start_int

    save_state(next_number)
    save_json(HITS_PATH, hits)
    save_json(OK_PATH, ok_map)

    details: Dict[str, object] = {"ban_no": ban_no, "reason": reason}
    details.update(extra)

    if resp is not None:
        details["status_code"] = resp.status_code
        details["headers"] = {k: v for k, v in resp.headers.items()}
        body_text = decode_body(resp) or ""
        if not body_text:
            try:
                body_text = resp.text  # as fallback
            except Exception:
                body_text = ""
        details["body_snippet"] = body_text[:2048]

    append_error_log("Fatal stop", details)

    # 終端輸出（精簡）
    print("\n[STOP] 遇到致命錯誤，程式已停止。原因：", reason)
    if resp is not None:
        print(f"[STOP] HTTP {resp.status_code}  Content-Type={resp.headers.get('Content-Type','')}")
        snippet = details.get("body_snippet", "")
        if isinstance(snippet, str):
            print("[STOP] Response snippet:", repr(snippet[:300]))

    sys.exit(1)


def _compute_429_wait_seconds(tries: int, cooldown_on_warn: float, ra_hdr: str) -> float:
    """
    計算 429 等待秒數：
    1) 若 Retry-After 是數字，優先使用（秒）
    2) 若 Retry-After 是 HTTP 日期，轉換成剩餘秒數
    3) 否則使用指數退避：base = cooldown_on_warn(或2.0)，wait = base * 2^(tries-1)
    """
    # Retry-After (秒)
    ra_hdr = (ra_hdr or "").strip()
    if ra_hdr:
        try:
            return max(0.0, float(ra_hdr))
        except ValueError:
            # 可能是日期
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(ra_hdr)
                if dt is not None:
                    now_ts = time.time()
                    return max(0.0, dt.timestamp() - now_ts)
            except Exception:
                pass

    base = cooldown_on_warn or 2.0
    exp_wait = base * (2 ** max(0, tries - 1))
    # 可視需要設定上限；若不想上限可直接 return exp_wait
    max_cap = 300.0  # 最長等 5 分鐘再重試一次
    return min(exp_wait, max_cap)


def post_company_with_429_retry(
    ban_no: str,
    session: requests.Session,
    timeout: float,
    max_429_retries: int,
    cooldown_on_warn: float,
    hits: Dict,
    ok_map: Dict,
    last_legal: Optional[str],
    start_int: int,
) -> Optional[dict]:
    """
    送出 POST；遇到 429 時依 Retry-After（或 fallback）睡眠後重試同一 BAN。
    預設不會因 429 自行結束（max_429_retries < 0 表示無限重試）。
    其他非 200 視為致命錯誤並停止。解析 JSON 失敗或 schema/verifySHidden 異常也視為致命錯誤。
    """
    global VERIFY_S_HIDDEN
    did_refresh_vhs = False
    tries = 0
    while True:
        try:
            resp = session.post(API_URL, json={"banNo": ban_no, "verifySHidden": VERIFY_S_HIDDEN}, timeout=timeout)
        except requests.RequestException as e:
            # 網路請求層級錯誤：視為致命（以免無限重試）
            fatal_stop_and_log(
                ban_no,
                reason=f"RequestException: {e}",
                resp=None,
                extra={},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        # 429：退避後重試同一 BAN
        if resp.status_code == 429:
            tries += 1
            ra_hdr = resp.headers.get("Retry-After", "")
            wait_sec = _compute_429_wait_seconds(tries=tries, cooldown_on_warn=cooldown_on_warn, ra_hdr=ra_hdr)
            print(f"\n[WARN] {ban_no} HTTP 429, 等 {wait_sec} 秒後重試（第 {tries} 次）")
            time.sleep(wait_sec)
            if max_429_retries >= 0 and tries >= max_429_retries:
                # 只有在使用者明確指定 >=0 時才會觸發上限；預設 -1 表無限重試
                fatal_stop_and_log(
                    ban_no,
                    reason=f"Too many 429 retries ({tries})",
                    resp=resp,
                    extra={"retry_after": ra_hdr},
                    hits=hits,
                    ok_map=ok_map,
                    last_legal=last_legal,
                    start_int=start_int,
                )
            continue

        # 非 200（且不是 429）：致命
        if resp.status_code != 200:
            fatal_stop_and_log(
                ban_no,
                reason=f"HTTP {resp.status_code}",
                resp=resp,
                extra={},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        # 嘗試 JSON
        try:
            data = resp.json()
        except ValueError:
            decoded = decode_body(resp)
            if decoded:
                try:
                    data = json.loads(decoded)
                except ValueError:
                    fatal_stop_and_log(
                        ban_no,
                        reason="Response not JSON (decoded)",
                        resp=resp,
                        extra={"decoded_snippet": decoded[:2048]},
                        hits=hits,
                        ok_map=ok_map,
                        last_legal=last_legal,
                        start_int=start_int,
                    )
            else:
                fatal_stop_and_log(
                    ban_no,
                    reason="Response not JSON",
                    resp=resp,
                    extra={"raw_snippet": resp.content[:2048].hex()[:2048]},
                    hits=hits,
                    ok_map=ok_map,
                    last_legal=last_legal,
                    start_int=start_int,
                )

        # 基本 schema 與 result
        if not isinstance(data, dict):
            fatal_stop_and_log(
                ban_no,
                reason="JSON root not object",
                resp=resp,
                extra={"json_type": str(type(data))},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        if data.get("result") != "success":
            # 自動刷新 verifySHidden（僅嘗試一次）
            errmsg = str(data.get("errmsg") or data.get("error") or data.get("message") or "")
            if (not did_refresh_vhs) and (
                "請透過網頁執行查詢" in errmsg or "please query data by web site" in errmsg
            ) and (get_verify_s_hidden is not None):
                try:
                    new_vhs = get_verify_s_hidden(session=session, timeout=int(timeout))
                except Exception:
                    # 取得新 verifySHidden 失敗，交由原本致命處理
                    pass
                else:
                    if isinstance(new_vhs, str) and new_vhs and new_vhs != VERIFY_S_HIDDEN:
                        VERIFY_S_HIDDEN = new_vhs
                        print(f"[INFO] 自動更新 VERIFY_S_HIDDEN -> {new_vhs}；重試同一統編 {ban_no}")
                        continue
            fatal_stop_and_log(
                ban_no,
                reason=f"result != success ({data.get('result')!r})",
                resp=resp,
                extra={"json": data},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        # verifySHidden 檢查（若回應內帶有 viewmodel.verifySHidden，需等於我們的常數）
        vm = data.get("viewmodel") or {}
        vhs = vm.get("verifySHidden")
        if vhs is not None and str(vhs) != VERIFY_S_HIDDEN:
            fatal_stop_and_log(
                ban_no,
                reason="verifySHidden mismatch (可能已失效或被更換)",
                resp=resp,
                extra={"viewmodel": vm},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        # schema 檢查：retrieveDataList 應存在（即使空陣列也可以）
        if "retrieveDataList" not in data:
            fatal_stop_and_log(
                ban_no,
                reason="Missing retrieveDataList",
                resp=resp,
                extra={"json_keys": list(data.keys())},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        return data


# ===== parsing helpers =====
def pick_year_row(json_obj: dict, target_year: str) -> Optional[List]:
    items = (json_obj or {}).get("retrieveDataList") or []
    for entry in items:
        if not isinstance(entry, list) or len(entry) < 7:
            continue
        if str(entry[6]).strip() == str(target_year):
            return entry
        if str(entry[1]).strip().startswith(str(target_year)):
            return entry
    return None


def is_A_to_K(value: Optional[str]) -> bool:
    if not value or not isinstance(value, str):
        return False
    v = value.strip().upper()
    return len(v) == 1 and "A" <= v <= "K"


def row_is_normal(entry: List) -> bool:
    if not entry or len(entry) < 7:
        return False
    first_seven = entry[:7]
    if any(x is None for x in first_seven):
        return False
    name_zh = str(entry[2]).strip() if entry[2] is not None else ""
    return name_zh != ""


def upsert_nested(d: Dict, ban: str, year: str, payload: Dict[str, str]) -> None:
    if ban not in d:
        d[ban] = {}
    d[ban][year] = payload


# ===== CLI / main =====
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="自動產生合法統編並查 API；含 429 重試與致命錯誤立即停機紀錄。")
    p.add_argument("--year", type=int, required=True, help="查詢年度（例如 113）。")
    p.add_argument("--start", default=None, help="起始 8 碼（含）。不指定則讀 state.json。")
    p.add_argument("--sleep", type=float, default=0.0, help="每次 API 呼叫間隔秒數（可設 0）。")
    p.add_argument("--checkpoint-every", type=int, default=200, help="每處理 N 個合法統編就落盤。")
    p.add_argument("--progress-every", type=int, default=20, help="每處理 N 個合法統編就刷新進度顯示。")
    p.add_argument("--pool-size", type=int, default=20, help="HTTP 連線池大小。")
    p.add_argument("--retries", type=int, default=3, help="失敗重試次數（非 429）。")
    p.add_argument("--backoff", type=float, default=0.3, help="重試退避因子（非 429）。")
    p.add_argument("--cooldown-on-warn", type=float, default=2.0, help="遇到非 JSON/錯誤時的暫停秒數（也作為 429 無 Retry-After 的 fallback 底值）。")
    p.add_argument(
        "--max-429-retries",
        type=int,
        default=-1,
        help="同一 BAN 碰到 429 的最大重試次數；預設 -1 表示無限重試。",
    )
    p.add_argument("--timeout", type=float, default=10.0, help="單次請求逾時秒數。")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # 續跑起點（整數位置）
    if args.start:
        start_int = int(args.start)
    else:
        start_int = load_state()
    start_valid = f"{start_int:08d}"

    # 載現有清單
    hits = load_json(HITS_PATH)
    ok_map = load_json(OK_PATH)

    # 產生器與 HTTP
    gen = uniform_number_stream(start_valid)
    session = create_session(pool_size=args.pool_size, retries=args.retries, backoff=args.backoff)

    processed = 0
    t0 = time.time()
    last_legal: Optional[str] = None

    try:
        for vat in gen:
            last_legal = vat

            data = post_company_with_429_retry(
                ban_no=vat,
                session=session,
                timeout=args.timeout,
                max_429_retries=args.max_429_retries,
                cooldown_on_warn=args.cooldown_on_warn,
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

            # data 若為 None（理論上不會，因為非致命 429 都已處理；致命會 exit）
            if data:
                row = pick_year_row(data, str(args.year))
                if row:
                    name_zh = row[2] if len(row) > 2 else None
                    name_en = row[3] if len(row) > 3 else None
                    import_grade = row[4] if len(row) > 4 else None
                    export_grade = row[5] if len(row) > 5 else None

                    # 正常資料 → ok.json
                    if row_is_normal(row):
                        upsert_nested(
                            ok_map,
                            vat,
                            str(args.year),
                            {
                                "name_zh": str(name_zh) if name_zh is not None else "",
                                "name_en": str(name_en) if name_en is not None else "",
                                "import_total": str(import_grade) if import_grade is not None else "",
                                "export_total": str(export_grade) if export_grade is not None else "",
                            },
                        )
                        save_json(OK_PATH, ok_map)
                        print(f"\nOK  {vat}  year={args.year}  name_zh={str(name_zh).strip()}")

                    # 命中（A~K） → hits.json
                    if is_A_to_K(import_grade) or is_A_to_K(export_grade):
                        upsert_nested(
                            hits,
                            vat,
                            str(args.year),
                            {
                                "name_zh": str(name_zh) if name_zh is not None else "",
                                "name_en": str(name_en) if name_en is not None else "",
                                "import_total": str(import_grade) if import_grade is not None else "",
                                "export_total": str(export_grade) if export_grade is not None else "",
                            },
                        )
                        save_json(HITS_PATH, hits)
                        print(f"HIT {vat}  year={args.year}  import={import_grade}  export={export_grade}")

            processed += 1

            # 進度列印
            if processed % args.progress_every == 0:
                elapsed = max(1e-6, time.time() - t0)
                rps = processed / elapsed
                next_number = int(vat) + 1
                print(
                    f"\r進度｜目前處理到: {next_number:08d}（最後合法: {last_legal}）"
                    f"｜已處理合法數: {processed}｜RPS: {rps:.2f}",
                    end="",
                    flush=True,
                )

            # checkpoint
            if processed % args.checkpoint_every == 0:
                next_number = int(vat) + 1
                save_state(next_number)
                save_json(HITS_PATH, hits)
                save_json(OK_PATH, ok_map)

            # pace
            if args.sleep > 0:
                time.sleep(args.sleep)

    except KeyboardInterrupt:
        next_number = int(last_legal) + 1 if last_legal else start_int
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        print(f"\n已中斷，狀態已保存。下次將從 {next_number:08d} 繼續。")

    except SystemExit:
        raise

    except Exception as exc:
        # 任何未捕捉的異常也落盤（不做詳細判斷）
        next_number = int(last_legal) + 1 if last_legal else start_int
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        append_error_log("Unhandled exception", {"error": repr(exc)})
        print(f"\n[STOP] 未預期錯誤：{exc!r}，狀態已保存。")
        sys.exit(1)

    else:
        next_number = 100_000_000
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        print("\n已完成全部區間掃描。")


if __name__ == "__main__":
    main()

