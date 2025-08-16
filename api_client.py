#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
api_client.py
與遠端 API 的互動與錯誤處理（含 429 退避、致命錯誤落盤與停止、VERIFY_S_HIDDEN 自動刷新）。
"""

from __future__ import annotations
import json
import sys
import time
from typing import Dict, Optional

import requests
import simple_logger as log

from persistence import save_state, save_json, append_error_log, HITS_PATH, OK_PATH
from http_utils import decode_body

API_URL = "https://fbfh.trade.gov.tw/fb/common/popGrade.action"

# 固定值（若伺服器更換，會觸發致命錯誤並停止）
VERIFY_S_HIDDEN = "1DrSTL1zk6l5itRvaE4eGQ=="

# 可選：同目錄若存在 verify_s_hidden_client.py，將在特定錯誤時嘗試刷新
try:
    from verify_s_hidden_client import get_verify_s_hidden  # type: ignore
except Exception:
    get_verify_s_hidden = None  # type: ignore


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
    寫入 state/hits/ok、記錄 errors.log，輸出錯誤，並結束程式。
    注意：state.json 記錄當前出錯的 ban_no（不 +1），避免下次略過。
    """
    try:
        next_number = int(ban_no)
    except Exception:
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
                body_text = resp.text
            except Exception:
                body_text = ""
        details["body_snippet"] = body_text[:2048]

    append_error_log("Fatal stop", details)

    log.error(f"\n[STOP] 遇到致命錯誤，程式已停止。原因： {reason}")
    if resp is not None:
        log.error(f"[STOP] HTTP {resp.status_code}  Content-Type={resp.headers.get('Content-Type','')}")
        snippet = details.get("body_snippet", "")
        if isinstance(snippet, str):
            log.error("[STOP] Response snippet: " + repr(snippet[:300]))

    sys.exit(1)


def _compute_429_wait_seconds(tries: int, cooldown_on_warn: float, ra_hdr: str) -> float:
    """
    計算 429 等待秒數：
    1) 若 Retry-After 是數字，優先使用（秒）
    2) 若 Retry-After 是 HTTP 日期，轉換成剩餘秒數
    3) 否則採用指數退避：base = cooldown_on_warn 或 2.0；wait = base * 2^(tries-1)，上限 300 秒
    """
    ra_hdr = (ra_hdr or "").strip()
    if ra_hdr:
        try:
            return max(0.0, float(ra_hdr))
        except ValueError:
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(ra_hdr)
                if dt is not None:
                    return max(0.0, dt.timestamp() - time.time())
            except Exception:
                pass

    base = cooldown_on_warn or 2.0
    exp_wait = base * (2 ** max(0, tries - 1))
    return min(exp_wait, 300.0)


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
    針對單一統編發送 POST；遇到 429 依策略等待後重試。
    非 200、解析失敗、schema 不符或 verifySHidden 異常 → 致命停止。
    """
    global VERIFY_S_HIDDEN
    did_refresh_vhs = False
    tries = 0

    while True:
        try:
            resp = session.post(
                API_URL,
                json={"banNo": ban_no, "verifySHidden": VERIFY_S_HIDDEN},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            fatal_stop_and_log(
                ban_no,
                reason=f"RequestException: {exc}",
                resp=None,
                extra={},
                hits=hits,
                ok_map=ok_map,
                last_legal=last_legal,
                start_int=start_int,
            )

        # 429：退避後重試同一統編
        if resp.status_code == 429:
            tries += 1
            ra_hdr = resp.headers.get("Retry-After", "")
            wait_sec = _compute_429_wait_seconds(tries=tries, cooldown_on_warn=cooldown_on_warn, ra_hdr=ra_hdr)
            log.warn(f"{ban_no} HTTP 429, 等 {wait_sec} 秒後重試（第 {tries} 次）")
            time.sleep(wait_sec)

            if max_429_retries >= 0 and tries >= max_429_retries:
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

        # 非 200（且非 429）：直接致命停止
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

        # 嘗試解析 JSON
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

        # result 檢查與自動刷新 verifySHidden
        if data.get("result") != "success":
            errmsg = str(data.get("errmsg") or data.get("error") or data.get("message") or "")
            if (not did_refresh_vhs) and (
                "請透過網頁執行查詢" in errmsg or "please query data by web site" in errmsg
            ) and (get_verify_s_hidden is not None):
                try:
                    new_vhs = get_verify_s_hidden(session=session, timeout=int(timeout))  # type: ignore
                except Exception:
                    pass
                else:
                    if isinstance(new_vhs, str) and new_vhs and new_vhs != VERIFY_S_HIDDEN:
                        VERIFY_S_HIDDEN = new_vhs
                        log.info(f"自動更新 VERIFY_S_HIDDEN -> {new_vhs}；重試同一統編 {ban_no}")
                        did_refresh_vhs = True
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

        # 檢查 verifySHidden 一致性（若回傳提供）
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

        # 檢查欄位存在
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
