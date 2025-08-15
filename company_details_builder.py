# -*- coding: utf-8 -*-
"""
company_details_builder.py

讀取同目錄 hits.json，對每個統一編號（banNo）呼叫：
  POST https://fbfh.trade.gov.tw/fb/common/popBasic.action

重點實作：
1) verifySHidden 由 verify_s_hidden_client.py 取得。
2) 優先以「JSON body + 最小標頭（模仿 curl）」送出（不帶 Referer/Origin/X-Requested-With、不攜帶 Cookie）。
3) 若被 WAF 擋或非 JSON，刷新 verifySHidden 後再以相同策略重試一次。
4) 仍失敗時退回 x-www-form-urlencoded + 最小標頭作最後嘗試。
5) 依指定 Key → 索引對應重構 retrieveDataList，並把評級年度、進出口評級英文代碼一併寫入輸出 JSON。

依賴：
- simple_logger.py（同目錄）
- verify_s_hidden_client.py（同目錄，需提供 get_verify_s_hidden）

公開 API：
    build_and_save(
        input_path: str = "hits.json",
        output_path: str = "company_details.json",
        timeout: int = 10
    ) -> dict
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import simple_logger as log
import verify_s_hidden_client as vsc


API_ENDPOINT = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"

# === 欄位對應（依你的 Key 順序 → API retrieveDataList 的索引） ===
# 參考你提供的回應範例推定索引：
#  0  統一編號
#  1  公司中文名
#  2  公司英文名
#  3  代表人
#  4  原始登記日期
#  5  核發日期（最後變更）
#  6  中文地址
#  7  英文地址
#  8  電話1
#  9  電話2
# 10  傳真
# 11  原中文名
# 12  原英文名
# 13  網址
# 14  Email
# 15  產品項目進口
# 16  產品項目出口
# 19  進口資格
# 20  出口資格
FIELD_MAPPING: List[Tuple[str, int]] = [
    ("business_account_no", 0),          # 1. 統一編號
    ("date_of_last_change", 5),          # 2. 核發日期
    ("original_registration_date", 4),   # 3. 原始登記日期
    ("company_name_zh", 1),              # 4. 廠商中文名稱
    ("company_name_en", 2),              # 5. 廠商英文名稱
    ("business_address_zh", 6),          # 6. 中文營業地址
    ("business_address_en", 7),          # 7. 英文營業地址
    ("representative", 3),               # 8. 代表人
    ("telephone_1", 8),                  # 9. 電話號碼 1
    ("telephone_2", 9),                  # 10. 電話號碼 2
    ("fax", 10),                         # 11. 傳真號碼
    ("former_name_zh", 11),              # 12. 廠商原中文名稱
    ("former_name_en", 12),              # 13. 廠商原英文名稱
    ("website", 13),                     # 14. 廠商網址
    ("email", 14),                       # 15. 電子信箱
    ("items_for_import", 15),            # 16. 產品項目進口
    ("items_for_export", 16),            # 17. 產品項目出口
    ("import_qualification", 19),        # 18. 進口資格
    ("export_qualification", 20),        # 19. 出口資格
]


# ========= 公開主流程 =========

def build_and_save(
    input_path: str = "hits.json",
    output_path: str = "company_details.json",
    timeout: int = 10,
) -> Dict[str, Dict[str, Any]]:
    """
    讀 hits.json，逐筆呼叫 API，重構結果並寫入 output_path。
    回傳重構後的整體 dict。
    """
    log.info("開始處理 hits 檔案…")
    hits = _load_hits_strict(input_path)

    # 取得一次 verifySHidden（以獨立 Session 取得；POST 端另走無 Cookie 路徑）
    token = _get_verify_token(timeout)

    result: Dict[str, Dict[str, Any]] = {}

    for ban_no, years_data in hits.items():
        if not isinstance(years_data, dict):
            log.warn(f"略過非 dict 年度資料：{ban_no}")
            continue

        for year, meta in years_data.items():
            import_code = _safe_get_str(meta, "import_total")
            export_code = _safe_get_str(meta, "export_total")

            row = _fetch_company_row_with_retry(
                ban_no=ban_no,
                token=token,
                timeout=timeout,
                on_token_refresh=lambda: _get_verify_token(timeout),
            )
            if row is None:
                log.warn(f"查無 retrieveDataList，略過：{ban_no}-{year}")
                continue

            details = _map_retrieve_row(row)

            enriched = {
                "rating_year": year,
                "import_total_code": import_code,
                "export_total_code": export_code,
                "details": details,
            }

            result.setdefault(ban_no, {})
            result[ban_no][year] = enriched

            log.success(f"完成：{ban_no}-{year}")

    _save_json(result, output_path)
    log.success(f"已輸出：{output_path}")
    return result


# ========= 私有輔助 =========

def _load_hits_strict(path: str) -> Dict[str, Any]:
    """只讀 hits.json（不支援 hit.json）。"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"已讀取：{path}")
    return data


def _get_verify_token(timeout: int) -> str:
    """
    以獨立 Session 取得 verifySHidden。
    使用 GET 抓 HTML，再解析 hidden 欄位。
    """
    log.info("取得 verifySHidden（獨立 Session）…")
    session = requests.Session()
    token = vsc.get_verify_s_hidden(session=session, save_to="", timeout=timeout)
    log.success("verifySHidden 取得成功。")
    return token


def _fetch_company_row_with_retry(
    ban_no: str,
    token: str,
    timeout: int,
    on_token_refresh,
) -> Optional[List[Any]]:
    """
    呼叫 API 的重試策略：
    1) JSON body + 最小標頭（模仿 curl），全新 Session（不攜帶 Cookie）。
    2) 若被擋或非 JSON，刷新 verifySHidden 後再以相同策略重試一次。
    3) 仍失敗則退回 x-www-form-urlencoded + 最小標頭做最後嘗試。
    """
    # 第一次嘗試：JSON + minimal headers（fresh session）
    row, need_refresh, _ = _fetch_company_row_json_minimal(
        session=None,
        ban_no=ban_no,
        token=token,
        timeout=timeout,
    )
    if row is not None:
        return row

    # 第二次：刷新 token 後再試一次 JSON
    if need_refresh:
        log.warn(f"{ban_no} JSON 最小標頭被擋，刷新 verifySHidden 後重試 JSON。")
        time.sleep(0.6)
        new_token = on_token_refresh()
        row2, _, _ = _fetch_company_row_json_minimal(
            session=None,
            ban_no=ban_no,
            token=new_token,
            timeout=timeout,
        )
        if row2 is not None:
            return row2

    # 第三次：退回 form-urlencoded + minimal headers
    log.warn(f"{ban_no} 退回 x-www-form-urlencoded 最終嘗試。")
    row3, _, _ = _fetch_company_row_form_minimal(
        session=None,
        ban_no=ban_no,
        token=token,
        timeout=timeout,
    )
    return row3


def _fetch_company_row_json_minimal(
    session: Optional[requests.Session],
    ban_no: str,
    token: str,
    timeout: int,
) -> Tuple[Optional[List[Any]], bool, Optional[int]]:
    """
    模仿成功的 curl：
      - Content-Type: application/json
      - Accept: application/json
      - 不帶 Referer/Origin/X-Requested-With
      - 使用 *新的* Session，避免攜帶 Cookie 觸發 WAF
    回傳 (row|None, need_refresh, http_status)
    """
    sess = session or requests.Session()
    headers = {
        "User-Agent": "curl/8.4.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    data = {"banNo": ban_no, "verifySHidden": token}

    try:
        resp = sess.post(API_ENDPOINT, headers=headers, json=data, timeout=timeout)
    except requests.RequestException as exc:
        log.error(f"HTTP 連線失敗（{ban_no} / JSON）：{exc!r}")
        return None, True, None

    status = resp.status_code
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        _log_non_json(resp, ban_no, note="JSON 模式：Content-Type 非 JSON 或被 WAF 攔截")
        return None, True, status

    try:
        payload = resp.json()
    except Exception as exc:
        _log_non_json(resp, ban_no, note=f"JSON 模式：JSON 解析失敗：{exc!r}")
        return None, True, status

    if payload.get("result") != "success":
        log.warn(f"JSON 模式：result!=success（{ban_no}）：{payload.get('result')}")
        return None, False, status

    data_list = payload.get("retrieveDataList") or []
    if not isinstance(data_list, list) or not data_list or not isinstance(data_list[0], list):
        _log_non_json(resp, ban_no, note="JSON 模式：retrieveDataList 結構異常")
        return None, True, status

    return data_list[0], False, status


def _fetch_company_row_form_minimal(
    session: Optional[requests.Session],
    ban_no: str,
    token: str,
    timeout: int,
) -> Tuple[Optional[List[Any]], bool, Optional[int]]:
    """
    最終退回方案：以表單送出，但仍採最小標頭（不帶 Referer/Origin/X-Requested-With）。
    有些站點其中一種會通過。
    """
    sess = session or requests.Session()
    headers = {
        "User-Agent": "curl/8.4.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    data = {"banNo": ban_no, "verifySHidden": token}

    try:
        resp = sess.post(API_ENDPOINT, headers=headers, data=data, timeout=timeout)
    except requests.RequestException as exc:
        log.error(f"HTTP 連線失敗（{ban_no} / FORM）：{exc!r}")
        return None, True, None

    status = resp.status_code
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        _log_non_json(resp, ban_no, note="FORM 模式：Content-Type 非 JSON 或被 WAF 攔截")
        return None, True, status

    try:
        payload = resp.json()
    except Exception as exc:
        _log_non_json(resp, ban_no, note=f"FORM 模式：JSON 解析失敗：{exc!r}")
        return None, True, status

    if payload.get("result") != "success":
        log.warn(f"FORM 模式：result!=success（{ban_no}）：{payload.get('result')}")
        return None, False, status

    data_list = payload.get("retrieveDataList") or []
    if not isinstance(data_list, list) or not data_list or not isinstance(data_list[0], list):
        _log_non_json(resp, ban_no, note="FORM 模式：retrieveDataList 結構異常")
        return None, True, status

    return data_list[0], False, status


def _log_non_json(resp: requests.Response, ban_no: str, note: str) -> None:
    """把非 JSON 情況的細節打進日誌，方便排查。"""
    ctype = resp.headers.get("Content-Type")
    snippet = (resp.text or "")[:300].replace("\n", " ")
    log.error(
        f"非 JSON 回應（{ban_no}）：{note} / 狀態碼={resp.status_code} / "
        f"Content-Type={ctype!r} / 片段={snippet!r}"
    )


def _map_retrieve_row(row: List[Any]) -> Dict[str, Any]:
    """依 FIELD_MAPPING 對應 retrieveDataList 的欄位，回傳結構化 dict。"""
    mapped: Dict[str, Any] = {}
    for key, idx in FIELD_MAPPING:
        mapped[key] = _safe_pick(row, idx)
    return mapped


def _safe_pick(row: List[Any], idx: int) -> Optional[Any]:
    """安全取值（避免越界），空字串視為 None。"""
    if idx < 0 or idx >= len(row):
        return None
    value = row[idx]
    if value in ("", None):
        return None
    return value


def _safe_get_str(d: Dict[str, Any], key: str) -> Optional[str]:
    """從 dict 取字串，若不存在或空字串則回 None。"""
    try:
        val = d.get(key)
        return val if isinstance(val, str) and val.strip() else None
    except Exception:
        return None


def _save_json(data: Dict[str, Any], path: str) -> None:
    """輸出 JSON（UTF-8、縮排）。"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

