# -*- coding: utf-8 -*-
"""
company_details_builder.py

讀取同目錄 hits.json，對每個統一編號（banNo）呼叫：
  POST https://fbfh.trade.gov.tw/fb/common/popBasic.action

重點：
1) 執行前比對 hits.json 與既有 company_details.json：
   - 以 hits.json 為準，找出 company_details.json 缺少的 (banNo, year) 才補抓。
   - 以統編 key 排序逐一比對，就算數量一致也會檢查是否有公司漏掉。
   - 不會重建整檔；補齊後立即落盤（避免中途失敗丟進度）。
2) 打 API 若遇 429 Too Many Requests，不略過該筆，採「等待後重試」直到非 429（指數退避＋抖動，最大 60 秒）。
3) verifySHidden 由 verify_s_hidden_client.py 取得。
4) 優先以 JSON body + 最小標頭（模仿 curl）送出；若被擋或非 JSON，刷新 verifySHidden 後再以相同策略重試一次；
   仍失敗時退回 x-www-form-urlencoded + 最小標頭作最後嘗試。
5) 依指定 Key → 索引對應重構 retrieveDataList，並把評級年度、進出口評級英文代碼一併寫入輸出 JSON。

依賴：
- fbfh_trade.logger
- verify_client.py（同封裝，需提供 get_verify_s_hidden）

公開 API：
    build_and_save(
        input_path: str = "hits.json",
        output_path: str = "company_details.json",
        timeout: int = 10
    ) -> dict
"""

from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import fbfh_trade.logger as log
from fbfh_trade.company import verify_client as vsc


API_ENDPOINT = "https://fbfh.trade.gov.tw/fb/common/popBasic.action"

# === 欄位對應（依你的 Key 順序 → API retrieveDataList 的索引） ===
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
    讀 hits.json，找出 company_details.json 缺少的 (banNo, year) 逐筆補抓並寫回。
    回傳合併後的整體 dict。
    """
    log.info("開始處理 hits 檔案…")
    hits = _load_hits_strict(input_path)

    # 載入既有輸出（若不存在則為 {}）
    existing = _load_existing_output(output_path)

    # 比對差異：只針對 hits.json 有、existing 沒有的 (banNo, year) 做補抓
    # 同時做 key 排序、數量與缺漏檢查的 log。
    missing_pairs = _diff_hits_vs_existing(hits, existing)

    if not missing_pairs:
        log.success("company_details.json 已與 hits.json 對齊，無需補抓。")
        return existing

    # 取得一次 verifySHidden（以獨立 Session 取得；POST 端另走無 Cookie 路徑）
    token = _get_verify_token(timeout)

    # 逐筆補抓，完成一筆就即時寫檔，避免長流程中途失敗丟進度
    for ban_no, year in missing_pairs:
        meta = hits.get(ban_no, {}).get(year, {})  # 安全取
        import_code = _safe_get_str(meta, "import_total")
        export_code = _safe_get_str(meta, "export_total")

        row = _fetch_company_row_with_retry(
            ban_no=ban_no,
            token=token,
            timeout=timeout,
            on_token_refresh=lambda: _get_verify_token(timeout),
        )
        if row is None:
            log.warn(f"查無 retrieveDataList，略過補抓：{ban_no}-{year}")
            # 即使這筆失敗也不中斷其他筆
            continue

        details = _map_retrieve_row(row)
        enriched = {
            "rating_year": year,
            "import_total_code": import_code,
            "export_total_code": export_code,
            "details": details,
        }

        existing.setdefault(ban_no, {})
        existing[ban_no][year] = enriched

        # 逐筆即時落盤
        _save_json(existing, output_path)
        log.success(f"已補齊：{ban_no}-{year}（並寫回 {output_path}）")

    log.success(f"處理完成，輸出保持於：{output_path}")
    return existing


# ========= 私有輔助：輸入/輸出與比對 =========

def _load_hits_strict(path: str) -> Dict[str, Any]:
    """只讀 hits.json（不支援 hit.json）。"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"已讀取：{path}")
    return data


def _load_existing_output(path: str) -> Dict[str, Any]:
    """讀取既有的 company_details.json；若不存在或壞檔，回空 dict。"""
    p = Path(path)
    if not p.exists():
        log.info(f"{path} 不存在，將從空檔開始補齊。")
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            log.warn(f"{path} 結構非 dict，忽略並從空檔開始。")
            return {}
        log.info(f"已讀取既有輸出：{path}")
        return data
    except Exception as exc:
        log.warn(f"讀取 {path} 失敗（{exc!r}），忽略並從空檔開始。")
        return {}


def _diff_hits_vs_existing(
    hits: Dict[str, Any],
    existing: Dict[str, Any],
) -> List[Tuple[str, str]]:
    """
    以 hits.json 為準，比對 existing（company_details.json），
    回傳 existing 缺少的 (banNo, year) 清單（以 banNo 排序，再以 year 排序）。
    並輸出比對資訊到 log（總數、缺漏公司、缺漏年度）。
    """
    # 以 banNo key 排序
    hits_bans = sorted(
        [k for k in hits.keys() if isinstance(hits.get(k), dict)])
    existing_bans = sorted(
        [k for k in existing.keys() if isinstance(existing.get(k), dict)])

    # 數量對比
    log.info(
        f"統編數量：hits.json={len(hits_bans)} / company_details.json={len(existing_bans)}")

    # 檢查缺漏公司
    missing_bans = [b for b in hits_bans if b not in existing_bans]
    if missing_bans:
        log.warn(
            f"company_details.json 缺少 {len(missing_bans)} 家公司（示例前 5 筆）：{missing_bans[:5]}")

    # 逐統編比對年度
    missing_pairs: List[Tuple[str, str]] = []
    for ban in hits_bans:
        hit_years = sorted([y for y in hits.get(
            ban, {}).keys() if isinstance(hits[ban].get(y), dict)])
        existed_years = sorted(list((existing.get(ban) or {}).keys()))
        # 缺少的年度
        diff_years = [y for y in hit_years if y not in existed_years]
        if diff_years:
            for y in diff_years:
                missing_pairs.append((ban, y))
            # log 範例
            log.warn(f"公司 {ban} 缺少年度 {diff_years}")

    if not missing_pairs:
        log.success("逐公司年度比對無缺漏。")
    else:
        log.info(f"總缺漏筆數（banNo, year）= {len(missing_pairs)}")

    # 以 banNo, year 排序輸出待補清單
    missing_pairs.sort(key=lambda x: (x[0], x[1]))
    return missing_pairs


def _save_json(data: Dict[str, Any], path: str) -> None:
    """輸出 JSON（UTF-8、縮排）。"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========= 私有輔助：verifySHidden 與請求發送 =========

def _get_verify_token(timeout: int) -> str:
    """
    以獨立 Session 取得 verifySHidden。
    使用 GET 抓 HTML，再解析 hidden 欄位。
    """
    log.info("取得 verifySHidden（獨立 Session）…")
    session = requests.Session()
    token = vsc.get_verify_s_hidden(
        session=session, save_to="", timeout=timeout)
    log.success("verifySHidden 取得成功。")
    return token


def _request_with_backoff(
    sess: requests.Session,
    method: str,
    url: str,
    *,
    max_sleep: float = 60.0,
    base_sleep: float = 1.0,
    **kwargs: Any,
) -> requests.Response:
    """
    發送 HTTP 請求；若遇 429 Too Many Requests，會「等待後重試」，直到非 429。
    - 退避：指數退避（1s, 2s, 4s, ...），上限 max_sleep（預設 60s），附加 0~0.5 秒隨機抖動。
    - 只針對 429 堅持重試；其他狀況交由上層邏輯判斷。
    """
    sleep_sec = base_sleep
    while True:
        resp = sess.request(method=method, url=url, **kwargs)
        if resp.status_code != 429:
            return resp
        # 429：等待後重試
        jitter = random.uniform(0, 0.5)
        wait_for = min(sleep_sec, max_sleep) + jitter
        log.warn(f"HTTP 429 Too Many Requests，{wait_for:.1f}s 後重試…")
        time.sleep(wait_for)
        # 指數增長，封頂
        sleep_sec = min(sleep_sec * 2, max_sleep)


def _fetch_company_row_with_retry(
    ban_no: str,
    token: str,
    timeout: int,
    on_token_refresh,
) -> Optional[List[Any]]:
    """
    呼叫 API 的重試策略：
    1) JSON body + 最小標頭（模仿 curl），全新 Session（不攜帶 Cookie）。
       - 若非 JSON 或結構異常，判斷是否需要刷新 token，再以相同策略再試一次。
    2) 仍失敗則退回 x-www-form-urlencoded + 最小標頭做最後嘗試。
    * 任何步驟中若遇 429，_request_with_backoff 會自動等待並重試，該 payload 不會略過。
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
    * 若收到 429，_request_with_backoff 會等待後自動重試，直到非 429。
    """
    sess = session or requests.Session()
    headers = {
        "User-Agent": "curl/8.4.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    data = {"banNo": ban_no, "verifySHidden": token}

    try:
        resp = _request_with_backoff(
            sess,
            "POST",
            API_ENDPOINT,
            headers=headers,
            json=data,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        log.error(f"HTTP 連線失敗（{ban_no} / JSON）：{exc!r}")
        return None, True, None

    status = resp.status_code
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        _log_non_json(
            resp, ban_no, note="JSON 模式：Content-Type 非 JSON 或被 WAF 攔截")
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
    * 若收到 429，_request_with_backoff 會等待後自動重試，直到非 429。
    """
    sess = session or requests.Session()
    headers = {
        "User-Agent": "curl/8.4.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    data = {"banNo": ban_no, "verifySHidden": token}

    try:
        resp = _request_with_backoff(
            sess,
            "POST",
            API_ENDPOINT,
            headers=headers,
            data=data,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        log.error(f"HTTP 連線失敗（{ban_no} / FORM）：{exc!r}")
        return None, True, None

    status = resp.status_code
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" not in ctype:
        _log_non_json(
            resp, ban_no, note="FORM 模式：Content-Type 非 JSON 或被 WAF 攔截")
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


# ========= 私有輔助：LOG 與資料整形 =========

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
