#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    # 優先使用 openpyxl 內建的非法字元正規表示式
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE  # type: ignore
except Exception:  # noqa: BLE001 - 需廣泛兼容舊版 openpyxl
    ILLEGAL_CHARACTERS_RE = None  # type: ignore[assignment]

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

# ----------------------------
# 常數與 I/O 路徑
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_JSON = BASE_DIR / "company_details.json"
OUTPUT_XLSX = BASE_DIR / "company_details.xlsx"

# 預期輸出欄位
HEADERS: List[str] = [
    "統一編號",
    "公司名稱",
    "電話號碼",
    "進口評級",
    "出口評級",
    "評等年度",
    "公司名稱(英文)",
    "代表人",
    "登記地址(中文)",
    "登記地址(英文)",
    "最近異動日期",
    "最初登記日期",
    "前名稱(中文)",
    "前名稱(英文)",
    "網站",
    "Email",
    "進口資格",
    "出口資格",
    "進口項目",
    "出口項目",
]

# 欄寬估算上限，避免因超長字串導致視覺與效能問題
MAX_COL_WIDTH = 60


# ----------------------------
# 工具函式
# ----------------------------
def _illegal_char_clean(s: str) -> str:
    """移除 Excel 禁用的控制字元。

    優先使用 openpyxl 的 ILLEGAL_CHARACTERS_RE。若不可用，改用手動 regex：
    [\x00-\x08 \x0B-\x0C \x0E-\x1F]（保留 \t \n \r）
    """
    if not s:
        return s
    if ILLEGAL_CHARACTERS_RE is not None:
        return ILLEGAL_CHARACTERS_RE.sub("", s)  # type: ignore[operator]
    # 備援：移除除 \t \n \r 以外的 C0 控制字元
    return re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", s)


def _sanitize_cell_value(value: Any) -> str:
    """將任意輸入清洗為可安全寫入 Excel 的字串。

    規則：
    - None -> ""
    - dict/list -> json.dumps(..., ensure_ascii=False)
    - 其他型別 -> str(value)
    - 移除 Excel 禁用控制字元
    """
    if value is None:
        return ""

    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)

    return _illegal_char_clean(text)


def _best_effort_concat(*parts: Optional[str], sep: str = " / ") -> str:
    """將多個可能為 None/空字串的欄位做 best-effort 串接。"""
    cleaned = [_sanitize_cell_value(p).strip() for p in parts if p]
    return sep.join([c for c in cleaned if c])


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"找不到輸入檔案：{path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _flatten_records(raw: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """將輸入 JSON 轉為列資料（dict）。

    假設結構（依使用者提供片段，不作過度臆測）：
    {
      "<統一編號>": {
        "<年度>": {
          "rating_year": "...",
          "import_total_code": "...",
          "export_total_code": "...",
          "details": {
             "company_name_zh": "...",
             "company_name_en": "...",
             "business_address_zh": "...",
             "business_address_en": "...",
             "telephone_1": "02-....",
             "telephone_2": null,
             "last_modified_date": "...",         # 可能存在；若無以空字串
             "initial_register_date": "...",      # 可能存在；若無以空字串
             "former_name_zh": "...",             # 可能存在；若無以空字串
             "former_name_en": "...",             # 可能存在；若無以空字串
             "website": "...",
             "email": "...",
             "import_qualification": "有 YES",
             "export_qualification": "有 YES",
             "items_for_import": null,
             "items_for_export": null,
             "representative": "..."              # 可能存在；若無以空字串
          }
        }
      }
    }

    注意：若鍵不存在，一律以空字串處理，不中斷。
    """
    for tax_id, years in raw.items():
        if not isinstance(years, dict):
            # 防禦性處理：資料異常時仍產出一列以便檢視
            yield {
                "統一編號": tax_id,
                "公司名稱": "",
                "電話號碼": "",
                "進口評級": "",
                "出口評級": "",
                "評等年度": "",
                "公司名稱(英文)": "",
                "代表人": "",
                "登記地址(中文)": "",
                "登記地址(英文)": "",
                "最近異動日期": "",
                "最初登記日期": "",
                "前名稱(中文)": "",
                "前名稱(英文)": "",
                "網站": "",
                "Email": "",
                "進口資格": "",
                "出口資格": "",
                "進口項目": "",
                "出口項目": "",
            }
            continue

        for _, payload in years.items():
            details = payload.get("details", {}) if isinstance(
                payload, dict) else {}
            # 映射各欄位（缺漏即以空字串）
            row = {
                "統一編號": tax_id,
                "公司名稱": details.get("company_name_zh", ""),
                "電話號碼": _best_effort_concat(
                    details.get("telephone_1"), details.get("telephone_2")
                ),
                "進口評級": payload.get("import_total_code", ""),
                "出口評級": payload.get("export_total_code", ""),
                "評等年度": payload.get("rating_year", ""),
                "公司名稱(英文)": details.get("company_name_en", ""),
                "代表人": details.get("representative", ""),
                "登記地址(中文)": details.get("business_address_zh", ""),
                "登記地址(英文)": details.get("business_address_en", ""),
                "最近異動日期": details.get("last_modified_date", ""),
                "最初登記日期": details.get("initial_register_date", ""),
                "前名稱(中文)": details.get("former_name_zh", ""),
                "前名稱(英文)": details.get("former_name_en", ""),
                "網站": details.get("website", ""),
                "Email": details.get("email", ""),
                "進口資格": details.get("import_qualification", ""),
                "出口資格": details.get("export_qualification", ""),
                "進口項目": details.get("items_for_import", ""),
                "出口項目": details.get("items_for_export", ""),
            }
            yield row


def _auto_fit_column_width(ws: Worksheet) -> None:
    """依儲存格內容粗估欄寬，並設上限。"""
    for col_cells in ws.columns:
        max_len = 0
        column_letter = col_cells[0].column_letter
        for cell in col_cells:
            try:
                text = "" if cell.value is None else str(cell.value)
            except Exception:
                text = ""
            max_len = max(max_len, len(text))
        ws.column_dimensions[column_letter].width = min(
            MAX_COL_WIDTH, max(10, max_len + 2))


def write_excel(rows: Iterable[Dict[str, Any]], path: Path) -> Tuple[int, int]:
    """將列資料寫入 Excel。

    參數：
        rows: 由 _flatten_records 產生的 dict 迭代器
        path: 輸出檔案路徑

    回傳：
        (total_rows, failed_rows)
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "company_details"

    # 1) 表頭
    ws.append(HEADERS)

    failed = 0
    total = 0

    for row_idx, row in enumerate(rows, start=2):  # Excel 第 2 列起為資料列
        total += 1
        try:
            values = [_sanitize_cell_value(row.get(h, "")) for h in HEADERS]
            ws.append(values)
        except Exception as exc:  # noqa: BLE001 - 實務上需捕獲所有寫入錯誤並標示
            failed += 1
            exc_name = exc.__class__.__name__
            print(
                f"[ERROR] 寫入 Excel 失敗：資料列 {row_idx} 寫入失敗（{exc_name}: {exc}）。"
                f" 欄位型別：{{h: type(row.get(h, '')).__name__ for h in HEADERS}} ",
                file=sys.stderr,
            )

    # 2) 凍結窗格：鎖定表頭
    ws.freeze_panes = "A2"

    # 3) 自動篩選
    ws.auto_filter.ref = ws.dimensions

    # 4) 欄寬估算
    _auto_fit_column_width(ws)

    # 5) 輸出
    wb.save(path)
    return total, failed


def main() -> None:
    try:
        raw = _read_json(INPUT_JSON)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 讀取輸入 JSON 失敗：{exc}", file=sys.stderr)
        sys.exit(2)

    rows = list(_flatten_records(raw))
    if not rows:
        print("[WARN] 輸入 JSON 解析後沒有任何資料列。仍會輸出只有表頭的 Excel。")

    try:
        total, failed = write_excel(rows, OUTPUT_XLSX)
    except Exception as exc:  # noqa: BLE001
        exc_name = exc.__class__.__name__
        print(f"[ERROR] 寫入 Excel 失敗（{exc_name}: {exc}）", file=sys.stderr)
        sys.exit(3)

    ok = total - failed
    print(f"[INFO] 完成輸出：{OUTPUT_XLSX}")
    print(f"[INFO] 總列數：{total}，成功：{ok}，失敗：{failed}")
    if failed > 0:
        # 若想遇錯即停，可改為 sys.exit(4)
        print("[WARN] 部分列寫入失敗，詳見上方錯誤訊息。")


if __name__ == "__main__":
    main()
