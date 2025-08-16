#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Read company_details.json in the same directory as the executable
and export to company_details.xlsx.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

from persistence import BASE_DIR  # 關鍵：exe 同目錄

try:
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.worksheet import Worksheet
except ImportError as exc:
    print(
        "[ERROR] 需要套件 openpyxl。\n"
        "請先安裝：pip install openpyxl",
        file=sys.stderr,
    )
    raise

# ========【在這裡定義輸出欄位順序，左到右 = 上到下】========
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

Extractor = Callable[[str, str, Dict[str, Any]], Any]  # (ban_no, year_key, year_payload) -> value


def _get_details(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get("details", {}) or {}


def _coalesce(*values: Any, sep: str = " / ") -> str:
    parts = [str(v) for v in values if v not in (None, "", "null")]
    return sep.join(parts)


FIELD_EXTRACTORS: Dict[str, Callable[[str, str, Dict[str, Any]], Any]] = {
    "統一編號": lambda ban, y, p: ban,
    "公司名稱": lambda ban, y, p: _get_details(p).get("company_name_zh", ""),
    "電話號碼": lambda ban, y, p: _coalesce(
        _get_details(p).get("telephone_1"),
        _get_details(p).get("telephone_2"),
    ),
    "進口評級": lambda ban, y, p: p.get("import_total_code", ""),
    "出口評級": lambda ban, y, p: p.get("export_total_code", ""),
    "評等年度": lambda ban, y, p: p.get("rating_year", y),
    "公司名稱(英文)": lambda ban, y, p: _get_details(p).get("company_name_en", ""),
    "代表人": lambda ban, y, p: _get_details(p).get("representative", ""),
    "登記地址(中文)": lambda ban, y, p: _get_details(p).get("business_address_zh", ""),
    "登記地址(英文)": lambda ban, y, p: _get_details(p).get("business_address_en", ""),
    "最近異動日期": lambda ban, y, p: _get_details(p).get("date_of_last_change", ""),
    "最初登記日期": lambda ban, y, p: _get_details(p).get("original_registration_date", ""),
    "前名稱(中文)": lambda ban, y, p: _get_details(p).get("former_name_zh", ""),
    "前名稱(英文)": lambda ban, y, p: _get_details(p).get("former_name_en", ""),
    "網站": lambda ban, y, p: _get_details(p).get("website", ""),
    "Email": lambda ban, y, p: _get_details(p).get("email", ""),
    "進口資格": lambda ban, y, p: _get_details(p).get("import_qualification", ""),
    "出口資格": lambda ban, y, p: _get_details(p).get("export_qualification", ""),
    "進口項目": lambda ban, y, p: _get_details(p).get("items_for_import", ""),
    "出口項目": lambda ban, y, p: _get_details(p).get("items_for_export", ""),
}

FORMATTERS: Dict[str, Callable[[Any], Any]] = {
    # 例如需要可在此補上格式化規則
}


def apply_custom_formatting(row_values: Dict[str, Any]) -> Dict[str, Any]:
    """Apply per-column formatting rules defined in FORMATTERS."""
    for header, fmt in FORMATTERS.items():
        if header in row_values:
            try:
                row_values[header] = fmt(row_values[header])
            except Exception:
                pass
    return row_values


# ===================== 核心流程 =====================

def load_json(path: Path) -> Dict[str, Any]:
    """Load and parse JSON file with UTF-8 encoding."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def iter_records(data: Dict[str, Any]) -> Iterable[Tuple[str, str, Dict[str, Any]]]:
    """Yield (ban_no, year_key, year_payload)."""
    for ban_no, years in (data or {}).items():
        if not isinstance(years, dict):
            continue
        for year_key, payload in years.items():
            if isinstance(payload, dict):
                yield ban_no, str(year_key), payload


def build_row(ban_no: str, year_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build a row dict keyed by HEADERS using FIELD_EXTRACTORS."""
    row: Dict[str, Any] = {}
    for header in HEADERS:
        extractor = FIELD_EXTRACTORS.get(header)
        value = extractor(ban_no, year_key, payload) if extractor else ""
        row[header] = "" if value in (None, "null") else value
    return apply_custom_formatting(row)


def write_excel(rows: List[Dict[str, Any]], output_path: Path) -> None:
    """Write rows into an Excel file with basic formatting."""
    wb = Workbook()
    ws = wb.active
    ws.title = "company_details"

    ws.append(HEADERS)
    for row in rows:
        ws.append([row.get(h, "") for h in HEADERS])

    _apply_sheet_format(ws)
    wb.save(output_path)


def _apply_sheet_format(ws: Worksheet) -> None:
    """Basic sheet niceties: freeze header, autofilter, naive column widths."""
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    max_len: Dict[int, int] = {}
    for r in ws.iter_rows(values_only=True):
        for idx, cell in enumerate(r, start=1):
            length = len(str(cell)) if cell is not None else 0
            max_len[idx] = max(max_len.get(idx, 0), length)

    for idx, length in max_len.items():
        col = get_column_letter(idx)
        ws.column_dimensions[col].width = min(length + 2, 60)


def main() -> None:
    """CLI entry."""
    base = BASE_DIR  # 關鍵：exe 同目錄
    input_path = base / "company_details.json"
    output_path = base / "company_details.xlsx"

    if not input_path.exists():
        print(f"[ERROR] 找不到檔案：{input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        data = load_json(input_path)
    except json.JSONDecodeError as exc:
        print(f"[ERROR] JSON 解析失敗：{exc}", file=sys.stderr)
        sys.exit(2)

    rows: List[Dict[str, Any]] = [build_row(b, y, p) for b, y, p in iter_records(data)]

    if not rows:
        print("[WARN] 沒有可輸出的資料。仍會產生含表頭的空檔。")

    try:
        write_excel(rows, output_path)
    except Exception as exc:
        print(f"[ERROR] 寫入 Excel 失敗：{exc}", file=sys.stderr)
        sys.exit(3)

    print(f"[OK] 已輸出：{output_path.name}  （{len(rows)} 筆）")


if __name__ == "__main__":
    main()
