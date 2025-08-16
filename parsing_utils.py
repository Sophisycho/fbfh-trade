#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parsing_utils.py
API 回傳資料解析（取年度列、等第判斷、正常列認定、巢狀 upsert）。
"""

from __future__ import annotations
from typing import Dict, List, Optional


def pick_year_row(json_obj: dict, target_year: str) -> Optional[List]:
    """
    從 retrieveDataList 中挑選符合 target_year 的列。
    - 主要比對 index 6（年份欄）或 index 1 前綴。
    """
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
    """是否為 A~K 單一等第字母。"""
    if not value or not isinstance(value, str):
        return False
    v = value.strip().upper()
    return len(v) == 1 and "A" <= v <= "K"


def row_is_normal(entry: List) -> bool:
    """基本完整性檢查：前 7 欄非 None，且中文名稱存在。"""
    if not entry or len(entry) < 7:
        return False
    first_seven = entry[:7]
    if any(x is None for x in first_seven):
        return False
    name_zh = str(entry[2]).strip() if entry[2] is not None else ""
    return name_zh != ""


def upsert_nested(d: Dict, ban: str, year: str, payload: Dict[str, str]) -> None:
    """對 d[ban][year] 進行 upsert。"""
    if ban not in d:
        d[ban] = {}
    d[ban][year] = payload
