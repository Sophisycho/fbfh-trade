#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""建立 company_details.json 後，接著輸出 company_details.xlsx（皆放在 exe 同目錄）。"""

from __future__ import annotations

from pathlib import Path
import simple_logger as log
from company_details_builder import build_and_save
from export_company_details import main as export_excel
from persistence import BASE_DIR  # 關鍵：exe 同目錄

INPUT_JSON_NAME = "hits.json"
OUTPUT_JSON_NAME = "company_details.json"


def run_pipeline() -> None:
    """先建置 JSON，再匯出 Excel。"""
    # 日誌等級（如需）
    log.PRINT_DEBUG = 1
    log.PRINT_INFO = 1
    log.PRINT_WARN = 1
    log.PRINT_ERROR = 1
    log.PRINT_SUCCESS = 1

    base = BASE_DIR
    input_path = base / INPUT_JSON_NAME
    output_path = base / OUTPUT_JSON_NAME

    # 會讀 exe 同目錄 hits.json，輸出 exe 同目錄 company_details.json
    build_and_save(input_path=str(input_path), output_path=str(output_path), timeout=10)

    # 確認 JSON 產生後再進行匯出
    if not output_path.exists():
        raise FileNotFoundError(f"找不到 {output_path}，無法進行 Excel 匯出。")

    # 直接呼叫 export_company_details.py 的 main()
    export_excel()


if __name__ == "__main__":
    run_pipeline()
