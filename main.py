#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""建立 company_details.json 後，接著輸出 company_details.xlsx。"""

from pathlib import Path
import simple_logger as log
from company_details_builder import build_and_save

# 匯出工具：沿用現有 CLI main()，失敗時會以 sys.exit 結束
from export_company_details import main as export_excel  # noqa: E402


INPUT_JSON = "hits.json"
OUTPUT_JSON = "company_details.json"


def run_pipeline() -> None:
    """先建置 JSON，再匯出 Excel。"""
    # 需要的話開啟更詳細的日誌
    log.PRINT_DEBUG = 1
    log.PRINT_INFO = 1
    log.PRINT_WARN = 1
    log.PRINT_ERROR = 1
    log.PRINT_SUCCESS = 1

    # 會讀同目錄 hits.json（或 hit.json），輸出 company_details.json
    build_and_save(input_path=INPUT_JSON, output_path=OUTPUT_JSON, timeout=10)

    # 確認 JSON 產生後再進行匯出
    if not Path(OUTPUT_JSON).exists():
        raise FileNotFoundError(f"找不到 {OUTPUT_JSON}，無法進行 Excel 匯出。")

    # 直接呼叫 export_company_details.py 的 main()
    # 該函式內部若發生錯誤會 sys.exit(...)；成功則印出 [OK]
    export_excel()


if __name__ == "__main__":
    run_pipeline()
