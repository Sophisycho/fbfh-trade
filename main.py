# demo_build.py
import simple_logger as log
from company_details_builder import build_and_save

# 需要的話開啟更詳細的日誌
log.PRINT_DEBUG = 1
log.PRINT_INFO = 1
log.PRINT_WARN = 1
log.PRINT_ERROR = 1
log.PRINT_SUCCESS = 1

# 會讀同目錄 hits.json（或 hit.json），輸出 company_details.json
build_and_save(input_path="hits.json", output_path="company_details.json", timeout=10)

