#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
runner.py
主流程入口：產生合法統編、呼叫 API、解析結果、持久化、與進度顯示。
- 改用內建 logger 輸出（進度亦改為 logger，每 N 筆一行）
- 新增 --cooldown-on-warn 參數供 429 退避基準秒數使用
- 啟動時輸出現有 ok/hits 的統計，方便確認不是空集合起跑
"""

from __future__ import annotations
import argparse
import sys
import time
from typing import Optional, Dict

import fbfh_trade.logger as log

from fbfh_trade.persistence import (
    load_state,
    save_state,
    load_json,
    save_json,
    append_error_log,
    HITS_PATH,
    OK_PATH,
    BASE_DIR,
)
from fbfh_trade.vat import uniform_number_stream  # 產生合法統編
from fbfh_trade.http import create_session  # Session 與重試
from fbfh_trade.api import post_company_with_429_retry  # 單筆 API 呼叫與 429 重試
from fbfh_trade.parsing import pick_year_row, is_A_to_K, row_is_normal, upsert_nested  # 解析回應資料
from pathlib import Path
from fbfh_trade.company.builder import build_and_save
from fbfh_trade.company.exporter import main as export_excel

COMPANY_DETAILS_PATH = BASE_DIR / "company_details.json"


def _pair_count(d: Dict) -> int:
    try:
        return sum(len(v) for v in d.values() if isinstance(v, dict))
    except Exception:
        return 0


def _interactive_args_if_needed() -> None:
    """
    If no CLI args are provided (double-click on Windows), prompt the user
    for required parameters and extend sys.argv accordingly. No change to
    the original business logic beyond argument population.
    """
    if len(sys.argv) > 1:
        return

    print("=== fbfh-trade runner 參數設定 ===")
    while True:
        year = input("請輸入民國年 (例如 113): ").strip()
        if year.isdigit():
            break
        print("年分需為數字，請重新輸入。")

    while True:
        sleep_str = input("請輸入輪詢間隔秒數 (例如 0.1): ").strip()
        try:
            float(sleep_str)
            break
        except ValueError:
            print("間隔需為數字（可含小數），請重新輸入。")

    sys.argv.extend(["--year", year, "--sleep", sleep_str])


_interactive_args_if_needed()


def parse_args() -> argparse.Namespace:
    """解析命令列參數。"""
    parser = argparse.ArgumentParser(
        description="自動產生合法統編並查 API；含 429 重試與致命錯誤立即停機紀錄。"
    )
    parser.add_argument("--year", type=int, required=True,
                        help="查詢年度（例如 113）。")
    parser.add_argument("--start", default=None,
                        help="起始 8 碼（含）。不指定則讀 state.json。")
    parser.add_argument("--sleep", type=float,
                        default=0.0, help="每次 API 呼叫間隔秒數。")
    parser.add_argument(
        "--checkpoint-every", type=int, default=200, help="每處理 N 個合法統編就落盤。"
    )
    parser.add_argument(
        "--progress-every", type=int, default=20, help="每處理 N 個合法統編就輸出一次進度。"
    )
    parser.add_argument("--pool-size", type=int,
                        default=20, help="HTTP 連線池大小。")
    parser.add_argument("--retries", type=int,
                        default=3, help="失敗重試次數（非 429）。")
    parser.add_argument("--backoff", type=float,
                        default=0.3, help="重試退避因子（非 429）。")
    parser.add_argument(
        "--max-429-retries",
        type=int,
        default=-1,
        help="同一 BAN 碰到 429 的最大重試次數；-1 代表無限重試。",
    )
    parser.add_argument("--timeout", type=float,
                        default=10.0, help="單次請求逾時秒數。")
    parser.add_argument(
        "--cooldown-on-warn",
        type=float,
        default=2.0,
        help="429 退避的基準秒數（秒）。留空或 0 時將採用 2.0。",
    )
    return parser.parse_args()


def _count_nested(d: Dict) -> str:
    """回傳 'X 個BAN / Y 年度' 的統計字串。"""
    try:
        bans = len(d)
        years = sum(len(v) for v in d.values() if isinstance(v, dict))
        return f"{bans} 個BAN / {years} 個年度資料"
    except Exception:
        return "0 個BAN / 0 個年度資料"


def main() -> None:
    """主執行流程。"""
    args = parse_args()

    # 決定續跑起點（整數位置）
    start_int = int(args.start) if args.start else load_state()
    start_valid = f"{start_int:08d}"

    # 載入既有結果（若檔毀損會被移到 .corrupt.<ts>，並回傳空 dict）
    hits = load_json(HITS_PATH)
    ok_map = load_json(OK_PATH)
    details = load_json(COMPANY_DETAILS_PATH)

    # 啟動時輸出載入統計，避免一開始就「空集合」卻沒感覺到
    log.info(f"載入 ok.json：{_count_nested(ok_map)}")
    log.info(f"載入 hits.json：{_count_nested(hits)}")
    if _pair_count(details) != _pair_count(hits):
        log.info(
            "company_details.json 與 hits.json 進度不一致，先行建置 company_details.json 並更新 Excel…"
        )
        while _pair_count(details) != _pair_count(hits):
            try:
                build_and_save(
                    input_path=str(HITS_PATH),
                    output_path=str(COMPANY_DETAILS_PATH),
                )
                export_excel()
            except Exception as exc:
                log.error(f"build_and_save/export failed: {exc!r}")
                break
            details = load_json(COMPANY_DETAILS_PATH)
    log.info(f"續跑起點：{start_valid}")

    # 初始化生成器與 HTTP session
    gen = uniform_number_stream(start_valid)
    session = create_session(pool_size=args.pool_size,
                             retries=args.retries, backoff=args.backoff)

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

            # 解析與存檔
            if data:
                row = pick_year_row(data, str(args.year))
                if row:
                    name_zh = row[2] if len(row) > 2 else None
                    name_en = row[3] if len(row) > 3 else None
                    import_grade = row[4] if len(row) > 4 else None
                    export_grade = row[5] if len(row) > 5 else None

                    # 正常資料 → 記錄至 ok.json
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
                        log.info(
                            f"OK  {vat}  year={args.year}  name_zh={str(name_zh).strip() if name_zh else ''}"
                        )

                    # 命中特殊等第（A~K） → 記錄至 hits.json
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
                        log.info(
                            f"HIT {vat}  year={args.year}  import={import_grade}  export={export_grade}"
                        )
                        try:
                            build_and_save(
                                input_path=str(HITS_PATH),
                                output_path=str(COMPANY_DETAILS_PATH),
                            )
                            export_excel()
                        except Exception as exc:
                            log.error(f"build_and_save/export failed: {exc!r}")

            processed += 1

            # 進度顯示（用 logger，每 N 筆一行）
            if processed % args.progress_every == 0:
                elapsed = max(1e-6, time.time() - t0)
                rps = processed / elapsed
                next_number = int(vat) + 1
                log.info(
                    f"進度｜目前處理到: {next_number:08d}（最後合法: {last_legal}）"
                    f"｜已處理合法數: {processed}｜RPS: {rps:.2f}"
                )

            # 定期 checkpoint（原子寫入＋備份由 persistence 保障）
            if processed % args.checkpoint_every == 0:
                next_number = int(vat) + 1
                save_state(next_number)
                save_json(HITS_PATH, hits)
                save_json(OK_PATH, ok_map)

            # 呼叫節流
            if args.sleep > 0:
                time.sleep(args.sleep)

    except KeyboardInterrupt:
        next_number = int(last_legal) + 1 if last_legal else start_int
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        log.info(f"\n已中斷，狀態已保存。下次將從 {next_number:08d} 繼續。")
    except SystemExit:
        raise
    except Exception as exc:
        next_number = int(last_legal) + 1 if last_legal else start_int
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        append_error_log("Unhandled exception", {"error": repr(exc)})
        log.error(f"\n[STOP] 未預期錯誤：{exc!r}，狀態已保存。")
        sys.exit(1)
    else:
        next_number = 100_000_000
        save_state(next_number)
        save_json(HITS_PATH, hits)
        save_json(OK_PATH, ok_map)
        log.info("\n已完成全部區間掃描。")


if __name__ == "__main__":
    main()
