#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
persistence.py
狀態與資料持久化工具（state.json / hits.json / ok.json / errors.log）。

更新重點：
- BASE_DIR 會在 PyInstaller frozen 模式下指向 exe 同目錄（Path(sys.executable).parent），
  非 frozen 則為本檔所在目錄（方便開發期）。
- 其餘原子寫入、備份、損毀搬移機制維持不變。
"""

from __future__ import annotations
import json
import os
import sys
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# === 取得應用程式根目錄（exe 同目錄 / 或腳本目錄） ===
def get_app_dir() -> Path:
    """回傳應用程式根目錄：PyInstaller 下為 exe 同目錄，否則為此檔案所在目錄。"""
    if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

# === 設定 ===
BASE_DIR = get_app_dir()
BACKUP_KEEP = int(os.getenv("PERSIST_BACKUP_KEEP", "1"))
BACKUP_ENABLED = os.getenv("PERSIST_BACKUP_DISABLE", "0") != "1"

STATE_PATH = BASE_DIR / "state.json"
HITS_PATH = BASE_DIR / "hits.json"
OK_PATH = BASE_DIR / "ok.json"
ERR_LOG_PATH = BASE_DIR / "errors.log"


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _atomic_write_text(path: Path, text: str) -> None:
    """以原子方式寫入文字檔。"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # 原子置換


def _list_backups(path: Path) -> List[Path]:
    """列出同目錄下對應檔案的備份清單（依 mtime 新→舊排序）。"""
    candidates = list(path.parent.glob(f"{path.name}.bak.*"))
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates


def _prune_backups(path: Path) -> None:
    """只保留最新 BACKUP_KEEP 份備份，其餘刪除。"""
    if not BACKUP_ENABLED:
        return
    backups = _list_backups(path)
    if BACKUP_KEEP < 0:
        return  # 負數代表不裁剪
    for old in backups[BACKUP_KEEP:]:
        try:
            old.unlink(missing_ok=True)
        except Exception:
            pass


def _backup_if_exists(path: Path) -> None:
    """若檔案存在且啟用備份，建立 timestamp 備份並做備份裁剪。"""
    if not BACKUP_ENABLED:
        return
    if path.exists():
        bak = path.with_suffix(path.suffix + f".bak.{_timestamp()}")
        try:
            shutil.copy2(path, bak)
        finally:
            _prune_backups(path)


def append_error_log(title: str, details: Dict[str, Any]) -> None:
    """將錯誤附加寫入 errors.log（逐行 JSON 物件）。"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"time": ts, "title": title, "details": details}
    with ERR_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _safe_load_json(path: Path) -> Dict:
    """讀取 JSON，失敗則把原檔轉存為 .corrupt.<ts> 並回傳空 dict。"""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        corrupt = path.with_suffix(path.suffix + f".corrupt.{_timestamp()}")
        try:
            path.rename(corrupt)
        finally:
            append_error_log(
                "Corrupt data JSON moved",
                {"path": str(path), "error": repr(exc), "moved_to": str(corrupt)},
            )
        return {}


def load_state() -> int:
    """讀取 state.json 的 next_number，檔案不存在回傳 0。若檔案毀損則轉存 .corrupt 並回傳 0。"""
    if not STATE_PATH.exists():
        return 0
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return int(data.get("next_number", 0))
    except Exception as exc:
        corrupt = STATE_PATH.with_suffix(STATE_PATH.suffix + f".corrupt.{_timestamp()}")
        try:
            STATE_PATH.rename(corrupt)
        finally:
            append_error_log("Corrupt state.json moved", {"error": repr(exc), "moved_to": str(corrupt)})
        return 0


def save_state(next_number: int) -> None:
    """
    寫入 state.json 的 next_number（原子寫入＋備份）。
    若內容未變更則略過寫入與備份。
    """
    payload = json.dumps({"next_number": next_number}, ensure_ascii=False, indent=2)
    if STATE_PATH.exists():
        try:
            if STATE_PATH.read_text(encoding="utf-8") == payload:
                return  # 無變更，略過
        except Exception:
            pass
    _backup_if_exists(STATE_PATH)
    _atomic_write_text(STATE_PATH, payload)


def load_json(path: Path) -> Dict:
    """
    讀取任意 JSON 檔為 dict；若不存在回傳空 dict。
    若 JSON 解析失敗，轉存 .corrupt 並回傳空 dict（避免覆蓋原始壞檔）。
    """
    if not path.exists():
        return {}
    return _safe_load_json(path)


def save_json(path: Path, obj: Dict) -> None:
    """
    將 dict 寫回 JSON 檔（UTF-8, 縮排，原子寫入＋備份）。
    若內容未變更則略過寫入與備份。
    """
    payload = json.dumps(obj, ensure_ascii=False, indent=2)
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == payload:
                return  # 無變更，略過
        except Exception:
            pass
    _backup_if_exists(path)
    _atomic_write_text(path, payload)
