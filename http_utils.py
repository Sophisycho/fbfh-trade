#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
http_utils.py
HTTP session 與回應解碼工具。
"""

from __future__ import annotations
import zlib
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session(pool_size: int, retries: int, backoff: float) -> requests.Session:
    """建立帶重試與連線池設定的 Session。"""
    session = requests.Session()

    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # 不主動要求 br，降低 brotli 解析不確定性
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "User-Agent": "uniform-runner/1.4",
            "Origin": "https://fbfh.trade.gov.tw",
            "Referer": "https://fbfh.trade.gov.tw/",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    return session


def try_brotli_decompress(raw: bytes) -> Optional[bytes]:
    """嘗試以 brotli 或 brotlicffi 解壓縮，失敗則回傳 None。"""
    try:
        import brotli  # type: ignore

        return brotli.decompress(raw)
    except Exception:
        try:
            import brotlicffi  # type: ignore

            return brotlicffi.decompress(raw)  # type: ignore
        except Exception:
            return None


def decode_body(resp: requests.Response) -> Optional[str]:
    """依 Content-Encoding 解碼 Response 內容為文字，失敗回傳 None。"""
    encoding = (resp.headers.get("Content-Encoding") or "").lower().strip()
    raw = resp.content
    try:
        if encoding == "gzip":
            return zlib.decompress(raw, zlib.MAX_WBITS | 16).decode(resp.encoding or "utf-8", errors="replace")
        if encoding == "deflate":
            try:
                return zlib.decompress(raw).decode(resp.encoding or "utf-8", errors="replace")
            except zlib.error:
                return zlib.decompress(raw, -zlib.MAX_WBITS).decode(resp.encoding or "utf-8", errors="replace")
        if encoding == "br":
            dec = try_brotli_decompress(raw)
            if dec is not None:
                return dec.decode(resp.encoding or "utf-8", errors="replace")
            return None
        return raw.decode(resp.encoding or resp.apparent_encoding or "utf-8", errors="replace")
    except Exception:
        return None
