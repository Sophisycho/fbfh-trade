# verify_s_hidden_client.py
# -*- coding: utf-8 -*-
"""
Fetch and extract 'verifySHidden' from the target HTML page.

- Endpoint (GET): https://fbfh.trade.gov.tw/fb/web/queryBasicf.do
- Use the SAME requests.Session across subsequent POSTs to keep cookies aligned.
- If you pass a session, it will be reused; otherwise a new one is created.

Public API:
    get_verify_s_hidden(
        session: Optional[requests.Session] = None,
        save_to: str = "",
        timeout: int = 10,
        headers: Optional[Dict[str, str]] = None,
    ) -> str
"""

from __future__ import annotations

import re
from typing import Optional, Dict

import requests

try:
    from bs4 import BeautifulSoup  # type: ignore
    _HAS_BS4 = True
except Exception:
    _HAS_BS4 = False

import simple_logger as log


API_ENDPOINT = "https://fbfh.trade.gov.tw/fb/web/queryBasicf.do"


class VerifySHiddenNotFoundError(RuntimeError):
    """Raised when verifySHidden cannot be located in the HTML response."""


def get_verify_s_hidden(
    session: Optional[requests.Session] = None,
    save_to: str = "",
    timeout: int = 10,
    headers: Optional[Dict[str, str]] = None,
) -> str:
    """
    Fetch HTML, extract 'verifySHidden', optionally save to file, and return it.

    Args:
        session: Optional shared requests.Session to preserve cookies.
        save_to: Output path. If "", token won't be persisted.
        timeout: Requests timeout (seconds).
        headers: Optional HTTP headers.

    Returns:
        The extracted verifySHidden value as a string.
    """
    sess = session or requests.Session()

    default_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    merged_headers = {**default_headers, **(headers or {})}

    log.info("開始請求 verifySHidden…")
    log.debug(f"GET {API_ENDPOINT}")
    resp = sess.get(API_ENDPOINT, headers=merged_headers, timeout=timeout)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        log.error(f"HTTP 錯誤：{exc} / 狀態碼={resp.status_code}")
        raise

    log.info(f"HTTP {resp.status_code} 成功取得回應。")
    html = resp.text

    token = _extract_verify_s_hidden(html)
    if save_to:
        try:
            with open(save_to, "w", encoding="utf-8") as f:
                f.write(token)
        except Exception as exc:
            log.warn(f"寫入 {save_to} 失敗：{exc!r}")
    else:
        log.warn("未提供輸出檔案路徑，僅回傳 token。")

    log.success("成功透過 BeautifulSoup/Regex 解析 verifySHidden。")
    return token


def _extract_verify_s_hidden(html: str) -> str:
    """Extract verifySHidden using BeautifulSoup if present, else fallback to regex."""
    if _HAS_BS4:
        try:
            soup = BeautifulSoup(html, "html.parser")
            el = soup.find("input", {"id": "verifySHidden"}) or soup.find(
                "input", {"name": "verifySHidden"}
            )
            if el:
                val = (el.get("value") or "").strip()
                if val:
                    return val
        except Exception as exc:
            log.warn(f"BeautifulSoup 解析失敗：{exc!r}")

    # Regex fallback
    input_tag_pattern = re.compile(
        r'<input\b[^>]*?(?:\b(?:id|name)\s*=\s*["\']verifySHidden["\'])[^>]*?>',
        re.IGNORECASE | re.DOTALL,
    )
    value_attr_pattern = re.compile(
        r'\bvalue\s*=\s*["\']([^"\']+)["\']',
        re.IGNORECASE | re.DOTALL,
    )
    m = input_tag_pattern.search(html)
    if m:
        vm = value_attr_pattern.search(m.group(0))
        if vm:
            val = vm.group(1).strip()
            if val:
                return val

    snippet = html[:500].replace("\n", " ")
    log.error(f"解析 verifySHidden 失敗，前 500 字元片段：{snippet}")
    raise VerifySHiddenNotFoundError("在 HTML 中找不到 verifySHidden。")

