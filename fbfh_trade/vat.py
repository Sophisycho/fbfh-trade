#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
vat_utils.py
統一編號（VAT）相關輔助：校驗、分解、與合法序列生成器。
"""

from __future__ import annotations
from typing import Generator, List, Tuple

WEIGHTS: List[int] = [1, 2, 1, 2, 1, 2, 4, 1]


def sum_digits(n: int) -> int:
    """二位數字和（例如 18 -> 1 + 8 = 9）。"""
    return n // 10 + n % 10


def explain_uniform_number(uniform_number: str) -> Tuple[List[int], List[int], int]:
    """
    依規則分解統編：
    - 回傳（逐位乘積、逐位加總、總和 Z）
    """
    if len(uniform_number) != 8 or not uniform_number.isdigit():
        raise ValueError("統一編號必須是 8 碼數字字串。")
    digits = [int(ch) for ch in uniform_number]
    products = [d * w for d, w in zip(digits, WEIGHTS)]
    per_digit_sums = [sum_digits(p) for p in products]
    z_total = sum(per_digit_sums)
    return products, per_digit_sums, z_total


def is_valid_uniform_number(uniform_number: str) -> bool:
    """是否為合法統編：Z % 5 == 0 或第七位是 7 且 (Z+1) % 5 == 0。"""
    _, _, z = explain_uniform_number(uniform_number)
    if z % 5 == 0:
        return True
    if uniform_number[6] == "7" and (z + 1) % 5 == 0:
        return True
    return False


def uniform_number_stream(start: str) -> Generator[str, None, None]:
    """
    從 start（含）開始產生合法統編字串（8 碼），直到 99_999_999 為止。
    """
    if len(start) != 8 or not start.isdigit():
        raise ValueError("start 必須是 8 碼數字字串。")
    n = int(start)
    while n <= 99_999_999:
        s = f"{n:08d}"
        if is_valid_uniform_number(s):
            yield s
        n += 1
