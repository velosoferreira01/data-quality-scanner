# -*- coding: utf-8 -*-
"""Validação local de CPF e CNPJ, sem consulta a serviços externos."""
from __future__ import annotations

import re
from typing import Any


def only_digits(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))


def _all_equal(value: str) -> bool:
    return bool(value) and len(set(value)) == 1


def is_valid_cpf(value: Any) -> bool:
    cpf = only_digits(value)
    if len(cpf) != 11 or _all_equal(cpf):
        return False
    nums = [int(x) for x in cpf]
    first = (sum(nums[i] * (10 - i) for i in range(9)) * 10) % 11
    first = 0 if first == 10 else first
    second = (sum(nums[i] * (11 - i) for i in range(10)) * 10) % 11
    second = 0 if second == 10 else second
    return nums[9] == first and nums[10] == second


def is_valid_cnpj(value: Any) -> bool:
    cnpj = only_digits(value)
    if len(cnpj) != 14 or _all_equal(cnpj):
        return False
    nums = [int(x) for x in cnpj]

    def digit(base: list[int], weights: list[int]) -> int:
        remainder = sum(n * w for n, w in zip(base, weights)) % 11
        return 0 if remainder < 2 else 11 - remainder

    d1 = digit(nums[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = digit(nums[:12] + [d1], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return nums[12] == d1 and nums[13] == d2


def mask_document(value: Any, kind: str) -> str:
    digits = only_digits(value)
    if kind.lower() == "cpf" and len(digits) == 11:
        return f"***.***.{digits[6:9]}-{digits[9:]}"
    if kind.lower() == "cnpj" and len(digits) == 14:
        return f"**.***.***/****-{digits[-2:]}"
    if not digits:
        return ""
    return "*" * max(0, len(digits) - 4) + digits[-4:]
