# -*- coding: utf-8 -*-
"""Validação estrutural de CEP e interface opcional para validação referencial."""
from __future__ import annotations

import re
from typing import Any


def normalize_cep(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))


def is_valid_cep_format(value: Any) -> bool:
    cep = normalize_cep(value)
    return len(cep) == 8 and cep != "00000000" and not (len(set(cep)) == 1)
