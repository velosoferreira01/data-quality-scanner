# -*- coding: utf-8 -*-
"""Detecção de colunas semânticas a partir de nomes e configuração explícita."""
from __future__ import annotations

import re
from typing import Iterable

PATTERNS = {
    "cpf": [r"(^|_)cpf($|_)", r"documento_cpf", r"nr_cpf", r"num_cpf"],
    "cnpj": [r"(^|_)cnpj($|_)", r"documento_cnpj", r"nr_cnpj", r"num_cnpj"],
    "cpf_cnpj": [r"cpf_cnpj", r"cnpj_cpf", r"documento_fiscal"],
    "uf": [r"(^|_)uf($|_)", r"sigla_uf", r"sg_uf", r"estado($|_)", r"state($|_)"],
    "municipio": [r"municipio", r"cidade", r"city", r"nm_municipio"],
    "codigo_ibge": [r"codigo_ibge", r"cod_ibge", r"id_municipio_ibge"],
    "cep": [r"(^|_)cep($|_)", r"codigo_postal", r"postal_code"],
}


def normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def detect_columns(columns: Iterable[str]) -> dict[str, str]:
    detected: dict[str, str] = {}
    for original in columns:
        normalized = normalize_column_name(original)
        for semantic_type, patterns in PATTERNS.items():
            if semantic_type in detected:
                continue
            if any(re.search(pattern, normalized) for pattern in patterns):
                detected[semantic_type] = str(original)
    return detected
