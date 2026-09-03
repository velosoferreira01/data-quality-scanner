# -*- coding: utf-8 -*-
"""Referências e validações geográficas brasileiras."""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

UF_CODES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}

UF_NAME_TO_CODE = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM",
    "bahia": "BA", "ceara": "CE", "distrito federal": "DF",
    "espirito santo": "ES", "goias": "GO", "maranhao": "MA",
    "mato grosso": "MT", "mato grosso do sul": "MS", "minas gerais": "MG",
    "para": "PA", "paraiba": "PB", "parana": "PR", "pernambuco": "PE",
    "piaui": "PI", "rio de janeiro": "RJ", "rio grande do norte": "RN",
    "rio grande do sul": "RS", "rondonia": "RO", "roraima": "RR",
    "santa catarina": "SC", "sao paulo": "SP", "sergipe": "SE",
    "tocantins": "TO",
}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_uf(value: Any) -> str:
    raw = normalize_text(value)
    if len(raw) == 2 and raw.upper() in UF_CODES:
        return raw.upper()
    return UF_NAME_TO_CODE.get(raw, "")


def is_valid_uf(value: Any) -> bool:
    return bool(normalize_uf(value))


def load_municipalities(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["codigo_ibge", "municipio", "uf", "municipio_normalizado"])
    df = pd.read_csv(p, dtype=str, encoding="utf-8")
    required = {"codigo_ibge", "municipio", "uf"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Referência de municípios sem colunas obrigatórias: {sorted(missing)}")
    df = df.copy()
    df["uf"] = df["uf"].map(normalize_uf)
    if "municipio_normalizado" not in df.columns:
        df["municipio_normalizado"] = df["municipio"].map(normalize_text)
    else:
        df["municipio_normalizado"] = df["municipio_normalizado"].map(normalize_text)
    df["codigo_ibge"] = df["codigo_ibge"].astype(str).str.replace(r"\D", "", regex=True)
    return df.drop_duplicates(["codigo_ibge", "uf", "municipio_normalizado"])


def build_municipality_sets(df: pd.DataFrame) -> tuple[set[tuple[str, str]], set[str], set[tuple[str, str]]]:
    if df.empty:
        return set(), set(), set()
    city_uf = set(zip(df["municipio_normalizado"], df["uf"]))
    ibge = set(df["codigo_ibge"])
    ibge_uf = set(zip(df["codigo_ibge"], df["uf"]))
    return city_uf, ibge, ibge_uf
