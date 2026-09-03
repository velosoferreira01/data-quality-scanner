# -*- coding: utf-8 -*-
"""Motor de validações cadastrais brasileiras."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .brazil_documents import is_valid_cnpj, is_valid_cpf, mask_document, only_digits
from .brazil_geography import (
    build_municipality_sets,
    is_valid_uf,
    load_municipalities,
    normalize_text,
    normalize_uf,
)
from .brazil_postal_code import is_valid_cep_format, normalize_cep
from .semantic_detector import detect_columns


@dataclass
class ValidationResult:
    validation_code: str
    validation_name: str
    column_name: str
    related_columns: str
    total_count: int
    evaluated_count: int
    valid_count: int
    invalid_count: int
    null_count: int
    valid_rate: float | None
    status: str
    reference_source: str
    details: str


def _status(rate: float | None) -> str:
    if rate is None:
        return "Não avaliado"
    if rate >= 0.99:
        return "Conforme"
    if rate >= 0.95:
        return "Adequado"
    if rate >= 0.90:
        return "Atenção"
    if rate >= 0.80:
        return "Deficiente"
    return "Crítico"


def _resolve_dataset_config(cfg: dict[str, Any], dataset: str) -> dict[str, Any]:
    datasets = cfg.get("datasets") or {}
    return datasets.get(dataset) or datasets.get(Path(dataset).name) or datasets.get("default") or {}


def _rule(
    code: str,
    name: str,
    column: str,
    valid_mask: pd.Series,
    null_mask: pd.Series,
    related: list[str] | None = None,
    reference: str = "",
    details: str = "",
) -> ValidationResult:
    total = int(len(valid_mask))
    evaluated = int((~null_mask).sum())
    valid = int((valid_mask & ~null_mask).sum())
    invalid = max(0, evaluated - valid)
    nulls = int(null_mask.sum())
    rate = round(valid / evaluated, 6) if evaluated else None
    return ValidationResult(
        validation_code=code,
        validation_name=name,
        column_name=column,
        related_columns=",".join(related or []),
        total_count=total,
        evaluated_count=evaluated,
        valid_count=valid,
        invalid_count=invalid,
        null_count=nulls,
        valid_rate=rate,
        status=_status(rate),
        reference_source=reference,
        details=details,
    )


def run_semantic_validation(
    df: pd.DataFrame,
    dataset: str,
    cfg: dict[str, Any],
    reference_dir: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dataset_cfg = _resolve_dataset_config(cfg, dataset)
    auto_detect = bool(dataset_cfg.get("auto_detect", cfg.get("auto_detect", True)))
    explicit = dict(dataset_cfg.get("columns") or {})
    detected = detect_columns(df.columns) if auto_detect else {}
    columns = {**detected, **{k: v for k, v in explicit.items() if v}}

    results: list[ValidationResult] = []
    invalid_rows: list[dict[str, Any]] = []

    def add_invalid(code: str, column: str, raw: pd.Series, valid: pd.Series, nulls: pd.Series, masker=None):
        limit = int(cfg.get("invalid_sample_limit", 100))
        bad_idx = raw.index[(~valid) & (~nulls)][:limit]
        for idx in bad_idx:
            value = raw.loc[idx]
            invalid_rows.append({
                "dataset": dataset,
                "row_number": int(idx) + 2 if isinstance(idx, int) else str(idx),
                "column_name": column,
                "validation_code": code,
                "masked_value": masker(value) if masker else str(value)[:120],
            })

    if columns.get("cpf") in df.columns:
        col = columns["cpf"]
        raw = df[col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        valid = raw.map(is_valid_cpf)
        results.append(_rule("BR_CPF", "CPF com dígitos verificadores válidos", col, valid, nulls))
        add_invalid("BR_CPF", col, raw, valid, nulls, lambda v: mask_document(v, "cpf"))

    if columns.get("cnpj") in df.columns:
        col = columns["cnpj"]
        raw = df[col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        valid = raw.map(is_valid_cnpj)
        results.append(_rule("BR_CNPJ", "CNPJ com dígitos verificadores válidos", col, valid, nulls))
        add_invalid("BR_CNPJ", col, raw, valid, nulls, lambda v: mask_document(v, "cnpj"))

    if columns.get("cpf_cnpj") in df.columns:
        col = columns["cpf_cnpj"]
        raw = df[col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        valid = raw.map(lambda v: is_valid_cpf(v) if len(only_digits(v)) == 11 else is_valid_cnpj(v))
        results.append(_rule("BR_CPF_CNPJ", "CPF ou CNPJ válido conforme quantidade de dígitos", col, valid, nulls))
        add_invalid("BR_CPF_CNPJ", col, raw, valid, nulls, lambda v: mask_document(v, "cpf" if len(only_digits(v)) == 11 else "cnpj"))

    if columns.get("uf") in df.columns:
        col = columns["uf"]
        raw = df[col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        valid = raw.map(is_valid_uf)
        results.append(_rule("BR_UF", "UF brasileira válida", col, valid, nulls, reference="IBGE - Unidades da Federação"))
        add_invalid("BR_UF", col, raw, valid, nulls)

    ref_dir = Path(reference_dir)
    municipality_file = ref_dir / str(cfg.get("municipalities_file", "brasil_municipios.csv"))
    municipalities = load_municipalities(municipality_file)
    city_uf_set, ibge_set, ibge_uf_set = build_municipality_sets(municipalities)

    city_col = columns.get("municipio")
    uf_col = columns.get("uf")
    if city_col in df.columns and uf_col in df.columns:
        raw_city = df[city_col]
        raw_uf = df[uf_col]
        nulls = raw_city.isna() | raw_city.astype(str).str.strip().eq("") | raw_uf.isna() | raw_uf.astype(str).str.strip().eq("")
        if city_uf_set:
            valid = pd.Series(
                [(normalize_text(c), normalize_uf(u)) in city_uf_set for c, u in zip(raw_city, raw_uf)],
                index=df.index,
            )
            details = "Município validado em conjunto com a UF."
            reference = str(municipality_file)
        else:
            valid = pd.Series(False, index=df.index)
            details = "Referência de municípios ausente. Execute scripts/update_brazil_references.py."
            reference = str(municipality_file)
        result = _rule("BR_MUNICIPIO_UF", "Município pertencente à UF informada", city_col, valid, nulls, [uf_col], reference, details)
        if not city_uf_set:
            result.valid_rate = None
            result.status = "Não avaliado"
        results.append(result)
        if city_uf_set:
            add_invalid("BR_MUNICIPIO_UF", city_col, raw_city, valid, nulls)

    ibge_col = columns.get("codigo_ibge")
    if ibge_col in df.columns:
        raw = df[ibge_col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        normalized = raw.astype(str).str.replace(r"\D", "", regex=True)
        if uf_col in df.columns and ibge_uf_set:
            valid = pd.Series([(code, normalize_uf(uf)) in ibge_uf_set for code, uf in zip(normalized, df[uf_col])], index=df.index)
            related = [uf_col]
        elif ibge_set:
            valid = normalized.isin(ibge_set)
            related = []
        else:
            valid = pd.Series(False, index=df.index)
            related = []
        result = _rule("BR_CODIGO_IBGE", "Código IBGE de município válido", ibge_col, valid, nulls, related, str(municipality_file))
        if not ibge_set:
            result.valid_rate = None
            result.status = "Não avaliado"
            result.details = "Referência de municípios ausente. Execute scripts/update_brazil_references.py."
        results.append(result)
        if ibge_set:
            add_invalid("BR_CODIGO_IBGE", ibge_col, raw, valid, nulls)

    if columns.get("cep") in df.columns:
        col = columns["cep"]
        raw = df[col]
        nulls = raw.isna() | raw.astype(str).str.strip().eq("")
        valid = raw.map(is_valid_cep_format)
        results.append(_rule("BR_CEP_FORMAT", "CEP com formato estrutural válido", col, valid, nulls, reference="Regra estrutural de 8 dígitos"))
        add_invalid("BR_CEP_FORMAT", col, raw, valid, nulls, lambda v: f"*****-{normalize_cep(v)[-3:]}" if len(normalize_cep(v)) >= 3 else "***")

    return pd.DataFrame([r.__dict__ for r in results]), pd.DataFrame(invalid_rows)
