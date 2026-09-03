# -*- coding: utf-8 -*-
"""Calcula as 12 dimensões BACEN com pesos configuráveis.

Dimensões sem controle/evidência são registradas como NÃO AVALIADA e não
reduzem artificialmente o score geral aplicável.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml

DIMENSION_ORDER = [
    "acessibilidade", "acuracia", "adaptabilidade", "clareza",
    "comparabilidade", "completude", "confiabilidade", "consistencia",
    "integridade", "rastreabilidade", "relevancia", "tempestividade",
]


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def clamp(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(max(0.0, min(10.0, float(value))), 4)


def classify(score: float | None) -> str:
    if score is None:
        return "Não avaliado"
    if score >= 9:
        return "Conforme"
    if score >= 8:
        return "Adequado"
    if score >= 7:
        return "Atenção"
    if score >= 5:
        return "Deficiente"
    return "Crítico"


def weighted(values: list[tuple[float | None, float]]) -> float | None:
    applicable = [(v, w) for v, w in values if v is not None and w > 0]
    if not applicable:
        return None
    denom = sum(w for _, w in applicable)
    return clamp(sum(float(v) * w for v, w in applicable) / denom)


def governance_value(cfg: dict, dataset: str, dimension: str) -> tuple[float | None, str]:
    gov = cfg.get("governance_assessments") or {}
    item = (gov.get(dataset) or gov.get(Path(dataset).name) or gov.get("default") or {}).get(dimension)
    if isinstance(item, dict):
        return clamp(item.get("score")), str(item.get("evidence") or "")
    return clamp(item), ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--config", default="config/bacen_dimensions.yml")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    dimensions_cfg = cfg.get("dimensions") or {}
    con = duckdb.connect(args.duckdb)

    run_id = args.run_id
    if not run_id:
        row = con.execute(
            f"SELECT run_id FROM {args.stg}.dq_table_scores_u ORDER BY scanned_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            print("[BACEN] Nenhuma execução técnica encontrada.")
            con.close()
            return
        run_id = str(row[0])

    technical = con.execute(
        f"SELECT * FROM {args.stg}.dq_table_scores_u WHERE run_id = ?", [run_id]
    ).fetchdf()
    if technical.empty:
        print(f"[BACEN] Nenhum dataset encontrado para run_id={run_id}.")
        con.close()
        return

    try:
        rules = con.execute(
            f"""SELECT object_name, AVG(rule_score) * 10 AS rule_score,
                       COUNT(*) AS rule_count, SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_count
                FROM {args.stg}.dq_table_scores_u_rules
                WHERE run_id = ? GROUP BY object_name""", [run_id]
        ).fetchdf()
    except Exception:
        rules = pd.DataFrame(columns=["object_name", "rule_score", "rule_count", "passed_count"])
    rule_map = {str(r.object_name): r for r in rules.itertuples(index=False)}

    try:
        semantic = con.execute(
            f"""SELECT object_name,
                       AVG(valid_rate) * 10 AS semantic_score,
                       AVG(CASE WHEN validation_code IN ('BR_UF','BR_MUNICIPIO_UF','BR_CODIGO_IBGE','BR_CEP_FORMAT') THEN valid_rate END) * 10 AS geo_score,
                       AVG(CASE WHEN validation_code IN ('BR_CPF','BR_CNPJ','BR_CPF_CNPJ') THEN valid_rate END) * 10 AS document_score,
                       COUNT(*) AS semantic_rule_count
                FROM {args.stg}.dq_semantic_validation_results
                WHERE run_id = ? AND valid_rate IS NOT NULL
                GROUP BY object_name""", [run_id]
        ).fetchdf()
    except Exception:
        semantic = pd.DataFrame(columns=["object_name", "semantic_score", "geo_score", "document_score", "semantic_rule_count"])
    semantic_map = {str(r.object_name): r for r in semantic.itertuples(index=False)}

    detail_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    for row in technical.to_dict("records"):
        dataset = str(row.get("object_name") or row.get("dataset_name") or row.get("dataset"))
        rule_obj = rule_map.get(dataset)
        rule_score = clamp(getattr(rule_obj, "rule_score", None)) if rule_obj else None
        rule_count = int(getattr(rule_obj, "rule_count", 0) or 0) if rule_obj else 0
        semantic_obj = semantic_map.get(dataset)
        semantic_score = clamp(getattr(semantic_obj, "semantic_score", None)) if semantic_obj else None
        geo_score = clamp(getattr(semantic_obj, "geo_score", None)) if semantic_obj else None
        document_score = clamp(getattr(semantic_obj, "document_score", None)) if semantic_obj else None
        semantic_rule_count = int(getattr(semantic_obj, "semantic_rule_count", 0) or 0) if semantic_obj else 0

        base = {
            "completude": clamp(row.get("completude")),
            "consistencia": clamp(row.get("consistencia")),
            "integridade": clamp(row.get("integridade")),
            "tempestividade": clamp(row.get("freshness")),
        }
        acuracia = weighted([
            (clamp(row.get("validade")), 0.35),
            (clamp(row.get("unicidade")), 0.20),
            (rule_score, 0.20),
            (semantic_score, 0.20),
            (None, 0.15),  # reconciliação externa: exige fonte oficial/evidência
        ])
        rastreabilidade = weighted([
            (10.0 if row.get("run_id") else None, 0.25),
            (10.0 if row.get("source_ref") else None, 0.25),
            (10.0 if row.get("source_type") else None, 0.20),
            (10.0 if rule_count > 0 else None, 0.10),
            (10.0 if semantic_rule_count > 0 else None, 0.05),
            (None, 0.15),  # lineage completo ainda depende de evidência
        ])
        confiabilidade = weighted([
            (base["consistencia"], 0.35),
            (base["integridade"], 0.30),
            (rule_score, 0.20),
            (semantic_score, 0.15),
        ])
        comparabilidade, comparabilidade_ev = governance_value(cfg, dataset, "comparabilidade")

        if geo_score is not None:
            base["consistencia"] = weighted([(base["consistencia"], 0.75), (geo_score, 0.25)])
            base["integridade"] = weighted([(base["integridade"], 0.75), (geo_score, 0.25)])

        scores: dict[str, float | None] = {
            "acuracia": acuracia,
            "comparabilidade": comparabilidade,
            "completude": base["completude"],
            "confiabilidade": confiabilidade,
            "consistencia": base["consistencia"],
            "integridade": base["integridade"],
            "rastreabilidade": rastreabilidade,
            "tempestividade": base["tempestividade"],
        }
        evidence_map = {"comparabilidade": comparabilidade_ev}
        for dim in ["acessibilidade", "adaptabilidade", "clareza", "relevancia"]:
            scores[dim], evidence_map[dim] = governance_value(cfg, dataset, dim)

        applicable_weight = 0.0
        weighted_points = 0.0
        evaluated_count = 0
        for dim in DIMENSION_ORDER:
            dc = dimensions_cfg.get(dim) or {}
            weight = float(dc.get("weight", 0) or 0)
            score = scores.get(dim)
            if score is not None:
                applicable_weight += weight
                weighted_points += score * weight
                evaluated_count += 1
            detail_rows.append({
                "run_id": run_id,
                "scanned_at": row.get("scanned_at"),
                "source_type": row.get("source_type"),
                "source_ref": row.get("source_ref"),
                "object_name": dataset,
                "dimension_code": dim,
                "dimension_name": dc.get("label", dim.title()),
                "weight": weight,
                "minimum_score": float(dc.get("minimum_score", 0) or 0),
                "evaluation_type": dc.get("evaluation_type", "automated"),
                "raw_score": score,
                "weighted_score": round(score * weight, 4) if score is not None else None,
                "status": classify(score),
                "applicable": score is not None,
                "evidence": evidence_map.get(dim, ""),
            })

        overall = clamp(weighted_points / applicable_weight) if applicable_weight > 0 else None
        coverage = round(100 * applicable_weight / sum(float((dimensions_cfg.get(d) or {}).get("weight", 0) or 0) for d in DIMENSION_ORDER), 2)
        summary = {
            "run_id": run_id,
            "scanned_at": row.get("scanned_at"),
            "source_type": row.get("source_type"),
            "source_ref": row.get("source_ref"),
            "object_name": dataset,
            "score_bacen": overall,
            "classification_bacen": classify(overall),
            "evaluated_dimensions": evaluated_count,
            "total_dimensions": 12,
            "applicable_weight": round(applicable_weight, 4),
            "coverage_percent": coverage,
        }
        for dim in DIMENSION_ORDER:
            summary[f"dim_{dim}"] = scores.get(dim)
        summary_rows.append(summary)

    detail_df = pd.DataFrame(detail_rows)
    summary_df = pd.DataFrame(summary_rows)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {args.stg}")
    con.register("detail_df", detail_df)
    con.register("summary_df", summary_df)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.stg}.dq_bacen_dimension_scores AS
        SELECT * FROM detail_df WHERE 1=0
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.stg}.dq_bacen_summary AS
        SELECT * FROM summary_df WHERE 1=0
    """)
    con.execute(f"DELETE FROM {args.stg}.dq_bacen_dimension_scores WHERE run_id = ?", [run_id])
    con.execute(f"DELETE FROM {args.stg}.dq_bacen_summary WHERE run_id = ?", [run_id])
    con.execute(f"INSERT INTO {args.stg}.dq_bacen_dimension_scores SELECT * FROM detail_df")
    con.execute(f"INSERT INTO {args.stg}.dq_bacen_summary SELECT * FROM summary_df")
    con.unregister("detail_df")
    con.unregister("summary_df")
    con.close()

    print(f"[BACEN] OK. run_id={run_id} datasets={len(summary_df)}")
    for r in summary_df[["object_name", "score_bacen", "classification_bacen", "coverage_percent"]].itertuples(index=False):
        print(f"[BACEN] {r.object_name}: score={r.score_bacen} status={r.classification_bacen} cobertura={r.coverage_percent}%")


if __name__ == "__main__":
    main()
