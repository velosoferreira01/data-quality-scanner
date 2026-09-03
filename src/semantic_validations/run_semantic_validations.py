# -*- coding: utf-8 -*-
"""Executa as validações cadastrais brasileiras para os datasets do run atual."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import duckdb
import pandas as pd
import yaml

from semantic_validations.semantic_engine import run_semantic_validation


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def read_dataset(path: Path, limit: int | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            return pd.read_csv(path, nrows=limit, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(path, nrows=limit, encoding="latin-1")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, nrows=limit)
    if suffix == ".parquet":
        df = pd.read_parquet(path)
        return df.head(limit) if limit else df
    raise ValueError(f"Formato não suportado para validação semântica: {suffix}")


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
        [schema, table],
    ).fetchone()[0])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--config", default="config/semantic_validations.yml")
    ap.add_argument("--reference-dir", default="data/reference")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--limit", type=int, default=100000)
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    con = duckdb.connect(args.duckdb)
    run_id = args.run_id
    if not run_id:
        row = con.execute(f"SELECT run_id FROM {args.stg}.dq_table_scores_u ORDER BY scanned_at DESC LIMIT 1").fetchone()
        if not row:
            print("[SEMANTIC] Nenhuma execução técnica encontrada.")
            con.close()
            return
        run_id = str(row[0])

    datasets = con.execute(
        f"SELECT DISTINCT run_id, scanned_at, source_type, source_ref, object_name FROM {args.stg}.dq_table_scores_u WHERE run_id=?",
        [run_id],
    ).fetchdf()

    summary_frames: list[pd.DataFrame] = []
    invalid_frames: list[pd.DataFrame] = []
    for row in datasets.to_dict("records"):
        source_type = str(row.get("source_type") or "")
        source_ref = str(row.get("source_ref") or "")
        dataset = str(row.get("object_name") or Path(source_ref).name)
        if source_type != "file" or not source_ref:
            print(f"[SEMANTIC] {dataset}: ignorado nesta versão (source_type={source_type or 'desconhecido'}).")
            continue
        path = Path(source_ref)
        if not path.exists():
            print(f"[SEMANTIC] {dataset}: arquivo não encontrado em {path}.")
            continue
        try:
            df = read_dataset(path, args.limit)
            result_df, invalid_df = run_semantic_validation(df, dataset, cfg, args.reference_dir)
        except Exception as exc:
            print(f"[SEMANTIC][WARN] {dataset}: {exc}")
            continue
        if result_df.empty:
            print(f"[SEMANTIC] {dataset}: nenhuma coluna cadastral reconhecida.")
            continue
        for frame in (result_df, invalid_df):
            if not frame.empty:
                frame.insert(0, "object_name", dataset)
                frame.insert(0, "source_ref", source_ref)
                frame.insert(0, "source_type", source_type)
                frame.insert(0, "scanned_at", row.get("scanned_at"))
                frame.insert(0, "run_id", run_id)
        summary_frames.append(result_df)
        if not invalid_df.empty:
            invalid_frames.append(invalid_df)
        print(f"[SEMANTIC] {dataset}: {len(result_df)} validações, {int(result_df['invalid_count'].sum())} ocorrências inválidas.")

    summary = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame(columns=[
        "run_id", "scanned_at", "source_type", "source_ref", "object_name", "validation_code", "validation_name",
        "column_name", "related_columns", "total_count", "evaluated_count", "valid_count", "invalid_count",
        "null_count", "valid_rate", "status", "reference_source", "details",
    ])
    invalid = pd.concat(invalid_frames, ignore_index=True) if invalid_frames else pd.DataFrame(columns=[
        "run_id", "scanned_at", "source_type", "source_ref", "object_name", "dataset", "row_number",
        "column_name", "validation_code", "masked_value",
    ])

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {args.stg}")
    con.register("semantic_summary_df", summary)
    con.register("semantic_invalid_df", invalid)
    con.execute(f"CREATE TABLE IF NOT EXISTS {args.stg}.dq_semantic_validation_results AS SELECT * FROM semantic_summary_df WHERE 1=0")
    con.execute(f"CREATE TABLE IF NOT EXISTS {args.stg}.dq_semantic_invalid_samples AS SELECT * FROM semantic_invalid_df WHERE 1=0")
    con.execute(f"DELETE FROM {args.stg}.dq_semantic_validation_results WHERE run_id=?", [run_id])
    con.execute(f"DELETE FROM {args.stg}.dq_semantic_invalid_samples WHERE run_id=?", [run_id])
    if not summary.empty:
        con.execute(f"INSERT INTO {args.stg}.dq_semantic_validation_results SELECT * FROM semantic_summary_df")
    if not invalid.empty:
        con.execute(f"INSERT INTO {args.stg}.dq_semantic_invalid_samples SELECT * FROM semantic_invalid_df")
    con.unregister("semantic_summary_df")
    con.unregister("semantic_invalid_df")
    con.close()
    print(f"[SEMANTIC] OK. run_id={run_id} datasets={summary['object_name'].nunique() if not summary.empty else 0}")


if __name__ == "__main__":
    main()
