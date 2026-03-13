# -*- coding: utf-8 -*-
"""
run_data_quality_pipeline.py

Executa o pipeline universal completo para:
- arquivos: csv, parquet, xlsx, xls
- bancos: postgres, mysql, duckdb

Saída:
- stg.dq_table_metrics_u
- stg.dq_column_metrics_u
- stg.dq_table_scores_u
- stg.dq_column_scores_u
- stg.dq_table_scores_u_rules
"""

import argparse
import datetime as dt
import socket
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import yaml


class SourceLoadError(RuntimeError):
    pass


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)


def is_datetime(s: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(s)


def column_metrics(df: pd.DataFrame, col: str) -> dict:
    s = df[col]
    total = int(len(s))
    nulls = int(s.isna().sum())
    nn = s.dropna()

    distinct_cnt = int(nn.nunique(dropna=True)) if len(nn) else 0
    null_rate = (nulls / total) if total else 0.0
    distinct_ratio = (distinct_cnt / len(nn)) if len(nn) else 0.0

    minv = None
    maxv = None
    avg_len = None
    max_len = None

    try:
        if len(nn):
            if is_numeric(nn):
                minv = str(nn.min())
                maxv = str(nn.max())
            elif is_datetime(nn):
                minv = str(nn.min())
                maxv = str(nn.max())
            else:
                ss = nn.astype(str)
                lens = ss.str.len()
                avg_len = float(lens.mean()) if len(lens) else None
                max_len = int(lens.max()) if len(lens) else None
    except Exception:
        pass

    return {
        "column_name": col,
        "dtype": str(s.dtype),
        "total": total,
        "nulls": nulls,
        "null_rate": float(null_rate),
        "distinct_cnt": distinct_cnt,
        "distinct_ratio": float(distinct_ratio),
        "min_value": minv,
        "max_value": maxv,
        "avg_len": avg_len,
        "max_len": max_len,
    }


def table_metrics(df: pd.DataFrame):
    row_count = int(df.shape[0])
    col_count = int(df.shape[1])
    null_cells = int(df.isna().sum().sum()) if row_count and col_count else 0
    total_cells = int(row_count * col_count)
    null_rate = (null_cells / total_cells) if total_cells else 0.0
    return row_count, col_count, null_cells, total_cells, float(null_rate)


def ensure_duckdb_tables(con: duckdb.DuckDBPyConnection, stg_schema: str):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg_schema};")

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {stg_schema}.dq_table_metrics_u (
            run_id        VARCHAR,
            scanned_at    TIMESTAMP,
            source_type   VARCHAR,
            source_ref    VARCHAR,
            object_name   VARCHAR,
            sample_rows   BIGINT,
            columns       BIGINT,
            null_cells    BIGINT,
            total_cells   BIGINT,
            null_rate     DOUBLE
        );
        """
    )

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {stg_schema}.dq_column_metrics_u (
            run_id          VARCHAR,
            scanned_at      TIMESTAMP,
            source_type     VARCHAR,
            source_ref      VARCHAR,
            object_name     VARCHAR,
            column_name     VARCHAR,
            dtype           VARCHAR,
            total           BIGINT,
            nulls           BIGINT,
            null_rate       DOUBLE,
            distinct_cnt    BIGINT,
            distinct_ratio  DOUBLE,
            min_value       VARCHAR,
            max_value       VARCHAR,
            avg_len         DOUBLE,
            max_len         BIGINT
        );
        """
    )


def insert_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, target: str):
    if df.empty:
        return
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {target} SELECT * FROM tmp_df")
    con.unregister("tmp_df")


def load_excel_all_sheets(path: Path, limit: int):
    xls = pd.ExcelFile(path)
    out = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        if limit and limit > 0:
            df = df.head(limit)
        out.append((f"{path.name}::{sheet}", df, str(path.resolve())))
    return out


def load_csv(path: Path, limit: int, sep=",", encoding="utf-8"):
    df = pd.read_csv(
        path,
        nrows=limit if limit and limit > 0 else None,
        sep=sep,
        encoding=encoding,
        low_memory=False,
    )
    return [(path.name, df, str(path.resolve()))]


def load_parquet(path: Path, limit: int):
    df = pd.read_parquet(path)
    if limit and limit > 0:
        df = df.head(limit)
    return [(path.name, df, str(path.resolve()))]


def scan_files_block(files_cfg: dict, limit: int):
    inbox = Path(files_cfg.get("inbox", "./data/inbox"))
    include_ext = [
        x.lower()
        for x in (files_cfg.get("include_ext") or ["csv", "parquet", "xlsx", "xls"])
    ]
    csv_sep = files_cfg.get("csv_sep", ",")
    csv_encoding = files_cfg.get("csv_encoding", "utf-8")

    datasets = []
    skipped_files = []

    if not inbox.exists():
        print(f"[FILES] inbox nao existe: {inbox}")
        return datasets, skipped_files

    for p in sorted(inbox.iterdir()):
        if not p.is_file():
            continue

        ext = p.suffix.lower().replace(".", "")
        if ext not in include_ext:
            continue

        try:
            if ext == "csv":
                datasets.extend(load_csv(p, limit=limit, sep=csv_sep, encoding=csv_encoding))
            elif ext == "parquet":
                datasets.extend(load_parquet(p, limit=limit))
            elif ext in ("xlsx", "xls"):
                datasets.extend(load_excel_all_sheets(p, limit=limit))
        except Exception as exc:
            skipped_files.append((p.name, str(exc)))
            print(f"[WARN] arquivo ignorado: {p.name} -> {exc}")

    return datasets, skipped_files


def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in [current.parent] + list(current.parents):
        if (parent / "config").exists() or (parent / "app.py").exists():
            return parent
    return Path.cwd().resolve()


def find_script(script_name: str) -> str:
    project_root = find_project_root()
    current_dir = Path(__file__).resolve().parent

    candidates = [
        current_dir / script_name,
        current_dir.parent / script_name,
        project_root / script_name,
        project_root / "src" / script_name,
        project_root / "src" / "pipeline" / script_name,
        project_root / "src" / "scoring" / script_name,
        project_root / "src" / "reports" / script_name,
        project_root / "src" / "scanning" / script_name,
    ]

    checked = []
    for path in candidates:
        checked.append(str(path))
        if path.exists() and path.is_file():
            return str(path)

    raise FileNotFoundError(
        f"Script nao encontrado: {script_name}\n"
        f"Locais verificados:\n- " + "\n- ".join(checked)
    )

def run_post_steps(duckdb_file: str, stg: str, rules_file: str, run_id: str, report: bool):
    py = sys.executable

    score_script = find_script("10_compute_scores_universal.py")
    column_score_script = find_script("12_compute_column_scores_universal.py")
    table_rules_script = find_script("13_compute_table_scores_with_rules_universal.py")
    dimension_score_script = find_script("14_compute_dimension_scores_universal.py")

    cmds = [
        [py, score_script, "--duckdb", duckdb_file, "--stg", stg, "--run_id", run_id],
        [py, column_score_script, "--duckdb", duckdb_file, "--stg", stg, "--rules", rules_file, "--run_id", run_id],
        [py, table_rules_script, "--duckdb", duckdb_file, "--stg", stg, "--run_id", run_id],
        [py, dimension_score_script, "--duckdb", duckdb_file, "--stg", stg, "--run_id", run_id],
    ]

    if report:
        report_script = find_script("11_show_report_like_image.py")
        cmds.append([py, report_script, "--duckdb", duckdb_file, "--stg", stg, "--run_id", run_id])

    for cmd in cmds:
        print("[RUN]", " ".join(cmd))
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise SystemExit(result.returncode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", required=True)
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--rules", required=True)
    ap.add_argument("--run_id", default=None)
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    cfg = load_yaml(args.sources)
    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    scanned_at = dt.datetime.now()

    out = duckdb.connect(args.duckdb)
    ensure_duckdb_tables(out, args.stg)

    table_rows = []
    col_rows = []

    files_cfg = cfg.get("files") or {}
    file_datasets, skipped_files = scan_files_block(files_cfg, limit=args.limit)

    for object_name, df, source_ref in file_datasets:
        sample_rows, columns, null_cells, total_cells, null_rate = table_metrics(df)

        table_rows.append({
            "run_id": run_id,
            "scanned_at": scanned_at,
            "source_type": "file",
            "source_ref": source_ref,
            "object_name": object_name,
            "sample_rows": sample_rows,
            "columns": columns,
            "null_cells": null_cells,
            "total_cells": total_cells,
            "null_rate": null_rate,
        })

        for col in df.columns:
            cm = column_metrics(df, col)
            col_rows.append({
                "run_id": run_id,
                "scanned_at": scanned_at,
                "source_type": "file",
                "source_ref": source_ref,
                "object_name": object_name,
                **cm,
            })

        print(f"[SCAN] file -> {object_name} rows={sample_rows} cols={columns}")

    insert_df(out, pd.DataFrame(table_rows), f"{args.stg}.dq_table_metrics_u")
    insert_df(out, pd.DataFrame(col_rows), f"{args.stg}.dq_column_metrics_u")

    out.close()

    print(f"[SCAN] run_id={run_id} datasets={len(table_rows)} col_metrics={len(col_rows)}")

    if not table_rows:
        print("Nenhum dataset carregado.")
        return

    run_post_steps(args.duckdb, args.stg, args.rules, run_id, args.report)
    print("[PIPELINE] COMPLETO.")


if __name__ == "__main__":
    main()
