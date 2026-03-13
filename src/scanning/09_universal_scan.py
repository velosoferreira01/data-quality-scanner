# -*- coding: utf-8 -*-
"""
09_universal_scan.py
Scanner universal: Postgres / DuckDB / Parquet / CSV
- Lê uma amostra (LIMIT/NROWS)
- Calcula métricas de tabela e coluna
- Salva em DuckDB (dq_lab.duckdb) no schema stg (ou outro)
"""

import argparse
import datetime as dt
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd


# ---------------------------
# Helpers (métricas)
# ---------------------------

def is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)


def is_datetime(s: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(s)


def column_metrics(df: pd.DataFrame, col: str):
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


# ---------------------------
# DuckDB storage
# ---------------------------

def ensure_duckdb_tables(con: duckdb.DuckDBPyConnection, stg_schema: str):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg_schema};")

    # tabelas "universal" (não conflitam com as suas atuais)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {stg_schema}.dq_table_metrics_u (
            run_id        VARCHAR,
            scanned_at    TIMESTAMP,
            source_type   VARCHAR,   -- postgres|duckdb|parquet|csv
            source_ref    VARCHAR,   -- host/db/schema ou dbfile ou path
            object_name   VARCHAR,   -- tabela ou arquivo
            sample_rows   BIGINT,
            columns       BIGINT,
            null_cells    BIGINT,
            total_cells   BIGINT,
            null_rate     DOUBLE
        );
    """)

    con.execute(f"""
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
    """)


def insert_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, target: str):
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {target} SELECT * FROM tmp_df;")
    con.unregister("tmp_df")


# ---------------------------
# Loaders
# ---------------------------

def load_from_postgres(args) -> list[tuple[str, pd.DataFrame, str]]:
    import psycopg2

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user, password=args.password
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type='BASE TABLE'
        ORDER BY table_name
    """, (args.schema,))
    tables = [r[0] for r in cur.fetchall()]

    if args.only:
        rx = re.compile(args.only)
        tables = [t for t in tables if rx.search(t)]

    results = []
    for t in tables:
        sql = f'SELECT * FROM "{args.schema}"."{t}" LIMIT %s'
        df = pd.read_sql_query(sql, conn, params=(args.limit,))
        results.append((t, df, f"{args.host}:{args.port}/{args.db}/{args.schema}"))

    conn.close()
    return results


def load_from_duckdb(args) -> list[tuple[str, pd.DataFrame, str]]:
    con = duckdb.connect(args.dbfile)

    # aceita "schema.table" ou "table"
    table = args.table
    query = f"SELECT * FROM {table} LIMIT {args.limit}"
    df = con.execute(query).df()

    con.close()
    return [(table, df, args.dbfile)]


def load_from_parquet(args) -> list[tuple[str, pd.DataFrame, str]]:
    path = Path(args.path).resolve()
    # pandas usa pyarrow por baixo (recomendado)
    df = pd.read_parquet(path)
    if args.limit and args.limit > 0:
        df = df.head(args.limit)
    return [(path.name, df, str(path))]


def load_from_csv(args) -> list[tuple[str, pd.DataFrame, str]]:
    path = Path(args.path).resolve()
    df = pd.read_csv(
        path,
        nrows=args.limit if args.limit and args.limit > 0 else None,
        sep=args.sep,
        encoding=args.encoding,
        low_memory=False,
    )
    return [(path.name, df, str(path))]


# ---------------------------
# Main
# ---------------------------

def main():
    parser = argparse.ArgumentParser("Universal Data Quality Scanner (Postgres/DuckDB/Parquet/CSV)")
    sub = parser.add_subparsers(dest="source", required=True)

    # Common output args
    def add_out(p):
        p.add_argument("--duckdb", dest="duckdb_out", required=True, help="Arquivo DuckDB (ex: dq_lab.duckdb)")
        p.add_argument("--stg", default="stg", help="Schema de saída no DuckDB (ex: stg)")
        p.add_argument("--limit", type=int, default=50000, help="Amostra por dataset")
        p.add_argument("--run_id", default=None, help="Se vazio, gera automaticamente")

    # Postgres
    pg = sub.add_parser("postgres")
    pg.add_argument("--host", required=True)
    pg.add_argument("--port", type=int, default=5432)
    pg.add_argument("--db", required=True)
    pg.add_argument("--user", required=True)
    pg.add_argument("--password", required=True)
    pg.add_argument("--schema", required=True)
    pg.add_argument("--only", default=None, help="Regex para filtrar tabelas (opcional)")
    add_out(pg)

    # DuckDB table scan
    dd = sub.add_parser("duckdb")
    dd.add_argument("--dbfile", required=True, help="Arquivo DuckDB de origem (pode ser o mesmo dq_lab.duckdb)")
    dd.add_argument("--table", required=True, help="Tabela de origem (ex: stg.src_material_data)")
    add_out(dd)

    # Parquet
    pq = sub.add_parser("parquet")
    pq.add_argument("--path", required=True)
    add_out(pq)

    # CSV
    cs = sub.add_parser("csv")
    cs.add_argument("--path", required=True)
    cs.add_argument("--sep", default=",")
    cs.add_argument("--encoding", default="utf-8")
    add_out(cs)

    args = parser.parse_args()

    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    scanned_at = dt.datetime.now()

    # Load datasets
    if args.source == "postgres":
        datasets = load_from_postgres(args)
        source_type = "postgres"
    elif args.source == "duckdb":
        datasets = load_from_duckdb(args)
        source_type = "duckdb"
    elif args.source == "parquet":
        datasets = load_from_parquet(args)
        source_type = "parquet"
    elif args.source == "csv":
        datasets = load_from_csv(args)
        source_type = "csv"
    else:
        print("[ERRO] source inválido")
        sys.exit(2)

    if not datasets:
        print("[DQ] Nenhum dataset encontrado para varrer.")
        sys.exit(0)

    out = duckdb.connect(args.duckdb_out)
    ensure_duckdb_tables(out, args.stg)

    table_rows = []
    col_rows = []

    for object_name, df, source_ref in datasets:
        sample_rows, columns, null_cells, total_cells, null_rate = table_metrics(df)

        table_rows.append({
            "run_id": run_id,
            "scanned_at": scanned_at,
            "source_type": source_type,
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
                "source_type": source_type,
                "source_ref": source_ref,
                "object_name": object_name,
                **cm,
            })

        print(f"[DQ] OK: {source_type} -> {object_name} (rows={sample_rows}, cols={columns}, null_rate={null_rate:.4f})")

    insert_df(out, pd.DataFrame(table_rows), f"{args.stg}.dq_table_metrics_u")
    insert_df(out, pd.DataFrame(col_rows), f"{args.stg}.dq_column_metrics_u")
    out.close()

    print(f"[DQ] Finalizado. run_id={run_id} datasets={len(table_rows)} col_metrics={len(col_rows)}")
    print(f"[DQ] Gravado em {args.duckdb_out} -> {args.stg}.dq_table_metrics_u / {args.stg}.dq_column_metrics_u")


if __name__ == "__main__":
    main()