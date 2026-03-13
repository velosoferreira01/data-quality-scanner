# -*- coding: utf-8 -*-
# 07_dq_scan_postgres.py
import argparse
import datetime as dt
import sys
import re

import pandas as pd
import duckdb
import psycopg2


def qident(name: str) -> str:
    """Quote SQL identifier safely (for DuckDB schema/table names)."""
    return '"' + name.replace('"', '""') + '"'


def pg_ident(name: str) -> str:
    """Quote SQL identifier safely for Postgres schema/table/column names."""
    return '"' + name.replace('"', '""') + '"'


def connect_pg(host: str, db: str, user: str, password: str, port: int):
    return psycopg2.connect(host=host, dbname=db, user=user, password=password, port=port)


def list_tables(conn, schema: str):
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return [r[0] for r in cur.fetchall()]


def fetch_sample_df(conn, schema: str, table: str, limit: int):
    sql = f"SELECT * FROM {pg_ident(schema)}.{pg_ident(table)} LIMIT %s"
    return pd.read_sql_query(sql, conn, params=(limit,))


def table_metrics(df: pd.DataFrame):
    row_count = int(df.shape[0])
    col_count = int(df.shape[1])
    null_cells = int(df.isna().sum().sum()) if row_count and col_count else 0
    total_cells = int(row_count * col_count)
    null_rate = (null_cells / total_cells) if total_cells else 0.0
    return row_count, col_count, null_cells, total_cells, float(null_rate)


def is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)


def is_datetime(s: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(s)


def column_metrics(df: pd.DataFrame, col: str):
    s = df[col]
    total = int(len(s))
    nulls = int(s.isna().sum())
    nn = s.dropna()

    distinct = int(nn.nunique(dropna=True)) if len(nn) else 0
    null_rate = (nulls / total) if total else 0.0
    distinct_rate = (distinct / len(nn)) if len(nn) else 0.0

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
        "distinct_cnt": distinct,
        "distinct_ratio": float(distinct_rate),
        "min_value": minv,
        "max_value": maxv,
        "avg_len": avg_len,
        "max_len": max_len,
    }


def ensure_duckdb_tables(con: duckdb.DuckDBPyConnection, stg_schema: str):
    # DuckDB: schema/table quoting com aspas duplas
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(stg_schema)};")

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qident(stg_schema)}.dq_table_metrics (
            run_id            VARCHAR,
            scanned_at        TIMESTAMP,
            source_host       VARCHAR,
            source_db         VARCHAR,
            source_schema     VARCHAR,
            table_name        VARCHAR,
            sample_rows       BIGINT,
            columns           BIGINT,
            null_cells        BIGINT,
            total_cells       BIGINT,
            null_rate         DOUBLE
        );
        """
    )

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qident(stg_schema)}.dq_column_metrics (
            run_id            VARCHAR,
            scanned_at        TIMESTAMP,
            source_host       VARCHAR,
            source_db         VARCHAR,
            source_schema     VARCHAR,
            table_name        VARCHAR,
            column_name       VARCHAR,
            dtype             VARCHAR,
            total             BIGINT,
            nulls             BIGINT,
            null_rate         DOUBLE,
            distinct_cnt      BIGINT,
            distinct_ratio    DOUBLE,
            min_value         VARCHAR,
            max_value         VARCHAR,
            avg_len           DOUBLE,
            max_len           BIGINT
        );
        """
    )


def insert_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, target: str):
    con.register("tmp_df", df)
    con.execute(f"INSERT INTO {target} SELECT * FROM tmp_df;")
    con.unregister("tmp_df")


def main():
    ap = argparse.ArgumentParser("Postgres -> DuckDB DQ Scan")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--db", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--dbfile", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--only", default=None, help="Regex para filtrar tabelas (opcional)")
    args = ap.parse_args()

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    scanned_at = dt.datetime.now()

    print(f"[DQ] run_id={run_id} scanned_at={scanned_at.isoformat(timespec='seconds')}")
    print(f"[DQ] Postgres: {args.host}:{args.port} db={args.db} schema={args.schema}")
    print(f"[DQ] DuckDB: {args.dbfile} schema={args.stg} limit/table={args.limit}")

    try:
        pg = connect_pg(args.host, args.db, args.user, args.password, args.port)
    except Exception as e:
        print(f"[ERRO] Conex o Postgres falhou: {e}")
        sys.exit(2)

    try:
        tables = list_tables(pg, args.schema)
        print(f"[DQ] Tabelas encontradas em {args.schema}: {tables}")
    except Exception as e:
        print(f"[ERRO] Listagem de tabelas falhou: {e}")
        pg.close()
        sys.exit(3)

    if args.only:
        rx = re.compile(args.only)
        tables = [t for t in tables if rx.search(t)]

    if not tables:
        print("[DQ] Nenhuma tabela encontrada.")
        pg.close()
        sys.exit(0)

    dcon = duckdb.connect(args.dbfile)
    ensure_duckdb_tables(dcon, args.stg)

    table_rows = []
    col_rows = []

    for i, t in enumerate(tables, 1):
        print(f"[DQ] ({i}/{len(tables)}) {args.schema}.{t}")
        sql_preview = f"SELECT * FROM \"{args.schema}\".\"{t}\" LIMIT {args.limit}"
        print(f"[DQ] SQL: {sql_preview}")
        try:
            df = fetch_sample_df(pg, args.schema, t, args.limit)
        except Exception as e:
            print(f"[WARN] Falha ao ler {args.schema}.{t}: {e}")
            continue

        sample_rows, columns, null_cells, total_cells, null_rate = table_metrics(df)

        table_rows.append(
            {
                "run_id": run_id,
                "scanned_at": scanned_at,
                "source_host": args.host,
                "source_db": args.db,
                "source_schema": args.schema,
                "table_name": t,
                "sample_rows": sample_rows,
                "columns": columns,
                "null_cells": null_cells,
                "total_cells": total_cells,
                "null_rate": null_rate,
            }
        )

        for col in df.columns:
            cm = column_metrics(df, col)
            col_rows.append(
                {
                    "run_id": run_id,
                    "scanned_at": scanned_at,
                    "source_host": args.host,
                    "source_db": args.db,
                    "source_schema": args.schema,
                    "table_name": t,
                    **cm,
                }
            )

    if table_rows:
        insert_df(
            dcon,
            pd.DataFrame(table_rows),
            f"{qident(args.stg)}.dq_table_metrics",
        )

    if col_rows:
        insert_df(
            dcon,
            pd.DataFrame(col_rows),
            f"{qident(args.stg)}.dq_column_metrics",
        )

    dcon.close()
    pg.close()
    print(f"[DQ] OK. tables={len(table_rows)} cols={len(col_rows)}")


if __name__ == "__main__":
    main()