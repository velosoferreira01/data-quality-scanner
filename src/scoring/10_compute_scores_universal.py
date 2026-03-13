# -*- coding: utf-8 -*-
"""
10_compute_scores_universal.py
Calcula score 0-10 para qualquer dataset escaneado pelo 09_universal_scan.py
Gera stg.dq_table_scores_u
"""

import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True, help="dq_lab.duckdb")
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--run_id", default=None, help="se vazio, usa o último run_id")
    args = ap.parse_args()

    con = duckdb.connect(args.duckdb)
    stg = args.stg

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {stg}.dq_table_scores_u (
            run_id              VARCHAR,
            scanned_at          TIMESTAMP,
            source_type         VARCHAR,
            source_ref          VARCHAR,
            object_name         VARCHAR,
            sample_rows         BIGINT,
            null_rate           DOUBLE,
            avg_distinct_ratio  DOUBLE,
            score_completeness  DOUBLE,
            score_uniqueness    DOUBLE,
            score_volume        DOUBLE,
            score_final         DOUBLE
        );
    """)

    if args.run_id:
        run_id = args.run_id
    else:
        row = con.execute(
            f"SELECT run_id FROM {stg}.dq_table_metrics_u ORDER BY scanned_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            print("[SCORE] Não há métricas universais ainda (rode o 09_universal_scan.py).")
            con.close()
            return
        run_id = row[0]

    print(f"[SCORE] run_id={run_id}")

    con.execute(f"DELETE FROM {stg}.dq_table_scores_u WHERE run_id = ?", [run_id])

    con.execute(f"""
        INSERT INTO {stg}.dq_table_scores_u
        WITH
        t AS (
            SELECT *
            FROM {stg}.dq_table_metrics_u
            WHERE run_id = ?
        ),
        c AS (
            SELECT
                run_id,
                source_type,
                source_ref,
                object_name,
                AVG(distinct_ratio) AS avg_distinct_ratio
            FROM {stg}.dq_column_metrics_u
            WHERE run_id = ?
            GROUP BY run_id, source_type, source_ref, object_name
        ),
        s AS (
            SELECT
                t.run_id,
                t.scanned_at,
                t.source_type,
                t.source_ref,
                t.object_name,
                t.sample_rows,
                t.null_rate,
                COALESCE(c.avg_distinct_ratio, 0.0) AS avg_distinct_ratio,

                ((1.0 - COALESCE(t.null_rate, 1.0)) * 10.0) AS score_completeness,
                (COALESCE(c.avg_distinct_ratio, 0.0) * 10.0) AS score_uniqueness,

                CASE
                    WHEN t.sample_rows >= 1000 THEN 10.0
                    WHEN t.sample_rows >= 100  THEN 7.0
                    WHEN t.sample_rows >= 10   THEN 4.0
                    ELSE 1.0
                END AS score_volume
            FROM t
            LEFT JOIN c
              ON t.run_id = c.run_id
             AND t.source_type = c.source_type
             AND t.source_ref = c.source_ref
             AND t.object_name = c.object_name
        )
        SELECT
            run_id,
            scanned_at,
            source_type,
            source_ref,
            object_name,
            sample_rows,
            null_rate,
            avg_distinct_ratio,
            score_completeness,
            score_uniqueness,
            score_volume,
            LEAST(10.0, GREATEST(0.0,
                0.6*score_completeness + 0.3*score_uniqueness + 0.1*score_volume
            )) AS score_final
        FROM s;
    """, [run_id, run_id])

    top = con.execute(f"""
        SELECT object_name, score_final, score_completeness, score_uniqueness, score_volume, sample_rows, null_rate, avg_distinct_ratio
        FROM {stg}.dq_table_scores_u
        WHERE run_id = ?
        ORDER BY score_final DESC
        LIMIT 20
    """, [run_id]).fetchall()

    print("[SCORE] Top:")
    for r in top:
        print(r)

    con.close()
    print("[SCORE] OK.")


if __name__ == "__main__":
    main()