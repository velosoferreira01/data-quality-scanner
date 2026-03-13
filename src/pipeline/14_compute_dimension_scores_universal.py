# -*- coding: utf-8 -*-
"""
14_compute_dimension_scores_universal.py
Gera score por dimensão no nível do dataset e salva em:
stg.dq_table_dimension_scores_u

Regra:
- As dimensões continuam sendo calculadas normalmente
- O score_final desta tabela passa a ser o score_final oficial
  já calculado no resumo geral (dq_table_scores_u_rules)
"""

import argparse
import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--run_id", default=None)
    args = ap.parse_args()

    con = duckdb.connect(args.duckdb)
    stg = args.stg

    if args.run_id:
        run_id = args.run_id
    else:
        row = con.execute(
            f"""
            SELECT run_id
            FROM {stg}.dq_table_scores_u
            ORDER BY scanned_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            print("[DIM] Nao ha dq_table_scores_u ainda.")
            con.close()
            return
        run_id = row[0]

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg}")

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {stg}.dq_table_dimension_scores_u (
            run_id                   VARCHAR,
            scanned_at               TIMESTAMP,
            source_type              VARCHAR,
            source_ref               VARCHAR,
            object_name              VARCHAR,
            dim_completude           DOUBLE,
            dim_unicidade            DOUBLE,
            dim_consistencia         DOUBLE,
            dim_validade             DOUBLE,
            dim_integridade_ref      DOUBLE,
            dim_freshness            DOUBLE,
            score_final              DOUBLE
        )
        """
    )

    con.execute(
        f"DELETE FROM {stg}.dq_table_dimension_scores_u WHERE run_id = ?",
        [run_id],
    )

    con.execute(
        f"""
        INSERT INTO {stg}.dq_table_dimension_scores_u
        WITH t AS (
            SELECT
                run_id,
                scanned_at,
                source_type,
                source_ref,
                object_name,
                score_completeness,
                score_uniqueness
            FROM {stg}.dq_table_scores_u
            WHERE run_id = ?
        ),
        c AS (
            SELECT
                run_id,
                object_name,
                AVG(score_final) AS avg_col_score_final,
                AVG(score_base) AS avg_col_score_base,
                AVG(score_rules) AS avg_col_score_rules
            FROM {stg}.dq_column_scores_u
            WHERE run_id = ?
            GROUP BY run_id, object_name
        ),
        r AS (
            SELECT
                run_id,
                object_name,
                score_final
            FROM {stg}.dq_table_scores_u_rules
            WHERE run_id = ?
        )
        SELECT
            t.run_id,
            t.scanned_at,
            t.source_type,
            t.source_ref,
            t.object_name,

            COALESCE(t.score_completeness, 0.0) AS dim_completude,
            COALESCE(t.score_uniqueness, 0.0) AS dim_unicidade,
            COALESCE(c.avg_col_score_final, 0.0) AS dim_consistencia,
            COALESCE(c.avg_col_score_base, 0.0) AS dim_validade,
            LEAST(10.0, GREATEST(0.0, 10.0 + COALESCE(c.avg_col_score_rules, 0.0))) AS dim_integridade_ref,
            0.0 AS dim_freshness,

            COALESCE(r.score_final, 0.0) AS score_final
        FROM t
        LEFT JOIN c
          ON t.run_id = c.run_id
         AND t.object_name = c.object_name
        LEFT JOIN r
          ON t.run_id = r.run_id
         AND t.object_name = r.object_name
        """,
        [run_id, run_id, run_id],
    )

    print(f"[DIM] OK. run_id={run_id}")

    top = con.execute(
        f"""
        SELECT
            object_name,
            dim_completude,
            dim_unicidade,
            dim_consistencia,
            dim_validade,
            dim_integridade_ref,
            dim_freshness,
            score_final
        FROM {stg}.dq_table_dimension_scores_u
        WHERE run_id = ?
        ORDER BY score_final DESC
        """,
        [run_id],
    ).fetchall()

    for r in top:
        print(r)

    con.close()


if __name__ == "__main__":
    main()