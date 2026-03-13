# -*- coding: utf-8 -*-
"""
13_compute_table_scores_with_rules_universal.py
- Calcula score final 0–10 por dataset incorporando penalidades de regras por coluna
- Usa: dq_table_scores_u (base) + dq_column_scores_u (regras)
- Salva: stg.dq_table_scores_u_rules
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
        row = con.execute(f"SELECT run_id FROM {stg}.dq_table_scores_u ORDER BY scanned_at DESC LIMIT 1").fetchone()
        if not row:
            print("[TABLE] Não há dq_table_scores_u ainda. Rode 10_compute_scores_universal.py.")
            con.close()
            return
        run_id = row[0]

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {stg}.dq_table_scores_u_rules (
            run_id              VARCHAR,
            scanned_at          TIMESTAMP,
            source_type         VARCHAR,
            source_ref          VARCHAR,
            object_name         VARCHAR,
            score_base          DOUBLE,
            rules_penalty_avg   DOUBLE,
            score_final         DOUBLE
        );
    """)

    con.execute(f"DELETE FROM {stg}.dq_table_scores_u_rules WHERE run_id=?", [run_id])

    # penalidade média das colunas (score_rules é negativo ou 0)
    con.execute(f"""
        INSERT INTO {stg}.dq_table_scores_u_rules
        WITH base AS (
            SELECT run_id, scanned_at, source_type, source_ref, object_name, score_final AS score_base
            FROM {stg}.dq_table_scores_u
            WHERE run_id = ?
        ),
        pen AS (
            SELECT run_id, object_name, AVG(score_rules) AS rules_penalty_avg
            FROM {stg}.dq_column_scores_u
            WHERE run_id = ?
            GROUP BY run_id, object_name
        )
        SELECT
            b.run_id,
            b.scanned_at,
            b.source_type,
            b.source_ref,
            b.object_name,
            b.score_base,
            COALESCE(p.rules_penalty_avg, 0.0) AS rules_penalty_avg,
            LEAST(10.0, GREATEST(0.0, b.score_base + COALESCE(p.rules_penalty_avg, 0.0))) AS score_final
        FROM base b
        LEFT JOIN pen p
          ON b.run_id = p.run_id AND b.object_name = p.object_name
    """, [run_id, run_id])

    top = con.execute(f"""
        SELECT object_name, score_base, rules_penalty_avg, score_final
        FROM {stg}.dq_table_scores_u_rules
        WHERE run_id=?
        ORDER BY score_final DESC
    """, [run_id]).fetchall()

    print(f"[TABLE] OK. run_id={run_id}")
    for r in top:
        print(r)

    con.close()


if __name__ == "__main__":
    main()