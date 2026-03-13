# -*- coding: utf-8 -*-
"""
12_compute_column_scores_universal.py
- Calcula score 0–10 por coluna com base nas métricas universais
- Aplica regras (not_null, unique, regex, range, allowed_values)
- Salva em stg.dq_column_scores_u

Requer: pyyaml
"""

import argparse
import re
import duckdb
import yaml


def load_rules(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def clamp(x: float, lo=0.0, hi=10.0) -> float:
    return max(lo, min(hi, x))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--rules", required=True, help="Arquivo YAML com regras (ex: 12_dq_rules.yml)")
    ap.add_argument("--run_id", default=None, help="Se vazio, usa o último run_id")
    ap.add_argument("--object", dest="object_name", default=None, help="Se vazio, calcula para todos do run_id")
    args = ap.parse_args()

    con = duckdb.connect(args.duckdb)
    stg = args.stg

    rules_doc = load_rules(args.rules)
    ds_rules = (rules_doc.get("datasets") or {})

    if args.run_id:
        run_id = args.run_id
    else:
        row = con.execute(f"SELECT run_id FROM {stg}.dq_table_metrics_u ORDER BY scanned_at DESC LIMIT 1").fetchone()
        if not row:
            print("[COL] Não há métricas universais ainda. Rode o 09_universal_scan.py primeiro.")
            con.close()
            return
        run_id = row[0]

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {stg}.dq_column_scores_u (
            run_id            VARCHAR,
            scanned_at        TIMESTAMP,
            source_type       VARCHAR,
            source_ref        VARCHAR,
            object_name       VARCHAR,
            column_name       VARCHAR,
            dtype             VARCHAR,
            total             BIGINT,
            null_rate         DOUBLE,
            distinct_ratio    DOUBLE,

            rule_not_null     BOOLEAN,
            rule_unique       BOOLEAN,
            rule_regex        VARCHAR,
            rule_range_min    DOUBLE,
            rule_range_max    DOUBLE,
            rule_allowed_vals VARCHAR,

            violations        BIGINT,
            score_base        DOUBLE,
            score_rules       DOUBLE,
            score_final       DOUBLE
        );
    """)

    # Reprocessamento
    if args.object_name:
        con.execute(f"DELETE FROM {stg}.dq_column_scores_u WHERE run_id=? AND object_name=?", [run_id, args.object_name])
    else:
        con.execute(f"DELETE FROM {stg}.dq_column_scores_u WHERE run_id=?", [run_id])

    # Seleciona colunas
    if args.object_name:
        rows = con.execute(f"""
            SELECT scanned_at, source_type, source_ref, object_name, column_name, dtype, total, null_rate, distinct_ratio
            FROM {stg}.dq_column_metrics_u
            WHERE run_id=? AND object_name=?
        """, [run_id, args.object_name]).fetchall()
    else:
        rows = con.execute(f"""
            SELECT scanned_at, source_type, source_ref, object_name, column_name, dtype, total, null_rate, distinct_ratio
            FROM {stg}.dq_column_metrics_u
            WHERE run_id=?
        """, [run_id]).fetchall()

    if not rows:
        print("[COL] Nenhuma métrica de coluna encontrada para calcular score.")
        con.close()
        return

    inserts = []
    for scanned_at, source_type, source_ref, object_name, column_name, dtype, total, null_rate, distinct_ratio in rows:
        # Base score (0–10)
        # - completude: (1-null_rate)*10 (peso 70%)
        # - unicidade: distinct_ratio*10 (peso 30%)
        score_base = clamp(0.7 * ((1.0 - (null_rate or 1.0)) * 10.0) + 0.3 * ((distinct_ratio or 0.0) * 10.0))

        # Regras (se existirem)
        r = (((ds_rules.get(object_name) or {}).get("columns") or {}).get(column_name) or {})

        rule_not_null = bool(r.get("not_null")) if "not_null" in r else None
        rule_unique = bool(r.get("unique")) if "unique" in r else None
        rule_regex = r.get("regex")
        rule_allowed = r.get("allowed_values")
        rule_range = r.get("range") or {}
        rule_min = rule_range.get("min")
        rule_max = rule_range.get("max")

        # Violations (aproximações usando métricas)
        violations = 0

        # not_null: se null_rate>0 → violações ~ total*null_rate
        if rule_not_null is True:
            violations += int(round((total or 0) * (null_rate or 0.0)))

        # unique: se distinct_ratio<1 → violações aproximadas
        if rule_unique is True:
            if distinct_ratio is None:
                violations += 0
            else:
                # Se distinct_ratio = distinct / non_null, então duplicados ~ non_null - distinct
                non_null = int(round((total or 0) * (1.0 - (null_rate or 0.0))))
                distinct_cnt_est = int(round((distinct_ratio or 0.0) * non_null))
                dup_est = max(0, non_null - distinct_cnt_est)
                violations += dup_est

        # regex/allowed/range: para checar de verdade, precisa olhar os valores.
        # Aqui vamos marcar como "não checado" via regras e não penalizar automaticamente.
        # (se você quiser validar de verdade, dá pra fazer no próximo passo lendo amostra e testando)
        # Mesmo assim, deixamos registrado o rule_regex/range/allowed.

        # Penalidade de regras: quanto mais violações, mais cai.
        # Aqui: -2 pontos se violação >= 1, -4 se >10% do total, -6 se >30% do total (clamp)
        score_rules = 0.0
        if total and violations > 0:
            ratio = violations / total
            if ratio > 0.30:
                score_rules = -6.0
            elif ratio > 0.10:
                score_rules = -4.0
            else:
                score_rules = -2.0

        score_final = clamp(score_base + score_rules)

        inserts.append([
            run_id, scanned_at, source_type, source_ref, object_name, column_name, dtype,
            int(total or 0), float(null_rate or 0.0), float(distinct_ratio or 0.0),
            rule_not_null, rule_unique, rule_regex,
            float(rule_min) if rule_min is not None else None,
            float(rule_max) if rule_max is not None else None,
            ",".join([str(x) for x in rule_allowed]) if isinstance(rule_allowed, list) else None,
            int(violations),
            float(score_base),
            float(score_rules),
            float(score_final),
        ])

    con.executemany(f"""
        INSERT INTO {stg}.dq_column_scores_u VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, inserts)

    print(f"[COL] OK. run_id={run_id} col_scores={len(inserts)}")
    con.close()


if __name__ == "__main__":
    main()