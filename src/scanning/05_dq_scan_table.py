# -*- coding: utf-8 -*-
import argparse
from datetime import datetime
import duckdb

def clamp_0_10(x: float) -> float:
    return max(0.0, min(10.0, x))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="dq_lab.duckdb")
    ap.add_argument("--table", required=True)
    ap.add_argument("--pk", default="")
    ap.add_argument("--required", default="")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    table = args.table
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    print("\n===== DATA QUALITY SCAN =====")
    print("Tabela:", table)
    print("Total registros:", total)

    # COMPLETUDE
    required_cols = [c.strip() for c in args.required.split(",") if c.strip()]
    if total == 0 or not required_cols:
        score_completude = 10.0
    else:
        null_rates = []
        for col in required_cols:
            nulls = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            null_rates.append(nulls / total)
        avg_null = sum(null_rates) / len(null_rates)
        score_completude = clamp_0_10(10 * (1 - avg_null))

    # UNICIDADE
    if total == 0 or not args.pk:
        score_unicidade = 10.0
    else:
        distinct = con.execute(
            f"SELECT COUNT(DISTINCT {args.pk}) FROM {table}"
        ).fetchone()[0]
        dup_rate = 1 - (distinct / total)
        score_unicidade = clamp_0_10(10 * (1 - dup_rate))

    # NOTA FINAL (simples por enquanto)
    nota_final = round((score_completude + score_unicidade) / 2, 2)

    print("Completude:", round(score_completude, 2))
    print("Unicidade:", round(score_unicidade, 2))
    print("NOTA FINAL:", nota_final)

    con.close()

if __name__ == "__main__":
    main()