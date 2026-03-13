# -*- coding: utf-8 -*-
"""
11_show_report_like_image.py
Mostra o relatório no console no formato:
Métrica | Valor | Significado
"""

import argparse
import duckdb


MEANINGS = {
    "score_final": "qualidade geral do dataset",
    "score_completeness": "poucos valores nulos (completude)",
    "score_uniqueness": "boa diversidade de valores (unicidade)",
    "score_volume": "volume de linhas (amostra)",
    "null_rate": "percentual de células nulas",
    "avg_distinct_ratio": "unicidade média das colunas",
}


def fmt(v):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.2f}".rstrip("0").rstrip(".")
    return str(v)


def print_table(rows):
    headers = ["Métrica", "Valor", "Significado"]
    widths = [len(h) for h in headers]
    for a, b, c in rows:
        widths[0] = max(widths[0], len(str(a)))
        widths[1] = max(widths[1], len(str(b)))
        widths[2] = max(widths[2], len(str(c)))

    def line(char="-"):
        return char * (sum(widths) + 6)

    print("\nInterpretando:\n")
    print(f"{headers[0]:<{widths[0]}}  {headers[1]:<{widths[1]}}  {headers[2]:<{widths[2]}}")
    print(line("-"))
    for a, b, c in rows:
        print(f"{a:<{widths[0]}}  {b:<{widths[1]}}  {c:<{widths[2]}}")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True, help="dq_lab.duckdb")
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--run_id", default=None, help="se vazio, usa o último run_id")
    ap.add_argument("--object", dest="object_name", default=None, help="dataset (tabela/arquivo) para mostrar (opcional)")
    args = ap.parse_args()

    con = duckdb.connect(args.duckdb)
    stg = args.stg

    if args.run_id:
        run_id = args.run_id
    else:
        row = con.execute(f"SELECT run_id FROM {stg}.dq_table_scores_u ORDER BY scanned_at DESC LIMIT 1").fetchone()
        if not row:
            print("Não há scores universais ainda. Rode: 09_universal_scan.py e depois 10_compute_scores_universal.py")
            con.close()
            return
        run_id = row[0]

    # dataset alvo (pega o melhor score do run, se não especificar)
    if args.object_name:
        object_name = args.object_name
    else:
        row = con.execute(f"""
            SELECT object_name
            FROM {stg}.dq_table_scores_u
            WHERE run_id = ?
            ORDER BY score_final DESC
            LIMIT 1
        """, [run_id]).fetchone()
        object_name = row[0]

    r = con.execute(f"""
        SELECT
            score_final,
            score_completeness,
            score_uniqueness,
            score_volume,
            null_rate,
            avg_distinct_ratio
        FROM {stg}.dq_table_scores_u
        WHERE run_id = ? AND object_name = ?
        LIMIT 1
    """, [run_id, object_name]).fetchone()

    if not r:
        print(f"Dataset não encontrado no run_id={run_id}: {object_name}")
        con.close()
        return

    score_final, score_completeness, score_uniqueness, score_volume, null_rate, avg_distinct_ratio = r

    rows = [
        ("score_final", fmt(score_final), MEANINGS["score_final"]),
        ("score_completeness", fmt(score_completeness), MEANINGS["score_completeness"]),
        ("score_uniqueness", fmt(score_uniqueness), MEANINGS["score_uniqueness"]),
        ("score_volume", fmt(score_volume), MEANINGS["score_volume"]),
        ("null_rate", fmt(null_rate), MEANINGS["null_rate"]),
        ("avg_distinct_ratio", fmt(avg_distinct_ratio), MEANINGS["avg_distinct_ratio"]),
    ]

    print(f"\nDataset: {object_name}")
    print(f"run_id:  {run_id}")
    print_table(rows)

    con.close()


if __name__ == "__main__":
    main()