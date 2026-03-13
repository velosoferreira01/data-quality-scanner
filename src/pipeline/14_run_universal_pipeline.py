# -*- coding: utf-8 -*-
"""
14_run_universal_pipeline.py
Orquestra:
09 -> (já executado antes ou por outro runner)
10 -> score por tabela
12 -> score por coluna com regras
13 -> score final da tabela com regras
11 -> relatório opcional
"""

import argparse
import os
import subprocess
import sys


def run_cmd(cmd: list[str]):
    print("\n[RUN]", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True, help="Arquivo dq_lab.duckdb")
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--rules", required=True, help="Arquivo YAML de regras")
    ap.add_argument("--run_id", default=None, help="Se vazio, usa o último run_id")
    ap.add_argument("--object", dest="object_name", default=None, help="Dataset específico para colunas/relatório")
    ap.add_argument("--report", action="store_true", help="Mostra relatório no final")
    args = ap.parse_args()

    py = sys.executable
    base = os.path.dirname(os.path.abspath(__file__))

    f10 = os.path.join(base, "10_compute_scores_universal.py")
    f11 = os.path.join(base, "11_show_report_like_image.py")
    f12 = os.path.join(base, "12_compute_column_scores_universal.py")
    f13 = os.path.join(base, "13_compute_table_scores_with_rules_universal.py")

    cmd10 = [py, f10, "--duckdb", args.duckdb, "--stg", args.stg]
    cmd12 = [py, f12, "--duckdb", args.duckdb, "--stg", args.stg, "--rules", args.rules]
    cmd13 = [py, f13, "--duckdb", args.duckdb, "--stg", args.stg]

    if args.run_id:
        cmd10 += ["--run_id", args.run_id]
        cmd12 += ["--run_id", args.run_id]
        cmd13 += ["--run_id", args.run_id]

    if args.object_name:
        cmd12 += ["--object", args.object_name]

    run_cmd(cmd10)
    run_cmd(cmd12)
    run_cmd(cmd13)

    if args.report:
        cmd11 = [py, f11, "--duckdb", args.duckdb, "--stg", args.stg]
        if args.run_id:
            cmd11 += ["--run_id", args.run_id]
        if args.object_name:
            cmd11 += ["--object", args.object_name]
        run_cmd(cmd11)

    print("\n[PIPELINE] OK.")


if __name__ == "__main__":
    main()