# -*- coding: utf-8 -*-
"""
16_export_universal_report.py
Exporta relatorio com 3 partes:
1. resumo geral por dataset
2. dimensoes por dataset
3. detalhe por coluna
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def empty_dimensions_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "run_id",
            "scanned_at",
            "source_type",
            "source_ref",
            "object_name",
            "dim_completude",
            "dim_unicidade",
            "dim_consistencia",
            "dim_validade",
            "dim_integridade_ref",
            "dim_freshness",
            "score_final",
        ]
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--stg", default="stg")
    ap.add_argument("--run_id", default=None)
    ap.add_argument("--outdir", default="./output")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.duckdb)

    required_tables = ["dq_table_scores_u_rules", "dq_column_scores_u"]
    missing_required = [name for name in required_tables if not table_exists(con, args.stg, name)]
    if missing_required:
        print(
            "[EXPORT] Faltam tabelas obrigatorias para exportacao: "
            + ", ".join(f"{args.stg}.{name}" for name in missing_required)
        )
        print("[EXPORT] Rode primeiro o pipeline completo para gerar scores e regras.")
        con.close()
        return

    if args.run_id:
        run_id = args.run_id
    else:
        row = con.execute(
            f"""
            SELECT run_id
            FROM {args.stg}.dq_table_scores_u_rules
            ORDER BY scanned_at DESC
            LIMIT 1
            """
        ).fetchone()

        if not row:
            print("[EXPORT] Nao ha run disponivel.")
            con.close()
            return

        run_id = row[0]

    summary = con.execute(
        f"""
        SELECT
            run_id,
            scanned_at,
            source_type,
            source_ref,
            object_name,
            score_base,
            rules_penalty_avg,
            score_final
        FROM {args.stg}.dq_table_scores_u_rules
        WHERE run_id = ?
        ORDER BY score_final DESC, object_name
        """,
        [run_id],
    ).fetchdf()

    if table_exists(con, args.stg, "dq_table_dimension_scores_u"):
        dimensions = con.execute(
            f"""
            SELECT
                run_id,
                scanned_at,
                source_type,
                source_ref,
                object_name,
                dim_completude,
                dim_unicidade,
                dim_consistencia,
                dim_validade,
                dim_integridade_ref,
                dim_freshness,
                score_final
            FROM {args.stg}.dq_table_dimension_scores_u
            WHERE run_id = ?
            ORDER BY score_final DESC, object_name
            """,
            [run_id],
        ).fetchdf()
    else:
        print(f"[EXPORT] Aviso: {args.stg}.dq_table_dimension_scores_u nao existe; exportando sem a aba de dimensoes preenchida.")
        dimensions = empty_dimensions_df()

    detail = con.execute(
        f"""
        SELECT
            run_id,
            scanned_at,
            source_type,
            source_ref,
            object_name,
            column_name,
            dtype,
            total,
            null_rate,
            distinct_ratio,
            rule_not_null,
            rule_unique,
            rule_regex,
            rule_range_min,
            rule_range_max,
            rule_allowed_vals,
            violations,
            score_base,
            score_rules,
            score_final
        FROM {args.stg}.dq_column_scores_u
        WHERE run_id = ?
        ORDER BY object_name, column_name
        """,
        [run_id],
    ).fetchdf()

    summary.to_csv(outdir / f"dq_summary_{run_id}.csv", index=False, encoding="utf-8-sig")
    dimensions.to_csv(outdir / f"dq_dimensions_{run_id}.csv", index=False, encoding="utf-8-sig")
    detail.to_csv(outdir / f"dq_detail_{run_id}.csv", index=False, encoding="utf-8-sig")

    summary_json = summary.to_json(orient="records", force_ascii=False, indent=2, date_format="iso")
    dimensions_json = dimensions.to_json(orient="records", force_ascii=False, indent=2, date_format="iso")
    detail_json = detail.to_json(orient="records", force_ascii=False, indent=2, date_format="iso")

    (outdir / f"dq_summary_{run_id}.json").write_text(summary_json, encoding="utf-8")
    (outdir / f"dq_dimensions_{run_id}.json").write_text(dimensions_json, encoding="utf-8")
    (outdir / f"dq_detail_{run_id}.json").write_text(detail_json, encoding="utf-8")

    html = f"""
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>Data Quality Report - {run_id}</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; }}
        h1, h2 {{ color: #1f2937; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 14px; }}
        th {{ background: #f3f4f6; text-align: left; }}
        tr:nth-child(even) {{ background: #fafafa; }}
      </style>
    </head>
    <body>
      <h1>Data Quality Report</h1>
      <p><strong>run_id:</strong> {run_id}</p>

      <h2>1. Resumo geral por dataset</h2>
      {summary.to_html(index=False, border=0)}

      <h2>2. Nota por dimensao</h2>
      {dimensions.to_html(index=False, border=0)}

      <h2>3. Detalhe por coluna</h2>
      {detail.to_html(index=False, border=0)}
    </body>
    </html>
    """
    (outdir / f"dq_report_{run_id}.html").write_text(html, encoding="utf-8")

    print("[EXPORT] OK")
    print(outdir / f"dq_summary_{run_id}.csv")
    print(outdir / f"dq_dimensions_{run_id}.csv")
    print(outdir / f"dq_detail_{run_id}.csv")
    print(outdir / f"dq_summary_{run_id}.json")
    print(outdir / f"dq_dimensions_{run_id}.json")
    print(outdir / f"dq_detail_{run_id}.json")
    print(outdir / f"dq_report_{run_id}.html")

    con.close()


if __name__ == "__main__":
    main()
