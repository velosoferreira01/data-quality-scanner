# -*- coding: utf-8 -*-
"""
Exporta relatorio com:
1. resumo geral por dataset
2. dimensoes por dataset
3. detalhe por coluna
"""

import argparse
from pathlib import Path
import duckdb
import pandas as pd
from playwright.sync_api import sync_playwright


def table_exists(con, schema, table):
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


def empty_dimensions_df():
    return pd.DataFrame(
        columns=[
            "run_id","scanned_at","source_type","source_ref","object_name",
            "dim_completude","dim_unicidade","dim_consistencia","dim_validade",
            "dim_integridade_ref","dim_freshness","score_final",
        ]
    )


def get_latest_run_id(con, schema):
    # tentativa principal
    try:
        row = con.execute(f"""
            SELECT run_id
            FROM {schema}.dq_table_scores_u_rules
            WHERE run_id IS NOT NULL
            ORDER BY run_id DESC
            LIMIT 1
        """).fetchone()
        if row:
            return row[0]
    except:
        pass

    # fallback
    try:
        row = con.execute(f"""
            SELECT run_id
            FROM {schema}.dq_table_scores_u
            WHERE run_id IS NOT NULL
            ORDER BY run_id DESC
            LIMIT 1
        """).fetchone()
        if row:
            return row[0]
    except:
        pass

    return None

def export_html_to_pdf(html_file: Path):
    """
    Converte HTML em PDF mantendo layout visual
    """
    pdf_file = html_file.with_suffix(".pdf")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        page.goto(html_file.as_uri(), wait_until="networkidle")
        page.wait_for_timeout(3000)

        page.pdf(
            path=str(pdf_file),
            format="A4",
            print_background=True,
            margin={
                "top": "12mm",
                "right": "10mm",
                "bottom": "12mm",
                "left": "10mm"
            }
        )

        browser.close()

    print(f"[EXPORT] PDF gerado: {pdf_file}")

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

    required = ["dq_table_scores_u_rules", "dq_column_scores_u"]
    missing = [t for t in required if not table_exists(con, args.stg, t)]

    if missing:
        print("[EXPORT] Faltam tabelas:", missing)
        return

    run_id = args.run_id or get_latest_run_id(con, args.stg)

    if not run_id:
        print("[EXPORT] Nao ha run disponivel.")
        return

    print(f"[EXPORT] Usando run_id: {run_id}")

    summary = con.execute(f"""
        SELECT *
        FROM {args.stg}.dq_table_scores_u_rules
        WHERE run_id = ?
    """, [run_id]).fetchdf()

    if table_exists(con, args.stg, "dq_table_dimension_scores_u"):
        dimensions = con.execute(f"""
            SELECT *
            FROM {args.stg}.dq_table_dimension_scores_u
            WHERE run_id = ?
        """, [run_id]).fetchdf()
    else:
        dimensions = empty_dimensions_df()

    detail = con.execute(f"""
        SELECT *
        FROM {args.stg}.dq_column_scores_u
        WHERE run_id = ?
    """, [run_id]).fetchdf()

    summary.to_csv(outdir / f"dq_summary_{run_id}.csv", index=False)
    dimensions.to_csv(outdir / f"dq_dimensions_{run_id}.csv", index=False)
    detail.to_csv(outdir / f"dq_detail_{run_id}.csv", index=False)

    html = f"""
    <html><body>
    <h1>Data Quality Report</h1>
    <h2>Resumo</h2>{summary.to_html(index=False)}
    <h2>Dimensoes</h2>{dimensions.to_html(index=False)}
    <h2>Detalhe</h2>{detail.to_html(index=False)}
    </body></html>
    """

    html_file = outdir / f"dq_report_{run_id}.html"
    html_file.write_text(html, encoding="utf-8")

    try:
        export_html_to_pdf(html_file)
    except Exception as e:
        print(f"[WARN] Falha ao gerar PDF: {e}")

    print("[EXPORT] OK")
    print(outdir)


if __name__ == "__main__":
    main()
