# -*- coding: utf-8 -*-
"""
export_ai_recommendations_dashboard.py

Gera um HTML executivo a partir do arquivo:
- dq_ai_recommendations_current.csv

Saídas:
- dq_ai_recommendations_dashboard.html

Uso:
python export_ai_recommendations_dashboard.py --csv ./output/dq_ai_recommendations_current.csv --outdir ./output
"""

import argparse
from datetime import datetime
from pathlib import Path
import html

import pandas as pd
import plotly.express as px


def safe_str(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def esc(v):
    return html.escape(safe_str(v))


def classify_recommendation(text: str) -> str:
    t = safe_str(text).lower()
    if any(k in t for k in ["null", "nulo", "falt", "missing", "complet"]):
        return "Completude"
    if any(k in t for k in ["duplic", "unique", "unic"]):
        return "Unicidade"
    if any(k in t for k in ["regex", "formato", "pattern", "padrao", "válid", "valid"]):
        return "Validade"
    if any(k in t for k in ["range", "faixa", "limite", "interval"]):
        return "Consistência"
    if any(k in t for k in ["fresh", "atual", "recen"]):
        return "Atualidade"
    return "Qualidade Geral"


def fig_to_html(fig):
    return fig.to_html(include_plotlyjs="cdn", full_html=False, config={
        "displaylogo": False,
        "responsive": True,
        "scrollZoom": True,
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--outdir", default="./output")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    df = pd.read_csv(csv_path)

    for col in ["run_id", "prioridade", "dataset_label", "recomendacao"]:
        if col not in df.columns:
            df[col] = ""

    df["run_id"] = df["run_id"].map(safe_str)
    df["prioridade"] = df["prioridade"].map(safe_str).replace("", "Não definida")
    df["dataset_label"] = df["dataset_label"].map(safe_str).replace("", "Não informado")
    df["recomendacao"] = df["recomendacao"].map(safe_str)
    df["tema"] = df["recomendacao"].apply(classify_recommendation)

    run_id = safe_str(df["run_id"].iloc[0]) if not df.empty else ""
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    total_recs = len(df)
    total_datasets = df["dataset_label"].nunique() if not df.empty else 0
    alta = int((df["prioridade"].str.lower() == "alta").sum()) if not df.empty else 0
    media = int((df["prioridade"].str.lower() == "média").sum() + (df["prioridade"].str.lower() == "media").sum()) if not df.empty else 0

    by_priority = (
        df.groupby("prioridade", dropna=False)
          .size()
          .reset_index(name="quantidade")
          .sort_values("quantidade", ascending=True)
    )

    by_dataset = (
        df.groupby("dataset_label", dropna=False)
          .size()
          .reset_index(name="quantidade")
          .sort_values("quantidade", ascending=True)
          .tail(10)
    )

    by_theme = (
        df.groupby("tema", dropna=False)
          .size()
          .reset_index(name="quantidade")
          .sort_values("quantidade", ascending=True)
    )

    fig_priority = px.bar(
        by_priority,
        x="quantidade",
        y="prioridade",
        orientation="h",
        title="Recomendações por Prioridade",
        text="quantidade",
    )

    fig_dataset = px.bar(
        by_dataset,
        x="quantidade",
        y="dataset_label",
        orientation="h",
        title="Top Datasets com Mais Recomendações",
        text="quantidade",
    )

    fig_theme = px.bar(
        by_theme,
        x="quantidade",
        y="tema",
        orientation="h",
        title="Recomendações por Tema",
        text="quantidade",
    )

    priority_html = fig_to_html(fig_priority)
    dataset_html = fig_to_html(fig_dataset)
    theme_html = fig_to_html(fig_theme)

    top_table = df[["prioridade", "dataset_label", "tema", "recomendacao"]].copy().head(30)

    rows = []
    for _, row in top_table.iterrows():
        rows.append(
            f"""
            <tr>
                <td>{esc(row['prioridade'])}</td>
                <td>{esc(row['dataset_label'])}</td>
                <td>{esc(row['tema'])}</td>
                <td>{esc(row['recomendacao'])}</td>
            </tr>
            """
        )

    html_out = f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Recomendações Inteligentes - MJV Data Quality</title>
<style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: Arial, Helvetica, sans-serif; background:#F5F7FB; color:#1F2937; }}
    .page {{ max-width: 1320px; margin: 0 auto; }}
    .cover {{ background: linear-gradient(135deg, #0B1F3A 0%, #15396B 45%, #0F5DFF 100%); color:white; border-radius:0 0 28px 28px; padding:36px 42px 46px 42px; box-shadow:0 18px 50px rgba(11,31,58,0.18); }}
    .cover-kicker {{ text-transform:uppercase; letter-spacing:2px; font-size:12px; opacity:0.85; font-weight:700; }}
    .cover h1 {{ margin:14px 0 10px 0; font-size:40px; line-height:1.08; }}
    .cover p {{ margin:0; max-width:920px; font-size:17px; line-height:1.6; color:rgba(255,255,255,0.92); }}
    .cover-meta {{ margin-top:22px; display:flex; gap:12px; flex-wrap:wrap; }}
    .pill {{ background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.18); color:white; padding:10px 14px; border-radius:999px; font-size:13px; font-weight:700; }}
    .container {{ padding:28px; }}
    .section {{ margin-top:24px; background:#FFFFFF; border:1px solid #D9E1EC; border-radius:24px; padding:26px; box-shadow:0 8px 24px rgba(15,23,42,0.05); }}
    .section-title {{ margin:0 0 18px 0; color:#0B1F3A; font-size:28px; }}
    .metrics-grid {{ display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:16px; }}
    .metric-card {{ background:white; border:1px solid #D9E1EC; border-radius:20px; padding:18px; min-height:132px; }}
    .metric-title {{ color:#6B7280; font-size:13px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; }}
    .metric-value {{ color:#0B1F3A; font-size:32px; font-weight:800; margin-top:8px; }}
    .metric-subtitle {{ color:#6B7280; font-size:13px; margin-top:8px; line-height:1.5; }}
    .chart-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .chart-grid-single {{ display:grid; grid-template-columns:1fr; gap:18px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th {{ text-align:left; background:#0B1F3A; color:white; padding:12px 14px; }}
    td {{ padding:12px 14px; border-bottom:1px solid #D9E1EC; vertical-align:top; }}
    tr:nth-child(even) td {{ background:#FBFCFE; }}
    .footer {{ padding:28px; text-align:center; color:#6B7280; font-size:12px; }}
    .note {{ color:#6B7280; font-size:13px; line-height:1.6; }}
    @media (max-width: 1100px) {{ .metrics-grid, .chart-grid {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
    <div class="page">
        <section class="cover">
            <div class="cover-kicker">MJV Data Quality</div>
            <h1>Recomendações Inteligentes de Correção</h1>
            <p>Painel executivo consolidado das recomendações geradas a partir das anomalias, colunas críticas e sinais de atenção identificados no processo de Data Quality.</p>
            <div class="cover-meta">
                <span class="pill">Run ID: {esc(run_id)}</span>
                <span class="pill">Gerado em: {esc(generated_at)}</span>
                <span class="pill">Origem: Regras Heurísticas</span>
            </div>
        </section>

        <div class="container">
            <section class="section">
                <h2 class="section-title">Sumário Executivo</h2>
                <div class="metrics-grid">
                    <div class="metric-card" style="border-top:5px solid #0F5DFF;">
                        <div class="metric-title">Recomendações Totais</div>
                        <div class="metric-value">{total_recs}</div>
                        <div class="metric-subtitle">Volume consolidado gerado para o run atual.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid #2EAD67;">
                        <div class="metric-title">Datasets Impactados</div>
                        <div class="metric-value">{total_datasets}</div>
                        <div class="metric-subtitle">Fontes com alguma recomendação associada.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid #D64545;">
                        <div class="metric-title">Prioridade Alta</div>
                        <div class="metric-value">{alta}</div>
                        <div class="metric-subtitle">Itens que pedem ação prioritária.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid #F28C28;">
                        <div class="metric-title">Prioridade Média</div>
                        <div class="metric-value">{media}</div>
                        <div class="metric-subtitle">Itens relevantes para o ciclo seguinte.</div>
                    </div>
                </div>
            </section>

            <section class="section">
                <h2 class="section-title">Painel Analítico</h2>
                <div class="chart-grid">
                    <div>{priority_html}</div>
                    <div>{theme_html}</div>
                </div>
            </section>

            <section class="section">
                <h2 class="section-title">Top Datasets com Mais Recomendações</h2>
                <div class="chart-grid-single">
                    <div>{dataset_html}</div>
                </div>
            </section>

            <section class="section">
                <h2 class="section-title">Detalhamento Executivo</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Prioridade</th>
                            <th>Dataset</th>
                            <th>Tema</th>
                            <th>Recomendação</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(rows)}
                    </tbody>
                </table>
            </section>

            <section class="section">
                <h2 class="section-title">Observação Metodológica</h2>
                <p class="note">
                    As recomendações deste painel foram geradas por lógica heurística do pipeline MJV Data Quality,
                    com base nas colunas críticas, scores e sinais de atenção encontrados no processo de validação.
                    Não se trata de um modelo generativo autônomo treinado especificamente para esse relatório.
                </p>
            </section>
        </div>

        <div class="footer">Recomendações Inteligentes MJV - 2026</div>
    </div>
</body>
</html>
"""

    out_path = outdir / "dq_ai_recommendations_dashboard.html"
    out_path.write_text(html_out, encoding="utf-8")
    print(f"[OK] HTML : {out_path}")


if __name__ == "__main__":
    main()
