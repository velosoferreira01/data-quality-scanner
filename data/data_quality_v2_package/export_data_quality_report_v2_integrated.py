
# -*- coding: utf-8 -*-
"""
export_data_quality_report_v2_integrated.py

Relatório premium executivo de Data Quality:
- usa um único run_id efetivo
- gera HTML premium com gráficos embutidos
- gera Excel com abas executivas e técnicas
- exporta radar, histórico e arquivos CSV auxiliares
"""

import argparse
import base64
from datetime import datetime
from io import BytesIO
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd


# =========================================================
# Utilidades
# =========================================================
def ensure_dir(path):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_str(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def html_escape(value):
    return (
        safe_str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def table_exists(con, schema, table_name):
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table_name],
    ).fetchone()
    return row is not None


def fetchdf_safe(con, sql, params=None):
    try:
        if params is None:
            return con.execute(sql).fetchdf()
        return con.execute(sql, params).fetchdf()
    except Exception:
        return pd.DataFrame()


def detect_score_col(df):
    for col in ["score", "score_final", "score_base"]:
        if col in df.columns:
            return col
    return None


def detect_classification_col(df):
    for col in ["classification", "classificacao"]:
        if col in df.columns:
            return col
    return None


def best_dataset_label(row):
    candidates = [
        row.get("dataset_label"),
        row.get("dataset_name"),
        row.get("dataset"),
        row.get("object_name"),
        row.get("source_name"),
        row.get("table_name"),
        row.get("file_name"),
        row.get("source_ref"),
        row.get("source"),
    ]
    for c in candidates:
        txt = safe_str(c)
        if txt:
            return txt
    return "Dataset não identificado"


def classify_score(score):
    try:
        score = float(score)
    except Exception:
        return "Sem classificação"
    if score >= 8:
        return "Bom"
    if score >= 7:
        return "Bom"
    if score >= 5:
        return "Atenção"
    return "Crítico"


def status_color(status):
    mapping = {
        "Excelente": "#2EAD67",
        "Bom": "#0F5DFF",
        "Atenção": "#F28C28",
        "Crítico": "#D64545",
        "Sem classificação": "#6B7280",
    }
    return mapping.get(status, "#6B7280")


def get_latest_run_id(con, schema):
    for table in ["dq_table_scores_u", "dq_table_scores_u_rules", "dq_column_scores_u"]:
        if not table_exists(con, schema, table):
            continue
        row = con.execute(
            f"""
            SELECT run_id
            FROM {schema}.{table}
            WHERE run_id IS NOT NULL
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    return None


# =========================================================
# Leitura de dados
# =========================================================
def load_summary(con, schema, run_id):
    for table in ["dq_table_scores_u", "dq_table_scores_u_rules"]:
        if not table_exists(con, schema, table):
            continue
        df = fetchdf_safe(con, f"SELECT * FROM {schema}.{table} WHERE run_id = ?", [run_id])
        if not df.empty:
            out = df.copy()
            if "dataset_label" not in out.columns:
                out["dataset_label"] = out.apply(best_dataset_label, axis=1)
            score_col = detect_score_col(out)
            if score_col:
                out[score_col] = pd.to_numeric(out[score_col], errors="coerce").fillna(0)
            class_col = detect_classification_col(out)
            if not class_col and score_col:
                out["classification"] = out[score_col].apply(classify_score)
            return out
    return pd.DataFrame()


def load_detail(con, schema, run_id):
    if not table_exists(con, schema, "dq_column_scores_u"):
        return pd.DataFrame()
    df = fetchdf_safe(con, f"SELECT * FROM {schema}.dq_column_scores_u WHERE run_id = ?", [run_id])
    if df.empty:
        return df
    out = df.copy()
    if "dataset_label" not in out.columns:
        out["dataset_label"] = out.apply(best_dataset_label, axis=1)
    score_col = detect_score_col(out)
    if score_col:
        out[score_col] = pd.to_numeric(out[score_col], errors="coerce").fillna(0)
    class_col = detect_classification_col(out)
    if not class_col and score_col:
        out["classification"] = out[score_col].apply(classify_score)
    return out


def derive_summary_from_detail(detail_df, run_id):
    if detail_df.empty:
        return pd.DataFrame()
    score_col = detect_score_col(detail_df)
    if not score_col:
        return pd.DataFrame()

    grouped = (
        detail_df.groupby("dataset_label", dropna=False)[score_col]
        .mean()
        .reset_index()
        .rename(columns={score_col: "score"})
    )
    grouped["run_id"] = run_id
    grouped["classification"] = grouped["score"].apply(classify_score)
    return grouped


def build_dimensions(summary_df, run_id):
    cols = [
        "run_id", "dataset_label", "dim_completude", "dim_unicidade",
        "dim_consistencia", "dim_validade", "dim_integridade_ref",
        "dim_freshness", "score_final"
    ]
    if summary_df.empty:
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame()
    out["run_id"] = summary_df["run_id"] if "run_id" in summary_df.columns else run_id
    out["dataset_label"] = summary_df["dataset_label"]

    dim_map = {
        "completude": "dim_completude",
        "unicidade": "dim_unicidade",
        "consistencia": "dim_consistencia",
        "validade": "dim_validade",
        "integridade": "dim_integridade_ref",
        "freshness": "dim_freshness",
    }

    for src, dst in dim_map.items():
        if src in summary_df.columns:
            out[dst] = pd.to_numeric(summary_df[src], errors="coerce").fillna(0)
        else:
            out[dst] = 0.0

    score_col = detect_score_col(summary_df)
    if score_col:
        out["score_final"] = pd.to_numeric(summary_df[score_col], errors="coerce").fillna(0)
    else:
        out["score_final"] = 0.0

    return out[cols]


def load_history(con, schema):
    for table in ["dq_table_scores_u", "dq_table_scores_u_rules"]:
        if not table_exists(con, schema, table):
            continue
        df = fetchdf_safe(con, f"SELECT * FROM {schema}.{table}")
        if df.empty:
            continue
        score_col = detect_score_col(df)
        if "run_id" not in df.columns or not score_col:
            continue
        tmp = df.copy()
        tmp[score_col] = pd.to_numeric(tmp[score_col], errors="coerce").fillna(0)
        history = (
            tmp.groupby("run_id", dropna=False)[score_col]
            .mean()
            .reset_index()
            .rename(columns={score_col: "avg_score"})
            .sort_values("run_id")
        )
        return history
    return pd.DataFrame(columns=["run_id", "avg_score"])


# =========================================================
# Preparação analítica
# =========================================================
def build_kpis(summary_df):
    if summary_df.empty:
        return {
            "datasets": 0,
            "score_medio": 0.0,
            "classificacao": "Sem classificação",
            "melhor_dataset": "Sem dados",
            "melhor_score": 0.0,
            "pior_dataset": "Sem dados",
            "pior_score": 0.0,
            "criticos": 0,
            "atencao": 0,
            "aceitaveis": 0,
        }

    work = summary_df.copy()
    score_col = detect_score_col(work)
    if not score_col:
        work["score"] = 0.0
        score_col = "score"

    work[score_col] = pd.to_numeric(work[score_col], errors="coerce").fillna(0)
    class_col = detect_classification_col(work)
    if not class_col:
        work["classification"] = work[score_col].apply(classify_score)
        class_col = "classification"

    avg = round(float(work[score_col].mean()), 2)
    best = work.sort_values(score_col, ascending=False).iloc[0]
    worst = work.sort_values(score_col, ascending=True).iloc[0]

    return {
        "datasets": int(len(work)),
        "score_medio": avg,
        "classificacao": classify_score(avg),
        "melhor_dataset": safe_str(best.get("dataset_label")),
        "melhor_score": round(float(best.get(score_col, 0)), 2),
        "pior_dataset": safe_str(worst.get("dataset_label")),
        "pior_score": round(float(worst.get(score_col, 0)), 2),
        "criticos": int((work[class_col] == "Crítico").sum()),
        "atencao": int((work[class_col] == "Atenção").sum()),
        "aceitaveis": int((work[class_col].isin(["Bom", "Excelente"])).sum()),
    }


def build_top_bottom(summary_df):
    if summary_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    work = summary_df.copy()
    score_col = detect_score_col(work)
    if not score_col:
        return pd.DataFrame(), pd.DataFrame()

    work[score_col] = pd.to_numeric(work[score_col], errors="coerce").fillna(0)
    class_col = detect_classification_col(work)
    cols = ["dataset_label", score_col]
    if class_col:
        cols.append(class_col)

    top = work.sort_values(score_col, ascending=False)[cols].head(5).copy()
    bottom = work.sort_values(score_col, ascending=True)[cols].head(5).copy()

    top = top.rename(columns={score_col: "score"})
    bottom = bottom.rename(columns={score_col: "score"})
    return top, bottom


def build_attention(detail_df):
    if detail_df.empty:
        return pd.DataFrame()
    work = detail_df.copy()
    score_col = detect_score_col(work)
    if not score_col:
        return pd.DataFrame()

    work[score_col] = pd.to_numeric(work[score_col], errors="coerce").fillna(0)
    cols = [c for c in ["dataset_label", "column_name", "dtype", score_col, "classification", "null_rate", "distinct_ratio", "violations"] if c in work.columns]
    out = work.sort_values(score_col, ascending=True)[cols].head(20).copy()
    out = out.rename(columns={score_col: "score"})
    return out


def summary_message(kpis):
    return (
        f"Foram avaliadas {kpis['datasets']} fontes de dados, com score executivo médio de "
        f"{kpis['score_medio']:.2f} e classificação geral '{kpis['classificacao']}'. "
        f"O resultado pede plano de ação priorizado, com foco em consistência, completude "
        f"e monitoramento contínuo. Foram identificadas {kpis['criticos']} fontes críticas "
        f"e {kpis['atencao']} em atenção."
    )


def executive_list_html(kpis):
    return f"""
    <ul class="exec">
        <li><strong>Fontes em condição aceitável ou superior:</strong> {kpis['aceitaveis']} de {kpis['datasets']}.</li>
        <li><strong>Fontes críticas:</strong> {kpis['criticos']}.</li>
        <li><strong>Fontes em atenção:</strong> {kpis['atencao']}.</li>
        <li><strong>Principal foco de correção:</strong> {html_escape(kpis['pior_dataset'])} ({kpis['pior_score']:.2f}).</li>
    </ul>
    """


def df_to_html(df):
    if df.empty:
        return "<p>Sem dados disponíveis.</p>"
    show = df.copy()
    for col in show.columns:
        if pd.api.types.is_numeric_dtype(show[col]):
            show[col] = show[col].round(4)
    return show.to_html(index=False, border=0, classes="dataframe")


# =========================================================
# Gráficos
# =========================================================
def fig_to_base64(fig):
    buffer = BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight", dpi=160)
    plt.close(fig)
    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode("utf-8")


def chart_matrix(summary_df):
    if summary_df.empty:
        return None

    work = summary_df.copy()
    score_col = detect_score_col(work)
    if not score_col:
        return None

    if "row_count" not in work.columns:
        work["row_count"] = range(1, len(work) + 1)

    work["row_count"] = pd.to_numeric(work["row_count"], errors="coerce").fillna(0)
    work[score_col] = pd.to_numeric(work[score_col], errors="coerce").fillna(0)

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.scatter(work["row_count"], work[score_col], s=220, alpha=0.85)
    for _, row in work.iterrows():
        ax.annotate(
            safe_str(row.get("dataset_label")),
            (row["row_count"], row[score_col]),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize=8,
        )
    ax.set_title("Matriz Executiva de Qualidade")
    ax.set_xlabel("Volume de Registros")
    ax.set_ylabel("Score")
    ax.grid(True, alpha=0.3)
    return fig_to_base64(fig)


def chart_distribution(summary_df):
    import textwrap

    if summary_df.empty:
        return None

    score_col = detect_score_col(summary_df)
    if not score_col:
        return None

    work = summary_df.copy().sort_values(score_col, ascending=True)
    work[score_col] = pd.to_numeric(work[score_col], errors="coerce").fillna(0)

    def short_label(text, width=28, max_lines=3):
        txt = safe_str(text)
        wrapped = textwrap.wrap(txt, width=width)
        if len(wrapped) > max_lines:
            wrapped = wrapped[:max_lines]
            wrapped[-1] = wrapped[-1][: max(0, width - 3)] + "..."
        return "\n".join(wrapped)

    work["dataset_label_plot"] = work["dataset_label"].apply(short_label)

    # Altura dinâmica conforme quantidade de datasets
    fig_height = max(6, 0.75 * len(work) + 2.5)

    fig, ax = plt.subplots(figsize=(12, fig_height))
    bars = ax.barh(work["dataset_label_plot"], work[score_col])

    ax.set_title("Distribuição dos Scores")
    ax.set_xlabel("Score")
    ax.set_ylabel("Dataset")
    ax.grid(True, axis="x", alpha=0.3)
    ax.set_xlim(0, max(10, work[score_col].max() + 0.8))

    # Rótulo do valor na ponta da barra
    for bar, value in zip(bars, work[score_col]):
        ax.text(
            bar.get_width() + 0.08,
            bar.get_y() + bar.get_height() / 2,
            f"{value:.2f}",
            va="center",
            fontsize=9,
        )

    plt.tight_layout()
    return fig_to_base64(fig)


def chart_dimensions(dim_df):
    if dim_df.empty:
        return None

    dim_cols = [c for c in ["dim_completude", "dim_unicidade", "dim_consistencia", "dim_validade", "dim_integridade_ref", "dim_freshness"] if c in dim_df.columns]
    if not dim_cols:
        return None

    avg = {c.replace("dim_", "").replace("_", " ").title(): pd.to_numeric(dim_df[c], errors="coerce").fillna(0).mean() for c in dim_cols}
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(list(avg.keys()), list(avg.values()))
    ax.set_title("Radar / Médias por Dimensão")
    ax.set_ylabel("Score Médio")
    ax.tick_params(axis="x", rotation=25)
    ax.grid(True, axis="y", alpha=0.3)
    return fig_to_base64(fig)


def chart_top5(top_df):
    if top_df.empty:
        return None
    work = top_df.copy()
    if "score" not in work.columns:
        return None
    work["score"] = pd.to_numeric(work["score"], errors="coerce").fillna(0)
    work = work.sort_values("score", ascending=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(work["dataset_label"], work["score"])
    ax.set_title("Ranking de Score - Top 5")
    ax.set_xlabel("Score")
    ax.set_ylabel("Dataset")
    ax.grid(True, axis="x", alpha=0.3)
    return fig_to_base64(fig)


def chart_history(history_df):
    if history_df.empty:
        return None
    work = history_df.copy()
    work["avg_score"] = pd.to_numeric(work["avg_score"], errors="coerce").fillna(0)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(work["run_id"], work["avg_score"], marker="o")
    ax.set_title("Histórico de Qualidade")
    ax.set_xlabel("Run ID")
    ax.set_ylabel("Score Médio")
    ax.tick_params(axis="x", rotation=30)
    ax.grid(True, alpha=0.3)
    return fig_to_base64(fig)


# =========================================================
# Exportações auxiliares
# =========================================================

def export_radar_html(path, run_id, dim_df):
    import plotly.graph_objects as go
    from datetime import datetime

    if dim_df is None or dim_df.empty:
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8"/>
            <title>Radar de Qualidade por Dimensão</title>
            <style>
                body {{ margin:0; font-family: Arial, Helvetica, sans-serif; background:#F5F7FB; color:#1F2937; }}
                .page {{ max-width: 1360px; margin:0 auto; }}
                .cover {{ background: linear-gradient(135deg, #0B1F3A 0%, #15396B 45%, #0F5DFF 100%); color:white; border-radius:0 0 28px 28px; padding:36px 42px 46px 42px; }}
                .section {{ margin:24px; background:#FFFFFF; border:1px solid #D9E1EC; border-radius:24px; padding:24px; }}
                .footer {{ padding:28px; text-align:center; color:#6B7280; font-size:12px; }}
                .cover-kicker {{ text-transform:uppercase; letter-spacing:2px; font-size:12px; opacity:0.85; font-weight:700; }}
                .cover h1 {{ margin:14px 0 10px 0; font-size:38px; line-height:1.1; }}
                .cover p {{ margin:0; font-size:16px; line-height:1.6; color:rgba(255,255,255,0.92); }}
                .cover-meta {{ margin-top:22px; display:flex; gap:12px; flex-wrap:wrap; }}
                .pill {{ background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.18); color:white; padding:10px 14px; border-radius:999px; font-size:13px; font-weight:700; }}
            </style>
        </head>
        <body>
            <div class="page">
                <section class="cover">
                    <div class="cover-kicker">MJV Data Quality</div>
                    <h1>Radar de Qualidade por Dimensão</h1>
                    <p>Visão Resumida da qualidade dos dados analisados pelo Data Quality MJV por dimensão.</p>
                    <div class="cover-meta">
                        <span class="pill">Run ID: {html_escape(run_id)}</span>
                        <span class="pill">Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}</span>
                        <span class="pill">Classificação Geral: Bom</span>
                    </div>
                </section>

                <section class="section">
                    <p>Sem dados para radar.</p>
                </section>

                <div class="footer">Qualidade por dimensão MJV - 2026</div>
            </div>
        </body>
        </html>
        """
        path.write_text(html, encoding="utf-8")
        print(f"[OK] Radar: {path}")
        return

    work = dim_df.copy()

    if "dataset_label" not in work.columns:
        if "table_name" in work.columns:
            work["dataset_label"] = work["table_name"]
        else:
            work["dataset_label"] = "Dataset"

    dim_map = [
        ("dim_consistencia", "Consistência"),
        ("dim_completude", "Completude"),
        ("dim_integridade_ref", "Integridade"),
        ("dim_freshness", "Atualidade"),
        ("dim_validade", "Validade"),
        ("dim_unicidade", "Unicidade"),
    ]

    available_dims = [(col, label) for col, label in dim_map if col in work.columns]

    if not available_dims:
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8"/>
            <title>Radar de Qualidade por Dimensão</title>
        </head>
        <body>
            <h1>Radar de Qualidade por Dimensão</h1>
            <p>Sem colunas de dimensão disponíveis.</p>
        </body>
        </html>
        """
        path.write_text(html, encoding="utf-8")
        print(f"[OK] Radar: {path}")
        return

    categories = [label for _, label in available_dims]
    categories_closed = categories + [categories[0]]

    fig = go.Figure()
    dataset_labels = []

    for _, row in work.iterrows():
        dataset = safe_str(row.get("dataset_label")) or "Dataset não identificado"
        dataset_labels.append(dataset)

        values = []
        for col, _label in available_dims:
            try:
                v = float(row.get(col, 0) if row.get(col, 0) is not None else 0)
            except Exception:
                v = 0.0
            values.append(v)

        values_closed = values + [values[0]]

        hover_lines = "<br>".join(
            [f"{label}: {val:.2f}" for label, val in zip(categories, values)]
        )

        fig.add_trace(
            go.Scatterpolar(
                r=values_closed,
                theta=categories_closed,
                fill="toself",
                name=dataset,
                visible=True,
                hovertemplate=f"<b>{dataset}</b><br>{hover_lines}<extra></extra>",
            )
        )

    buttons = [
        dict(
            label="Todos os datasets",
            method="update",
            args=[
                {"visible": [True] * len(dataset_labels)},
                {"title": "Radar de Qualidade por Dimensão - Todos os datasets"},
            ],
        )
    ]

    for i, dataset in enumerate(dataset_labels):
        visible = [False] * len(dataset_labels)
        visible[i] = True
        buttons.append(
            dict(
                label=dataset[:60],
                method="update",
                args=[
                    {"visible": visible},
                    {"title": f"Radar de Qualidade por Dimensão - {dataset}"},
                ],
            )
        )

    fig.update_layout(
        title="Radar de Qualidade por Dimensão - Todos os datasets",
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10],
                tick0=0,
                dtick=2,
            )
        ),
        template="plotly_white",
        height=760,
        updatemenus=[
            dict(
                type="dropdown",
                direction="down",
                x=1.02,
                y=1.12,
                xanchor="left",
                yanchor="top",
                buttons=buttons,
                showactive=True,
            )
        ],
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1.0,
            xanchor="left",
            x=1.02
        ),
        margin=dict(l=60, r=280, t=90, b=50),
    )

    chart_html = fig.to_html(
        include_plotlyjs="cdn",
        full_html=False,
        config={
            "displaylogo": False,
            "scrollZoom": True,
            "responsive": True,
        },
    )

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="utf-8"/>
        <title>Radar de Qualidade por Dimensão</title>
        <style>
            body {{ margin:0; font-family: Arial, Helvetica, sans-serif; background:#F5F7FB; color:#1F2937; }}
            .page {{ max-width: 1360px; margin:0 auto; }}
            .cover {{ background: linear-gradient(135deg, #0B1F3A 0%, #15396B 45%, #0F5DFF 100%); color:white; border-radius:0 0 28px 28px; padding:36px 42px 46px 42px; box-shadow:0 18px 50px rgba(11,31,58,0.18); }}
            .cover-kicker {{ text-transform:uppercase; letter-spacing:2px; font-size:12px; opacity:0.85; font-weight:700; }}
            .cover h1 {{ margin:14px 0 10px 0; font-size:38px; line-height:1.1; }}
            .cover p {{ margin:0; max-width:880px; font-size:16px; line-height:1.6; color:rgba(255,255,255,0.92); }}
            .cover-meta {{ margin-top:22px; display:flex; gap:12px; flex-wrap:wrap; }}
            .pill {{ background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.18); color:white; padding:10px 14px; border-radius:999px; font-size:13px; font-weight:700; }}
            .section {{ margin:24px; background:#FFFFFF; border:1px solid #D9E1EC; border-radius:24px; padding:24px; box-shadow:0 8px 24px rgba(15,23,42,0.05); }}
            .footer {{ padding:28px; text-align:center; color:#6B7280; font-size:12px; }}
            .hint {{ font-size:13px; color:#6B7280; margin-top:8px; }}
        </style>
    </head>
    <body>
        <div class="page">
            <section class="cover">
                <div class="cover-kicker">MJV Data Quality</div>
                <h1>Radar de Qualidade por Dimensão</h1>
                <p>Visão Resumida da qualidade dos dados analisados pelo Data Quality MJV por dimensão.</p>
                <div class="cover-meta">
                    <span class="pill">Run ID: {html_escape(run_id)}</span>
                    <span class="pill">Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}</span>
                    <span class="pill">Classificação Geral: Bom</span>
                </div>
            </section>

            <section class="section">
                {chart_html}
                <div class="hint">Nota: Use o menu no canto superior direito para filtrar por dataset.</div>
            </section>

            <section class="section">
                <h2 style="margin:0 0 18px 0; font-size:28px; color:#0B1F3A;">Detalhamento das Métricas Avaliadas</h2>
                <p style="margin:0 0 18px 0; font-size:15px; line-height:1.7; color:#4B5563;">
                    Abaixo está o significado de cada dimensão monitorada no radar de qualidade MJV.
                </p>
                <div style="overflow-x:auto;">
                    <table style="width:100%; border-collapse:separate; border-spacing:0; font-size:15px; line-height:1.6; border:1px solid #D9E1EC; border-radius:18px; overflow:hidden;">
                        <thead>
                            <tr style="background:linear-gradient(135deg, #0B1F3A 0%, #15396B 100%); color:#FFFFFF;">
                                <th style="text-align:left; padding:16px 18px; width:220px; font-size:14px; letter-spacing:0.3px;">Métrica</th>
                                <th style="text-align:left; padding:16px 18px; font-size:14px; letter-spacing:0.3px;">Detalhamento</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Completude</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que verifica o nível de preenchimento das informações do dataset, medindo a presença de campos nulos, vazios ou incompletos. Quanto maior a nota, menor a ausência de dados relevantes.</td>
                            </tr>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Unicidade</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que verifica se os registros são únicos, sem duplicidades indevidas. Ajuda a identificar repetição de linhas ou valores que podem distorcer análises, indicadores e processos operacionais.</td>
                            </tr>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Consistência</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que avalia se os dados seguem um padrão coerente entre colunas, formatos e regras esperadas. Ela ajuda a identificar conflitos, divergências de estrutura e incoerências lógicas dentro do conjunto analisado.</td>
                            </tr>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Validade</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que mede se os valores observados respeitam o domínio esperado, como formatos válidos, faixas aceitáveis e padrões conhecidos. É importante para identificar campos preenchidos, porém incorretos.</td>
                            </tr>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Integridade</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que analisa a preservação das relações esperadas entre os dados, incluindo vínculos de referência e coerência estrutural. Ajuda a detectar lacunas, quebras de relacionamento e informações desconectadas.</td>
                            </tr>
                            <tr>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; font-weight:700; color:#0F172A; background:#F8FAFC;">Atualidade</td>
                                <td style="padding:16px 18px; border-top:1px solid #E5E7EB; color:#374151;">Métrica que verifica se os dados estão atualizados e aderentes ao período esperado para uso analítico ou operacional. Ela apoia a avaliação de defasagem temporal e risco de decisão com base em informação antiga.</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div class="hint">Leitura recomendada: notas mais baixas indicam maior prioridade de correção naquela dimensão específica.</div>
            </section>


            <div class="footer">Qualidade por dimensão MJV - 2026</div>
        </div>
    </body>
    </html>
    """
    path.write_text(html, encoding="utf-8")
    print(f"[OK] Radar: {path}")


def export_history_html(path, history_df):
    import plotly.graph_objects as go
    from datetime import datetime

    if history_df is None or history_df.empty:
        html = f"""
        <!DOCTYPE html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8"/>
            <style>
                body {{ margin:0; font-family: Arial; background:#F5F7FB; }}
                .page {{ max-width:1200px; margin:0 auto; }}
                .cover {{ background: linear-gradient(135deg,#0B1F3A,#0F5DFF); color:white; padding:30px; }}
                .section {{ margin:20px; background:white; padding:20px; border-radius:12px; }}
                .footer {{ text-align:center; padding:20px; font-size:12px; color:#6B7280; }}
            </style>
        </head>
        <body>
            <div class="page">
                <div class="cover">
                    <h1>Evolução da Qualidade dos Dados</h1>
                    <p>Histórico da qualidade dos dados ao longo das execuções</p>
                </div>
                <div class="section">
                    <p>Sem dados históricos disponíveis.</p>
                </div>
                <div class="footer">Histórico de qualidade MJV - 2026</div>
            </div>
        </body>
        </html>
        """
        path.write_text(html, encoding="utf-8")
        return

    # Garantir ordenação
    history_df = history_df.sort_values("run_id")

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=history_df["run_id"],
            y=history_df["avg_score"] if "avg_score" in history_df.columns else history_df["score"],
            mode="lines+markers",
            name="Score Qualidade",
        )
    )

    fig.update_layout(
        title="Evolução do Score de Qualidade",
        xaxis_title="Run ID",
        yaxis_title="Score",
        template="plotly_white",
        height=500,
    )

    chart_html = fig.to_html(include_plotlyjs="cdn", full_html=False)

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="utf-8"/>
        <style>
            body {{ margin:0; font-family: Arial, Helvetica, sans-serif; background:#F5F7FB; color:#1F2937; }}
            .page {{ max-width: 1360px; margin:0 auto; }}

            .cover {{
                background: linear-gradient(135deg, #0B1F3A 0%, #15396B 45%, #0F5DFF 100%);
                color:white;
                padding:36px;
                border-radius:0 0 28px 28px;
            }}

            .cover h1 {{ margin:0; font-size:32px; }}
            .cover p {{ margin-top:10px; }}

            .meta {{
                margin-top:15px;
                font-size:13px;
                opacity:0.9;
            }}

            .section {{
                margin:24px;
                background:#FFFFFF;
                border-radius:20px;
                padding:24px;
                box-shadow:0 6px 20px rgba(0,0,0,0.05);
            }}

            .footer {{
                padding:28px;
                text-align:center;
                color:#6B7280;
                font-size:12px;
            }}
        </style>
    </head>
    <body>
        <div class="page">

            <div class="cover">
                <h1>Evolução da Qualidade dos Dados</h1>
                <p>Histórico da qualidade dos dados ao longo das execuções do Data Quality MJV</p>

                <div class="meta">
                    Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}
                </div>
            </div>

            <div class="section">
                {chart_html}
            </div>

            <div class="footer">
                Qualidade histórica MJV - 2026
            </div>

        </div>
    </body>
    </html>
    """

    path.write_text(html, encoding="utf-8")
    print(f"[OK] Histórico: {path}")


def export_support_files(outdir, run_id, detail_df, dim_df, history_df, attention_df):
    detail_df.to_csv(outdir / "dq_current_detail.csv", index=False, encoding="utf-8-sig")
    dim_df.to_csv(outdir / "dq_dimension_scores_current.csv", index=False, encoding="utf-8-sig")

    recs = []
    if attention_df.empty:
        recs.append({"run_id": run_id, "prioridade": "Média", "dataset_label": "", "recomendacao": "Nenhuma recomendação disponível para o run atual."})
    else:
        for _, row in attention_df.head(10).iterrows():
            recs.append({
                "run_id": run_id,
                "prioridade": "Alta",
                "dataset_label": safe_str(row.get("dataset_label")),
                "recomendacao": f"Revisar a coluna {safe_str(row.get('column_name'))} do dataset {safe_str(row.get('dataset_label'))}."
            })
    pd.DataFrame(recs).to_csv(outdir / "dq_ai_recommendations_current.csv", index=False, encoding="utf-8-sig")

    export_radar_html(outdir / "dq_radar_chart.html", run_id, dim_df)
    export_history_html(outdir / "dq_history_chart.html", history_df)


# =========================================================
# Excel e HTML
# =========================================================
def export_excel(path, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, sheet_name="Resumo Executivo", index=False)
        summary_df.to_excel(writer, sheet_name="Resumo por Dataset", index=False)
        dim_df.to_excel(writer, sheet_name="Scores por Dimensão", index=False)
        top_df.to_excel(writer, sheet_name="Top 5", index=False)
        bottom_df.to_excel(writer, sheet_name="Bottom 5", index=False)
        attention_df.to_excel(writer, sheet_name="Maior Atenção", index=False)
        history_df.to_excel(writer, sheet_name="Histórico", index=False)
        detail_df.to_excel(writer, sheet_name="Detalhe Técnico", index=False)
    print(f"[OK] Excel: {path}")


def export_html(path, run_id, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df):
    matrix_img = chart_matrix(summary_df)
    dist_img = chart_distribution(summary_df)
    dim_img = chart_dimensions(dim_df)
    top_img = chart_top5(top_df)

    chart_block = ""
    if matrix_img:
        chart_block += f"""
        <section class="section">
            <h2 class="section-title">Matriz Executiva de Qualidade</h2>
            <img class="chart" src="data:image/png;base64,{matrix_img}" alt="Matriz de Qualidade" />
        </section>
        """

    panel_imgs = []
    if dist_img:
        panel_imgs.append(f'<img class="chart" src="data:image/png;base64,{dist_img}" alt="Distribuição dos Scores" />')
    if dim_img:
        panel_imgs.append(f'<img class="chart" src="data:image/png;base64,{dim_img}" alt="Radar de Qualidade por Dimensão" />')

    panel_block = ""
    if panel_imgs:
        panel_block = f"""
        <section class="section">
            <h2 class="section-title">Painel Analítico</h2>
            <div class="chart-grid">
                {''.join(panel_imgs)}
            </div>
        </section>
        """

    rank_block = ""
    if top_img:
        rank_block = f"""
        <section class="section">
            <h2 class="section-title">Ranking de Score - Top 5</h2>
            <div class="chart-grid-single">
                <img class="chart" src="data:image/png;base64,{top_img}" alt="Ranking Top 5" />
            </div>
        </section>
        """

    html = f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Relatório de Validação e Qualidade de Dados</title>
<style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: Arial, Helvetica, sans-serif; background:#F5F7FB; color:#1F2937; }}
    .page {{ max-width: 1320px; margin: 0 auto; }}
    .cover {{ background: linear-gradient(135deg, #0B1F3A 0%, #15396B 45%, #0F5DFF 100%); color:white; border-radius:0 0 28px 28px; padding:36px 42px 46px 42px; box-shadow:0 18px 50px rgba(11,31,58,0.18); }}
    .cover-top {{ display:flex; justify-content:space-between; align-items:center; gap:20px; }}
    .cover-kicker {{ text-transform:uppercase; letter-spacing:2px; font-size:12px; opacity:0.85; font-weight:700; }}
    .cover h1 {{ margin:14px 0 10px 0; font-size:42px; line-height:1.08; }}
    .cover p {{ margin:0; max-width:880px; font-size:17px; line-height:1.6; color:rgba(255,255,255,0.92); }}
    .cover-meta {{ margin-top:22px; display:flex; gap:12px; flex-wrap:wrap; }}
    .pill {{ background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.18); color:white; padding:10px 14px; border-radius:999px; font-size:13px; font-weight:700; }}
    .container {{ padding:28px; }}
    .section {{ margin-top:24px; background:#FFFFFF; border:1px solid #D9E1EC; border-radius:24px; padding:26px; box-shadow:0 8px 24px rgba(15,23,42,0.05); }}
    .section-title {{ margin:0 0 18px 0; color:#0B1F3A; font-size:28px; }}
    .section-subtitle {{ margin:0 0 8px 0; color:#0B1F3A; font-size:20px; }}
    .lead {{ color:#6B7280; line-height:1.7; font-size:16px; }}
    .metrics-grid {{ display:grid; grid-template-columns:repeat(5, minmax(0,1fr)); gap:16px; }}
    .metric-card {{ background:white; border:1px solid #D9E1EC; border-radius:20px; padding:18px; min-height:132px; }}
    .metric-title {{ color:#6B7280; font-size:13px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; }}
    .metric-value {{ color:#0B1F3A; font-size:32px; font-weight:800; margin-top:8px; }}
    .metric-subtitle {{ color:#6B7280; font-size:13px; margin-top:8px; line-height:1.5; }}
    .split {{ display:grid; grid-template-columns:1.15fr .85fr; gap:18px; align-items:start; }}
    .panel {{ background:white; border:1px solid #D9E1EC; border-radius:20px; padding:18px; }}
    .chart {{ width:100%; border-radius:16px; border:1px solid #D9E1EC; background:white; }}
    .chart-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    .chart-grid-single {{ display:grid; grid-template-columns:1fr; gap:18px; }}
    .callout {{ background:linear-gradient(135deg, #EAF2FF 0%, white 100%); border:1px solid #CFE0FF; border-radius:20px; padding:20px; }}
    ul.exec {{ margin:12px 0 0 0; padding-left:22px; }}
    ul.exec li {{ margin-bottom:10px; line-height:1.6; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th {{ text-align:left; background:#0B1F3A; color:white; padding:12px 14px; }}
    td {{ padding:12px 14px; border-bottom:1px solid #D9E1EC; vertical-align:top; }}
    tr:nth-child(even) td {{ background:#FBFCFE; }}
    .footer {{ padding:28px; text-align:center; color:#6B7280; font-size:12px; }}
    @media (max-width: 1100px) {{ .metrics-grid {{ grid-template-columns:repeat(2, minmax(0,1fr)); }} .split, .chart-grid {{ grid-template-columns:1fr; }} }}
    @media (max-width: 720px) {{ .metrics-grid {{ grid-template-columns:1fr; }} .container {{ padding:16px; }} .cover {{ padding:26px 22px 34px 22px; border-radius:0 0 20px 20px; }} .cover h1 {{ font-size:30px; }} }}
</style>
</head>
<body>
    <div class="page">
        <section class="cover">
            <div class="cover-top">
                <div>
                    <div class="cover-kicker">MJV Data Quality</div>
                    <h1>Relatório de Validação e Qualidade de Dados</h1>
                    <p>Visão gerencial consolidada da saúde das fontes avaliadas, com leitura executiva, matriz de qualidade, ranking de desempenho e direcionadores de priorização para tomada de decisão.</p>
                    <div class="cover-meta">
                        <span class="pill">Run ID: {html_escape(run_id)}</span>
                        <span class="pill">Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M")}</span>
                        <span class="pill">Classificação Geral: {html_escape(kpis['classificacao'])}</span>
                    </div>
                </div>
                <div><div style="font-size:28px;font-weight:800;letter-spacing:1px;">MJV</div></div>
            </div>
        </section>

        <div class="container">
            <section class="section">
                <h2 class="section-title">Sumário Executivo</h2>
                <div class="metrics-grid">
                    <div class="metric-card" style="border-top:5px solid #0F5DFF;">
                        <div class="metric-title">Fontes Avaliadas</div>
                        <div class="metric-value">{kpis['datasets']}</div>
                        <div class="metric-subtitle">Escopo total da avaliação executada.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid {status_color(kpis['classificacao'])};">
                        <div class="metric-title">Score Executivo</div>
                        <div class="metric-value">{kpis['score_medio']:.2f}</div>
                        <div class="metric-subtitle">Média consolidada do ambiente avaliado.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid {status_color(kpis['classificacao'])};">
                        <div class="metric-title">Classificação Geral</div>
                        <div class="metric-value">{html_escape(kpis['classificacao'])}</div>
                        <div class="metric-subtitle">Leitura executiva da maturidade atual.</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid #2EAD67;">
                        <div class="metric-title">Melhor Fonte</div>
                        <div class="metric-value">{kpis['melhor_score']:.2f}</div>
                        <div class="metric-subtitle">{html_escape(kpis['melhor_dataset'])}</div>
                    </div>
                    <div class="metric-card" style="border-top:5px solid #D64545;">
                        <div class="metric-title">Pior Fonte</div>
                        <div class="metric-value">{kpis['pior_score']:.2f}</div>
                        <div class="metric-subtitle">{html_escape(kpis['pior_dataset'])}</div>
                    </div>
                </div>
            </section>

            <section class="section">
                <div class="split">
                    <div class="panel">
                        <h3 class="section-subtitle">Resumo de Avaliação</h3>
                        <p class="lead">{html_escape(summary_message(kpis))}</p>
                        <div style="margin-top:14px;"><span style="display:inline-block;padding:8px 14px;border-radius:999px;background:{status_color(kpis['classificacao'])};color:#FFFFFF;font-weight:700;font-size:13px;">{html_escape(kpis['classificacao'])}</span></div>
                    </div>
                    <div class="callout">
                        <h3 class="section-subtitle" style="margin-top:0;">Resumo Fonte de Dados</h3>
                        {executive_list_html(kpis)}
                    </div>
                </div>
            </section>

            {chart_block}
            {panel_block}
            {rank_block}

            <section class="section">
                <h2 class="section-title">Resumo por Dataset</h2>
                {df_to_html(summary_df)}
            </section>

            <section class="section">
                <h2 class="section-title">Scores por Dimensão</h2>
                {df_to_html(dim_df)}
            </section>

            <section class="section">
                <h2 class="section-title">Colunas com Maior Atenção</h2>
                {df_to_html(attention_df)}
            </section>

            <section class="section">
                <h2 class="section-title">Detalhe Técnico</h2>
                {df_to_html(detail_df)}
            </section>
        </div>

        <div class="footer">MJV Data Quality • Relatório Executivo</div>
    </div>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")
    print(f"[OK] HTML : {path}")


# =========================================================
# Processo principal
# =========================================================
def run(duckdb_path, schema, outdir, run_id=None, logo=None):
    outdir_path = ensure_dir(outdir)
    con = duckdb.connect(duckdb_path)

    effective_run_id = run_id or get_latest_run_id(con, schema)
    if not effective_run_id:
        raise RuntimeError("Nenhum run_id encontrado.")

    summary_df = load_summary(con, schema, effective_run_id)
    detail_df = load_detail(con, schema, effective_run_id)

    if summary_df.empty and not detail_df.empty:
        summary_df = derive_summary_from_detail(detail_df, effective_run_id)

    dim_df = build_dimensions(summary_df, effective_run_id)
    history_df = load_history(con, schema)
    kpis = build_kpis(summary_df)
    top_df, bottom_df = build_top_bottom(summary_df)
    attention_df = build_attention(detail_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Arquivos premium principais
    premium_xlsx = outdir_path / f"dq_report_premium_mjv_v2_{timestamp}.xlsx"
    premium_html = outdir_path / f"dq_report_premium_mjv_v2_{timestamp}.html"
    export_excel(premium_xlsx, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df)
    export_html(premium_html, effective_run_id, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df)

    # Arquivos executivos compatíveis com pipeline atual
    exec_xlsx = outdir_path / f"dq_executive_report_v2_{timestamp}.xlsx"
    exec_html = outdir_path / f"dq_executive_report_v2_{timestamp}.html"
    export_excel(exec_xlsx, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df)
    export_html(exec_html, effective_run_id, kpis, summary_df, dim_df, detail_df, top_df, bottom_df, attention_df, history_df)

    export_support_files(outdir_path, effective_run_id, detail_df, dim_df, history_df, attention_df)

    print(f"[OK] Excel: {exec_xlsx}")
    print(f"[OK] HTML : {exec_html}")
    print(f"[OK] Recomendações IA: {outdir_path / 'dq_ai_recommendations_current.csv'}")
    print(f"[OK] Detalhe atual: {outdir_path / 'dq_current_detail.csv'}")
    print(f"[OK] Dimensões atuais: {outdir_path / 'dq_dimension_scores_current.csv'}")

    con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--schema", default="stg")
    parser.add_argument("--outdir", default="./output")
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--logo", default=None)
    args = parser.parse_args()

    run(
        duckdb_path=args.duckdb,
        schema=args.schema,
        outdir=args.outdir,
        run_id=args.run_id,
        logo=args.logo,
    )


if __name__ == "__main__":
    main()
