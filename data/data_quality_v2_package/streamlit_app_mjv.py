
# -*- coding: utf-8 -*-
from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

MJV_NAVY = "#0B1F3A"
MJV_BLUE = "#0F5DFF"
MJV_TEAL = "#00B8A9"
MJV_BG = "#F5F7FB"

st.set_page_config(
    page_title="MJV Data Quality Executive Dashboard",
    page_icon="📊",
    layout="wide"
)

st.markdown("""
<style>
.main {background-color: #F5F7FB;}
.block-container {padding-top: 1.2rem; padding-bottom: 1rem;}
.card-kpi {
    background: white; border-radius: 18px; padding: 16px 18px;
    box-shadow: 0 4px 18px rgba(11,31,58,.08); border: 1px solid #D9E1EC;
}
h1, h2, h3 {color: #0B1F3A;}
</style>
""", unsafe_allow_html=True)

st.title("MJV • Data Quality Executive Dashboard")
st.caption("Padrão diretoria | Histórico | Radar | Priorização automática")

with st.sidebar:
    st.header("Configuração")
    duckdb_path = st.text_input("DuckDB", r"C:\Users\Daniel\Downloads\Data_Quality\dq_lab.duckdb")
    history_schema = st.text_input("Schema histórico", "dq_history")
    st.markdown("---")
    st.markdown("**Modo executivo**")
    show_technical = st.toggle("Exibir visão técnica", value=False)

@st.cache_data(show_spinner=False)
def load_data(duckdb_path: str, history_schema: str):
    con = duckdb.connect(duckdb_path, read_only=False)
    run_df = con.execute(f"SELECT * FROM {history_schema}.dq_run ORDER BY run_timestamp DESC").df()
    ds_df = con.execute(f"SELECT * FROM {history_schema}.dq_dataset_score_history ORDER BY run_timestamp DESC, dataset_name").df()
    dim_df = con.execute(f"SELECT * FROM {history_schema}.dq_dimension_score_history ORDER BY run_timestamp DESC, dataset_name, dimension_name").df()
    rec_df = con.execute(f"SELECT * FROM {history_schema}.dq_ai_recommendations ORDER BY run_timestamp DESC, priority_score DESC").df()
    con.close()
    return run_df, ds_df, dim_df, rec_df

try:
    run_df, ds_df, dim_df, rec_df = load_data(duckdb_path, history_schema)
except Exception as e:
    st.error(f"Não foi possível abrir a base histórica: {e}")
    st.stop()

if run_df.empty or ds_df.empty:
    st.warning("Nenhum histórico encontrado ainda. Execute primeiro o export_data_quality_report_v2_integrated.py.")
    st.stop()

run_options = run_df["run_id"].astype(str).tolist()
selected_run = st.sidebar.selectbox("Run ID", run_options)
run_row = run_df[run_df["run_id"].astype(str) == str(selected_run)].iloc[0]

run_ds = ds_df[ds_df["run_id"].astype(str) == str(selected_run)].copy()
run_dim = dim_df[dim_df["run_id"].astype(str) == str(selected_run)].copy()
run_rec = rec_df[rec_df["run_id"].astype(str) == str(selected_run)].copy()

domains = ["Todos"] + sorted([d for d in run_ds.get("source_type", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()])
domain_filter = st.sidebar.selectbox("Tipo de Fonte", domains)
if domain_filter != "Todos":
    run_ds = run_ds[run_ds["source_type"].astype(str) == domain_filter]
    selected_names = run_ds["dataset_name"].tolist()
    run_dim = run_dim[run_dim["dataset_name"].isin(selected_names)]
    run_rec = run_rec[run_rec["dataset_name"].isin(selected_names)]

dataset_opts = ["Todos"] + sorted(run_ds["dataset_name"].astype(str).unique().tolist())
dataset_filter = st.sidebar.selectbox("Dataset", dataset_opts)
if dataset_filter != "Todos":
    run_ds = run_ds[run_ds["dataset_name"].astype(str) == dataset_filter]
    run_dim = run_dim[run_dim["dataset_name"].astype(str) == dataset_filter]
    run_rec = run_rec[run_rec["dataset_name"].astype(str) == dataset_filter]

avg_score = round(float(run_ds["score_overall"].mean()), 2)
critical_count = int((run_ds["classification_overall"] == "Crítico").sum())
attention_count = int((run_ds["classification_overall"] == "Atenção").sum())
best_ds = run_ds.sort_values("score_overall", ascending=False).head(1)
worst_ds = run_ds.sort_values("score_overall", ascending=True).head(1)

c1, c2, c3, c4, c5 = st.columns(5)
for col, title, value in [
    (c1, "Score Geral", avg_score),
    (c2, "Datasets", int(len(run_ds))),
    (c3, "Críticos", critical_count),
    (c4, "Em Atenção", attention_count),
    (c5, "Run", str(selected_run)),
]:
    with col:
        st.markdown(f"<div class='card-kpi'><div style='font-size:12px;color:#6B7280'>{title}</div><div style='font-size:28px;font-weight:700;color:#0B1F3A'>{value}</div></div>", unsafe_allow_html=True)

st.markdown("### Resumo Executivo")
left, right = st.columns([1.15, 1])
with left:
    ranking = run_ds.sort_values("score_overall", ascending=False)[["dataset_name", "score_overall", "classification_overall", "priority_index"]]
    fig_rank = px.bar(
        ranking.head(10),
        x="score_overall",
        y="dataset_name",
        orientation="h",
        text="score_overall",
        title="Top Fontes por Score"
    )
    fig_rank.update_layout(template="plotly_white", height=420, yaxis_title="")
    st.plotly_chart(fig_rank, use_container_width=True)

with right:
    dist = run_ds["classification_overall"].value_counts().reset_index()
    dist.columns = ["Classificação", "Quantidade"]
    fig_pie = px.pie(dist, names="Classificação", values="Quantidade", title="Distribuição por Classificação")
    fig_pie.update_layout(template="plotly_white", height=420)
    st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("### Score por Dimensão")
dim_pivot = run_dim.pivot_table(index="dataset_name", columns="dimension_name", values="dimension_score", aggfunc="mean").reset_index()
if not dim_pivot.empty:
    fig_radar = go.Figure()
    dimensions = [c for c in dim_pivot.columns if c != "dataset_name"]
    for _, row in dim_pivot.iterrows():
        fig_radar.add_trace(go.Scatterpolar(
            r=[row[d] for d in dimensions],
            theta=dimensions,
            fill="toself",
            name=row["dataset_name"]
        ))
    fig_radar.update_layout(
        template="plotly_white",
        title="Radar de Qualidade por Dimensão",
        polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
        height=560
    )
    st.plotly_chart(fig_radar, use_container_width=True)

st.markdown("### Histórico da Qualidade")
hist = ds_df.groupby("run_timestamp", as_index=False)["score_overall"].mean().sort_values("run_timestamp")
fig_hist = px.line(hist, x="run_timestamp", y="score_overall", markers=True, title="Histórico do Score Médio")
fig_hist.update_layout(template="plotly_white", height=420, yaxis_range=[0,10], xaxis_title="")
st.plotly_chart(fig_hist, use_container_width=True)

st.markdown("### Recomendações IA Priorizadas")
if run_rec.empty:
    st.info("Não há recomendações para o filtro atual.")
else:
    view_cols = [
        "dataset_name", "column_name", "priority_band", "priority_score",
        "recommendation_text", "owner_suggestion", "estimated_effort", "estimated_impact"
    ]
    st.dataframe(run_rec[view_cols], use_container_width=True, hide_index=True)

if show_technical:
    st.markdown("### Visão Técnica")
    t1, t2 = st.tabs(["Datasets", "Dimensões"])
    with t1:
        st.dataframe(run_ds.sort_values("score_overall", ascending=True), use_container_width=True, hide_index=True)
    with t2:
        st.dataframe(run_dim.sort_values(["dataset_name", "dimension_score"]), use_container_width=True, hide_index=True)
