import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st


# =========================================================
# MJV DATA QUALITY APP - MVP
# =========================================================
# Objetivo:
# - Rodar o pipeline atual de Data Quality por interface web
# - Exibir cards executivos
# - Listar relatórios gerados
# - Visualizar CSVs de resultado
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"
CONFIG_DIR = PROJECT_ROOT / "config"
PIPELINE_PATH = PROJECT_ROOT / "src" / "pipeline" / "run_data_quality_pipeline.py"

DEFAULT_SOURCES = CONFIG_DIR / "sources.runtime.yml"
DEFAULT_RULES = CONFIG_DIR / "12_dq_rules.yml"
DEFAULT_DUCKDB = PROJECT_ROOT / "dq_lab.duckdb"
DEFAULT_STG = "stg"


# -----------------------------
# Configuração visual
# -----------------------------
st.set_page_config(
    page_title="MJV Data Quality App",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


CUSTOM_CSS = """
<style>
    .main {
        background-color: #f7f8fb;
    }

    .mjv-header {
        padding: 24px 28px;
        border-radius: 22px;
        background: linear-gradient(135deg, #111827 0%, #24123f 55%, #6d28d9 100%);
        color: white;
        margin-bottom: 24px;
        box-shadow: 0 10px 30px rgba(17, 24, 39, 0.18);
    }

    .mjv-title {
        font-size: 34px;
        font-weight: 800;
        margin-bottom: 6px;
    }

    .mjv-subtitle {
        font-size: 16px;
        opacity: 0.90;
    }

    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 18px;
        box-shadow: 0 8px 22px rgba(15, 23, 42, 0.08);
        border: 1px solid #eef0f4;
    }

    .section-card {
        background-color: white;
        padding: 22px;
        border-radius: 18px;
        box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
        border: 1px solid #eef0f4;
        margin-bottom: 18px;
    }

    .small-muted {
        color: #64748b;
        font-size: 13px;
    }

    .success-box {
        padding: 14px 16px;
        border-radius: 14px;
        background-color: #ecfdf5;
        border: 1px solid #bbf7d0;
        color: #166534;
    }

    .warn-box {
        padding: 14px 16px;
        border-radius: 14px;
        background-color: #fffbeb;
        border: 1px solid #fde68a;
        color: #92400e;
    }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# -----------------------------
# Funções auxiliares
# -----------------------------
def find_latest_file(pattern: str) -> Path | None:
    files = sorted(OUTPUT_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def list_output_files() -> list[Path]:
    if not OUTPUT_DIR.exists():
        return []
    return sorted(
        [p for p in OUTPUT_DIR.iterdir() if p.is_file()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def load_csv_if_exists(patterns: list[str]) -> pd.DataFrame | None:
    for pattern in patterns:
        file_path = find_latest_file(pattern)
        if file_path and file_path.exists():
            try:
                return pd.read_csv(file_path)
            except Exception:
                try:
                    return pd.read_csv(file_path, sep=";")
                except Exception:
                    return None
    return None


def run_pipeline(sources: Path, rules: Path, duckdb: Path, stg: str, limit: int | None) -> tuple[int, str, str]:
    if not PIPELINE_PATH.exists():
        return 1, "", f"Pipeline não encontrado em: {PIPELINE_PATH}"

    cmd = [
        sys.executable,
        str(PIPELINE_PATH),
        "--sources",
        str(sources),
        "--duckdb",
        str(duckdb),
        "--stg",
        stg,
        "--rules",
        str(rules),
        "--outdir",
        str(OUTPUT_DIR),
    ]

    if limit and limit > 0:
        cmd.extend(["--limit", str(limit)])

    process = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        shell=False,
    )

    return process.returncode, process.stdout, process.stderr


def classify_score(score: float | None) -> str:
    if score is None:
        return "Sem classificação"
    if score >= 8.5:
        return "Excelente"
    if score >= 7.0:
        return "Aceitável"
    if score >= 5.0:
        return "Atenção"
    return "Crítico"


def get_score_from_df(df: pd.DataFrame | None) -> float | None:
    if df is None or df.empty:
        return None

    possible_cols = ["score_final", "score", "score_base", "dq_score", "overall_score"]
    for col in possible_cols:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce").dropna()
            if not values.empty:
                return round(float(values.mean()), 2)
    return None


# -----------------------------
# Sidebar
# -----------------------------
with st.sidebar:
    st.markdown("## MJV Data Quality")
    st.caption("Scanner executivo de qualidade de dados")

    page = st.radio(
        "Navegação",
        [
            "Dashboard Executivo",
            "Executar Validação",
            "Relatórios Gerados",
            "Configurações",
        ],
    )

    st.divider()
    st.markdown("### Ambiente")
    st.caption(f"Projeto: `{PROJECT_ROOT.name}`")
    st.caption(f"Output: `{OUTPUT_DIR}`")


# -----------------------------
# Header
# -----------------------------
st.markdown(
    """
    <div class="mjv-header">
        <div class="mjv-title">MJV Data Quality App</div>
        <div class="mjv-subtitle">
            Validação, score e governança de qualidade de dados em uma experiência executiva.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# -----------------------------
# Página: Dashboard Executivo
# -----------------------------
if page == "Dashboard Executivo":
    summary_df = load_csv_if_exists([
        "dq_summary_*.csv",
        "dq_current_detail.csv",
        "dq_dimension_scores_current.csv",
        "*summary*.csv",
    ])

    detail_df = load_csv_if_exists([
        "dq_current_detail.csv",
        "dq_detail_*.csv",
        "*detail*.csv",
    ])

    score = get_score_from_df(summary_df)
    classification = classify_score(score)
    output_files = list_output_files()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Score médio", "-" if score is None else f"{score:.2f}")
    with col2:
        st.metric("Classificação", classification)
    with col3:
        st.metric("Arquivos gerados", len(output_files))
    with col4:
        last_run = "Sem execução"
        if output_files:
            last_run = datetime.fromtimestamp(output_files[0].stat().st_mtime).strftime("%d/%m/%Y %H:%M")
        st.metric("Última execução", last_run)

    st.markdown("### Visão geral")

    if summary_df is None or summary_df.empty:
        st.markdown(
            """
            <div class="warn-box">
                Nenhum resultado encontrado ainda. Acesse <b>Executar Validação</b> para rodar o scanner.
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.dataframe(summary_df, use_container_width=True, height=360)

        numeric_cols = summary_df.select_dtypes(include="number").columns.tolist()
        score_cols = [c for c in numeric_cols if "score" in c.lower()]

        if score_cols:
            selected_score_col = score_cols[0]
            label_col = None
            for possible in ["dataset", "table_name", "table", "source", "arquivo"]:
                if possible in summary_df.columns:
                    label_col = possible
                    break

            if label_col:
                chart_df = summary_df[[label_col, selected_score_col]].dropna().head(20)
                st.markdown("### Ranking de qualidade")
                st.bar_chart(chart_df.set_index(label_col))

    if detail_df is not None and not detail_df.empty:
        with st.expander("Ver detalhe técnico"):
            st.dataframe(detail_df, use_container_width=True, height=360)


# -----------------------------
# Página: Executar Validação
# -----------------------------
elif page == "Executar Validação":
    st.markdown("### Executar nova validação")

    with st.container():
        st.markdown("Configure os parâmetros de execução do pipeline.")

        col1, col2 = st.columns(2)
        with col1:
            sources_path = st.text_input("Arquivo de fontes", value=str(DEFAULT_SOURCES))
            rules_path = st.text_input("Arquivo de regras", value=str(DEFAULT_RULES))
        with col2:
            duckdb_path = st.text_input("Banco DuckDB", value=str(DEFAULT_DUCKDB))
            stg_schema = st.text_input("Schema STG", value=DEFAULT_STG)

        limit_rows = st.number_input(
            "Limite de linhas por fonte (0 para sem limite)",
            min_value=0,
            value=80000,
            step=10000,
        )

        run_button = st.button("Executar Data Quality", type="primary", use_container_width=True)

    if run_button:
        with st.spinner("Executando validação de qualidade..."):
            return_code, stdout, stderr = run_pipeline(
                sources=Path(sources_path),
                rules=Path(rules_path),
                duckdb=Path(duckdb_path),
                stg=stg_schema,
                limit=int(limit_rows) if limit_rows > 0 else None,
            )

        if return_code == 0:
            st.markdown(
                """
                <div class="success-box">
                    Pipeline executado com sucesso. Acesse o Dashboard Executivo ou Relatórios Gerados.
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.error("Erro ao executar o pipeline.")

        with st.expander("Log de execução"):
            st.code(stdout or "Sem saída padrão.", language="text")
            if stderr:
                st.code(stderr, language="text")


# -----------------------------
# Página: Relatórios Gerados
# -----------------------------
elif page == "Relatórios Gerados":
    st.markdown("### Relatórios e arquivos de saída")

    files = list_output_files()

    if not files:
        st.info("Nenhum arquivo encontrado na pasta output.")
    else:
        for file in files:
            modified_at = datetime.fromtimestamp(file.stat().st_mtime).strftime("%d/%m/%Y %H:%M")
            size_kb = file.stat().st_size / 1024

            with st.container():
                col1, col2, col3 = st.columns([5, 2, 2])
                with col1:
                    st.markdown(f"**{file.name}**")
                    st.caption(f"Atualizado em {modified_at}")
                with col2:
                    st.caption(f"{size_kb:.1f} KB")
                with col3:
                    with open(file, "rb") as f:
                        st.download_button(
                            label="Baixar",
                            data=f,
                            file_name=file.name,
                            mime="application/octet-stream",
                            key=f"download_{file.name}",
                        )
                st.divider()

        html_files = [f for f in files if f.suffix.lower() == ".html"]
        if html_files:
            st.markdown("### Visualização HTML")
            selected_html = st.selectbox("Selecione um relatório HTML", html_files, format_func=lambda p: p.name)
            html_content = selected_html.read_text(encoding="utf-8", errors="ignore")
            st.components.v1.html(html_content, height=720, scrolling=True)


# -----------------------------
# Página: Configurações
# -----------------------------
elif page == "Configurações":
    st.markdown("### Checklist do projeto")

    checks = {
        "Pipeline principal": PIPELINE_PATH.exists(),
        "Pasta config": CONFIG_DIR.exists(),
        "Arquivo sources.runtime.yml": DEFAULT_SOURCES.exists(),
        "Arquivo 12_dq_rules.yml": DEFAULT_RULES.exists(),
        "Pasta output": OUTPUT_DIR.exists(),
        "Banco DuckDB": DEFAULT_DUCKDB.exists(),
    }

    for item, ok in checks.items():
        if ok:
            st.success(f"OK - {item}")
        else:
            st.warning(f"Pendente - {item}")

    st.markdown("### Como executar")
    st.code(
        """
# Instalar dependências
pip install streamlit pandas

# Rodar o app
streamlit run app_streamlit.py
        """.strip(),
        language="powershell",
    )

    st.markdown("### Próximas evoluções")
    st.write(
        """
        - Tela para cadastrar fontes de dados sem editar YAML.
        - Histórico de execuções com comparação entre runs.
        - Login e perfis de acesso.
        - Alertas automáticos por e-mail ou Teams.
        - Deploy em Docker, Azure App Service ou Databricks Apps.
        """
    )
