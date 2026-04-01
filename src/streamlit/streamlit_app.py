# -*- coding: utf-8 -*-
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import streamlit as st


APP_BG = "#F4F7FB"
APP_NAVY = "#0B1F3A"
APP_BLUE = "#0F5DFF"
APP_BORDER = "#D9E1EC"

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SCAN_DIR = PROJECT_ROOT / "data"
DEFAULT_DUCKDB = PROJECT_ROOT / "dq_lab.duckdb"
DEFAULT_OUTPUT = PROJECT_ROOT / "output"
DEFAULT_LOGO = PROJECT_ROOT / "docs" / "assets" / "logo_mjv.png"


st.set_page_config(
    page_title="Scaning Data Quality MJV",
    page_icon="SQ",
    layout="wide",
)

st.markdown(
    f"""
    <style>
    .stApp {{
        background:
            radial-gradient(circle at top left, rgba(15,93,255,.10), transparent 30%),
            linear-gradient(180deg, #FFFFFF 0%, {APP_BG} 100%);
    }}
    .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1180px;
    }}
    .hero {{
        background: linear-gradient(135deg, {APP_NAVY} 0%, #14345F 45%, {APP_BLUE} 100%);
        color: white;
        border-radius: 24px;
        padding: 28px 32px;
        box-shadow: 0 18px 42px rgba(11, 31, 58, 0.18);
        margin-bottom: 20px;
    }}
    .card {{
        background: rgba(255,255,255,.92);
        border: 1px solid {APP_BORDER};
        border-radius: 20px;
        padding: 18px;
        box-shadow: 0 10px 28px rgba(15, 23, 42, 0.05);
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
        <div style="font-size:12px; letter-spacing:1.6px; text-transform:uppercase; opacity:.9;">MJV Data Quality</div>
        <h1 style="margin:10px 0 8px 0;">Aplicativo de Varredura e Relatorio</h1>
        <p style="margin:0; max-width:780px; line-height:1.6; opacity:.92;">
            Escolha a pasta de arquivos, execute o pipeline e gere os relatorios HTML e Excel sem depender
            de caminhos fixos no seu computador.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns([1.1, 0.9], gap="large")

with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Configuracao da execucao")
    scan_dir = st.text_input("Pasta para varredura", str(DEFAULT_SCAN_DIR))
    duckdb_path = st.text_input("Arquivo DuckDB", str(DEFAULT_DUCKDB))
    output_dir = st.text_input("Pasta de saida", str(DEFAULT_OUTPUT))
    schema = st.text_input("Schema", "stg")
    logo_path = st.text_input("Logo do relatorio", str(DEFAULT_LOGO))
    st.caption("Dica: informe uma pasta com arquivos .csv, .xlsx, .xls ou .parquet.")
    run_clicked = st.button("Executar scan e gerar relatorio", type="primary", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Como funciona")
    st.write("1. O app monta a configuracao com a pasta escolhida.")
    st.write("2. Executa o pipeline principal do projeto.")
    st.write("3. Gera relatorios em Excel e HTML na pasta de saida.")
    st.write("4. Exibe os arquivos gerados para abrir rapidamente.")
    st.markdown("</div>", unsafe_allow_html=True)


def run_pipeline(
    scan_dir: str,
    duckdb_path: str,
    output_dir: str,
    schema: str,
    logo_path: str,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "app.py"),
        "--config",
        str(PROJECT_ROOT / "config" / "config.yml"),
        "--input-dir",
        scan_dir,
        "--duckdb",
        duckdb_path,
        "--stg",
        schema,
        "--outdir",
        output_dir,
        "--logo",
        logo_path,
    ]
    return subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)


if run_clicked:
    scan_path = Path(scan_dir).expanduser()
    if not scan_path.exists() or not scan_path.is_dir():
        st.error("A pasta de varredura informada nao existe ou nao e uma pasta valida.")
    else:
        with st.spinner("Executando pipeline de Data Quality..."):
            result = run_pipeline(scan_dir, duckdb_path, output_dir, schema, logo_path)

        if result.returncode != 0:
            st.error("O pipeline falhou. Veja os detalhes abaixo.")
            st.code((result.stdout or "") + "\n" + (result.stderr or ""), language="text")
        else:
            st.success("Processo concluido com sucesso.")
            st.code(result.stdout or "", language="text")

            outdir = Path(output_dir).expanduser()
            html_files = sorted(outdir.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
            xlsx_files = sorted(outdir.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)

            if html_files or xlsx_files:
                st.subheader("Arquivos gerados")
                for file in html_files[:3] + xlsx_files[:3]:
                    st.write(str(file.resolve()))
