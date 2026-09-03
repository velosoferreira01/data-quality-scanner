# -*- coding: utf-8 -*-
"""
MJV Data Quality Platform - Interface Web (MVP)

Coloque este arquivo na raiz do projeto, ao lado do app.py:

data-quality-scanner/
├── app.py
├── web_app.py
├── config/
├── data/
├── output/
└── src/

Execução:
    python -m streamlit run web_app.py
"""

from __future__ import annotations

import base64
import io
import json
import os
import re
import subprocess
import sys
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yaml

from PIL import Image


PROJECT_ROOT = Path(__file__).resolve().parent
APP_SCRIPT = PROJECT_ROOT / "app.py"
BASE_CONFIG = PROJECT_ROOT / "config" / "config.multibanco.yml"

DATA_JOBS_DIR = PROJECT_ROOT / "data" / "jobs"
OUTPUT_JOBS_DIR = PROJECT_ROOT / "output" / "jobs"
LOGO_MJV_PATH = PROJECT_ROOT / "docs" / "assets" / "Logo_mjv.png"

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".parquet"}
HTML_PRIORITY = (
    "dq_report_premium_mjv_v2",
    "dq_executive_report_v2",
    "dq_report_premium_mjv",
    "dq_radar_chart",
    "dq_history_chart",
)


class JobExecutionError(RuntimeError):
    """Erro controlado durante a execução de uma análise."""


def configure_page() -> None:
    page_icon = Image.open(LOGO_MJV_PATH) if LOGO_MJV_PATH.exists() else "📊"

    st.set_page_config(
        page_title="MJV - Data Quality Platform",
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.4rem;
                padding-bottom: 3rem;
            }

            .mjv-header {
                border: 1px solid rgba(128, 128, 128, 0.25);
                border-radius: 18px;
                padding: 1.5rem 1.7rem;
                margin-bottom: 1rem;
                background:
                    linear-gradient(
                        135deg,
                        rgba(128, 90, 213, 0.12),
                        rgba(0, 180, 216, 0.08)
                    );
            }

            .mjv-header h1 {
                margin: 0;
                font-size: 2.1rem;
                line-height: 1.2;
            }

            .mjv-header p {
                margin: 0.55rem 0 0 0;
                opacity: 0.82;
                font-size: 1rem;
            }

            .status-card {
                border: 1px solid rgba(128, 128, 128, 0.25);
                border-radius: 14px;
                padding: 1rem 1.15rem;
                margin: 0.6rem 0 1rem 0;
            }

            div[data-testid="stDownloadButton"] button {
                width: 100%;
            }

            div[data-testid="stButton"] button {
                width: 100%;
            }

            .mjv-title-area {
                padding: 0.5rem 0;
            }

            .mjv-title-area h1 {
                margin: 0;
                font-size: 2.2rem;
                font-weight: 750;
                line-height: 1.15;
            }

            .mjv-title-area p {
                margin: 0.55rem 0 0 0;
                color: #94a3b8;
                font-size: 1rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def ensure_project_structure() -> None:
    DATA_JOBS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JOBS_DIR.mkdir(parents=True, exist_ok=True)


def validate_project() -> list[str]:
    errors: list[str] = []

    if not APP_SCRIPT.exists():
        errors.append(f"Arquivo não encontrado: {APP_SCRIPT}")

    if not BASE_CONFIG.exists():
        errors.append(f"Arquivo não encontrado: {BASE_CONFIG}")

    return errors


def create_run_id() -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"{timestamp}_{suffix}"


def sanitize_filename(filename: str) -> str:
    original = Path(filename).name
    stem = Path(original).stem
    suffix = Path(original).suffix.lower()

    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._")
    if not safe_stem:
        safe_stem = "arquivo"

    return f"{safe_stem}{suffix}"


def unique_destination(folder: Path, filename: str) -> Path:
    destination = folder / filename
    if not destination.exists():
        return destination

    stem = destination.stem
    suffix = destination.suffix
    counter = 2

    while True:
        candidate = folder / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def save_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            payload,
            file,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )


def metadata_path(run_id: str) -> Path:
    return DATA_JOBS_DIR / run_id / "metadata.json"


def save_metadata(run_id: str, payload: dict[str, Any]) -> None:
    path = metadata_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def load_metadata(run_id: str) -> dict[str, Any]:
    path = metadata_path(run_id)
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return {}


def prepare_job(uploaded_files: list[Any]) -> dict[str, Any]:
    run_id = create_run_id()

    job_dir = DATA_JOBS_DIR / run_id
    input_dir = job_dir / "input"
    output_dir = OUTPUT_JOBS_DIR / run_id
    duckdb_path = job_dir / "dq_lab.duckdb"
    job_config_path = job_dir / "config.runtime.yml"
    sources_runtime_path = job_dir / "sources.runtime.yml"

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    saved_files: list[str] = []

    for uploaded_file in uploaded_files:
        safe_name = sanitize_filename(uploaded_file.name)
        destination = unique_destination(input_dir, safe_name)

        with destination.open("wb") as file:
            file.write(uploaded_file.getbuffer())

        saved_files.append(destination.name)

    config = load_yaml(BASE_CONFIG)

    scan_config = config.setdefault("scan", {})
    scan_config["input_dir"] = str(input_dir.resolve())
    scan_config["file_patterns"] = [
        "*.csv",
        "*.xlsx",
        "*.xls",
        "*.parquet",
    ]

    duckdb_config = config.setdefault("duckdb", {})
    duckdb_config["path"] = str(duckdb_path.resolve())
    duckdb_config.setdefault("schema", "stg")

    output_config = config.setdefault("output", {})
    output_config["dir"] = str(output_dir.resolve())

    save_yaml(job_config_path, config)

    metadata = {
        "run_id": run_id,
        "status": "preparado",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "started_at": None,
        "finished_at": None,
        "duration_seconds": None,
        "input_files": saved_files,
        "input_dir": str(input_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "duckdb_path": str(duckdb_path.resolve()),
        "config_path": str(job_config_path.resolve()),
        "sources_runtime_path": str(sources_runtime_path.resolve()),
        "return_code": None,
    }
    save_metadata(run_id, metadata)

    return metadata


def execute_job(run_id: str) -> dict[str, Any]:
    metadata = load_metadata(run_id)
    if not metadata:
        raise JobExecutionError(f"Execução não encontrada: {run_id}")

    started_at = time.time()
    metadata["status"] = "processando"
    metadata["started_at"] = datetime.now().isoformat(timespec="seconds")
    save_metadata(run_id, metadata)

    command = [
        sys.executable,
        "-B",
        str(APP_SCRIPT),
        "--config",
        metadata["config_path"],
        "--sources-runtime",
        metadata["sources_runtime_path"],
        "--duckdb",
        metadata["duckdb_path"],
        "--outdir",
        metadata["output_dir"],
    ]

    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"

    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
    )

    duration = time.time() - started_at

    metadata["finished_at"] = datetime.now().isoformat(timespec="seconds")
    metadata["duration_seconds"] = round(duration, 2)
    metadata["return_code"] = result.returncode
    metadata["technical_log"] = result.stdout or ""

    if result.returncode != 0:
        metadata["status"] = "erro"
        save_metadata(run_id, metadata)
        raise JobExecutionError(
            "O pipeline terminou com erro. "
            "Consulte os detalhes técnicos exibidos na tela."
        )

    metadata["status"] = "concluído"
    save_metadata(run_id, metadata)
    return metadata


def get_output_files(run_id: str) -> list[Path]:
    output_dir = OUTPUT_JOBS_DIR / run_id
    if not output_dir.exists():
        return []

    return sorted(
        [
            path
            for path in output_dir.rglob("*")
            if path.is_file()
        ],
        key=lambda path: (path.suffix.lower(), path.name.lower()),
    )


def friendly_file_type(path: Path) -> str:
    mapping = {
        ".html": "Relatório HTML",
        ".pdf": "Relatório PDF",
        ".csv": "Arquivo CSV",
        ".xlsx": "Planilha Excel",
        ".xls": "Planilha Excel",
        ".json": "Arquivo JSON",
        ".duckdb": "Banco DuckDB",
    }
    return mapping.get(path.suffix.lower(), "Arquivo")


def mime_type(path: Path) -> str:
    mapping = {
        ".html": "text/html",
        ".pdf": "application/pdf",
        ".csv": "text/csv",
        ".xlsx": (
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        ".xls": "application/vnd.ms-excel",
        ".json": "application/json",
        ".zip": "application/zip",
    }
    return mapping.get(path.suffix.lower(), "application/octet-stream")


def sort_html_files(files: list[Path]) -> list[Path]:
    html_files = [path for path in files if path.suffix.lower() == ".html"]

    def priority(path: Path) -> tuple[int, str]:
        lowered = path.stem.lower()
        for index, prefix in enumerate(HTML_PRIORITY):
            if lowered.startswith(prefix):
                return index, path.name.lower()
        return len(HTML_PRIORITY), path.name.lower()

    return sorted(html_files, key=priority)


def build_zip_bytes(run_id: str, files: list[Path]) -> bytes:
    output_dir = OUTPUT_JOBS_DIR / run_id
    buffer = io.BytesIO()

    with zipfile.ZipFile(
        buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as zip_file:
        for path in files:
            archive_name = path.relative_to(output_dir)
            zip_file.write(path, archive_name)

    buffer.seek(0)
    return buffer.getvalue()


def format_duration(value: Any) -> str:
    try:
        return f"{float(value):.1f} segundos"
    except Exception:
        return "—"


def list_runs() -> list[str]:
    if not DATA_JOBS_DIR.exists():
        return []

    runs = [
        path.name
        for path in DATA_JOBS_DIR.iterdir()
        if path.is_dir()
    ]
    return sorted(runs, reverse=True)


def render_pdf(path: Path) -> None:
    pdf_bytes = path.read_bytes()
    encoded = base64.b64encode(pdf_bytes).decode("ascii")

    components.html(
        f"""
        <iframe
            src="data:application/pdf;base64,{encoded}"
            width="100%"
            height="900"
            style="border: 1px solid #ddd; border-radius: 12px;">
        </iframe>
        """,
        height=920,
        scrolling=True,
    )


def render_html_report(path: Path) -> None:
    html_content = path.read_text(
        encoding="utf-8",
        errors="replace",
    )
    components.html(
        html_content,
        height=900,
        scrolling=True,
    )


def render_csv_preview(path: Path) -> None:
    try:
        dataframe = pd.read_csv(path, nrows=300)
        st.dataframe(
            dataframe,
            use_container_width=True,
            hide_index=True,
        )
    except Exception as exc:
        st.info(f"Pré-visualização indisponível: {exc}")


def render_run_summary(run_id: str) -> None:
    metadata = load_metadata(run_id)
    files = get_output_files(run_id)

    if not metadata:
        st.warning("Os metadados desta execução não foram encontrados.")
        return

    status = metadata.get("status", "desconhecido")

    if status == "concluído":
        st.success("Processamento concluído com sucesso.")
    elif status == "erro":
        st.error("A execução terminou com erro.")
    elif status == "processando":
        st.info("A execução está em processamento.")
    else:
        st.info(f"Status da execução: {status}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Run ID", run_id)
    col2.metric(
        "Arquivos de entrada",
        len(metadata.get("input_files", [])),
    )
    col3.metric("Arquivos gerados", len(files))
    col4.metric(
        "Tempo de execução",
        format_duration(metadata.get("duration_seconds")),
    )

    if not files:
        technical_log = metadata.get("technical_log", "")
        if technical_log:
            with st.expander("Detalhes técnicos"):
                st.code(technical_log, language="text")
        return

    html_files = sort_html_files(files)
    pdf_files = [
        path for path in files if path.suffix.lower() == ".pdf"
    ]
    csv_files = [
        path for path in files if path.suffix.lower() == ".csv"
    ]

    tab_html, tab_pdf, tab_data, tab_downloads, tab_details = st.tabs(
        [
            "Relatórios HTML",
            "Relatórios PDF",
            "Dados e indicadores",
            "Downloads",
            "Detalhes da execução",
        ]
    )

    with tab_html:
        if not html_files:
            st.info("Nenhum relatório HTML foi gerado.")
        else:
            selected_name = st.selectbox(
                "Selecione o relatório: ",
                [path.name for path in html_files],
                key=f"html_select_{run_id}",
            )
            selected_path = next(
                path for path in html_files
                if path.name == selected_name
            )

            left, right = st.columns([3, 1])
            with left:
                st.caption(str(selected_path))
            with right:
                st.download_button(
                    "Baixar HTML",
                    data=selected_path.read_bytes(),
                    file_name=selected_path.name,
                    mime="text/html",
                    key=f"download_html_{run_id}_{selected_path.name}",
                )

            render_html_report(selected_path)

    with tab_pdf:
        if not pdf_files:
            st.info("Nenhum relatório PDF foi gerado.")
        else:
            selected_name = st.selectbox(
                "Selecione o relatório PDF",
                [path.name for path in pdf_files],
                key=f"pdf_select_{run_id}",
            )
            selected_path = next(
                path for path in pdf_files
                if path.name == selected_name
            )

            st.download_button(
                "Baixar PDF selecionado",
                data=selected_path.read_bytes(),
                file_name=selected_path.name,
                mime="application/pdf",
                key=f"download_pdf_{run_id}_{selected_path.name}",
            )

            render_pdf(selected_path)

    with tab_data:
        if not csv_files:
            st.info("Nenhum arquivo CSV foi gerado.")
        else:
            selected_name = st.selectbox(
                "Selecione o arquivo CSV",
                [path.name for path in csv_files],
                key=f"csv_select_{run_id}",
            )
            selected_path = next(
                path for path in csv_files
                if path.name == selected_name
            )

            st.download_button(
                "Baixar CSV selecionado",
                data=selected_path.read_bytes(),
                file_name=selected_path.name,
                mime="text/csv",
                key=f"download_csv_{run_id}_{selected_path.name}",
            )

            render_csv_preview(selected_path)

    with tab_downloads:
        st.download_button(
            "Baixar todos os resultados em ZIP",
            data=build_zip_bytes(run_id, files),
            file_name=f"data_quality_{run_id}.zip",
            mime="application/zip",
            key=f"download_zip_{run_id}",
        )

        st.markdown("#### Arquivos disponíveis")

        for index, path in enumerate(files):
            col_name, col_type, col_download = st.columns([5, 2, 2])

            with col_name:
                st.write(path.name)

            with col_type:
                st.caption(friendly_file_type(path))

            with col_download:
                st.download_button(
                    "Baixar",
                    data=path.read_bytes(),
                    file_name=path.name,
                    mime=mime_type(path),
                    key=f"download_{run_id}_{index}_{path.name}",
                )

    with tab_details:
        st.json(
            {
                key: value
                for key, value in metadata.items()
                if key != "technical_log"
            }
        )

        technical_log = metadata.get("technical_log", "")
        if technical_log:
            with st.expander("Log técnico do processamento"):
                st.code(technical_log, language="text")


def render_sidebar() -> str | None:
    st.sidebar.title("MJV - Data Quality")
    st.sidebar.caption("Histórico de execuções")

    runs = list_runs()
    if not runs:
        st.sidebar.info("Ainda não existem execuções.")
        return None

    current = st.session_state.get("selected_run_id")
    default_index = runs.index(current) if current in runs else 0

    selected = st.sidebar.selectbox(
        "Selecione uma execução",
        runs,
        index=default_index,
    )

    metadata = load_metadata(selected)
    if metadata:
        st.sidebar.caption(
            f"Status: {metadata.get('status', 'desconhecido')}"
        )
        st.sidebar.caption(
            f"Criada em: {metadata.get('created_at', '—')}"
        )

    return selected


def render_upload_area() -> None:
    st.markdown("### Data Scanning:")

    uploaded_files = st.file_uploader(
        "Envie os arquivos que serão analisados",
        type=["csv", "xlsx", "xls", "parquet"],
        accept_multiple_files=True,
        help=(
            "Formatos aceitos: CSV, XLSX, XLS e Parquet. "
            "Cada execução possui uma pasta isolada."
        ),
    )

    if uploaded_files:
        invalid_files = [
            file.name
            for file in uploaded_files
            if Path(file.name).suffix.lower() not in ALLOWED_EXTENSIONS
        ]

        if invalid_files:
            st.error(
                "Arquivos com extensão não permitida: "
                + ", ".join(invalid_files)
            )
            return

        st.caption(
            f"{len(uploaded_files)} arquivo(s) selecionado(s)."
        )

        for uploaded_file in uploaded_files:
            size_mb = uploaded_file.size / (1024 * 1024)
            st.write(f"✓ {uploaded_file.name} — {size_mb:.2f} MB")

    execute = st.button(
        "Executar análise de qualidade",
        type="primary",
        disabled=not uploaded_files,
    )

    if not execute:
        return

    progress = st.progress(0, text="Preparando a execução...")

    try:
        progress.progress(
            10,
            text="Criando ambiente isolado da execução...",
        )
        metadata = prepare_job(uploaded_files)

        run_id = metadata["run_id"]
        st.session_state["selected_run_id"] = run_id

        progress.progress(
            25,
            text="Arquivos recebidos. Iniciando o scanner...",
        )

        with st.spinner(
            "Processando dados e gerando relatórios. "
            "Esta etapa pode levar alguns minutos..."
        ):
            progress.progress(
                40,
                text="Executando regras e validações...",
            )
            result_metadata = execute_job(run_id)

        progress.progress(
            90,
            text="Organizando relatórios e downloads...",
        )
        time.sleep(0.2)
        progress.progress(
            100,
            text="Processamento concluído com sucesso.",
        )

        st.session_state["selected_run_id"] = result_metadata["run_id"]
        st.session_state["analysis_completed"] = True

        st.rerun()

    except JobExecutionError as exc:
        progress.empty()
        st.error(str(exc))

        run_id = st.session_state.get("selected_run_id")
        if run_id:
            metadata = load_metadata(run_id)
            technical_log = metadata.get("technical_log", "")
            if technical_log:
                with st.expander(
                    "Detalhes técnicos do erro",
                    expanded=True,
                ):
                    st.code(technical_log, language="text")

    except Exception as exc:
        progress.empty()
        st.exception(exc)


def main() -> None:
    configure_page()
    ensure_project_structure()

    if st.session_state.pop("analysis_completed", False):
        st.success(
            "Análise concluída. Os relatórios estão disponíveis abaixo."
        )

    logo_col, title_col = st.columns(
        [1, 10],
        vertical_alignment="center",
    )

    with logo_col:
        if LOGO_MJV_PATH.exists():
            st.image(
                str(LOGO_MJV_PATH),
                width=90,
            )
        else:
            st.warning(f"Logo não encontrada em: {LOGO_MJV_PATH}")

    with title_col:
        st.markdown(
            """
            <div class="mjv-title-area">
                <h1>Data Quality Platform</h1>
                <p>
                    Upload, processamento, validação regulatória BACEN
                    e geração centralizada de relatórios.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    project_errors = validate_project()
    if project_errors:
        st.error("A estrutura do projeto está incompleta.")
        for error in project_errors:
            st.code(error)
        st.stop()

    selected_from_sidebar = render_sidebar()

    render_upload_area()

    selected_run_id = st.session_state.get(
        "selected_run_id",
        selected_from_sidebar,
    )

    if selected_from_sidebar:
        selected_run_id = selected_from_sidebar
        st.session_state["selected_run_id"] = selected_from_sidebar

    if selected_run_id:
        st.divider()
        st.markdown("### Resultado da execução")
        render_run_summary(selected_run_id)


if __name__ == "__main__":
    main()