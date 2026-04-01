# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from dotenv import load_dotenv

from playwright.sync_api import sync_playwright

import time


PROJECT_ROOT = Path(__file__).resolve().parent


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def replace_env_vars(value: Any) -> Any:
    """
    Resolve placeholders como:
    - ${POSTGRES_HOST}
    - textos mistos com ${VAR}/subpasta
    """
    pattern = re.compile(r"\$\{([^}]+)\}")

    if isinstance(value, str):
        def _repl(match: re.Match[str]) -> str:
            env_name = match.group(1)
            return os.getenv(env_name, "")
        return pattern.sub(_repl, value)

    if isinstance(value, dict):
        return {k: replace_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [replace_env_vars(v) for v in value]

    return value


def load_config(config_path: str) -> dict:
    cfg_path = resolve_path(config_path)
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return replace_env_vars(cfg)


def normalize_extensions(patterns: List[str]) -> List[str]:
    include_ext: List[str] = []
    for pattern in patterns:
        item = str(pattern).strip()
        if item.startswith("*."):
            include_ext.append(item[2:])
        elif item.startswith("."):
            include_ext.append(item[1:])
        else:
            include_ext.append(item.replace("*", "").replace(".", ""))
    # remove duplicados preservando ordem
    seen = set()
    ordered = []
    for ext in include_ext:
        if ext and ext not in seen:
            seen.add(ext)
            ordered.append(ext)
    return ordered


def build_file_source(cfg: dict) -> dict:
    scan_cfg = cfg.get("scan", {})
    patterns = scan_cfg.get("file_patterns", ["*.csv", "*.xlsx", "*.xls", "*.parquet"])
    return {
        "inbox": scan_cfg.get("input_dir", "./data"),
        "include_ext": normalize_extensions(patterns),
        "csv_sep": scan_cfg.get("csv_sep", ","),
        "csv_encoding": scan_cfg.get("csv_encoding", "utf-8"),
    }


def to_int(value: Any, default: int) -> int:
    try:
        if value in ("", None):
            return default
        return int(value)
    except Exception:
        return default


def build_database_sources(cfg: dict) -> List[dict]:
    databases_cfg = cfg.get("databases", {})
    if not isinstance(databases_cfg, dict):
        raise ValueError("O bloco 'databases' do config deve ser um dicionário.")

    db_type_map = {
        "postgres": "postgres",
        "mysql": "mysql",
        "sqlserver": "sqlserver",
        "oracle": "oracle",
        "mariadb": "mariadb",
        "sqlite": "sqlite",
        "duckdb_source": "duckdb",
    }

    sources: List[dict] = []

    for key, db_cfg in databases_cfg.items():
        if not isinstance(db_cfg, dict):
            continue
        if not db_cfg.get("enabled", False):
            continue

        source_type = db_type_map.get(key, key)
        entry = {
            "name": f"{key}_source",
            "type": source_type,
            "limit": to_int(db_cfg.get("limit", 50000), 50000),
        }

        if source_type in {"sqlite", "duckdb"}:
            entry["dbfile"] = db_cfg.get("dbfile", "")
            entry["include_tables"] = db_cfg.get("include_tables", [])
        else:
            entry["host"] = db_cfg.get("host", "")
            entry["port"] = to_int(db_cfg.get("port", 0), 0)
            entry["db"] = db_cfg.get("db", "")
            entry["user"] = db_cfg.get("user", "")
            entry["password"] = db_cfg.get("password", "")
            entry["include_schemas"] = db_cfg.get("include_schemas", [])
            entry["include_tables"] = db_cfg.get("include_tables", [])

        sources.append(entry)

    return sources


def validate_enabled_databases(cfg: dict) -> None:
    databases_cfg = cfg.get("databases", {})
    if not isinstance(databases_cfg, dict):
        return

    required_network = ["host", "port", "db", "user", "password"]
    required_file = ["dbfile"]

    errors: List[str] = []

    for key, db_cfg in databases_cfg.items():
        if not isinstance(db_cfg, dict) or not db_cfg.get("enabled", False):
            continue

        if key in {"sqlite", "duckdb_source"}:
            missing = [field for field in required_file if not str(db_cfg.get(field, "")).strip()]
        else:
            missing = [field for field in required_network if not str(db_cfg.get(field, "")).strip()]

        if missing:
            errors.append(f"- {key}: faltando {', '.join(missing)}")

    if errors:
        raise ValueError(
            "Existem bancos habilitados com parâmetros obrigatórios ausentes:\n" + "\n".join(errors)
        )


def build_sources_yaml(cfg: dict, runtime_path: str) -> Path:
    runtime_file = resolve_path(runtime_path)
    runtime_file.parent.mkdir(parents=True, exist_ok=True)

    sources_payload = {
        "files": build_file_source(cfg),
        "databases": build_database_sources(cfg),
    }

    with open(runtime_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(
            sources_payload,
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )

    return runtime_file


def run_command(cmd: List[str]) -> None:
    cmd = [str(x) for x in cmd]
    print("\n[RUN]", " ".join(cmd), "\n")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def find_pipeline_script() -> str:
    candidates = [
        PROJECT_ROOT / "src" / "pipeline" / "run_data_quality_pipeline.py",
        PROJECT_ROOT / "run_data_quality_pipeline.py",
        PROJECT_ROOT / "src" / "pipeline" / "15_run_sources_pipeline.py",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    raise FileNotFoundError(
        "Não encontrei o script do pipeline. Verifique se existe um destes arquivos:\n"
        "- src/pipeline/run_data_quality_pipeline.py\n"
        "- run_data_quality_pipeline.py\n"
        "- src/pipeline/15_run_sources_pipeline.py"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orquestrador do Data Quality com suporte a config multibanco"
    )
    parser.add_argument("--config", default="config/config.multibanco.yml", help="Arquivo principal de configuração")
    parser.add_argument("--env-file", default=".env", help="Arquivo .env com variáveis")
    parser.add_argument("--sources-runtime", default="config/sources.runtime.yml", help="Arquivo runtime gerado automaticamente")
    parser.add_argument("--duckdb", default=None, help="Sobrescreve o caminho do DuckDB")
    parser.add_argument("--stg", default=None, help="Sobrescreve o schema de staging")
    parser.add_argument("--rules", default=None, help="Sobrescreve o arquivo de regras")
    parser.add_argument("--outdir", default=None, help="Sobrescreve o diretório de saída")
    parser.add_argument("--skip-run", action="store_true", help="Apenas gera o sources.runtime.yml sem executar o pipeline")
    return parser.parse_args()

def export_html_file_to_pdf(html_file: Path, wait_ms: int = 3000) -> Path:
    """
    Converte um HTML local em PDF com o mesmo nome.
    Exemplo:
    dq_report.html -> dq_report.pdf
    """
    pdf_file = html_file.with_suffix(".pdf")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        page.goto(html_file.resolve().as_uri(), wait_until="networkidle")
        page.wait_for_timeout(wait_ms)

        page.pdf(
            path=str(pdf_file),
            format="A4",
            print_background=True,
            margin={
                "top": "12mm",
                "right": "10mm",
                "bottom": "12mm",
                "left": "10mm",
            },
        )

        browser.close()

    return pdf_file


def export_all_htmls_to_pdf(outdir: str, started_at: float) -> None:
    """
    Converte apenas os HTMLs gerados/modificados nesta execução.
    """
    outdir_path = Path(outdir).resolve()

    if not outdir_path.exists():
        print(f"[WARN] Diretório de saída não encontrado para PDF: {outdir_path}")
        return

    html_files = sorted(
        [
            f for f in outdir_path.glob("*.html")
            if f.stat().st_mtime >= started_at
        ]
    )

    if not html_files:
        print(f"[INFO] Nenhum HTML novo encontrado para converter em PDF em: {outdir_path}")
        return

    print(f"[INFO] Convertendo {len(html_files)} HTML(s) do run atual em PDF...")

    for html_file in html_files:
        try:
            pdf_file = export_html_file_to_pdf(html_file)
            print(f"[OK] PDF  : {pdf_file}")
        except Exception as e:
            print(f"[WARN] Falha ao converter {html_file.name} para PDF: {e}")


def main() -> None:
    started_at = time.time()
    args = parse_args()

    env_path = resolve_path(args.env_file)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)

    cfg = load_config(args.config)
    validate_enabled_databases(cfg)

    runtime_path = build_sources_yaml(cfg, args.sources_runtime)

    duckdb_path = args.duckdb or cfg.get("duckdb", {}).get("path", "./dq_lab.duckdb")
    stg_schema = args.stg or cfg.get("duckdb", {}).get("schema", "stg")
    rules_path = args.rules or cfg.get("rules", {}).get("path", "config/12_dq_rules.yml")
    outdir_path = args.outdir or cfg.get("output", {}).get("dir", "./output")

    print("[INFO] Config carregado com sucesso")
    print(f"[INFO] Runtime gerado em: {runtime_path}")
    print(f"[INFO] Bancos habilitados: {len(build_database_sources(cfg))}")

    if args.skip_run:
        print("[INFO] Execução do pipeline ignorada por --skip-run")
        return

    pipeline_script = find_pipeline_script()

    cmd = [
        sys.executable,
        pipeline_script,
        "--sources",
        str(runtime_path),
        "--duckdb",
        str(duckdb_path),
        "--stg",
        str(stg_schema),
        "--rules",
        str(rules_path),
        "--outdir",
        str(outdir_path),
    ]

    run_command(cmd)

    try:
        export_all_htmls_to_pdf(outdir_path, started_at)
    except Exception as e:
        print(f"[WARN] Falha geral na exportação de PDFs: {e}")

    print("\n")
    print("[OK] Scanning Executado com sucesso! Favor checar os relatórios gerados em OUTPUT")
    print(f"[OK] Caminho: {Path(outdir_path).resolve()}")
    print("\n")


if __name__ == "__main__":
    main()
