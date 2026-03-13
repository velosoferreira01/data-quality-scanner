# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv


def replace_env_vars(value):
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1]
        return os.getenv(env_name, "")
    if isinstance(value, dict):
        return {k: replace_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [replace_env_vars(v) for v in value]
    return value


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return replace_env_vars(cfg)


def build_sources_yaml(cfg: dict, temp_sources_path: str) -> None:
    input_dir = cfg["scan"]["input_dir"]
    patterns = cfg["scan"].get("file_patterns", ["*.csv", "*.xlsx", "*.parquet"])

    include_ext = []
    for p in patterns:
        p = str(p).strip()
        if p.startswith("*."):
            include_ext.append(p[2:])
        elif p.startswith("."):
            include_ext.append(p[1:])
        else:
            include_ext.append(p.replace("*", "").replace(".", ""))

    sources = {
        "files": {
            "inbox": input_dir,
            "include_ext": include_ext,
            "csv_sep": cfg["scan"].get("csv_sep", ","),
            "csv_encoding": cfg["scan"].get("csv_encoding", "utf-8"),
        },
        "databases": [],
    }

    pg = cfg.get("postgres", {})
    if pg.get("enabled", False):
        sources["databases"].append(
            {
                "name": "postgres_local",
                "type": "postgres",
                "host": pg["host"],
                "port": pg.get("port", 5432),
                "db": pg["db"],
                "user": pg["user"],
                "password": pg["password"],
                "include_schemas": pg.get("include_schemas", ["public"]),
                "include_tables": pg.get("include_tables", []),
                "limit": pg.get("limit", 50000),
            }
        )

    with open(temp_sources_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(sources, f, sort_keys=False, allow_unicode=True)


def run_command(cmd: list[str]) -> None:
    print(f"\n[RUN] {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def validate_postgres_env(cfg: dict) -> None:
    pg = cfg.get("postgres", {})
    if not pg.get("enabled", False):
        return

    required = ["host", "db", "user", "password"]
    missing = [k for k in required if not str(pg.get(k, "")).strip()]

    if missing:
        raise ValueError(
            f"Postgres enabled, but missing config values: {', '.join(missing)}. "
            f"Check your .env file."
        )


def main():
    load_dotenv(dotenv_path=Path(".env"))

    parser = argparse.ArgumentParser(description="Universal Data Quality Scanner")
    parser.add_argument("--config", default="config/config.yml", help="Caminho do arquivo de configuração")
    parser.add_argument("--input-dir", help="Sobrescreve a pasta de entrada")
    parser.add_argument("--duckdb", help="Sobrescreve o caminho do DuckDB")
    parser.add_argument("--outdir", help="Sobrescreve a pasta de saída")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.input_dir:
        cfg["scan"]["input_dir"] = args.input_dir
    if args.duckdb:
        cfg["duckdb"]["path"] = args.duckdb
    if args.outdir:
        cfg["output"]["dir"] = args.outdir

    validate_postgres_env(cfg)

    temp_sources = "config/sources.runtime.yml"
    build_sources_yaml(cfg, temp_sources)

    duckdb_path = cfg["duckdb"]["path"]
    stg_schema = cfg["duckdb"].get("schema", "stg")
    rules_path = cfg["rules"]["path"]
    output_dir = cfg["output"]["dir"]

    run_command(
        [
            sys.executable,
            "src/pipeline/run_data_quality_pipeline.py",
            "--sources",
            temp_sources,
            "--duckdb",
            duckdb_path,
            "--stg",
            stg_schema,
            "--rules",
            rules_path,
            "--report",
        ]
    )

    run_command(
        [
            sys.executable,
            "src/reports/export_data_quality_report.py",
            "--duckdb",
            duckdb_path,
            "--stg",
            stg_schema,
            "--outdir",
            output_dir,
        ]
    )

    print("\n[OK] Scanner executado com sucesso.")
    print(f"[OK] Relatórios disponíveis em: {Path(output_dir).resolve()}")


if __name__ == "__main__":
    main()