# -*- coding: utf-8 -*-
"""
run_data_quality_pipeline.py (VERSÃO CORRIGIDA)

Objetivo:
- Evitar recursão acidental
- Corrigir localização dos scripts de relatório
- Aceitar os argumentos esperados pelo app.py
- Tentar gerar os outputs:
    - dq_report_premium_mjv
    - dq_ai_recommendations_current
    - dq_current_detail
    - dq_dimension_scores_current
    - dq_history_chart
    - dq_radar_chart
    - dq_executive_report

Uso:
    python src/pipeline/run_data_quality_pipeline.py --sources config/sources.runtime.yml --duckdb ./dq_lab.duckdb --stg stg --rules config/12_dq_rules.yml --outdir ./output
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional


class CommandExecutionError(RuntimeError):
    pass


def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in [current.parent] + list(current.parents):
        if (parent / "src").exists() or (parent / "config").exists() or (parent / "data").exists():
            return parent
    return Path.cwd().resolve()


PROJECT_ROOT = find_project_root()


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def ensure_directory(path_str: str) -> Path:
    path = resolve_path(path_str)
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_rules_path(path_str: str) -> str:
    """
    Corrige casos quebrados como:
    'config/python export_data_quality_report.py2_dq_rules.yml'
    """
    raw = str(path_str).strip().replace("\\", "/")

    if Path(raw).exists():
        return raw

    known_candidates = [
        "config/12_dq_rules.yml",
        "12_dq_rules.yml",
        "config/python/12_dq_rules.yml",
        "config/dq_rules.yml",
    ]

    raw_lower = raw.lower()
    if "12_dq_rules" in raw_lower or "dq_rules" in raw_lower or "export_data_quality_report.py2_dq_rules.yml" in raw_lower:
        for candidate in known_candidates:
            candidate_abs = resolve_path(candidate)
            if candidate_abs.exists():
                return str(candidate_abs)

    return raw


def find_first_existing(candidates: list[str]) -> Optional[str]:
    for candidate in candidates:
        p = resolve_path(candidate)
        if p.exists() and p.is_file():
            return str(p)
    return None


def find_report_script(script_name: str) -> Optional[str]:
    candidates = [
        script_name,
        f"src/reports/{script_name}",
        f"src/pipeline/{script_name}",
        f"data/data_quality_v2_package/{script_name}",
        f"data/{script_name}",
        f"reports/{script_name}",
    ]
    return find_first_existing(candidates)


def find_scan_script() -> Optional[str]:
    candidates = [
        "15_run_sources_pipeline.py",
        "src/pipeline/15_run_sources_pipeline.py",
        "src/15_run_sources_pipeline.py",
        "run_sources_pipeline.py",
        "src/pipeline/run_sources_pipeline.py",
    ]
    return find_first_existing(candidates)


def run_command(cmd: Iterable[str], cwd: Optional[Path] = None, required: bool = True) -> None:
    cmd_list = [str(x) for x in cmd]
    print("[RUN]", " ".join(cmd_list))
    result = subprocess.run(cmd_list, cwd=str(cwd or PROJECT_ROOT))
    if result.returncode != 0 and required:
        raise CommandExecutionError(
            f"Falha ao executar comando (exit code {result.returncode}): {' '.join(cmd_list)}"
        )


def build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline principal de Data Quality")
    parser.add_argument("--sources", default="config/sources.runtime.yml", help="Arquivo de fontes")
    parser.add_argument("--duckdb", default="./dq_lab.duckdb", help="Arquivo DuckDB")
    parser.add_argument("--stg", default="stg", help="Schema de staging")
    parser.add_argument("--rules", default="config/12_dq_rules.yml", help="Arquivo de regras YAML")
    parser.add_argument("--outdir", default="./output", help="Diretório de saída")
    parser.add_argument("--logo", default="./docs/assets/logo_mjv.png", help="Logo institucional")
    parser.add_argument("--run-id", default=None, help="Run ID específico")
    parser.add_argument("--limit", type=int, default=100000, help="Limite de linhas por dataset")
    parser.add_argument("--skip-legacy-report", action="store_true", help="Não gera o relatório legado premium")
    parser.add_argument("--skip-v2-report", action="store_true", help="Não gera o relatório premium v2 integrado")
    return parser.parse_args()


def main() -> None:
    args = build_args()

    py = sys.executable
    outdir = ensure_directory(args.outdir)
    logo_path = resolve_path(args.logo)
    rules_path = normalize_rules_path(args.rules)

    print(f"[INFO] PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"[INFO] Regras utilizadas: {rules_path}")

    # 1) Scanner técnico base (se existir no projeto)
    scan_script = find_scan_script()
    if scan_script:
        print(f"[INFO] Scanner encontrado: {scan_script}")
        scan_cmd = [
            py,
            scan_script,
            "--sources", str(resolve_path(args.sources)),
            "--duckdb", str(resolve_path(args.duckdb)),
            "--stg", args.stg,
            "--rules", str(rules_path),
        ]
        if args.limit is not None:
            scan_cmd.extend(["--limit", str(args.limit)])
        run_command(scan_cmd)
    else:
        print("[WARN] Scanner não encontrado. Seguindo para geração dos relatórios com a base existente no DuckDB.")

    # 2) Relatório legado premium
    if not args.skip_legacy_report:
        legacy_script = find_report_script("export_data_quality_report.py")
        if legacy_script:
            print(f"[INFO] Relatório legado encontrado: {legacy_script}")
            legacy_cmd = [
                py,
                legacy_script,
                "--duckdb", str(resolve_path(args.duckdb)),
                "--stg", args.stg,
                "--outdir", str(outdir),
            ]
            if logo_path.exists():
                legacy_cmd.extend(["--logo", str(logo_path)])
            if args.run_id:
                legacy_cmd.extend(["--run-id", args.run_id])
            run_command(legacy_cmd)
        else:
            print("[WARN] export_data_quality_report.py não encontrado. Relatório legado não será gerado.")

    # 3) Relatório premium v2 integrado
    if not args.skip_v2_report:
        v2_script = find_report_script("export_data_quality_report_v2_integrated.py")
        if v2_script:
            print(f"[INFO] Relatório V2 encontrado: {v2_script}")
            v2_cmd = [
                py,
                v2_script,
                "--duckdb", str(resolve_path(args.duckdb)),
                "--schema", args.stg,
                "--outdir", str(outdir),
            ]
            if logo_path.exists():
                v2_cmd.extend(["--logo", str(logo_path)])
            if args.run_id:
                v2_cmd.extend(["--run-id", args.run_id])
            run_command(v2_cmd)
        else:
            raise FileNotFoundError(
                "Script não encontrado: export_data_quality_report_v2_integrated.py\n"
                "Verifique se ele está em uma destas pastas:\n"
                "- data/data_quality_v2_package/\n"
                "- src/reports/\n"
                "- data/\n"
                "- reports/"
            )

    print("\n[OK] Pipeline executado com sucesso.")
    print(f"[OK] Relatórios disponíveis em: {outdir}")
    print("[OK] Outputs esperados:")
    print(f"     - {outdir / 'dq_ai_recommendations_current.csv'}")
    print(f"     - {outdir / 'dq_current_detail.csv'}")
    print(f"     - {outdir / 'dq_dimension_scores_current.csv'}")
    print(f"     - {outdir / 'dq_history_chart.html'}")
    print(f"     - {outdir / 'dq_radar_chart.html'}")
    print("     - dq_report_premium_mjv_YYYYMMDD_HHMMSS.xlsx / .html")
    print("     - dq_executive_report_v2_YYYYMMDD_HHMMSS.xlsx / .html")
    print("     - dq_report_premium_mjv_v2_YYYYMMDD_HHMMSS.xlsx / .html")


if __name__ == "__main__":
    main()
