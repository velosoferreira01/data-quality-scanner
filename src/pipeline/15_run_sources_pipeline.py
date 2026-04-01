# -*- coding: utf-8 -*-
"""
15_run_sources_pipeline.py

Pipeline universal de scanning para Data Quality.
Compatível com a chamada feita por:
    src/pipeline/run_data_quality_pipeline.py

Entrada esperada:
    python 15_run_sources_pipeline.py \
        --sources config/sources.runtime.yml \
        --duckdb ./dq_lab.duckdb \
        --stg stg \
        --rules config/12_dq_rules.yml \
        --limit 80000

O objetivo é:
- Ler arquivos e bancos descritos no sources.yml
- Calcular métricas de tabela e coluna
- Aplicar scoring simples e robusto
- Salvar no DuckDB tabelas universais consumidas pelos relatórios:
    - stg.dq_table_metrics_u
    - stg.dq_column_metrics_u
    - stg.dq_table_scores_u
    - stg.dq_column_scores_u
    - stg.dq_table_scores_u_rules
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import warnings
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd
import yaml

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None


# =========================================================
# HELPERS
# =========================================================
def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def now_run_id() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def clamp_0_10(x: float) -> float:
    return max(0.0, min(10.0, float(x)))


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_str(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def normalize_ext_list(include_ext: Iterable[str] | None) -> set[str]:
    out: set[str] = set()
    for item in include_ext or []:
        s = safe_str(item).lower()
        s = s.replace("*", "")
        if s.startswith("."):
            s = s[1:]
        if s:
            out.add(s)
    return out


def is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)


def is_datetime(s: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(s)


def parse_possible_datetime(series: pd.Series) -> pd.Series:
    try:
        if pd.api.types.is_datetime64_any_dtype(series):
            return pd.to_datetime(series, errors="coerce")
        if pd.api.types.is_numeric_dtype(series):
            return pd.Series([pd.NaT] * len(series), index=series.index)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.to_datetime(series, errors="coerce")
    except Exception:
        return pd.Series([pd.NaT] * len(series), index=series.index)


# =========================================================
# LEITURA DE FONTES
# =========================================================
def read_csv_dataset(path: Path, limit: int | None, sep: str, encoding: str) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    df = pd.read_csv(
        path,
        nrows=limit if limit and limit > 0 else None,
        sep=sep,
        encoding=encoding,
        low_memory=False,
    )
    meta = {
        "source_type": "file",
        "source": "file",
        "source_name": path.name,
        "file_name": path.name,
        "source_ref": str(path.resolve()),
        "object_name": path.name,
        "dataset_name": path.name,
        "table_name": "",
        "sheet_name": "",
    }
    return [(path.name, df, meta)]


def read_parquet_dataset(path: Path, limit: int | None) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    df = pd.read_parquet(path)
    if limit and limit > 0:
        df = df.head(limit)
    meta = {
        "source_type": "file",
        "source": "file",
        "source_name": path.name,
        "file_name": path.name,
        "source_ref": str(path.resolve()),
        "object_name": path.name,
        "dataset_name": path.name,
        "table_name": "",
        "sheet_name": "",
    }
    return [(path.name, df, meta)]


def read_excel_dataset(path: Path, limit: int | None) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    xls = pd.ExcelFile(path)
    datasets: list[tuple[str, pd.DataFrame, dict[str, Any]]] = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        if limit and limit > 0:
            df = df.head(limit)
        dataset_name = f"{path.name}::{sheet}"
        meta = {
            "source_type": "file",
            "source": "file",
            "source_name": path.name,
            "file_name": path.name,
            "source_ref": str(path.resolve()),
            "object_name": dataset_name,
            "dataset_name": dataset_name,
            "table_name": sheet,
            "sheet_name": sheet,
        }
        datasets.append((dataset_name, df, meta))
    return datasets


def collect_file_datasets(files_cfg: dict, limit: int | None) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    inbox = Path(files_cfg.get("inbox", "")).expanduser()
    if not inbox.exists():
        print(f"[WARN] Pasta de entrada não encontrada: {inbox}")
        return []

    include_ext = normalize_ext_list(files_cfg.get("include_ext"))
    csv_sep = safe_str(files_cfg.get("csv_sep") or ",") or ","
    csv_encoding = safe_str(files_cfg.get("csv_encoding") or "utf-8") or "utf-8"

    datasets: list[tuple[str, pd.DataFrame, dict[str, Any]]] = []
    for path in sorted(inbox.iterdir()):
        if not path.is_file():
            continue
        ext = path.suffix.lower().lstrip(".")
        if include_ext and ext not in include_ext:
            continue

        try:
            if ext == "csv":
                datasets.extend(read_csv_dataset(path, limit, csv_sep, csv_encoding))
            elif ext == "parquet":
                datasets.extend(read_parquet_dataset(path, limit))
            elif ext in {"xlsx", "xls"}:
                datasets.extend(read_excel_dataset(path, limit))
            else:
                print(f"[WARN] Extensão não suportada, ignorando: {path.name}")
        except Exception as e:
            print(f"[WARN] Falha ao ler arquivo {path.name}: {e}")

    return datasets


def pg_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def collect_postgres_datasets(db_cfg: dict, limit: int | None) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    if psycopg2 is None:
        print("[WARN] psycopg2 não disponível. Bancos Postgres serão ignorados.")
        return []

    host = safe_str(db_cfg.get("host"))
    port = int(db_cfg.get("port", 5432) or 5432)
    dbname = safe_str(db_cfg.get("database") or db_cfg.get("db"))
    user = safe_str(db_cfg.get("user"))
    password = safe_str(db_cfg.get("password"))
    schema = safe_str(db_cfg.get("schema") or "public") or "public"
    only = safe_str(db_cfg.get("only")) or None
    source_name = safe_str(db_cfg.get("name") or f"postgres_{dbname}_{schema}")

    if not (host and dbname and user):
        print(f"[WARN] Configuração Postgres incompleta para {source_name}. Ignorando.")
        return []

    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    tables = [r[0] for r in cur.fetchall()]
    cur.close()

    if only:
        rx = re.compile(only)
        tables = [t for t in tables if rx.search(t)]

    datasets: list[tuple[str, pd.DataFrame, dict[str, Any]]] = []
    for table in tables:
        query = f"SELECT * FROM {pg_ident(schema)}.{pg_ident(table)} LIMIT %s"
        try:
            df = pd.read_sql_query(query, conn, params=(limit if limit and limit > 0 else 50000,))
            dataset_name = f"{schema}.{table}"
            meta = {
                "source_type": "postgres",
                "source": "postgres",
                "source_name": source_name,
                "file_name": "",
                "source_ref": f"{host}:{port}/{dbname}/{schema}",
                "object_name": dataset_name,
                "dataset_name": dataset_name,
                "table_name": table,
                "sheet_name": "",
            }
            datasets.append((dataset_name, df, meta))
        except Exception as e:
            print(f"[WARN] Falha ao ler tabela {schema}.{table}: {e}")

    conn.close()
    return datasets


def collect_all_datasets(sources_cfg: dict, limit: int | None) -> list[tuple[str, pd.DataFrame, dict[str, Any]]]:
    datasets: list[tuple[str, pd.DataFrame, dict[str, Any]]] = []

    files_cfg = sources_cfg.get("files") or {}
    if files_cfg:
        datasets.extend(collect_file_datasets(files_cfg, limit))

    for db_cfg in sources_cfg.get("databases") or []:
        db_type = safe_str(db_cfg.get("type")).lower()
        if db_type == "postgres":
            try:
                datasets.extend(collect_postgres_datasets(db_cfg, limit))
            except Exception as e:
                print(f"[WARN] Falha ao processar banco {safe_str(db_cfg.get('name'))}: {e}")
        else:
            print(f"[WARN] Tipo de banco não suportado: {db_type}")

    return datasets


# =========================================================
# MÉTRICAS E SCORE
# =========================================================
def table_metrics(df: pd.DataFrame) -> tuple[int, int, int, int, float]:
    row_count = int(df.shape[0])
    col_count = int(df.shape[1])
    null_cells = int(df.isna().sum().sum()) if row_count and col_count else 0
    total_cells = int(row_count * col_count)
    null_rate = (null_cells / total_cells) if total_cells else 0.0
    return row_count, col_count, null_cells, total_cells, float(null_rate)


def column_metrics(df: pd.DataFrame, col: str) -> dict[str, Any]:
    s = df[col]
    total = int(len(s))
    nulls = int(s.isna().sum())
    nn = s.dropna()

    distinct_cnt = int(nn.nunique(dropna=True)) if len(nn) else 0
    distinct_ratio = (distinct_cnt / len(nn)) if len(nn) else 0.0
    null_rate = (nulls / total) if total else 0.0

    minv = None
    maxv = None
    avg_len = None
    max_len = None

    try:
        if len(nn):
            if is_numeric(nn):
                minv = str(nn.min())
                maxv = str(nn.max())
            elif is_datetime(nn):
                minv = str(nn.min())
                maxv = str(nn.max())
            else:
                ss = nn.astype(str)
                lens = ss.str.len()
                avg_len = float(lens.mean()) if len(lens) else None
                max_len = int(lens.max()) if len(lens) else None
    except Exception:
        pass

    return {
        "column_name": col,
        "dtype": str(s.dtype),
        "total": total,
        "nulls": nulls,
        "null_rate": float(null_rate),
        "distinct_cnt": distinct_cnt,
        "distinct_ratio": float(distinct_ratio),
        "min_value": minv,
        "max_value": maxv,
        "avg_len": avg_len,
        "max_len": max_len,
    }


def classify_score(score: float) -> str:
    if score >= 9:
        return "Excelente"
    if score >= 7:
        return "Bom"
    if score >= 5:
        return "Atenção"
    return "Crítico"


def build_dimension_scores(df: pd.DataFrame) -> dict[str, float]:
    rows, cols = df.shape
    total_cells = rows * cols

    if rows == 0 or cols == 0:
        return {
            "completude": 0.0,
            "unicidade": 0.0,
            "consistencia": 0.0,
            "validade": 0.0,
            "integridade": 0.0,
            "freshness": 5.0,
        }

    # completude
    null_rate = float(df.isna().sum().sum() / total_cells) if total_cells else 1.0
    completude = clamp_0_10(10 * (1 - null_rate))

    # unicidade: média do ratio de unicidade por coluna
    uniq_parts: list[float] = []
    for c in df.columns:
        s = df[c].dropna()
        if len(s) == 0:
            uniq_parts.append(1.0)
        else:
            uniq_parts.append(float(s.nunique(dropna=True) / len(s)))
    unicidade = clamp_0_10(10 * (sum(uniq_parts) / len(uniq_parts))) if uniq_parts else 0.0

    # consistencia: penaliza tipos muito mistos em colunas object
    consist_parts: list[float] = []
    for c in df.columns:
        s = df[c].dropna()
        if len(s) == 0:
            consist_parts.append(1.0)
            continue
        if pd.api.types.is_object_dtype(s):
            types_seen = s.map(lambda x: type(x).__name__).nunique(dropna=True)
            consist_parts.append(1.0 if types_seen <= 1 else max(0.0, 1 - (types_seen - 1) * 0.25))
        else:
            consist_parts.append(1.0)
    consistencia = clamp_0_10(10 * (sum(consist_parts) / len(consist_parts))) if consist_parts else 0.0

    # validade: proporção de valores legíveis/úteis
    valid_parts: list[float] = []
    for c in df.columns:
        s = df[c]
        nn = s.dropna()
        if len(nn) == 0:
            valid_parts.append(1.0)
            continue
        if is_numeric(nn):
            valid_parts.append(1.0)
        elif is_datetime(nn):
            valid_parts.append(1.0)
        else:
            as_dt = parse_possible_datetime(nn)
            dt_ratio = float(as_dt.notna().mean()) if len(as_dt) else 0.0
            txt = nn.astype(str).str.strip()
            non_blank_ratio = float((txt != "").mean()) if len(txt) else 0.0
            valid_parts.append(max(dt_ratio, non_blank_ratio))
    validade = clamp_0_10(10 * (sum(valid_parts) / len(valid_parts))) if valid_parts else 0.0

    # integridade: aproximação via colunas referenciais / ids obrigatórios
    id_like = [c for c in df.columns if re.search(r"(^id$|^id_|_id$|codigo|chave|key)", c, re.I)]
    if id_like:
        integ_parts: list[float] = []
        for c in id_like:
            s = df[c]
            ratio = float((~s.isna()).mean()) if len(s) else 0.0
            if s.dropna().empty:
                uniq = 0.0
            else:
                uniq = float(s.dropna().nunique(dropna=True) / len(s.dropna()))
            integ_parts.append((ratio + uniq) / 2)
        integridade = clamp_0_10(10 * (sum(integ_parts) / len(integ_parts)))
    else:
        integridade = clamp_0_10((completude + unicidade) / 2)

    # freshness: tenta localizar alguma coluna temporal
    date_like_cols = [c for c in df.columns if re.search(r"date|data|dt_|_dt|timestamp|measured_at|created|updated", c, re.I)]
    freshness = 5.0
    if date_like_cols:
        best_ratio = 0.0
        for c in date_like_cols:
            ratio = float(parse_possible_datetime(df[c]).notna().mean())
            best_ratio = max(best_ratio, ratio)
        freshness = clamp_0_10(10 * best_ratio)

    return {
        "completude": round(completude, 4),
        "unicidade": round(unicidade, 4),
        "consistencia": round(consistencia, 4),
        "validade": round(validade, 4),
        "integridade": round(integridade, 4),
        "freshness": round(freshness, 4),
    }


def overall_score(dim_scores: dict[str, float]) -> float:
    weights = {
        "completude": 0.25,
        "unicidade": 0.20,
        "consistencia": 0.15,
        "validade": 0.15,
        "integridade": 0.15,
        "freshness": 0.10,
    }
    value = sum(dim_scores[k] * weights[k] for k in weights)
    return round(clamp_0_10(value), 4)


# =========================================================
# REGRAS
# =========================================================
def load_rules_map(rules_path: str | None) -> dict[str, Any]:
    if not rules_path:
        return {}
    path = Path(rules_path)
    if not path.exists():
        print(f"[WARN] Arquivo de regras não encontrado: {rules_path}")
        return {}
    cfg = load_yaml(str(path))
    return cfg.get("datasets") or {}


def evaluate_single_rule(series: pd.Series, rule_name: str, rule_value: Any) -> tuple[bool, float, str]:
    s = series
    nn = s.dropna()

    try:
        if rule_name == "not_null":
            rate = float((~s.isna()).mean()) if len(s) else 0.0
            passed = (rate == 1.0) if bool(rule_value) else True
            return passed, rate, f"preenchimento={rate:.4f}"

        if rule_name == "unique":
            if len(nn) == 0:
                ratio = 0.0
            else:
                ratio = float(nn.nunique(dropna=True) / len(nn))
            passed = (ratio == 1.0) if bool(rule_value) else True
            return passed, ratio, f"unicidade={ratio:.4f}"

        if rule_name == "regex":
            if len(nn) == 0:
                return True, 1.0, "sem valores não nulos"
            pattern = re.compile(str(rule_value))
            ok = nn.astype(str).map(lambda x: bool(pattern.match(x))).mean()
            return bool(ok == 1.0), float(ok), f"aderência_regex={ok:.4f}"

        if rule_name == "range":
            if len(nn) == 0:
                return True, 1.0, "sem valores não nulos"
            num = pd.to_numeric(nn, errors="coerce")
            ok_mask = num.notna()
            minv = rule_value.get("min") if isinstance(rule_value, dict) else None
            maxv = rule_value.get("max") if isinstance(rule_value, dict) else None
            if minv is not None:
                ok_mask &= num >= minv
            if maxv is not None:
                ok_mask &= num <= maxv
            score = float(ok_mask.mean()) if len(ok_mask) else 0.0
            return bool(score == 1.0), score, f"aderência_range={score:.4f}"

        if rule_name == "allowed_values":
            if len(nn) == 0:
                return True, 1.0, "sem valores não nulos"
            allowed = set(map(str, rule_value or []))
            score = float(nn.astype(str).map(lambda x: x in allowed).mean()) if len(nn) else 0.0
            return bool(score == 1.0), score, f"aderência_allowed={score:.4f}"

        return True, 1.0, f"regra_ignorada={rule_name}"
    except Exception as e:
        return False, 0.0, f"erro_regra={rule_name}: {e}"


def evaluate_rules_for_dataset(df: pd.DataFrame, dataset_name: str, rules_map: dict[str, Any]) -> list[dict[str, Any]]:
    dataset_rules = rules_map.get(dataset_name) or rules_map.get(Path(dataset_name).name) or {}
    columns_cfg = dataset_rules.get("columns") or {}
    out: list[dict[str, Any]] = []

    for col_name, col_rules in columns_cfg.items():
        if col_name not in df.columns:
            out.append(
                {
                    "column_name": col_name,
                    "rule_name": "column_exists",
                    "rule_value": "required",
                    "passed": False,
                    "score": 0.0,
                    "details": "coluna não encontrada no dataset",
                }
            )
            continue

        for rule_name, rule_value in (col_rules or {}).items():
            passed, score, details = evaluate_single_rule(df[col_name], rule_name, rule_value)
            out.append(
                {
                    "column_name": col_name,
                    "rule_name": rule_name,
                    "rule_value": json.dumps(rule_value, ensure_ascii=False) if isinstance(rule_value, (dict, list)) else safe_str(rule_value),
                    "passed": bool(passed),
                    "score": round(float(score), 4),
                    "details": details,
                }
            )
    return out


# =========================================================
# DUCKDB
# =========================================================
def ensure_duckdb_tables(con: duckdb.DuckDBPyConnection, stg_schema: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(stg_schema)};")

    # Recria as tabelas universais para evitar conflitos com esquemas antigos
    # já existentes no DuckDB. Como são tabelas de staging, a recriação é segura
    # e evita erros do tipo: "table has 10 columns but 17 values were supplied".
    targets_sql = [
        f'DROP TABLE IF EXISTS {qident(stg_schema)}.dq_table_metrics_u;',
        f'DROP TABLE IF EXISTS {qident(stg_schema)}.dq_column_metrics_u;',
        f'DROP TABLE IF EXISTS {qident(stg_schema)}.dq_table_scores_u;',
        f'DROP TABLE IF EXISTS {qident(stg_schema)}.dq_column_scores_u;',
        f'DROP TABLE IF EXISTS {qident(stg_schema)}.dq_table_scores_u_rules;',
        f"""
        CREATE TABLE {qident(stg_schema)}.dq_table_metrics_u (
            run_id         VARCHAR,
            scanned_at     TIMESTAMP,
            source_type    VARCHAR,
            source         VARCHAR,
            source_name    VARCHAR,
            source_ref     VARCHAR,
            object_name    VARCHAR,
            dataset_name   VARCHAR,
            dataset        VARCHAR,
            table_name     VARCHAR,
            file_name      VARCHAR,
            sheet_name     VARCHAR,
            sample_rows    BIGINT,
            columns        BIGINT,
            null_cells     BIGINT,
            total_cells    BIGINT,
            null_rate      DOUBLE
        );
        """,
        f"""
        CREATE TABLE {qident(stg_schema)}.dq_column_metrics_u (
            run_id          VARCHAR,
            scanned_at      TIMESTAMP,
            source_type     VARCHAR,
            source          VARCHAR,
            source_name     VARCHAR,
            source_ref      VARCHAR,
            object_name     VARCHAR,
            dataset_name    VARCHAR,
            dataset         VARCHAR,
            table_name      VARCHAR,
            file_name       VARCHAR,
            sheet_name      VARCHAR,
            column_name     VARCHAR,
            dtype           VARCHAR,
            total           BIGINT,
            nulls           BIGINT,
            null_rate       DOUBLE,
            distinct_cnt    BIGINT,
            distinct_ratio  DOUBLE,
            min_value       VARCHAR,
            max_value       VARCHAR,
            avg_len         DOUBLE,
            max_len         BIGINT
        );
        """,
        f"""
        CREATE TABLE {qident(stg_schema)}.dq_table_scores_u (
            run_id          VARCHAR,
            scanned_at      TIMESTAMP,
            source_type     VARCHAR,
            source          VARCHAR,
            source_name     VARCHAR,
            source_ref      VARCHAR,
            object_name     VARCHAR,
            dataset_name    VARCHAR,
            dataset         VARCHAR,
            table_name      VARCHAR,
            file_name       VARCHAR,
            sheet_name      VARCHAR,
            row_count       BIGINT,
            column_count    BIGINT,
            score           DOUBLE,
            classification  VARCHAR,
            completude      DOUBLE,
            unicidade       DOUBLE,
            consistencia    DOUBLE,
            validade        DOUBLE,
            integridade     DOUBLE,
            freshness       DOUBLE,
            dimension_count BIGINT
        );
        """,
        f"""
        CREATE TABLE {qident(stg_schema)}.dq_column_scores_u (
            run_id          VARCHAR,
            scanned_at      TIMESTAMP,
            source_type     VARCHAR,
            source          VARCHAR,
            source_name     VARCHAR,
            source_ref      VARCHAR,
            object_name     VARCHAR,
            dataset_name    VARCHAR,
            dataset         VARCHAR,
            table_name      VARCHAR,
            file_name       VARCHAR,
            sheet_name      VARCHAR,
            column_name     VARCHAR,
            dtype           VARCHAR,
            score           DOUBLE,
            classification  VARCHAR,
            completude      DOUBLE,
            unicidade       DOUBLE,
            consistencia    DOUBLE,
            validade        DOUBLE,
            integridade     DOUBLE,
            freshness       DOUBLE,
            total           BIGINT,
            nulls           BIGINT,
            null_rate       DOUBLE,
            distinct_cnt    BIGINT,
            distinct_ratio  DOUBLE
        );
        """,
        f"""
        CREATE TABLE {qident(stg_schema)}.dq_table_scores_u_rules (
            run_id          VARCHAR,
            scanned_at      TIMESTAMP,
            source_type     VARCHAR,
            source          VARCHAR,
            source_name     VARCHAR,
            source_ref      VARCHAR,
            object_name     VARCHAR,
            dataset_name    VARCHAR,
            dataset         VARCHAR,
            table_name      VARCHAR,
            file_name       VARCHAR,
            sheet_name      VARCHAR,
            column_name     VARCHAR,
            rule_name       VARCHAR,
            rule_value      VARCHAR,
            passed          BOOLEAN,
            rule_score      DOUBLE,
            details         VARCHAR
        );
        """,
    ]

    for sql in targets_sql:
        con.execute(sql)


def insert_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, target: str) -> None:
    if df.empty:
        return

    target_clean = target.replace('"', '')
    target_cols_df = con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = {sql_str(target_clean.split('.')[0])} AND table_name = {sql_str(target_clean.split('.')[1])} ORDER BY ordinal_position").df()
    target_cols = target_cols_df['column_name'].tolist()
    if not target_cols:
        raise RuntimeError(f"Tabela alvo não encontrada: {target}")

    df2 = df.copy()
    for col in target_cols:
        if col not in df2.columns:
            df2[col] = None
    df2 = df2[target_cols]

    con.register("tmp_df", df2)
    cols_sql = ", ".join(qident(c) for c in target_cols)
    con.execute(f"INSERT INTO {target} ({cols_sql}) SELECT {cols_sql} FROM tmp_df")
    con.unregister("tmp_df")


# =========================================================
# MAIN
# =========================================================
def build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Scanner universal de fontes para Data Quality")
    ap.add_argument("--sources", required=True, help="Arquivo YAML com as fontes")
    ap.add_argument("--duckdb", required=True, help="Arquivo DuckDB de saída")
    ap.add_argument("--stg", default="stg", help="Schema no DuckDB")
    ap.add_argument("--rules", default="", help="Arquivo de regras YAML")
    ap.add_argument("--limit", type=int, default=50000, help="Limite de linhas por dataset")
    ap.add_argument("--run-id", default=None, help="Run ID opcional")
    return ap.parse_args()


def main() -> None:
    args = build_args()
    run_id = args.run_id or now_run_id()
    scanned_at = dt.datetime.now()

    sources_cfg = load_yaml(args.sources)
    rules_map = load_rules_map(args.rules)

    datasets = collect_all_datasets(sources_cfg, args.limit)
    if not datasets:
        print("[WARN] Nenhum dataset encontrado para scanning.")
        return

    dcon = duckdb.connect(args.duckdb)
    ensure_duckdb_tables(dcon, args.stg)

    table_metrics_rows: list[dict[str, Any]] = []
    column_metrics_rows: list[dict[str, Any]] = []
    table_score_rows: list[dict[str, Any]] = []
    column_score_rows: list[dict[str, Any]] = []
    rule_rows: list[dict[str, Any]] = []

    for dataset_label, df, meta in datasets:
        sample_rows, columns, null_cells, total_cells, null_rate = table_metrics(df)
        dims = build_dimension_scores(df)
        score = overall_score(dims)
        classification = classify_score(score)

        common = {
            "run_id": run_id,
            "scanned_at": scanned_at,
            "source_type": safe_str(meta.get("source_type")),
            "source": safe_str(meta.get("source")),
            "source_name": safe_str(meta.get("source_name")),
            "source_ref": safe_str(meta.get("source_ref")),
            "object_name": safe_str(meta.get("object_name")) or dataset_label,
            "dataset_name": safe_str(meta.get("dataset_name")) or dataset_label,
            "dataset": safe_str(meta.get("dataset_name")) or dataset_label,
            "table_name": safe_str(meta.get("table_name")),
            "file_name": safe_str(meta.get("file_name")),
            "sheet_name": safe_str(meta.get("sheet_name")),
        }

        table_metrics_rows.append(
            {
                **common,
                "sample_rows": sample_rows,
                "columns": columns,
                "null_cells": null_cells,
                "total_cells": total_cells,
                "null_rate": null_rate,
            }
        )

        table_score_rows.append(
            {
                **common,
                "row_count": sample_rows,
                "column_count": columns,
                "score": score,
                "classification": classification,
                "completude": dims["completude"],
                "unicidade": dims["unicidade"],
                "consistencia": dims["consistencia"],
                "validade": dims["validade"],
                "integridade": dims["integridade"],
                "freshness": dims["freshness"],
                "dimension_count": 6,
            }
        )

        for col in df.columns:
            cm = column_metrics(df, col)
            col_dims = {
                "completude": round(clamp_0_10(10 * (1 - cm["null_rate"])), 4),
                "unicidade": round(clamp_0_10(10 * cm["distinct_ratio"]), 4),
                "consistencia": 10.0,
                "validade": 10.0 if cm["total"] == 0 or cm["nulls"] < cm["total"] else 0.0,
                "integridade": round(clamp_0_10((10 * (1 - cm["null_rate"]) + 10 * cm["distinct_ratio"]) / 2), 4),
                "freshness": 5.0,
            }
            col_score = overall_score(col_dims)
            col_class = classify_score(col_score)

            column_metrics_rows.append({**common, **cm})
            column_score_rows.append(
                {
                    **common,
                    "column_name": cm["column_name"],
                    "dtype": cm["dtype"],
                    "score": col_score,
                    "classification": col_class,
                    "completude": col_dims["completude"],
                    "unicidade": col_dims["unicidade"],
                    "consistencia": col_dims["consistencia"],
                    "validade": col_dims["validade"],
                    "integridade": col_dims["integridade"],
                    "freshness": col_dims["freshness"],
                    "total": cm["total"],
                    "nulls": cm["nulls"],
                    "null_rate": cm["null_rate"],
                    "distinct_cnt": cm["distinct_cnt"],
                    "distinct_ratio": cm["distinct_ratio"],
                }
            )

        dataset_rules = evaluate_rules_for_dataset(df, common["dataset_name"], rules_map)
        for rr in dataset_rules:
            rule_rows.append(
                {
                    **common,
                    "column_name": rr["column_name"],
                    "rule_name": rr["rule_name"],
                    "rule_value": rr["rule_value"],
                    "passed": rr["passed"],
                    "rule_score": rr["score"],
                    "details": rr["details"],
                }
            )

        print(f"[SCAN] {common['source_type']} -> {common['dataset_name']} rows={sample_rows} cols={columns}")

    target_schema = qident(args.stg)
    insert_df(dcon, pd.DataFrame(table_metrics_rows), f"{target_schema}.dq_table_metrics_u")
    insert_df(dcon, pd.DataFrame(column_metrics_rows), f"{target_schema}.dq_column_metrics_u")
    insert_df(dcon, pd.DataFrame(table_score_rows), f"{target_schema}.dq_table_scores_u")
    insert_df(dcon, pd.DataFrame(column_score_rows), f"{target_schema}.dq_column_scores_u")
    insert_df(dcon, pd.DataFrame(rule_rows), f"{target_schema}.dq_table_scores_u_rules")
    dcon.close()

    print(f"[OK] Scan concluído. run_id={run_id}")
    print(f"[OK] DuckDB: {args.duckdb}")
    print(f"[OK] Tabelas atualizadas em {args.stg}:")
    print("     - dq_table_metrics_u")
    print("     - dq_column_metrics_u")
    print("     - dq_table_scores_u")
    print("     - dq_column_scores_u")
    print("     - dq_table_scores_u_rules")


if __name__ == "__main__":
    main()
