# -*- coding: utf-8 -*-
import os, re, glob, argparse, hashlib, warnings
from datetime import datetime, timezone
import pandas as pd
import duckdb

# ----- Config -----
WEIGHTS = {
  "completude": 0.20,
  "unicidade": 0.15,
  "validade": 0.15,
  "consistencia": 0.20,
  "integridade": 0.15,
  "freshness": 0.15
}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# silenciar warnings chatos
warnings.filterwarnings("ignore", message="Workbook contains no default style")
warnings.filterwarnings("ignore", message="Could not infer format")
warnings.filterwarnings("ignore", message="Parsing dates in %Y-%m-%d %H:%M:%S format")


def sanitize_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    if not name:
        name = "obj"
    if name[0].isdigit():
        name = f"s_{name}"
    return name


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def detect_and_load_excel(path: str, sheet_name=0) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype_backend="numpy_nullable")
    df = df.dropna(how="all")

    # Se vier 1 coluna com "csv colado" -> split em 4 partes
    if df.shape[1] == 1:
        s = df.iloc[:, 0].dropna().astype(str)
        comma_counts = s.str.count(",")
        if len(comma_counts) and (comma_counts >= 3).mean() >= 0.6:
            parts = s.str.split(",", n=3, expand=True)
            while parts.shape[1] < 4:
                parts[parts.shape[1]] = None
            parts.columns = ["col1", "col2", "col3", "col4"]
            df = parts

    df.columns = [sanitize_name(c) for c in df.columns]
    return df


def load_csv(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype_backend="numpy_nullable")
    except Exception:
        df = pd.read_csv(path, sep=";", dtype_backend="numpy_nullable")

    df = df.dropna(how="all")
    df.columns = [sanitize_name(c) for c in df.columns]
    return df


def clamp_0_10(x: float) -> float:
    return max(0.0, min(10.0, float(x)))


def list_cols(con, table):
    return [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]


def try_parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True, dayfirst=True)


def ensure_dq_runs_schema(con: duckdb.DuckDBPyConnection):
    """
    Garante que a tabela dq_runs existe com a coluna file_hash.
    Se existir sem file_hash (versão antiga), recria mantendo o banco.
    """
    con.execute("""
    CREATE TABLE IF NOT EXISTS dq_runs (
      run_ts TIMESTAMP,
      source_file VARCHAR,
      file_hash VARCHAR,
      table_name VARCHAR,
      total_registros BIGINT,
      completude DOUBLE,
      unicidade DOUBLE,
      validade DOUBLE,
      consistencia DOUBLE,
      integridade DOUBLE,
      freshness DOUBLE,
      nota_final DOUBLE
    )
    """)

    cols = [r[0] for r in con.execute("DESCRIBE dq_runs").fetchall()]
    if "file_hash" not in cols:
        # tabela antiga: recria
        con.execute("DROP TABLE IF EXISTS dq_runs")
        con.execute("""
        CREATE TABLE dq_runs (
          run_ts TIMESTAMP,
          source_file VARCHAR,
          file_hash VARCHAR,
          table_name VARCHAR,
          total_registros BIGINT,
          completude DOUBLE,
          unicidade DOUBLE,
          validade DOUBLE,
          consistencia DOUBLE,
          integridade DOUBLE,
          freshness DOUBLE,
          nota_final DOUBLE
        )
        """)


def compute_scores_generic(con: duckdb.DuckDBPyConnection, table: str):
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    cols = list_cols(con, table)
    if total == 0 or not cols:
        return total, {k: 0.0 for k in WEIGHTS.keys()}, 0.0

    sample_n = min(5000, total)
    df = con.execute(f"SELECT * FROM {table} USING SAMPLE {sample_n} ROWS").fetchdf()

    # 1) COMPLETUDE
    nonnull_rates = []
    for c in cols:
        nonnull = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {c} IS NOT NULL").fetchone()[0]
        nonnull_rates.append(nonnull / total)
    score_completude = clamp_0_10(10 * (sum(nonnull_rates) / len(nonnull_rates)))

    # 2) UNICIDADE (linha inteira)
    concat_expr = " || '|' || ".join([f"COALESCE(CAST({c} AS VARCHAR),'')" for c in cols])
    distinct_rows = con.execute(f"SELECT COUNT(DISTINCT ({concat_expr})) FROM {table}").fetchone()[0]
    dup_rate = 1 - (distinct_rows / total)
    score_unicidade = clamp_0_10(10 * (1 - dup_rate))

    # 3) VALIDADE (heurística)
    validity_parts = []
    for c in df.columns:
        s = df[c]

        # email
        if "email" in c:
            ss = s.dropna().astype(str).str.strip()
            if len(ss) > 0:
                ok = ss.apply(lambda x: bool(EMAIL_RE.match(x))).mean()
                validity_parts.append(ok)

        # data (por nome)
        if any(t in c for t in ["data", "dt", "date", "updated", "update", "created"]):
            ss = s.dropna()
            if len(ss) > 0:
                parsed = try_parse_datetime(ss.astype(str))
                ok = parsed.notna().mean()
                validity_parts.append(ok)

        # numérico (heurística)
        if s.dropna().astype(str).str.contains(r"\d", regex=True).mean() >= 0.8:
            ss = s.dropna().astype(str).str.strip()
            if len(ss) > 0:
                def to_num(x):
                    x = x.replace("%", "").strip()
                    x2 = x.replace(".", "").replace(",", ".")
                    try:
                        float(x2)
                        return True
                    except:
                        return False
                ok = ss.apply(to_num).mean()
                validity_parts.append(ok)

    score_validade = 10.0 if not validity_parts else clamp_0_10(10 * (sum(validity_parts) / len(validity_parts)))

    # 4) CONSISTÊNCIA (mistura de tipo)
    consistency_parts = []
    for c in df.columns:
        s = df[c].dropna()
        if len(s) == 0:
            continue

        s_str = s.astype(str).str.strip()

        def num_ok(x):
            x = x.replace("%", "").strip()
            x2 = x.replace(".", "").replace(",", ".")
            try:
                float(x2); return True
            except:
                return False

        is_email = ("email" in c) and s_str.apply(lambda x: bool(EMAIL_RE.match(x))).mean() > 0.5
        is_date = try_parse_datetime(s_str).notna().mean() > 0.7
        is_num = s_str.apply(num_ok).mean() > 0.7

        if is_email: typ = "email"
        elif is_date: typ = "date"
        elif is_num: typ = "num"
        else: typ = "text"

        if typ == "email":
            flags = s_str.apply(lambda x: "email" if EMAIL_RE.match(x) else "other")
        elif typ == "date":
            flags = try_parse_datetime(s_str).notna().map(lambda ok: "date" if ok else "other")
        elif typ == "num":
            flags = s_str.apply(lambda x: "num" if num_ok(x) else "other")
        else:
            flags = s_str.apply(lambda x: "text" if x != "" else "other")

        dom = flags.value_counts(normalize=True).iloc[0]
        consistency_parts.append(dom)

    score_consistencia = 10.0 if not consistency_parts else clamp_0_10(10 * (sum(consistency_parts) / len(consistency_parts)))

    # 5) INTEGRIDADE (proxy por colunas id/codigo/cd)
    integ_parts = []
    for c in df.columns:
        if any(k in c for k in ["id", "codigo", "cd", "cod"]):
            s = df[c]
            nn = s.notna().mean()
            s_str = s.dropna().astype(str).str.strip()
            if len(s_str) == 0:
                continue
            alnum_rate = s_str.str.match(r"^[A-Za-z0-9\-_]+$").mean()
            integ_parts.append(0.7 * nn + 0.3 * alnum_rate)

    score_integridade = 10.0 if not integ_parts else clamp_0_10(10 * (sum(integ_parts) / len(integ_parts)))

    # 6) FRESHNESS (se achar coluna de data)
    freshness_score = 0.0
    date_cols = [c for c in df.columns if any(t in c for t in ["data","dt","date","updated","update","created"])]

    best_col = None
    best_ok = 0.0
    for c in date_cols:
        ss = df[c].dropna().astype(str)
        if len(ss) == 0:
            continue
        ok = try_parse_datetime(ss).notna().mean()
        if ok > best_ok:
            best_ok = ok
            best_col = c

    if best_col and best_ok >= 0.7:
        parsed = try_parse_datetime(df[best_col].dropna().astype(str)).dropna()
        if len(parsed) > 0:
            last_ts = parsed.max().to_pydatetime()
            now = datetime.now(timezone.utc)
            age_hours = (now - last_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600.0
            sla = 24.0  # default
            freshness_score = clamp_0_10(10 * (1 - age_hours / sla))

    scores = {
        "completude": round(score_completude, 2),
        "unicidade": round(score_unicidade, 2),
        "validade": round(score_validade, 2),
        "consistencia": round(score_consistencia, 2),
        "integridade": round(score_integridade, 2),
        "freshness": round(freshness_score, 2),
    }

    nota_final = 0.0
    for k, w in WEIGHTS.items():
        nota_final += scores[k] * w
    nota_final = round(clamp_0_10(nota_final), 2)

    return total, scores, nota_final


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inbox", default="./data/inbox")
    ap.add_argument("--db", default="dq_lab.duckdb")
    ap.add_argument("--schema", default="stg")
    ap.add_argument("--mode", default="skip", choices=["skip", "append", "replace"],
                    help="skip=nao reprocessa se hash igual | append=sempre roda e guarda historico | replace=mantem so a ultima execucao por arquivo")
    args = ap.parse_args()

    os.makedirs(args.inbox, exist_ok=True)

    con = duckdb.connect(args.db)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
    ensure_dq_runs_schema(con)

    files = sorted(
        glob.glob(os.path.join(args.inbox, "*.xlsx")) +
        glob.glob(os.path.join(args.inbox, "*.csv"))
    )

    if not files:
        print(f"⚠️ Nenhum arquivo encontrado em {args.inbox}")
        return

    processed = 0
    skipped = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        base = sanitize_name(os.path.splitext(fname)[0])
        table = f"{args.schema}.src_{base}"

        fhash = sha256_file(fpath)

        # ----- MODE behavior -----
        if args.mode == "skip":
            already = con.execute(
                "SELECT COUNT(*) FROM dq_runs WHERE source_file = ? AND file_hash = ?",
                [fname, fhash]
            ).fetchone()[0]
            if already > 0:
                print(f"⏭️  Pulando (sem mudanças): {fname}")
                skipped += 1
                continue

        if args.mode == "replace":
            con.execute("DELETE FROM dq_runs WHERE source_file = ?", [fname])

        print(f"\n=== Processando: {fname} ===")

        if fname.lower().endswith(".xlsx"):
            df = detect_and_load_excel(fpath, sheet_name=0)
        else:
            df = load_csv(fpath)

        con.register("df_in", df)
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_in")

        total, scores, nota = compute_scores_generic(con, table)

        con.execute(
            "INSERT INTO dq_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                datetime.utcnow(),
                fname,
                fhash,
                table,
                total,
                scores["completude"],
                scores["unicidade"],
                scores["validade"],
                scores["consistencia"],
                scores["integridade"],
                scores["freshness"],
                nota
            ]
        )

        processed += 1
        print(f"✅ tabela: {table} | rows={total} | nota={nota}")
        print("   ", scores)

    # ----- Relatório final: última execução por arquivo -----
    print("\n===== RESUMO (última execução por arquivo) =====")
    summary_df = con.execute("""
      SELECT r.*
      FROM dq_runs r
      JOIN (
        SELECT source_file, MAX(run_ts) AS max_ts
        FROM dq_runs
        GROUP BY source_file
      ) m
      ON r.source_file = m.source_file AND r.run_ts = m.max_ts
      ORDER BY r.run_ts DESC
    """).fetchdf()

    print(summary_df[[
        "run_ts","source_file","total_registros",
        "completude","unicidade","validade","consistencia","integridade","freshness","nota_final"
    ]])

    print(f"\nProcessados: {processed} | Pulados: {skipped} | Total na inbox: {len(files)}")
    con.close()


if __name__ == "__main__":
    main()