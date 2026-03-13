# -*- coding: utf-8 -*-
import re
import argparse
import pandas as pd
import duckdb

def sanitize_name(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"s_{name}"
    return name

def split_single_column_if_csvlike(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se a planilha vier como 1 coluna com strings contendo vírgulas,
    quebra em colunas separadas automaticamente.
    """
    if df.shape[1] != 1:
        return df

    col = df.columns[0]
    s = df[col].dropna().astype(str)

    # heurística: Validação das linhas tem + vírgulas, é "csv colado"
    comma_counts = s.str.count(",")
    if len(comma_counts) == 0:
        return df

    if (comma_counts >= 3).mean() < 0.6:
        return df

    # split em até > 4 partes 
    parts = s.str.split(",", n=3, expand=True)

    # algumas linhas podem ter menos colunas; completa
    while parts.shape[1] < 4:
        parts[parts.shape[1]] = None

    parts.columns = ["unidade", "produto", "tipo", "valor_raw"]
    out = parts

    # limpeza
    for c in ["unidade", "produto", "tipo"]:
        out[c] = out[c].astype("string").str.strip()

    out["valor_raw"] = out["valor_raw"].astype("string").str.strip()

    # tenta converter valor para número (suporta %)
    def parse_val(x):
        if x is None:
            return None
        x = str(x).strip()
        if x == "":
            return None
        is_pct = x.endswith("%")
        x2 = x.replace("%", "").replace(".", "").replace(",", ".")  # PT-BR -> float
        try:
            v = float(x2)
            if is_pct:
                return v / 100.0
            return v
        except:
            return None

    out["valor"] = out["valor_raw"].map(parse_val)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Caminho do .xlsx (ex: ./data/arquivo.xlsx)")
    ap.add_argument("--db", default="dq_lab.duckdb")
    ap.add_argument("--schema", default="stg")
    ap.add_argument("--prefix", default="xl_")
    ap.add_argument("--mode", choices=["replace", "append"], default="replace")
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")

    xls = pd.ExcelFile(args.file)
    print(f"📄 Excel: {args.file}")
    print(f"🧾 Abas: {xls.sheet_names}")

    for sheet in xls.sheet_names:
        df = pd.read_excel(args.file, sheet_name=sheet, header=None, dtype_backend="numpy_nullable")

        # remove linhas totalmente vazias do arquivo em excel
        df = df.dropna(how="all")

        # autodetect: se for 1 coluna com vírgulas, quebra
        df = split_single_column_if_csvlike(df)

        # se ainda tiver colunas sem nome (caso não tenha split), nomeia
        df.columns = [sanitize_name(c) for c in df.columns]

        table = f"{args.schema}.{args.prefix}{sanitize_name(sheet)}"

        con.register("df_in", df)

        if args.mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_in")
        else:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df_in WHERE 1=0")
            con.execute(f"INSERT INTO {table} SELECT * FROM df_in")

        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"✅ '{sheet}' -> {table} (rows={total}, cols={len(df.columns)})")
        print("   colunas:", list(df.columns))

    con.close()
    print("🎉 Ingestão concluída.")

if __name__ == "__main__":
    main()