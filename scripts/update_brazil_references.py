# -*- coding: utf-8 -*-
"""Atualiza a referência de municípios via API oficial de localidades do IBGE."""
from __future__ import annotations

import argparse
import json
import unicodedata
import urllib.request
from pathlib import Path

import pandas as pd

IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


def normalize(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(c for c in text if not unicodedata.combining(c))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="data/reference")
    args = ap.parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    with urllib.request.urlopen(IBGE_URL, timeout=60) as response:
        payload = json.load(response)

    rows = []
    for item in payload:
        region = item.get("microrregiao") or {}
        meso = region.get("mesorregiao") or {}
        uf = meso.get("UF") or {}
        if not uf:
            immediate = item.get("regiao-imediata") or {}
            intermediate = immediate.get("regiao-intermediaria") or {}
            uf = intermediate.get("UF") or {}
        rows.append({
            "codigo_ibge": str(item.get("id") or ""),
            "municipio": item.get("nome") or "",
            "uf": uf.get("sigla") or "",
            "municipio_normalizado": normalize(item.get("nome") or ""),
        })
    df = pd.DataFrame(rows).drop_duplicates(["codigo_ibge"])
    target = outdir / "brasil_municipios.csv"
    df.to_csv(target, index=False, encoding="utf-8")
    print(f"[OK] {target}: {len(df)} municípios")


if __name__ == "__main__":
    main()
