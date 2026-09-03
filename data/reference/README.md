# Referências brasileiras

- `brasil_ufs.csv`: lista das 27 unidades federativas.
- `brasil_municipios.csv`: gerado localmente pelo comando:

```powershell
python scripts/update_brazil_references.py
```

O script consulta a API oficial de localidades do IBGE e grava código IBGE, município, UF e nome normalizado.

A validação completa de CEP por logradouro não está habilitada por padrão. Nesta versão, o CEP é validado estruturalmente (8 dígitos). Uma base de CEP ou API externa poderá ser adicionada posteriormente com cache e controle de volume.
