###  ######################################################################################  ###
###                                 Data Quality Scanner - MJV                               ###
###                     Scanning de análise  de qualidade de dados - MJV.                    ###
###  ######################################################################################  ###

# Validações cadastrais brasileiras

## Escopo da primeira versão

- CPF: estrutura, rejeição de sequências repetidas e dígitos verificadores.
- CNPJ: estrutura, rejeição de sequências repetidas e dígitos verificadores.
- UF: sigla ou nome válido das 27 unidades federativas.
- Município + UF: validação cruzada com referência oficial de localidades do IBGE.
- Código IBGE: existência e, quando houver UF, coerência com a UF informada.
- CEP: validação estrutural de 8 dígitos. A consulta completa por logradouro fica desabilitada nesta etapa.

## Atualizar municípios

Com a `.venv` ativa e internet disponível:

```powershell
python scripts/update_brazil_references.py
```

Isso criará:

```text
data/reference/brasil_municipios.csv
```

## Configurar nomes de colunas

O scanner tenta detectar os campos automaticamente. Para nomes específicos, edite:

```text
config/semantic_validations.yml
```

Exemplo:

```yaml
datasets:
  cadastro_clientes.csv:
    columns:
      cpf: nr_cpf
      cnpj: nr_cnpj
      uf: sg_uf
      municipio: nm_municipio
      codigo_ibge: cod_ibge
      cep: nr_cep
```

## Execução

O comando permanece:

```powershell
python app.py
```

Novas tabelas DuckDB:

- `stg.dq_semantic_validation_results`
- `stg.dq_semantic_invalid_samples`

Novos CSVs de relatório:

- `output/dq_semantic_validation_current.csv`
- `output/dq_semantic_invalid_samples_current.csv`

CPF e CNPJ são mascarados nas amostras inválidas. O arquivo não confirma situação cadastral na Receita Federal; apenas validade matemática do documento.
