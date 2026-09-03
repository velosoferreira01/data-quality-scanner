###  ######################################################################################  ###
###                                 Data Quality Scanner - MJV                               ###
###                     Scanning de análise  de qualidade de dados - MJV.                    ###
###  ######################################################################################  ###

# Extensão BACEN — Resolução Conjunta nº 18/2025

Esta versão preserva o scanner técnico e acrescenta uma camada regulatória com as 12 dimensões de qualidade da informação.

## Arquivos novos

- `config/bacen_dimensions.yml`: pesos, metas, tipos de avaliação e evidências manuais.
- `src/scoring/16_compute_bacen_scores.py`: cálculo ponderado das 12 dimensões.

## Tabelas novas no DuckDB

- `stg.dq_bacen_dimension_scores`: uma linha por dataset e dimensão.
- `stg.dq_bacen_summary`: score BACEN consolidado por dataset.

## Execução

O comando continua igual:

```powershell
python app.py
```

O pipeline executará o scanner, calculará o score BACEN e atualizará os relatórios V2.

Para executar apenas o cálculo BACEN:

```powershell
python src/scoring/16_compute_bacen_scores.py `
  --duckdb .\dq_lab.duckdb `
  --stg stg `
  --config .\config\bacen_dimensions.yml
```

## Política para dimensões sem evidência

Uma dimensão sem controle ou evidência é marcada como `Não avaliado`. Ela não recebe nota zero e não entra no denominador do score aplicável. O relatório informa `coverage_percent`, permitindo distinguir score alto com cobertura parcial de avaliação completa.

## Evidências de governança

As dimensões Acessibilidade, Adaptabilidade, Clareza e Relevância dependem de evidências. Elas podem ser preenchidas em `config/bacen_dimensions.yml`:

```yaml
governance_assessments:
  clientes.csv:
    acessibilidade:
      score: 8.0
      evidence: "Matriz de perfis aprovada pelo Data Owner"
    clareza:
      score: 9.0
      evidence: "Dicionário de dados versão 3"
```

## Pesos iniciais

Os pesos totalizam 100% e são configuráveis. Acurácia, Completude, Confiabilidade, Consistência e Integridade possuem maior participação inicial. Antes do uso regulatório formal, os pesos e critérios devem ser homologados pelas áreas de Riscos, Compliance, Governança de Dados e Auditoria.
