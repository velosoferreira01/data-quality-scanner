
# Data Quality v2 Integrado

## Arquivos
- `export_data_quality_report_v2_integrated.py`
- `streamlit_app_mjv.py`
- `ai_recommendation_engine.py`
- `ddl_history.sql`

## Onde colocar
Você informou que a pasta está em:
`C:\Users\Daniel\Downloads\Data_Quality\data\data_quality_v2_package`

Sugestão:
1. Copie `export_data_quality_report_v2_integrated.py` para a raiz do projeto `Data_Quality`
2. Mantenha `streamlit_app_mjv.py` dentro da pasta `data_quality_v2_package` ou mova para a raiz
3. Garanta que o arquivo original `export_data_quality_report.py` continue na raiz do projeto

## Como executar o relatório v2
Na raiz do projeto:
```bash
python export_data_quality_report_v2_integrated.py
```

## O que ele gera
- Relatório Excel premium
- Relatório HTML premium
- Radar Chart HTML em Plotly
- Histórico HTML em Plotly
- CSV com score por dimensão
- CSV com recomendações IA
- Persistência em `dq_history` dentro do mesmo DuckDB

## Como abrir o dashboard Streamlit
```bash
streamlit run streamlit_app_mjv.py
```

## Dependências
```bash
pip install plotly streamlit duckdb pandas openpyxl matplotlib
```

## Observações
- A integração reaproveita o `export_data_quality_report.py` atual
- O cálculo por dimensão usa heurística robusta com fallback quando certas colunas não existem
- A priorização automática classifica recomendações em Baixa, Média, Alta e Imediata
