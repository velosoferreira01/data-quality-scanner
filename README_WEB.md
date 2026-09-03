###  ######################################################################################  ###
###                                 Data Quality Scanner - MJV                               ###
###                     Scanning de análise  de qualidade de dados - MJV.                    ###
###  ######################################################################################  ###

## Onde colocar

Copie `web_app.py` para a raiz do projeto:

```text
C:\Users\USUARIO\data-quality-scanner\web_app.py
```

Copie `config.toml` para:

```text
C:\Users\USUARIO\data-quality-scanner\.streamlit\config.toml
```

## Instalação

No PowerShell, com a `.venv` ativada:

```powershell
pip install -r requirements_web.txt
python -m playwright install chromium
```

## Execução

Na raiz do projeto:

```powershell
python -m streamlit run web_app.py
```

A aplicação será aberta no navegador, normalmente em:

```text
http://localhost:8501
```

## Pastas criadas automaticamente

```text
data/jobs/<run_id>/input
data/jobs/<run_id>/dq_lab.duckdb
output/jobs/<run_id>
```

Cada execução possui arquivos de entrada, banco DuckDB e relatórios separados.
