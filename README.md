###  ######################################################################################  ###
###                                 Data Quality Scanner - MJV                               ###
###                     Scanning de análise  de qualidade de dados - MJV.                    ###
###  ######################################################################################  ###

## Pré-requisitos
Antes de executar, tenha instalado:

- Python 3.11 ou superior
- pip
- Dependências do projeto em requirements.txt


Suporta múltiplas fontes:

- CSV
- Excel
- Parquet
- DuckDB
- PostgreSQL usa porta 5432
- MySQL/MariaDB usam porta 3306
- SQL Server usa porta 1433
- Oracle usa porta 1521
- SQLite e DuckDB usam caminho de arquivo


###  ######################################################################################  ###
###  #################################### PASSO A PASSO ###################################  ###
###  ######################################################################################  ###


##### Para instalar e executar no Windows:
## git clone https://github.com/MJV/data-quality-scanner.git
cd data-quality-scanner


##### Instalação
No terminal, dentro da pasta do projeto:
Execute
      - pip install -r requirements.txt
      - python -m pip install -r requirements.txt

##### Configuração

1) Arquivo config/config.yml
Use esse arquivo para as configurações gerais do projeto.

2) Arquivo -- config/arquivos sources.runtime e config.multibanco
   - sources.runtime.yml na linha inbox   ### Ajustar o diretório dos datasets para que leiam as tabelas ou arquivos
   - config.multibanco na linha input_dir ### Ajustar o diretório dos datasets para que leiam as tabelas ou arquivos
Nele você define quais arquivos, pastas ou bancos serão lidos pelo scanner da MJV.

3) Arquivo .env
Crie um .env 
Obs.: a partir do .env.example abaixo informe e configure apenas as variáveis que realmente for usar.
- exemplo:
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=meu_banco
   POSTGRES_USER=usuario
   POSTGRES_PASSWORD=senha


Criar o ambiente virtual

O ambiente virtual separa as bibliotecas do projeto das demais instalações do computador.

Na raiz do projeto:
Execute: py -3.11 -m venv .venv

Caso py não exista:
Execute: python -m venv .venv

Ativar o ambiente
.\.venv\Scripts\Activate.ps1

O terminal deverá mostrar:
(.venv) PS C:\...\data-quality-scanner>

4) Arquivo config/12_dq_rules.yml
Define as regras e pesos usados no cálculo de score de qualidade.

5)  Instalar as bibliotecas

Com o ambiente ativo:

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

Caso exista requirements_genai.txt:
Execute: python -m pip install -r requirements_genai.txt

Para conferir:
Execute: Test-Path .\requirements_genai.txt

Instale o navegador usado na geração de PDF:
Execute: python -m playwright install chromium

Confira as bibliotecas:
Execute: python -m pip list

Algumas bibliotecas esperadas:

pandas
duckdb
streamlit
matplotlib
openpyxl
plotly
python-dotenv

Arquivo .env

Crie a partir do exemplo:

Copy-Item .\.env.example .\.env

Caso não exista:

New-Item .\.env -ItemType File

Abra:

notepad .\.env

Configuração do Ollama:

MJV_GENAI_ENABLED=true
MJV_GENAI_PROVIDER=ollama
MJV_GENAI_MODEL=llama3.2:3b
MJV_OLLAMA_BASE_URL=http://localhost:11434
MJV_OLLAMA_TIMEOUT_SECONDS=420
MJV_OLLAMA_MAX_ATTEMPTS=2

Exemplo opcional para PostgreSQL:

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=meu_banco
POSTGRES_USER=usuario
POSTGRES_PASSWORD=senha


###  ######################################################################################  ###
###  ################## HABILITAR BANCO DE DADOS ##########################################  ###
###  ######################################################################################  ###
Como habilitar uma fonte de dados
A recomendação para os primeiros testes é habilitar apenas uma fonte por vez.

###  ######################################################################################  ###
###  ###################### EXECUÇÃO DO SCANNING ##########################################  ###
###  ######################################################################################  ###

# Executar em Venv 0. Executar as Dependencias: 
   
   python mjv_runner.py setup.mjv
   
   python mjv_runner.py run.mjv

### #######
### Caso queira executar manualmente sem .venv
# Execute no PowerShell:
   - python app.py

# Esse comando faz:
- Gera o sources.runtime.yml
- Executa o pipelines
- grava as métricas. No caso está gravando no banco container do BD DuckDB
- gera os relatórios na pasta output

# Saídas geradas
Após a execução, o projeto pode gerar arquivos como:
- Relatórios HTML
- Relatórios Excel
- Arquivos CSV (Auxiliar)
- Arquivos PDF
- Base DuckDB com métricas consolidadas

Exemplo:
output/
└── jobs/
    └── 20260730_132438_c4ce05/
        ├── dq_report_premium_mjv_v2_YYYYMMDD_HHMMSS.html
        ├── dq_report_premium_mjv_v2_YYYYMMDD_HHMMSS.xlsx
        ├── dq_executive_report_v2_YYYYMMDD_HHMMSS.html
        ├── dq_executive_report_v2_YYYYMMDD_HHMMSS.xlsx
        ├── dq_radar_chart.html
        ├── dq_history_chart.html
        ├── dq_current_detail.csv
        ├── dq_dimension_scores_current.csv
        ├── dq_bacen_summary_current.csv
        ├── dq_bacen_dimension_detail_current.csv
        ├── dq_ai_recommendations_current.csv
        └── dq_genai_analysis_current.json


###  ######################################################################################  ###
###  ############################### NOTAS ################################################  ###
###  ######################################################################################  ###
1) Banco local do scanner
O scanner mjv - salva os resultados no arquivo DuckDB:
- dq_lab.duckdb

2) Consultando os resultados no DuckDB
import duckdb

con = duckdb.connect("dq_lab.duckdb")

df = con.execute(\"\"\"
SELECT *
FROM stg.dq_table_scores_u_rules
ORDER BY score_final DESC
\"\"\").df()

print(df)

con.close()


3) Exemplo para listar tabelas do schema stg:

import duckdb

con = duckdb.connect("dq_lab.duckdb")

print(con.execute(\"\"\"
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'stg'
\"\"\").fetchall())

con.close()