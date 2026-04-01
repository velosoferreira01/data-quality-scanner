Arquivos gerados:

1. config.yml.
   Basta ativar enabled: true no banco desejado.

2. .env.multibanco.example
   Template de variáveis de ambiente.
   Copie para .env e ajuste apenas os valores necessários.

Observações:
- PostgreSQL usa porta 5432
- MySQL/MariaDB usam porta 3306
- SQL Server usa porta 1433
- Oracle usa porta 1521
- SQLite e DuckDB usam caminho de arquivo

Sugestão de uso:
- deixe apenas 1 banco enabled: true por vez no início dos testes

