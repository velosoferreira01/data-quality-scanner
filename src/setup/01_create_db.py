import duckdb
import pandas as pd
from datetime import datetime, timedelta

db_path = "dq_lab.duckdb"
con = duckdb.connect(db_path)

# Reset
con.execute("DROP TABLE IF EXISTS dim_cliente;")
con.execute("DROP TABLE IF EXISTS fato_pedido;")

# Tabelas
con.execute("""
CREATE TABLE dim_cliente (
  id_cliente INTEGER PRIMARY KEY,
  nome       VARCHAR,
  email      VARCHAR,
  dt_cadastro TIMESTAMP
);
""")

con.execute("""
CREATE TABLE fato_pedido (
  id_pedido  INTEGER,
  id_cliente INTEGER,
  vl_total   DOUBLE,
  status     VARCHAR,
  dt_pedido  TIMESTAMP,
  dt_entrega TIMESTAMP,
  ingest_ts  TIMESTAMP
);
""")

now = datetime.now()

clientes = pd.DataFrame([
  {"id_cliente": 1, "nome": "Ana",   "email": "ana@email.com",   "dt_cadastro": now - timedelta(days=90)},
  {"id_cliente": 2, "nome": "Bruno", "email": "bruno@email.com", "dt_cadastro": now - timedelta(days=30)},
  {"id_cliente": 3, "nome": "Carla", "email": None,             "dt_cadastro": now - timedelta(days=10)},
])

pedidos = pd.DataFrame([
  {"id_pedido": 1001, "id_cliente": 1,   "vl_total": 120.50, "status": "PAGO",        "dt_pedido": now - timedelta(days=20), "dt_entrega": now - timedelta(days=18), "ingest_ts": now - timedelta(hours=1)},
  {"id_pedido": 1002, "id_cliente": 2,   "vl_total": 80.00,  "status": "PENDENTE",    "dt_pedido": now - timedelta(days=10), "dt_entrega": now - timedelta(days=5),  "ingest_ts": now - timedelta(hours=2)},
  {"id_pedido": 1003, "id_cliente": 3,   "vl_total": 50.00,  "status": "CANCELADO",   "dt_pedido": now - timedelta(days=3),  "dt_entrega": now - timedelta(days=2),  "ingest_ts": now - timedelta(hours=3)},

  # Duplicado (unicidade)
  {"id_pedido": 1003, "id_cliente": 3,   "vl_total": 50.00,  "status": "CANCELADO",   "dt_pedido": now - timedelta(days=3),  "dt_entrega": now - timedelta(days=2),  "ingest_ts": now - timedelta(hours=3)},

  # FK quebrada (cliente 999 não existe)
  {"id_pedido": 1004, "id_cliente": 999, "vl_total": 30.00,  "status": "PAGO",        "dt_pedido": now - timedelta(days=2),  "dt_entrega": now - timedelta(days=1),  "ingest_ts": now - timedelta(hours=1)},

  # Valor inválido (negativo)
  {"id_pedido": 1005, "id_cliente": 1,   "vl_total": -10.00, "status": "PAGO",        "dt_pedido": now - timedelta(days=1),  "dt_entrega": now,                      "ingest_ts": now - timedelta(hours=1)},

  # Status inválido
  {"id_pedido": 1006, "id_cliente": 2,   "vl_total": 15.00,  "status": "DESCONHECIDO","dt_pedido": now - timedelta(days=1),  "dt_entrega": now,                      "ingest_ts": now - timedelta(hours=1)},

  # Consistência quebrada (entrega antes do pedido)
  {"id_pedido": 1007, "id_cliente": 2,   "vl_total": 99.99,  "status": "PAGO",        "dt_pedido": now - timedelta(days=1),  "dt_entrega": now - timedelta(days=2),  "ingest_ts": now - timedelta(hours=1)},

  # dt_pedido nulo (completude)
  {"id_pedido": 1008, "id_cliente": 1,   "vl_total": 25.00,  "status": "PAGO",        "dt_pedido": None,                      "dt_entrega": now,                      "ingest_ts": now - timedelta(hours=1)},

  # Freshness ruim (ingest_ts antigo)
  {"id_pedido": 1009, "id_cliente": 1,   "vl_total": 70.00,  "status": "PAGO",        "dt_pedido": now - timedelta(days=60), "dt_entrega": now - timedelta(days=58), "ingest_ts": now - timedelta(days=10)},
])

con.register("clientes_df", clientes)
con.register("pedidos_df", pedidos)

con.execute("INSERT INTO dim_cliente SELECT * FROM clientes_df;")
con.execute("INSERT INTO fato_pedido SELECT * FROM pedidos_df;")

print("✅ Banco criado:", db_path)
print("Clientes:", con.execute("SELECT COUNT(*) FROM dim_cliente").fetchone()[0])
print("Pedidos:", con.execute("SELECT COUNT(*) FROM fato_pedido").fetchone()[0])

con.close()
