import duckdb
import pandas as pd
from datetime import datetime, timedelta

db_path = "dq_lab.duckdb"
con = duckdb.connect(db_path)

now = datetime.now()

# Novos pedidos (mistura de bons e ruins para ver a nota variar)
novos_pedidos = pd.DataFrame([
  # Bons
  {"id_pedido": 1010, "id_cliente": 1, "vl_total": 40.00,  "status": "PAGO",
   "dt_pedido": now - timedelta(days=2), "dt_entrega": now - timedelta(days=1), "ingest_ts": now - timedelta(hours=1)},

  {"id_pedido": 1011, "id_cliente": 2, "vl_total": 55.50,  "status": "PENDENTE",
   "dt_pedido": now - timedelta(days=1), "dt_entrega": None, "ingest_ts": now - timedelta(hours=1)},

  # Ruins (pra impactar o score)
  # duplicado id_pedido (unicidade)
  {"id_pedido": 1010, "id_cliente": 1, "vl_total": 40.00,  "status": "PAGO",
   "dt_pedido": now - timedelta(days=2), "dt_entrega": now - timedelta(days=1), "ingest_ts": now - timedelta(hours=1)},

  # FK quebrada
  {"id_pedido": 1012, "id_cliente": 9999, "vl_total": 10.00, "status": "PAGO",
   "dt_pedido": now - timedelta(days=1), "dt_entrega": now, "ingest_ts": now - timedelta(hours=1)},

  # valor inválido
  {"id_pedido": 1013, "id_cliente": 2, "vl_total": -5.00, "status": "PAGO",
   "dt_pedido": now - timedelta(days=1), "dt_entrega": now, "ingest_ts": now - timedelta(hours=1)},

  # status inválido
  {"id_pedido": 1014, "id_cliente": 3, "vl_total": 12.00, "status": "ERRO_STATUS",
   "dt_pedido": now - timedelta(days=1), "dt_entrega": now, "ingest_ts": now - timedelta(hours=1)},

  # consistência: entrega antes do pedido
  {"id_pedido": 1015, "id_cliente": 1, "vl_total": 99.90, "status": "PAGO",
   "dt_pedido": now - timedelta(days=1), "dt_entrega": now - timedelta(days=2), "ingest_ts": now - timedelta(hours=1)},
])

con.register("novos_pedidos_df", novos_pedidos)
con.execute("INSERT INTO fato_pedido SELECT * FROM novos_pedidos_df;")

total = con.execute("SELECT COUNT(*) FROM fato_pedido").fetchone()[0]
print("✅ Novos registros inseridos em fato_pedido.")
print("Total agora:", total)

con.close()
