import duckdb
from datetime import datetime

db_path = "dq_lab.duckdb"
con = duckdb.connect(db_path)

required_cols = ["id_pedido", "id_cliente", "vl_total", "status", "dt_pedido"]
sla_hours = 24

weights = {
  "completude": 0.20,
  "unicidade": 0.15,
  "consistencia": 0.20,
  "validade": 0.15,
  "integridade": 0.15,
  "freshness": 0.15
}

total = con.execute("SELECT COUNT(*) FROM fato_pedido").fetchone()[0]

# -------------------------
# 1) COMPLETUDE
# -------------------------
null_rates = []
for c in required_cols:
    nulls = con.execute(f"SELECT COUNT(*) FROM fato_pedido WHERE {c} IS NULL").fetchone()[0]
    null_rates.append(nulls / total if total else 0)

avg_null_rate = sum(null_rates)/len(null_rates)
score_completude = max(0, min(10, 10*(1-avg_null_rate)))

# -------------------------
# 2) UNICIDADE
# -------------------------
distinct_pk = con.execute("SELECT COUNT(DISTINCT id_pedido) FROM fato_pedido").fetchone()[0]
dup_rate = 1 - (distinct_pk/total)
score_unicidade = max(0, min(10, 10*(1-dup_rate)))

# -------------------------
# 3) VALIDADE
# -------------------------
invalid_count = con.execute("""
SELECT COUNT(*) FROM fato_pedido
WHERE vl_total < 0
   OR status NOT IN ('PAGO','PENDENTE','CANCELADO')
""").fetchone()[0]

invalid_rate = invalid_count/total
score_validade = max(0, min(10, 10*(1-invalid_rate)))

# -------------------------
# 4) CONSISTÊNCIA
# -------------------------
viol_count = con.execute("""
SELECT COUNT(*) FROM fato_pedido
WHERE dt_pedido IS NOT NULL
  AND dt_entrega IS NOT NULL
  AND dt_pedido > dt_entrega
""").fetchone()[0]

viol_rate = viol_count/total
score_consistencia = max(0, min(10, 10*(1-viol_rate)))

# -------------------------
# 5) INTEGRIDADE REFERENCIAL
# -------------------------
fk_missing = con.execute("""
SELECT COUNT(*) FROM fato_pedido p
LEFT JOIN dim_cliente c ON p.id_cliente = c.id_cliente
WHERE p.id_cliente IS NOT NULL AND c.id_cliente IS NULL
""").fetchone()[0]

fk_missing_rate = fk_missing/total
score_integridade = max(0, min(10, 10*(1-fk_missing_rate)))

# -------------------------
# 6) FRESHNESS
# -------------------------
last_ts = con.execute("SELECT MAX(ingest_ts) FROM fato_pedido").fetchone()[0]
delay_hours = (datetime.now() - last_ts).total_seconds()/3600
raw = 10 * (1 - (delay_hours/sla_hours))
score_freshness = max(0, min(10, raw))

# -------------------------
# NOTA FINAL
# -------------------------
scores = {
  "completude": score_completude,
  "unicidade": score_unicidade,
  "consistencia": score_consistencia,
  "validade": score_validade,
  "integridade": score_integridade,
  "freshness": score_freshness
}

nota_final = sum(scores[k] * weights[k] for k in scores.keys())

print("\n====== RESULTADO DATA QUALITY ======")
print("Total registros:", total)
print("Completude:", round(score_completude,2))
print("Unicidade:", round(score_unicidade,2))
print("Validade:", round(score_validade,2))
print("Consistência:", round(score_consistencia,2))
print("Integridade:", round(score_integridade,2))
print("Freshness:", round(score_freshness,2))
print("NOTA FINAL:", round(nota_final,2))

con.close()
