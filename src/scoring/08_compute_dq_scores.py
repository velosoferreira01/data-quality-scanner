import argparse
import duckdb

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbfile", required=True, help="dq_lab.duckdb")
    ap.add_argument("--stg", default="stg", help="schema no DuckDB (ex: stg)")
    ap.add_argument("--run_id", default=None, help="se vazio, usa o último run_id")
    args = ap.parse_args()

    con = duckdb.connect(args.dbfile)
    stg = args.stg

    # garante tabela de output
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {stg};")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {stg}.dq_table_scores (
            run_id            VARCHAR,
            scanned_at        TIMESTAMP,
            source_host       VARCHAR,
            source_db         VARCHAR,
            source_schema     VARCHAR,
            table_name        VARCHAR,
            sample_rows       BIGINT,
            null_rate         DOUBLE,
            avg_distinct_ratio DOUBLE,
            score_completeness DOUBLE,
            score_uniqueness  DOUBLE,
            score_volume      DOUBLE,
            score_final       DOUBLE
        );
    """)

    # pega run_id alvo (último ou o informado)
    if args.run_id:
        run_id = args.run_id
    else:
        run_id = con.execute(
            f"SELECT run_id FROM {stg}.dq_table_metrics ORDER BY scanned_at DESC LIMIT 1"
        ).fetchone()
        if not run_id:
            print("[SCORE] Não há dq_table_metrics para calcular score ainda.")
            con.close()
            return
        run_id = run_id[0]

    print(f"[SCORE] Calculando score para run_id={run_id}")

    # Remove score antigo do mesmo run_id (pra permitir reprocessar)
    con.execute(f"DELETE FROM {stg}.dq_table_scores WHERE run_id = ?", [run_id])

    # Insere scores
    # - completeness: (1 - null_rate) * 10
    # - uniqueness:  avg(distinct_ratio) * 10 (média das colunas)
    # - volume:      10 se sample_rows >= 1000, 7 se >= 100, 4 se >= 10, senão 1
    # - final:       0.6*comp + 0.3*uniq + 0.1*vol (clamp 0..10)
    con.execute(f"""
        INSERT INTO {stg}.dq_table_scores
        WITH
        t AS (
            SELECT *
            FROM {stg}.dq_table_metrics
            WHERE run_id = ?
        ),
        c AS (
            SELECT
                run_id,
                table_name,
                AVG(distinct_ratio) AS avg_distinct_ratio
            FROM {stg}.dq_column_metrics
            WHERE run_id = ?
            GROUP BY run_id, table_name
        ),
        s AS (
            SELECT
                t.run_id,
                t.scanned_at,
                t.source_host,
                t.source_db,
                t.source_schema,
                t.table_name,
                t.sample_rows,
                t.null_rate,
                COALESCE(c.avg_distinct_ratio, 0.0) AS avg_distinct_ratio,

                ((1.0 - COALESCE(t.null_rate, 1.0)) * 10.0) AS score_completeness,
                (COALESCE(c.avg_distinct_ratio, 0.0) * 10.0) AS score_uniqueness,

                CASE
                    WHEN t.sample_rows >= 1000 THEN 10.0
                    WHEN t.sample_rows >= 100  THEN 7.0
                    WHEN t.sample_rows >= 10   THEN 4.0
                    ELSE 1.0
                END AS score_volume
            FROM t
            LEFT JOIN c
            ON t.run_id = c.run_id AND t.table_name = c.table_name
        )
        SELECT
            run_id,
            scanned_at,
            source_host,
            source_db,
            source_schema,
            table_name,
            sample_rows,
            null_rate,
            avg_distinct_ratio,
            score_completeness,
            score_uniqueness,
            score_volume,
            LEAST(10.0, GREATEST(0.0,
                0.6*score_completeness + 0.3*score_uniqueness + 0.1*score_volume
            )) AS score_final
        FROM s;
    """, [run_id, run_id])

    # mostra top/low
    top = con.execute(f"""
        SELECT table_name, score_final, score_completeness, score_uniqueness, score_volume, sample_rows, null_rate, avg_distinct_ratio
        FROM {stg}.dq_table_scores
        WHERE run_id = ?
        ORDER BY score_final DESC
        LIMIT 20
    """, [run_id]).fetchall()

    print("[SCORE] Top scores:")
    for r in top:
        print(r)

    con.close()
    print("[SCORE] OK.")

if __name__ == "__main__":
    main()