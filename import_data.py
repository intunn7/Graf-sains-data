
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

# Konfigurasi koneksi
URI = "neo4j://127.0.0.1:7687"
USERNAME = "neo4j"
PASSWORD = input("Masukkan password Neo4j: ")

# File sumber: hanya data training
CSV_FILE = Path("data/events_train.csv")
BATCH_SIZE = 5000

if not CSV_FILE.exists():
    raise FileNotFoundError(f"File tidak ditemukan: {CSV_FILE}")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

QUERIES = {
    "view": """
        UNWIND $rows AS row
        MERGE (v:Visitor {visitorId: row.visitorid})
        MERGE (i:Item {itemId: row.itemid})
        CREATE (v)-[:VIEWED {
            timestamp: row.timestamp
        }]->(i)
    """,
    "addtocart": """
        UNWIND $rows AS row
        MERGE (v:Visitor {visitorId: row.visitorid})
        MERGE (i:Item {itemId: row.itemid})
        CREATE (v)-[:ADDED_TO_CART {
            timestamp: row.timestamp
        }]->(i)
    """,
    "transaction": """
        UNWIND $rows AS row
        MERGE (v:Visitor {visitorId: row.visitorid})
        MERGE (i:Item {itemId: row.itemid})
        CREATE (v)-[:BOUGHT {
            timestamp: row.timestamp,
            transactionId: row.transactionid
        }]->(i)
    """
}


def import_batch(tx, query, rows):
    tx.run(query, rows=rows).consume()


try:
    driver.verify_connectivity()

    with driver.session(database="neo4j") as session:
        # Cegah impor ulang ke graf yang sudah berisi data.
        existing = session.run(
            "MATCH (n) RETURN count(n) AS jumlah"
        ).single()["jumlah"]

        if existing > 0:
            raise RuntimeError(
                f"Database sudah memiliki {existing} node. "
                "Impor dihentikan agar tidak menggandakan data."
            )

        # Indeks unik untuk pencarian node yang lebih efisien.
        session.run("""
            CREATE CONSTRAINT visitor_id_unique IF NOT EXISTS
            FOR (v:Visitor) REQUIRE v.visitorId IS UNIQUE
        """).consume()

        session.run("""
            CREATE CONSTRAINT item_id_unique IF NOT EXISTS
            FOR (i:Item) REQUIRE i.itemId IS UNIQUE
        """).consume()

        total = sum(
            len(chunk)
            for chunk in pd.read_csv(
                CSV_FILE, usecols=["timestamp"], chunksize=100_000
            )
        )

        counts = {
            "view": 0,
            "addtocart": 0,
            "transaction": 0
        }

        print(f"Total baris training: {total:,}")

        for chunk in tqdm(
            pd.read_csv(CSV_FILE, chunksize=BATCH_SIZE),
            desc="Mengimpor batch"
        ):
            # Pastikan nilai ID berbentuk bilangan bulat.
            for col in ["timestamp", "visitorid", "itemid"]:
                chunk[col] = chunk[col].astype("int64")

            # Nilai kosong transactionid diubah menjadi None.
            chunk["transactionid"] = chunk[
                "transactionid"
            ].where(chunk["transactionid"].notna(), None)

            for event, query in QUERIES.items():
                subset = chunk[chunk["event"] == event]

                if subset.empty:
                    continue

                columns = [
                    "timestamp",
                    "visitorid",
                    "itemid",
                    "transactionid"
                ]

                rows = subset[columns].to_dict("records")

                session.execute_write(
                    import_batch, query, rows
                )

                counts[event] += len(rows)

        print("\nImpor selesai.")
        print(f"VIEWED       : {counts['view']:,}")
        print(f"ADDED_TO_CART: {counts['addtocart']:,}")
        print(f"BOUGHT       : {counts['transaction']:,}")
        print(f"Total relasi : {sum(counts.values()):,}")

finally:
    driver.close()
