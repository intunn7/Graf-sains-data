
from neo4j import GraphDatabase

URI = "neo4j://127.0.0.1:7687"
USERNAME = "neo4j"
PASSWORD = input("Masukkan password Neo4j: ")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

try:
    driver.verify_connectivity()
    print("Koneksi Neo4j berhasil!")

    with driver.session() as session:
        result = session.run("RETURN 1 AS angka")
        record = result.single()
        print("Hasil query:", record["angka"])

except Exception as error:
    print("Koneksi gagal:", error)

finally:
    driver.close()
