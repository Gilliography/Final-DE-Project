import pandas as pd
import psycopg2

def load(path):
    df = pd.read_csv(path)

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO taxi_rides VALUES (%s,%s,%s,%s,%s,%s)
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()