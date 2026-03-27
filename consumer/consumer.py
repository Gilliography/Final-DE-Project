from kafka import KafkaConsumer
import psycopg2, json

conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)

cur = conn.cursor()

consumer = KafkaConsumer(
    "taxi_rides",
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    data = msg.value

    cur.execute(
        "INSERT INTO taxi_rides (ride_id, location, fare) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (data["ride_id"], data["location"], data["fare"])
    )
    conn.commit()

    print("Inserted:", data)