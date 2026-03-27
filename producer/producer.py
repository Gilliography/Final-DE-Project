from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "ride_id": random.randint(1, 1000000),
        "location": random.choice(["JFK", "LGA", "NYC"]),
        "fare": round(random.uniform(5, 100), 2)
    }

    producer.send("taxi_rides", data)
    print("Sent:", data)
    time.sleep(2)