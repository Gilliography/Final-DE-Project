#!/bin/bash
echo "Checking services..."

curl -f http://localhost:8080 || echo "Airflow not ready"
nc -z localhost 9092 || echo "Kafka not ready"
nc -z localhost 5432 || echo "Postgres not ready"

echo "Done"