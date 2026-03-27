CREATE TABLE IF NOT EXISTS taxi_rides (
    ride_id INT PRIMARY KEY,
    location TEXT,
    fare FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);