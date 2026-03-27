import pandas as pd

def transform(path):
    df = pd.read_csv(path)

    df = df[['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime',
             'passenger_count','trip_distance','fare_amount']]

    df.columns = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount"
    ]

    df = df[df.trip_distance > 0]

    df.to_csv('/opt/airflow/data/clean.csv', index=False)

    return "/opt/airflow/data/clean.csv"