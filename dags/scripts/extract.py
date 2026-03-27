import pandas as pd

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

def extract():
    df = pd.read_csv(URL, compression='gzip')
    df.to_csv('/opt/airflow/data/raw.csv', index=False)
    return "/opt/airflow/data/raw.csv"