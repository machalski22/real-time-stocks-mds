import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
from airflow.sdk import dag, task
from datetime import datetime, timedelta

#Define access variables
load_dotenv()
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

MINIO_ENDPOINT = "http://minio:9000"
BUCKET = "bronze-trans-folder"
LOCAL_DIR = "/tmp/minio_downloads"  # use absolute path for Airflow

SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    "minio_to_snowflake",
    default_args=default_args,
    schedule="*/1 * * * *",
    catchup=False,
)
def minio_to_snowflake():

    @task
    def download_from_minio():
        os.makedirs(LOCAL_DIR, exist_ok=True)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        local_files = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files.append(local_file)
        return local_files

    @task
    def load_to_snowflake(local_files):
        if not local_files:
            print("No files to load.")
            return

        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = conn.cursor()

        for f in local_files:
            print(f"Loading {f}")
            print(f"PUT file://{f} -> @%bronze_stock_quotes_raw")
            cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")
            print(f"Uploaded {f} to Snowflake stage")

        cur.execute("""
            COPY INTO BRONZE_STOCK_QUOTES_RAW
            FROM (
            SELECT $1
            FROM @%bronze_stock_quotes_raw 
            )
            FILE_FORMAT = (TYPE = JSON);
            """)
        print("COPY INTO executed")

        cur.close()
        conn.close()

    load_to_snowflake(download_from_minio())

minio_to_snowflake()