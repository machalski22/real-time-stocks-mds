import time
import json
import boto3
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

#Define access variables
load_dotenv()
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY_ID:
    raise RuntimeError("AWS_ACCESS_KEY_ID environment variable not set")
if not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError("AWS_SECRET_ACCESS_KEY environment variable not set")

#minio
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

bucket = "bronze-trans-folder"
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["localhost:29092"],
    api_version=(0, 11, 5),
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-consumer",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consumer streaming and saving to MinIO")


for message in consumer:
    print('message')
    record =message.value
    symbol = record.get("symbol")
    ts = record.get("fetched_at",int(time.time()))
    key = f"{symbol}/{ts}.json"
    print(key)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} - s3://{bucket}/{key}")