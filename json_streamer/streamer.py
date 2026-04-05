import pandas as pd
import json
from kafka import KafkaProducer
import logging
# import time

logging.basicConfig(level=logging.INFO)

# time.sleep(10)

def stream_data(i: int, producer: KafkaProducer):
    df = pd.read_csv(f"/data/MOCK_DATA ({i}).csv")
    date_cols = ["sale_date", "product_release_date", "product_expiry_date"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col],
                                 errors="coerce").dt.strftime("%Y-%m-%d")

    id_cols = ["id", "sale_customer_id", "sale_seller_id", "sale_product_id"]
    for col in id_cols:
        df[col] = df[col] + 1000 * i

    df = df.astype(object).where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    for event in records:
        future = producer.send('sales_topic', event)
        try:
            future.get(timeout=10)
        except KafkaError as e:
            logging.error(f"Failed to send: {e}")


producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, allow_nan=False).encode('utf-8'))

for i in range(10):
    stream_data(i, producer)
