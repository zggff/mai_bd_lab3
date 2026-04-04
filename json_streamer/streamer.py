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
        if col in df.columns:
            df[col] = pd.to_datetime(df[col],
                                     errors="coerce").dt.strftime("%Y-%m-%d")
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

    # event = {
    #     "id": row["id"],
    #     "customer_first_name": row["customer_first_name"],
    #     "customer_last_name": row["customer_last_name"],
    #     "customer_age": row["customer_age"],
    #     "customer_email": row["customer_email"],
    #     "customer_country": row["customer_country"],
    #     "customer_postal_code": row["customer_postal_code"],
    #     "customer_pet_type": row["customer_pet_type"],
    #     "customer_pet_name": row["customer_pet_name"],
    #     "customer_pet_breed": row["customer_pet_breed"],
    #     "seller_first_name": row["seller_first_name"],
    #     "seller_last_name": row["seller_last_name"],
    #     "seller_email": row["seller_email"],
    #     "seller_country": row["seller_country"],
    #     "seller_postal_code": row["seller_postal_code"],
    #     "product_name": row["product_name"],
    #     "product_category": row["product_category"],
    #     "product_price": row["product_price"],
    #     "product_quantity": row["product_quantity"],
    #     "sale_date": row["sale_date"],
    #     "sale_customer_id": row["sale_customer_id"],
    #     "sale_seller_id": row["sale_seller_id"],
    #     "sale_product_id": row["sale_product_id"],
    #     "sale_quantity": row["sale_quantity"],
    #     "sale_total_price": row["sale_total_price"],
    #     "store_name": row["store_name"],
    #     "store_location": row["store_location"],
    #     "store_city": row["store_city"],
    #     "store_state": row["store_state"],
    #     "store_country": row["store_country"],
    #     "store_phone": row["store_phone"],
    #     "store_email": row["store_email"],
    #     "pet_category": row["pet_category"],
    #     "product_weight": row["product_weight"],
    #     "product_color": row["product_color"],
    #     "product_size": row["product_size"],
    #     "product_brand": row["product_brand"],
    #     "product_material": row["product_material"],
    #     "product_description": row["product_description"],
    #     "product_rating": row["product_rating"],
    #     "product_reviews": row["product_reviews"],
    #     "product_release_date": row["product_release_date"],
    #     "product_expiry_date": row["product_expiry_date"],
    #     "supplier_name": row["supplier_name"],
    #     "supplier_contact": row["supplier_contact"],
    #     "supplier_email": row["supplier_email"],
    #     "supplier_phone": row["supplier_phone"],
    #     "supplier_address": row["supplier_address"],
    #     "supplier_city": row["supplier_city"],
    #     "supplier_country": row["supplier_country"]
    # }
