
import time
import psycopg2
import logging
import pandas as pd
import numpy as np
import sqlalchemy 
import uuid
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

max_attempts = 10

for attempt in range(max_attempts):
    try:
        conn = psycopg2.connect(
            dbname="pd_dw",
            user="f_compass",
            password="trilha_de",
            host="postgres",
            port="5432"
        )
        logging.info("Postgres Docker connection completed")
        break
    except psycopg2.OperationalError as e:
        logging.warning(f"Attempt nº {attempt + 1} out of {max_attempts} failed: {e}")
        time.sleep(3)
else:
    logging.critical("Connection attempt number is over...")
    raise Exception("Connection with Postgres Docker failed!")


cur = conn.cursor()

# criando tabela dimensao order_status
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_id UUID PRIMARY KEY,
    order_status VARCHAR(40)
);
""")
conn.commit()
logging.info("Table dim_order_status created")

# criando tabela dimensao customer
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id UUID PRIMARY KEY,
    customer_city VARCHAR(80),
    customer_state VARCHAR(2)
);
""")
conn.commit()
logging.info("Table dim_customer created")

# criando tabela dimensao product
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_product (
    product_id UUID PRIMARY KEY,
    product_category VARCHAR(80)
);
""")
conn.commit()
logging.info("Table dim_product created")

# criando tabela dimensao payment_method
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_id UUID PRIMARY KEY,
    payment_method VARCHAR(50),
    payment_sequential INTEGER
);
""")
conn.commit()
logging.info("Table dim_payment_method created")

# criando tabela dimensao time
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_time (
    order_time_id UUID PRIMARY KEY,
    order_datetime TIMESTAMP,
    order_day SMALLINT,
    order_month VARCHAR(20),
    order_trimester INTEGER,
    order_year INTEGER,
    order_hour TIME
);
""")
conn.commit()
logging.info("Table dim_time created")

# criando tabela fato order
cur.execute("""
CREATE TABLE IF NOT EXISTS fato_order (
    order_id UUID PRIMARY KEY,
    score SMALLINT,
    payment_value DECIMAL(10,2),     
    product_price DECIMAL(10,2),      
    freight_value DECIMAL(10,2),
    installments SMALLINT,      
    number_of_items SMALLINT,
    order_time_id UUID,
    order_customer_id UUID,
    order_product_id UUID,
    order_payment_method_id UUID,
    order_status_id UUID,
    FOREIGN KEY (order_time_id) REFERENCES dim_time(order_time_id),
    FOREIGN KEY (order_customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (order_product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (order_payment_method_id) REFERENCES dim_payment_method(payment_method_id),
    FOREIGN KEY (order_status_id) REFERENCES dim_order_status(status_id)
);
""")
conn.commit()
logging.info("Table fact_order created")

cur.close()
conn.close()
logging.info("Connection closed!")





def uuid_generate():
  return uuid.uuid4()


def uuid_removing_hyphens(uuid):
    return str(uuid).replace("-", "")

df = pd.read_csv('./input/olist_order_items_dataset.csv')
df_grouped_by_id = df.groupby("order_id")["order_item_id"].max().reset_index()
df_grouped_by_id = df_grouped_by_id.drop_duplicates(subset=['order_id'])
new_df = pd.merge(df_grouped_by_id, df, on=["order_id", "order_item_id"], how="inner")
new_df.head(15)



new_df['status_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
new_df['status_id'] = new_df['status_id'].apply(uuid_removing_hyphens)
new_df.head(5)


new_df['payment_method_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
new_df['payment_method_id'] = new_df['payment_method_id'].apply(uuid_removing_hyphens)
new_df.head(5)

new_df['time_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
new_df['time_id'] = new_df['time_id'].apply(uuid_removing_hyphens)
new_df.head(5)



df_payment = pd.read_csv('./input/olist_order_payments_dataset.csv')
df_payment = df_payment.drop_duplicates(subset=['order_id','payment_sequential'])
df_merge_order_payment = pd.merge(new_df,df_payment, on='order_id', how='left')
df_merge_order_payment.head(5)




df_status = pd.read_csv('./input/olist_orders_dataset.csv')
df_status = df_status.drop_duplicates(subset=['order_id'])
df_merge_order_status = pd.merge(df_merge_order_payment, df_status[['order_id','customer_id','order_status','order_purchase_timestamp']], on='order_id', how='left')
df_merge_order_status.head(5)



df_products = pd.read_csv('./input/olist_products_dataset.csv')
df_products = df_products.drop_duplicates(subset=['product_id'])
df_merge_order_produtcs = pd.merge(df_merge_order_status, df_products[['product_id','product_category_name']], on='product_id', how='left')
df_merge_order_produtcs.head(5)


df_local = pd.read_csv('./input/olist_customers_dataset.csv')
df_local = df_local.drop_duplicates(subset=['customer_id','customer_unique_id'])
df_merge_order_local = pd.merge(df_merge_order_produtcs, df_local[['customer_id', 'customer_city', 'customer_state']], on='customer_id', how='left')
df_merge_order_local.head(5)



missing_values = df_merge_order_local.isnull().sum()
print(missing_values)

d = df_merge_order_local[df_merge_order_local['payment_type'].isnull()]
d




try:
    client = MongoClient("mongodb://f_compass:trilha_de@mongodb:27017/")
    db = client["ecommerce"]

    logging.info(f"DBs in mongo: {client.list_database_names()}")
    logging.info(f"Collections: {db.list_collection_names()}")
    logging.info("MongoDB Docker connection completed")

except ConnectionFailure:
    logging.critical("Connection with MongoDB Docker failed!")

collection_reviews = db["order_reviews"]

reviews_doc = collection_reviews.find({})
reviews_df = pd.DataFrame(reviews_doc)

df_full_merged = pd.merge(df_merge_order_local, reviews_df[['order_id','review_score']], on='order_id', how='left')
print(df_full_merged.head(5))
client.close()

