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

def create_table(conn, script, table_name):
    try:
        cur = conn.cursor()
        cur.execute(script)
        conn.commit()
        logging.info(f'Table {table_name} created!')
    except psycopg2.Error as e:
        logging.critical(f"Table {table_name} not created - {e.pgerror}")
        raise e

def close_connection(conn):
    conn.close()
    logging.info("Connection closed!")

def uuid_generate():
  return uuid.uuid4()

def creating_id_column(df, id_column_name):
    df[f'{id_column_name}'] = df.apply(lambda x:uuid_generate(), axis=1)
    return df



ORDER_STATUS_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_status VARCHAR(40) UNIQUE
    );
"""

CUSTOMER_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id UUID PRIMARY KEY,
    customer_city VARCHAR(80),
    customer_state VARCHAR(2)
    );
"""

PRODUCT_TABLE_SCRIPT ="""
CREATE TABLE IF NOT EXISTS dim_product ( 
    product_id UUID PRIMARY KEY, 
    product_category VARCHAR(80) 
);
"""

PAYMENT_METHOD_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_payment_method ( 
    payment_method_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    payment_method VARCHAR(50), 
    payment_sequential INTEGER 
);
"""

TIME_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_time (
    order_time_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    order_datetime TIMESTAMP UNIQUE, 
    order_day VARCHAR(20), 
    order_month VARCHAR(20), 
    order_trimester INTEGER, 
    order_year INTEGER, 
    order_date DATE,
    order_hour TIME 
);
"""

ORDER_FACT_TABLE_SCRIPT ="""
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
"""

# criando tabelas do modelo dimensional
create_table(conn, ORDER_STATUS_TABLE_SCRIPT,'dim_order_status')
create_table(conn, CUSTOMER_TABLE_SCRIPT,'dim_customer')
create_table(conn, PRODUCT_TABLE_SCRIPT,'dim_product')
create_table(conn, PAYMENT_METHOD_TABLE_SCRIPT,'dim_payment_method')
create_table(conn, TIME_TABLE_SCRIPT,'dim_time')
create_table(conn, ORDER_FACT_TABLE_SCRIPT,'fact_order')

# fechando conexao com banco
close_connection(conn)


# juntando DFs para tratamento dos dados
try:
    df = pd.read_csv('./input/olist_order_items_dataset.csv')
    df_grouped_by_id = df.groupby("order_id")["order_item_id"].max().reset_index()
    df_grouped_by_id = df_grouped_by_id.drop_duplicates(subset=['order_id'])
    new_df = pd.merge(df_grouped_by_id, df, on=["order_id", "order_item_id"], how="inner")

    df_payment = pd.read_csv('./input/olist_order_payments_dataset.csv')
    df_payment = df_payment.drop_duplicates(subset=['order_id','payment_sequential'])
    df_merge_order_payment = pd.merge(new_df,df_payment, on='order_id', how='left')

    df_status = pd.read_csv('./input/olist_orders_dataset.csv')
    df_status = df_status.drop_duplicates(subset=['order_id'])
    df_merge_order_status = pd.merge(df_merge_order_payment, df_status[['order_id','customer_id','order_status','order_purchase_timestamp']], on='order_id', how='left')

    df_products = pd.read_csv('./input/olist_products_dataset.csv')
    df_products = df_products.drop_duplicates(subset=['product_id'])
    df_merge_order_produtcs = pd.merge(df_merge_order_status, df_products[['product_id','product_category_name']], on='product_id', how='left')

    df_local = pd.read_csv('./input/olist_customers_dataset.csv')
    df_local = df_local.drop_duplicates(subset=['customer_id','customer_unique_id'])
    df_merge_order_local = pd.merge(df_merge_order_produtcs, df_local[['customer_id', 'customer_city', 'customer_state']], on='customer_id', how='left')

except Exception as e:
    logging.critical(f"Error in data processing: {e}")
    raise e


#conectando com mongodb 
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
client.close()

#cria campos faltantes
df_full_merged['payment_value'] = df_full_merged['order_item_id'] * df_full_merged['price'] + df_full_merged['freight_value'] * df_full_merged['order_item_id'] 
df_full_merged['order_datetime'] = pd.to_datetime(df_full_merged['order_purchase_timestamp'])
df_full_merged['order_time'] = df_full_merged['order_datetime'].dt.time
df_full_merged['order_date'] = df_full_merged['order_datetime'].dt.date
df_full_merged['order_day'] = df_full_merged['order_datetime'].dt.day_name()
df_full_merged['order_month'] = df_full_merged['order_datetime'].dt.month_name()
df_full_merged['order_trimester'] = df_full_merged['order_datetime'].dt.quarter
df_full_merged['order_year'] = df_full_merged['order_datetime'].dt.year

#tratando de dados NaN, renomeando colunas e excluindo colunas inutilizaveis

df_full_merged.rename({
    "order_item_id":"number_of_items",
    "payment_type":"payment_method",
    "product_category_name":"product_category",
    "payment_installments":"installments",
    "review_score":"score"
}, inplace=True)

df_full_merged.fillna({
    'score': np.nan, 
    'payment_sequential': np.nan,
    'payment_method': 'Não informado.', 
    'product_category': 'Categoria não informada.', 
    'installments': np.nan 
}, inplace=True)

df_full_merged.drop(columns=['seller_id',
                             'shipping_limit_date'
                            ], inplace=True, errors='ignore')

df_final = df_full_merged.where(pd.notnull(df_full_merged), None)


#conectando com postgres com sqlalchemy para popular banco
try:
    DATABASE_URL = 'postgresql://f_compass:trilha_de@postgres:5432/pd_dw'
    engine = sqlalchemy.create_engine(DATABASE_URL)
    logging.info("Connection with Postgres Docker completed!")
    con_alchemy = engine.connect()
except Exception:
    logging.critical("Connection with Postgres Docker failed!")

df_status_final = df_final[['order_status']]

with engine.begin() as conn:
    for _, row in df_status_final.iterrows():
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO dim_order_status (order_status)
                VALUES (:order_status)
                ON CONFLICT (order_status) DO NOTHING;
            """),
            {
                "order_status": row["order_status"]
            }
        )


df_time_final = df_final[['order_datetime', 'order_day', 'order_month', 'order_trimester', 'order_year', 'order_date', 'order_time']]

with engine.begin() as conn:
    for _, row in df_time_final.iterrows():
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO dim_time (order_datetime, order_day, order_month, order_trimester, order_year, order_date, order_hour)
                VALUES (:order_datetime, :order_day, :order_month, :order_trimester, :order_year, :order_date, :order_time)
                ON CONFLICT (order_datetime) DO NOTHING;
            """),
            {
                "order_datetime": row["order_datetime"],
                "order_day": row["order_day"],
                "order_month": row["order_month"],
                "order_trimester": row["order_trimester"],
                "order_year": row["order_year"],
                "order_date": row["order_date"],
                "order_time": row["order_time"]
            }
        )


df_customer_final = df_final[['customer_id', 'customer_city', 'customer_state']]

with engine.begin() as conn:
    for _, row in df_customer_final.iterrows():
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO dim_customer (customer_id, customer_city, customer_state)
                VALUES (:customer_id, :customer_city, :customer_state)
                ON CONFLICT (customer_id) DO NOTHING;
            """),
            {
                "customer_id": row["customer_id"],
                "customer_city": row["customer_city"],
                "customer_state": row["customer_state"]
            }
        )


df_product_final = df_final[['product_id', 'product_category']]

with engine.begin() as conn:
    for _, row in df_product_final.iterrows():
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO dim_product (product_id, product_category)
                VALUES (:product_id, :product_category)
                ON CONFLICT (product_id) DO NOTHING;
            """),
            {
                "product_id": row["product_id"],
                "product_category": row["product_category"]
            }
        )

df_payment_final = df_final[['payment_method', 'payment_sequential']]