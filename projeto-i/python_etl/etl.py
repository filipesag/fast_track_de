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

def close_connection(conn):
    conn.close()
    logging.info("Connection closed!")
    
def uuid_generate():
  return uuid.uuid4()


ORDER_STATUS_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_id UUID PRIMARY KEY,
    order_status VARCHAR(40)
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
    payment_method_id UUID PRIMARY KEY, 
    payment_method VARCHAR(50), 
    payment_sequential INTEGER 
);
"""

TIME_TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS dim_time (
    order_time_id UUID PRIMARY KEY, 
    order_datetime TIMESTAMP, 
    order_day SMALLINT, 
    order_month VARCHAR(20), 
    order_trimester INTEGER, 
    order_year INTEGER, 
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





df = pd.read_csv('./input/olist_order_items_dataset.csv')
df_grouped_by_id = df.groupby("order_id")["order_item_id"].max().reset_index()
df_grouped_by_id = df_grouped_by_id.drop_duplicates(subset=['order_id'])
new_df = pd.merge(df_grouped_by_id, df, on=["order_id", "order_item_id"], how="inner")
new_df.head(15)



new_df['status_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
new_df.head(5)


new_df['payment_method_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
new_df.head(5)

new_df['time_id'] = new_df.apply(lambda x:uuid_generate(), axis=1)
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


df_full_merged['payment_value'] = df_full_merged['order_item_id'] * df_full_merged['price'] + df_full_merged['freight_value'] * df_full_merged['order_item_id'] 


df_full_merged.fillna({
    'review_score':np.nan,
    'payment_sequential':np.nan,
    'payment_type':'Não informado.',
    'product_category_name':'Categoria não informada.',
    'payment_installments':np.nan
}, inplace=True)
missing_values = df_full_merged.isnull().sum()
print(missing_values)

df_full_merged.rename({
    "order_item_id":"number_of_items",
    "payment_type":"payment_method",
    "product_category_name":"product_category",
    "payment_installments":"installments",
    "review_score":"score",
    "order_purchase_timestamp":"order_datetime",
}, inplace=True)

df_full_merged.drop(columns=['seller_id',
                             'shipping_limit_date',
                             'seller_id'], inplace=True)


df_full_merged.reset_index(drop=True, inplace=True)

df_final = df_full_merged.where(pd.notnull(df_full_merged), None)

missing_values = df_final.isnull().sum()
print(missing_values)



DATABASE_URL = 'postgresql://f_compass:trilha_de@postgres:5432/pd_dw'
engine = None

try:
    engine = sqlalchemy.create_engine(DATABASE_URL)
    logging.info("Connection with Postgres Docker completed!")
    con_alchemy = engine.connect()
except Exception:
    logging.critical("Connection with Postgres Docker failed!")

df_status_final = df_final[['status_id', 'order_status']]

with engine.begin() as conn:
    for _, row in df_status_final.iterrows():
        conn.execute(
            sqlalchemy.text("""
                INSERT INTO dim_order_status (status_id, status_name)
                VALUES (:status_id, :status_name)
                ON CONFLICT (status_id) DO NOTHING;
            """),
            {
                "status_id": row["status_id"],
                "status_name": row["order_status"]
            }
        )



d1 = pd.read_sql_query("SELECT * FROM dim_order_status", con=engine)
print(d1)