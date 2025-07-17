import time
import psycopg2
import logging
import pandas as pd
import numpy as np
import sqlalchemy 
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from sqlalchemy.exc import SQLAlchemyError
import uuid

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
        logging.info("Postgres Docker connection completed - psycopg2")
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

def to_uuid(x):
    return x if isinstance(x, uuid.UUID) else uuid.UUID(x)


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
    payment_method VARCHAR(50) UNIQUE
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
CREATE TABLE IF NOT EXISTS fact_order (
    order_id UUID PRIMARY KEY,
    score INTEGER,
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
    df_payment = df_payment.drop_duplicates(subset=['order_id','payment_type'])
    # concatenando formas de pagamento para um único registro quando feito com multiplas formas
    df_payment_concat = df_payment.groupby('order_id')['payment_type'].agg(lambda x: ', '.join(sorted(set(x)))) \
    .reset_index()
    df_merge_order_payment = new_df.merge(df_payment_concat[['order_id','payment_type']], on='order_id', how='left').merge(df_payment[['order_id','payment_installments']], on='order_id', how='left')

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
reviews_collection = collection_reviews.find({})
reviews_df = pd.DataFrame(reviews_collection)

#adicionando as reviews no df principal
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

#dropando colunas inutilizaveis
df_full_merged.drop(columns=['seller_id',
                             'shipping_limit_date'
                            ], inplace=True, errors='ignore')


# tratando dados nulos/vazios
df_full_merged['payment_type'] = df_full_merged['payment_type'].fillna('Not defined').astype(str)
df_full_merged['product_category_name'] = df_full_merged['product_category_name'].replace('nan', np.nan)
df_full_merged['product_category_name'] = df_full_merged['product_category_name'].fillna('Not informed').astype(str)
df_full_merged['customer_city'] = df_full_merged['customer_city'].fillna('Not informed').astype(str)
df_full_merged['customer_state'] = df_full_merged['customer_state'].fillna('Not informed').astype(str)
df_full_merged['review_score'] = df_full_merged['review_score'].replace('nan', np.nan)
df_full_merged['review_score'] = df_full_merged['review_score'].fillna(-1).astype(int)
df_full_merged['payment_installments'] = df_full_merged['payment_installments'].replace('nan', np.nan)
df_full_merged['payment_installments'] = df_full_merged['payment_installments'].fillna(-1).astype(int)

#conectando com postgres com sqlalchemy para popular banco
try:
    DATABASE_URL = 'postgresql://f_compass:trilha_de@postgres:5432/pd_dw'
    engine = sqlalchemy.create_engine(DATABASE_URL)
    logging.info("Postgres Docker connection completed - sqlalchemy")
    con_alchemy = engine.connect()
except Exception:
    logging.critical("Connection with Postgres Docker failed!")

missing_values = df_full_merged.isnull().sum()
print(missing_values)


df_status_final = df_full_merged[['order_status']].copy()
try:
    with engine.begin() as conn:
        df_status_final.to_sql("stage_order_status", con_alchemy, index=False, if_exists="replace")
        conn.execute(sqlalchemy.text("""
            MERGE INTO dim_order_status AS tgt
            USING stage_order_status AS src
            ON tgt.order_status = src.order_status
            WHEN NOT MATCHED THEN
                INSERT (order_status) VALUES (src.order_status);
        """))
    logging.info("INSERT operation in dim_order_status table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in dim_order_status: {e}")

df_time_final = df_full_merged[['order_datetime', 'order_day', 'order_month', 'order_trimester', 'order_year', 'order_date', 'order_time']].copy()
try:
    with engine.begin() as conn:
        df_time_final.to_sql("stage_time_final", con_alchemy, index=False, if_exists="replace")         
        conn.execute(sqlalchemy.text("""
            MERGE INTO dim_time AS tgt
            USING stage_time_final AS src
            ON tgt.order_datetime = src.order_datetime
            WHEN NOT MATCHED THEN
                INSERT (order_datetime,order_day,order_month,order_trimester,order_year,order_date,order_hour) 
                VALUES (src.order_datetime,src.order_day,src.order_month,src.order_trimester,src.order_year,src.order_date,src.order_time);
        """)
        )
    logging.info("INSERT operation in dim_time table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in dim_time: {e}")


df_customer_final = df_full_merged[['customer_id', 'customer_city', 'customer_state']].copy()
df_customer_final['customer_id'] = df_customer_final['customer_id'].apply(to_uuid)
try:
    with engine.begin() as conn:
        df_customer_final.to_sql("stage_customer_final", con_alchemy, index=False, if_exists="replace",dtype={"customer_id": sqlalchemy.dialects.postgresql.UUID})
        conn.execute(
            sqlalchemy.text("""
                MERGE INTO dim_customer AS tgt
                USING stage_customer_final AS src
                ON tgt.customer_id = src.customer_id
                WHEN NOT MATCHED THEN
                INSERT (customer_id,customer_city,customer_state) 
                VALUES (src.customer_id,src.customer_city,src.customer_state);
            """),
        )
    logging.info("INSERT operation in dim_customer table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in dim_customer: {e}")


df_product_final = df_full_merged[['product_id', 'product_category_name']].copy()
df_product_final['product_id'] = df_product_final['product_id'].apply(to_uuid)
try:
    with engine.begin() as conn:
        df_product_final.to_sql("stage_product_final", con_alchemy, index=False, if_exists="replace",dtype={"product_id": sqlalchemy.dialects.postgresql.UUID})
        conn.execute(
            sqlalchemy.text("""
                MERGE INTO dim_product AS tgt
                USING stage_product_final AS src
                ON tgt.product_id = src.product_id
                WHEN NOT MATCHED THEN
                INSERT (product_id,product_category) 
                VALUES (src.product_id,src.product_category_name);
            """)
        )
    logging.info("INSERT operation in dim_product table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in dim_product: {e}")

df_payment_final = df_full_merged[['payment_type']].copy()
try:
    with engine.begin() as conn:
        df_payment_final.to_sql("stage_payment_final", con_alchemy, index=False, if_exists="replace")
        conn.execute(
            sqlalchemy.text("""
                MERGE INTO dim_payment_method AS tgt
                USING stage_payment_final AS src
                ON tgt.payment_method = src.payment_type
                WHEN NOT MATCHED THEN
                INSERT (payment_method) 
                VALUES (src.payment_type);
            """)
        )
    logging.info("INSERT operation in dim_payment table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in dim_payment_method: {e}")

QUERY_DIM_ORDER_STATUS = """
SELECT status_id, order_status FROM dim_order_status
"""

QUERY_DIM_TIME = """
SELECT order_time_id, order_datetime, order_day, order_month, order_trimester, order_year, order_date, order_hour FROM dim_time
"""

QUERY_DIM_PAYMENT_METHOD = """
SELECT payment_method_id, payment_method FROM dim_payment_method
"""

#query nas tabelas dimensoes com ids novos para merge
df_dim_order_status = pd.read_sql(QUERY_DIM_ORDER_STATUS, con=con_alchemy)
df_dim_time = pd.read_sql(QUERY_DIM_TIME, con=con_alchemy)
df_dim_payment_method = pd.read_sql(QUERY_DIM_PAYMENT_METHOD, con=con_alchemy)

#merge dos dados da tabela dimensao para df principal
df_fact_order = df_full_merged.merge(df_dim_order_status, on='order_status', how='left').merge(df_dim_time, on='order_datetime', how='left').merge(df_dim_payment_method, left_on='payment_type', right_on='payment_method', how='left')

#obtendo valores para tabela fato
df_fact_order_final = df_fact_order[['order_id', 'review_score', 'payment_value', 'price', 'freight_value', 'payment_installments', 'order_item_id', 'order_time_id', 'customer_id', 'product_id', 'payment_method_id', 'status_id']].copy()
df_fact_order_final['order_id'] = df_fact_order_final['order_id'].apply(to_uuid)
df_fact_order_final['order_time_id'] = df_fact_order_final['order_time_id'].apply(to_uuid)
df_fact_order_final['customer_id'] = df_fact_order_final['customer_id'].apply(to_uuid)
df_fact_order_final['payment_method_id'] = df_fact_order_final['payment_method_id'].apply(to_uuid)
try:
    with engine.begin() as conn:
        df_fact_order_final.to_sql("stage_fact_final", con=engine, index=False, if_exists="replace", dtype={
            "order_id": sqlalchemy.dialects.postgresql.UUID,
            "customer_id": sqlalchemy.dialects.postgresql.UUID,
            "product_id": sqlalchemy.dialects.postgresql.UUID,
            "order_time_id": sqlalchemy.dialects.postgresql.UUID,
            "payment_method_id": sqlalchemy.dialects.postgresql.UUID,
            "status_id": sqlalchemy.dialects.postgresql.UUID,
        })
        conn.execute(
            sqlalchemy.text("""
                MERGE INTO fact_order AS tgt
                USING stage_fact_final AS src
                ON tgt.order_id = src.order_id
                WHEN NOT MATCHED THEN
                INSERT (order_id,score,payment_value,product_price,freight_value,installments,number_of_items,order_time_id,order_customer_id,order_product_id,order_payment_method_id,order_status_id) 
                VALUES (src.order_id,src.review_score,src.payment_value,src.price,src.freight_value,src.payment_installments,src.order_item_id,src.order_time_id,src.customer_id,src.product_id,src.payment_method_id,src.status_id);
            """)
        )
    logging.info("INSERT operation in fact table completed...")
except SQLAlchemyError as e:
    logging.critical(f"Error during INSERT operation in fact_order: {e}")