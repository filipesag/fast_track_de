import time
import psycopg2
import logging
import pandas as pd

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
    raise Exception("Connection failed!")


cur = conn.cursor()

# criando tabela dimensao order_status
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_id UUID PRIMARY KEY,
    status_name VARCHAR(40)
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
    product_category VARCHAR(80),
    product_weight_g INTEGER
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




status_df = pd.read_csv("input/olist_orders_dataset.csv")
status_df.head(5)


