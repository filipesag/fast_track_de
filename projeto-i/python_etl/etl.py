import time
import psycopg2
import logging

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
cur.execute("SELECT NOW();")
now = cur.fetchone()
logging.info(f"Data atual no banco: {now}")

cur.close()
conn.close()
logging.info("Connection closed!")
