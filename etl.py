import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries


logging.basicConfig(level=logging.INFO)


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()          
        except Exception as e:
            logging.error(f"Error loading staging table: {e}")

def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting into table: {e}")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        logging.info('Loading data into staging tables...')
        load_staging_tables(cur, conn)
        logging.info('Inserting data into final tables...')
        insert_tables(cur, conn)

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()
            logging.info('Connection closed.')
        logging.info('Job finished.')

if __name__ == "__main__":
    main()
