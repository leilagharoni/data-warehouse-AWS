import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries


logging.basicConfig(level=logging.INFO)

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error dropping table: {e}")

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        logging.info('Dropping all tables...')
        drop_tables(cur, conn)
        logging.info('Creating tables...')
        create_tables(cur, conn)

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()
            logging.info('Connection closed.')
        logging.info('Job finished.')

if __name__ == "__main__":
    main()
