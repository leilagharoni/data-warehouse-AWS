import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(level=logging.INFO)

def load_staging_tables(cur, conn):
    """
    Load data into staging tables from source data using the provided cursor and connection.

    Args:
        cur: A cursor object to execute database commands.
        conn: A connection object to the database.

    This function iterates over the list of copy table queries and executes each one to load
    data into the staging tables. If an error occurs during the execution, it logs the error message.
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()          
        except Exception as e:
            logging.error(f"Error loading staging table: {e}")

def insert_tables(cur, conn):
    """
    Insert data into final tables from staging tables using the provided cursor and connection.

    Args:
        cur: A cursor object to execute database commands.
        conn: A connection object to the database.

    This function iterates over the list of insert table queries and executes each one to insert
    data into the final tables. If an error occurs during the execution, it logs the error message.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting into table: {e}")

def main():
    """
    Main function to connect to the database, load data into staging tables, and insert data into final tables.

    This function reads the database configuration from 'dwh.cfg', establishes a connection
    to the database, and calls the functions to load data into staging tables and insert data
    into final tables. It also handles exceptions and ensures that the connection is closed
    after the operations are complete.
    """
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
