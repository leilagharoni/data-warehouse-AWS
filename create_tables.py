import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO)

def drop_tables(cur, conn):
    """ Drop tables in the database.

    Args:
        cur(psycopg2.cursor): cursor object from psycopg2's connection.
        conn(psycopg2.connection): connection object of psycopg2.

    This function iterates over the list of drop table queries and executes each one.
    If an error occurs during the execution, it logs the error message.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error dropping table: {e}")

def create_tables(cur, conn):
    """Create tables in the database.

    Args:
        cur(psycopg2.cursor): cursor object from psycopg2's connection.
        conn(psycopg2.connection): connection object of psycopg2.

    This function iterates over the list of create table queries and executes each one.
    If an error occurs during the execution, it logs the error message.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")

def main():
    """
    Main function to connect to the database, drop existing tables, and create new tables.

    This function reads the database configuration from 'dwh.cfg', establishes a connection
    to the database, and calls the functions to drop and create tables. It also handles
    exceptions and ensures that the connection is closed after the operations are complete.
    """
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
