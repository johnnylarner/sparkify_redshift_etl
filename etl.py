import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
import boto3
import logging
from io import StringIO



def copy_csv_to_staging_table(
    file_path, 
    target_table: str,
    cur,
    conn
    )-> bool:
    """
    Copy csv written to buffer memory to sparkifydb.
    '|' used as delimiter to improve csv parsing stability.

    Args:
    df: pandas DataFrame.
    target_table: str containing target table in sparkifydb.
    cur: psycopy2 cursor object with connection to sparkifydb.
    """
    
    with pd.read_csv(file_path, sep="|",
                    quotechar="'", 
                    header=[0],
                    chunksize=1000) as writer:
        for chunk in writer:
            print("chunk")
            try:
                cur.copy_from(file_path, target_table,
                                null='null', sep='|')
                conn.commit()                
                return True
            except psycopg2.DatabaseError as e:
                logging.error(e)
                return False


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config['CLUSTER']["DB_HOST"],
        config['CLUSTER']["DB_NAME"],
        config['CLUSTER']["DB_USER"],
        config['CLUSTER']["DB_PASSWORD"],
        config['CLUSTER']["DB_PORT"],
    ))

    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()