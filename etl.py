import configparser
import psycopg2
from sql_queries import copy_table_queries, song_user_artist_songplay_table_insert, time_table_insert_query
import logging

def load_staging_tables(cur, conn):
    """
    Copies song and log data from prestaged CSVs
    to event and song staging tables
    """

    for query in copy_table_queries:
        print("Copying to staging table")
        cur.execute(query)
        conn.commit()

def insert_time_table(cur, conn, time_table_insert_query):
    """
    Selects unique timestamps from songplay staging data
    and breaks them up into units before inserting into
    the time dimension table.
    """
    
    select, insert = time_table_insert_query
    try:
        cur.execute(select)
        results = cur.fetchall()
    except psycopg2.Error as e:
        logging.error(e)
    print(f"{len(results)} rows of data found!")
    for row in results:

        # Break down into date parts
        timestamp = row[0]
        hour = timestamp.hour
        day = timestamp.day
        week = timestamp.isocalendar()[1]
        month = timestamp.month
        weekday = timestamp.isocalendar()[2]
        vals = (timestamp, hour, day, week, month, weekday)
        try:
            cur.execute(insert, vals)
            conn.commit()
        except psycopg2.Error as e:
            logging.error(e)

def insert_tables(cur, conn, insert_queries):
    """
    Inserts data not requiring additional transformation
    into corresponding fact and dimension tables.

    Each query pairing has a select and insert query.
    """


    for select, insert in insert_queries:

        try:
            cur.execute(select)
            results = cur.fetchall()
        except psycopg2.Error as e:
            logging.error(e)
            continue


        print(f"{len(results)} rows of data found!")
        
        for row in results:
            print(row)
            try:
                cur.execute(insert, row)
                conn.commit()
            except psycopg2.Error as e:
                logging.error(e)

        print("Table insertion complete")


def main():
    """
    Connect to Redshift DB
    
    Load staging data

    Transform and insert time data

    Insert remaining dimension and fact tables
    """
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
    insert_time_table(cur, conn, time_table_insert_query)
    insert_tables(cur, conn, song_user_artist_songplay_table_insert)

    conn.close()


if __name__ == "__main__":
    main()