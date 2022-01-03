import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.DatabaseError as e:
            print(e)



def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.DatabaseError as e:
            print(e)


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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()