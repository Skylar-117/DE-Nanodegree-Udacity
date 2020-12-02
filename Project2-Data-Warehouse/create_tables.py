import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop existing tables in Sparkify database.
    
    Parameters
    ==========
    cur: cursory to the connected database, used for execute SQL queries.
    conn: connection to Sparkify Postgres database.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error: there is an error while executing: {query}")
            print(e)

    print("Tables have been successfully dropped.")


def create_tables(cur, conn):
    """
    Create 5 new tables: fact table - songplays, dimension tables - songs, artists, time, users
    
    Parameters
    ==========
    cur: cursory to the connected database, used for execute SQL queries.
    conn: connection to Sparkify Postgres database.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error: there is an error while executing: {query}")
            print(e)

    print("Tables have been successfully created.")


def main():
    """
    Connect to AWS Redshift. Create new Postgres database Sparkify. Drop existing tables. Create 5 new tables. Close database connection.
    
    Parameters (from dwh.cfg)
    ==========
    host      AWS Redshift cluster address.
    dbname    DB name.
    user      Username for the DB.
    password  Password for the DB.
    port      DB port to connect to.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()