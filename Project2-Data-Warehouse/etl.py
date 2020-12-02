import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy and insert source data to staging tables
    
    This function is to execute Redshift COPY command to insert source data from S3 to staging tables.
    
    Parameters
    ==========
    cur: cursory to the connected database, used for execute SQL queries.
    conn: connection to Sparkify Postgres database.
    """
    print("\nStart loading data from S3 to Reshift tables...")
    for query in copy_table_queries:
        print('------------------')
        print(f'Processing query: {query}')
        cur.execute(query)
        conn.commit()
        print('\nSucceed!')
    print("All files have been copied successfully.")


def insert_tables(cur, conn):
    """Insert data from staging tables to analytic tables.
    
    After creating staging tables, data is then further inserted from staging tables to analytic tables which are songplays/songs/users/artists/time.
    
    Parameters
    ==========
    cur: cursory to the connected database, used for execute SQL queries.
    conn: connection to Sparkify Postgres database.
    """
    print("\nStart inserting data from staging tables into analytic tables...")
    for query in insert_table_queries:
        print('------------------')
        print(f'Processing query: {query}')
        cur.execute(query)
        conn.commit()
        print('\nSucceed!')
    print("All files have been copied successfully.")


def main():
    """
    Connect to AWS Redshift. Create new Postgres database Sparkify. Drop existing tables. Create 2 staging tables and 5 analytic tables. Close database connection.
    
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()