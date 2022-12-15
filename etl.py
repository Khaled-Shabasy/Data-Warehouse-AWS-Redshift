import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads data from s3 buckets into the staging tables.
    
        Parameters:
                cur: the cursor object.
                conn: Connection to the Redshift cluster

        Returns:
                none.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Populates the analytics tables -fact and dimension tables- with data from the staging tables
    
        Parameters:
                cur: the cursor object.
                conn: Connection to the Redshift cluster

        Returns:
                none.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Establishes connection to the Redshift cluster.
    - Creates the cursor to execute all the commands wrapped by the connection.
    - Executes loading data into the staging tables.
    - Executes inserting data into the analytics tables.
    - Closes connection.
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()