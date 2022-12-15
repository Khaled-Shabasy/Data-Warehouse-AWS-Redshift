import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops all tables if existed to start my dataset from scratch in case of restarting the ETL process.
    
        Parameters:
                cur: the cursor object.
                conn: Connection to the Redshift cluster

        Returns:
                none.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates tables. 
    
        Parameters:
                cur: the cursor object.
                conn: Connection to the Redshift cluster

        Returns:
                none.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Establishes connection to the Redshift cluster.
    - Creates the cursor to execute all the commands wrapped by the connection.
    - Executes dropping tables if existed.
    - Executes creating tables.
    - Closes connection.
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()