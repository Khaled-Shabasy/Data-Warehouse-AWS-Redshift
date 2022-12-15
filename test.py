import configparser
import psycopg2
from sql_queries import test_table_queries


def num_records_in_tables(cur, conn):
    for query in test_table_queries:
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)
                



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    num_records_in_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()