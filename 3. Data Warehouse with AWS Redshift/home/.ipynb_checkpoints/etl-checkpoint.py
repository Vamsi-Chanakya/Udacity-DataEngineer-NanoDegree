__version__ = '0.1'
__author__ = 'Vamsi Chanakya'

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, table_list

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def count_tables(cur, conn):
    for table_name in table_list:
        query = ('SELECT COUNT(*) from {};').format(table_name)
        cur.execute(query)
        numberOfRows = cur.fetchone()[0]
        print('numberOfRows in table ' + table_name + ' are ' + str(numberOfRows))
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    count_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()