import pandas as pd
import sqlite3
from pandas import DataFrame
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import OrderedDict


def prepare_sqlite_db_load_products(product_path):

    #In a production environment use config file to hold Jane DB
    #We use it directly for this purposes
    conn = sqlite3.connect('jane.db')
    c = conn.cursor()

    #Drop and Recreate products Table in the jane Database
    #In a production implementation - products table will be present in the DB
    #A separate process will be used to add or update the products table when they change or when new products come up

    c.execute('''
        drop table if exists products
    ''')
    c.execute('''
        create table products (
             id int primary key,
             amount text,
             brand text,
             lineage text,
             name text not null,
             product_type text,
             product_subtype text,
             url text
        )
    ''')

    #Convert Products csv to dataframe and load to products table in Database and commit

    df = pd.read_csv(product_path)
    df.to_sql('products', conn, if_exists='append', index=False)

    conn.commit()

    return conn
#conn.close()
