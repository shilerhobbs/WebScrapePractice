import sqlite3
from io import StringIO
import json

import csv

import time

import pandas as pd
import requests


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64)'}

# Any URL that gives a .cvs file should work with the below function
eve_echoes_market_url = 'https://api.eve-echoes-market.com/market-stats/stats.csv'

item_url = 'https://api.eve-echoes-market.com/market-stats/'


# Provide a URL that  gives a .csv file and the name of the table in your database to add it to.
def download_csv(url, database_table_name):

    # Opens the database connection with the given database name. if the name is not found it will make one
    database_connection = sqlite3.connect('EE_Historical_Market.db')

    # Downloads the raw text of the given url  -  Not sure if this is the way your supposed to do this
    csv_text = requests.get(url, headers=headers).text

    # Converts the string into a pandas data table - again not sure if you should do this but it works
    df = pd.read_csv(StringIO(csv_text), sep=",")

    # Save the file to a csv file using pandas - could probly just save the file instead of converting then saving
    df.to_csv(f'{database_table_name}.csv')

    with database_connection:
        # Use pandas to add the cvs file to the database table and if it exists replace it
        # you can also use 'append' to just add it to the table
        df.to_sql(database_table_name, database_connection, if_exists='replace')

    database_connection.close()


download_csv(eve_echoes_market_url, 'EVE ECHOES MARKET')

#TODO Comment all this stuff

def get_item_data(database_connection, item_name, item_id):
    print(item_name)

    item_enteries = requests.get(f'{item_url}{item_id}', headers=headers).text

    data = json.loads(item_enteries)

    data_df = pd.json_normalize(data)

    with database_connection:
        # Use pandas to add the cvs file to the database table and if it exists replace it
        # you can also use 'append' to just add it to the table
        data_df.to_sql(item_name, database_connection, if_exists='append')

        remove_duplicates(database_connection, item_name)

def update_all_items():

    start_time = time.time()

    database_connection = sqlite3.connect('Your_Database_Here.db')

    csv_text = requests.get(eve_echoes_market_url, headers=headers).text

    df = pd.read_csv(StringIO(csv_text), sep=",")

    csv_file = open('Market Data Lookup.csv')

    csv_dict = csv.DictReader(csv_file)

    lookup_dict = {}

    for row in csv_dict:
        lookup_dict[int(row['ID'])] = {'main_category': row['Main Category'], 'sub_category': row['Sub Category']}

    csv_file.close()
    
    for index, row in df.iterrows():

        item_id = df.at[index, 'item_id']
        name_str = df.at[index, 'name'].replace(' ', '_').replace("'", '').replace('-', '_').replace(':', '')
        item_name = name_str.replace('.', '').replace('(', '_').replace(')', '')
        get_item_data(database_connection, item_name, item_id)

        try:

            category = lookup_dict[item_id]['main_category']

            if category == 'Manufacturing Materials' or category == 'Pilot Service':

                get_csv_from_database(database_connection, item_name)

                print(item_name)

        except KeyError as e:
            print(f'Couldnt find id  {e}')

    with database_connection:
        # Use pandas to add the cvs file to the database table and if it exists replace it
        # you can also use 'append' to just add it to the table
        df.to_sql('TABLE_NAME', database_connection, if_exists='replace')

    database_connection.close()

    finish_time = time.time()

    total_time = finish_time - start_time

    print(f'Finished all items in {total_time}seconds or {total_time/60}min')



def remove_duplicates(database_connection, table_name):
    
    cursor = database_connection.cursor()
    
    query = f'DELETE FROM {table_name} WHERE rowid NOT IN (SELECT min(rowid) FROM {table_name} GROUP BY time)'
    
    cursor.execute(query)



def get_csv_from_database(database_connection, table_name):

    query = f'SELECT * FROM {table_name}'

    df = pd.read_sql_query(query, database_connection)

    df.to_csv(f'csv_files/{table_name}.csv')

    try:

        df.to_csv(f'/var/www/html/csv_files/{table_name}.csv')

    except FileNotFoundError as e:
        print(e)



last_run = 0

while True:
    if time.time() - last_run >= 86400:
        update_all_items()
        last_run = time.time()
    
    else:
        time.sleep(1)
