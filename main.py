import sqlite3
from io import StringIO

import pandas as pd
import requests

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64)'}

# Any URL that gives a .cvs file should work with the below function
eve_echoes_market_url = 'https://api.eve-echoes-market.com/market-stats/stats.csv'


# Provide a URL that  gives a .csv file and the name of the table in your database to add it to.
def pull_market_data(url, database_table_name):

    # Opens the database connection with the given database name. if the name is not found it will make one
    database_connection = sqlite3.connect('Your_Database_Here.db')

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


pull_market_data(eve_echoes_market_url, 'EVE ECHOES MARKET')




