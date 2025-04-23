import requests
import numpy as np 
import pandas as pd 
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ['Name','MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
exchange_rate_csv = './exchange_rate.csv'

def logging_process(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        

def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            # Find all <a> tags in the second column
            a_tags = col[1].find_all('a')
            
            # The bank name is in the <a> tag that does not contain an <img> tag
            bank_name = None
            for a_tag in a_tags:
                if not a_tag.find('img'):  # Skip <a> tags with images (flags)
                    bank_name = a_tag.text.strip()
                    break
            if bank_name:
                # Extract market cap from the third column and clean it
                market_cap = col[2].text.strip().replace(',', '')
                # Create a dictionary for the new row
                data_dict = {
                    "Name": bank_name,
                    "MC_USD_Billion": float(market_cap)
                }
                
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            else:
                print("Warning: Bank name not found in row:", row)
    return df


def transform(df, exchange_rate_csv):
    exchange_rate = pd.read_csv(exchange_rate_csv).set_index('Currency').to_dict()['Rate']

    # Add new columns for GBP, EUR, and INR
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
 
def load_to_db(df,sql_conn,table_name):
    # Load the DataFrame into a SQL database
    df.to_sql(table_name, sql_conn, if_exists='replace', index='False')

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# Logging and execution
logging_process('Preliminaries complete. Initiating ETL process')

# Step 1: Extract data
df = extract(url, table_attributes)
logging_process('Data extraction complete')

# Step 2: Transform data
df = transform(df, exchange_rate_csv)
logging_process('Data transformation complete')

# Step 3: Print the transformed DataFrame
print(df)

load_to_csv(df, csv_path)
logging_process('Data saved to CSV file')

sql_conn = sqlite3.connect('World_Economies.db')
logging_process('SQL Connection initiated.')
load_to_db(df, sql_conn, table_name)
logging_process('Data loaded to Database as table. Running the query')
query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_conn)

# Query 2: Print the average market capitalization of all banks in Billion GBP
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, sql_conn)

# Query 3: Print the names of the top 5 banks
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query3, sql_conn)
sql_conn.close()
# Step 4: Log completion
logging_process('ETL process completed successfully')