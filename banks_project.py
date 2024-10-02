#Importing libraries to use
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import datetime

#Project environment variables
url= "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
path_exchange_rate="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
attributes_extraction= ['Name','MC_USD_Billion']
attributes_final=['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
output_csv="./Largest_banks_data.csv"
db_name="Banks.db"
table_name="Largest_banks"
log_file="code_log.txt"

def log_progress(messange):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format= "%Y-%m-%d %H:%M:%S"
    now= datetime.datetime.now()
    date= now.strftime(timestamp_format)

    with open (log_file,'a') as file:
        file.write(messange+" , "+ date +"\n")

def extract(url, attributes_extraction):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df= pd.DataFrame(columns=attributes_extraction)

    request= requests.get(url).text
    soup= BeautifulSoup(request,"html.parser")
    table= soup.find('table')
    rows= table.find_all('tr')
    for row in rows:
        col= row.find_all('td')
        if(len(col)!=0):
            data={'Name':col[1].find_all('a')[1].contents[0],'MC_USD_Billion':col[2].contents[0]}
            df1=pd.DataFrame(data, index=[0])
            df= pd.concat([df,df1],ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    df_final= pd.DataFrame(columns=attributes_final)
    df_final['Name']=df['Name']
    df_final['MC_USD_Billion']=df['MC_USD_Billion'].str.replace("\n","").str.strip().astype(float)
   
    df_rate= pd.read_csv(csv_path)
    eur= df_rate[df_rate['Currency']=='EUR']['Rate'].iloc[0]
    gbp= df_rate[df_rate['Currency']=='GBP']['Rate'].iloc[0]
    inr= df_rate[df_rate['Currency']=='INR']['Rate'].iloc[0]
    df_final['MC_EUR_Billion']= round(df_final['MC_USD_Billion'] * eur,2)
    df_final['MC_GBP_Billion']=round(df_final['MC_USD_Billion'] * gbp,2)
    df_final['MC_INR_Billion']=round(df_final['MC_USD_Billion'] * inr,2)
    
    return df_final

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path,index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    query=pd.read_sql(query_statement,sql_connection)
    print(query)
    


#calling functions
log_progress("Starts the extraction")
d= extract(url,attributes_extraction)
log_progress("The transformation begins")
d_f= transform(d,path_exchange_rate)
log_progress("Load to csv")
load_to_csv(d_f,output_csv)

log_progress("Load to db")
conn= sqlite3.connect(db_name)
load_to_db(d_f,conn,table_name)
log_progress("Run queries")
run_query("SELECT * FROM Largest_banks",conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks",conn)
run_query("SELECT Name from Largest_banks LIMIT 5",conn)
conn.close()

 
