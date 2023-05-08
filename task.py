import pandas as pd
import requests
import datetime
import urllib.parse
from datetime import datetime
import pyodbc
import logging 

logging.basicConfig(filename=r"C:\Users\Lion\Desktop\scraper.log",level=logging.DEBUG, format = '%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')

# define parameters for our request to guardian api for sysdate-1
parameters={
    'from-date' : '2016-01-01',
    'to-date' : str(datetime.today())[:10],
    'q' : 'Justin%20AND%20Trudeau%20OR%20JustinTrudeau',
    'format' : 'json',
    'page' : 1,
    'page-size' : 10,
    'api-key' : "your-api-key"
            }

logging.info("Extracting information about Justin Trudeau for sysdate-1")

# add parameters to the main link 
main_url='https://content.guardianapis.com/search?'
url=main_url+ urllib.parse.urlencode(parameters).replace('%2520', '%20')

# extract result of our responses and add them to a list to make a dataframe later
data= requests.get(url).json()
my_data = []
for page in range(200):
    response = requests.get(url)
    data = response.json()
    my_data.extend(data['response']['results'])
    page = page + 1

# just made a dataframe out of created list
df = pd.DataFrame(my_data)
logging.info("make a dataframe out of the json information we have exctracted")

# save the dataframe to a local folder as a csv file
df.to_csv(r"C:\Users\Lion\Desktop\Test.csv",sep=",")
logging.info("save dataframe as a csv file in local folder to put it on the database")

# Connect to SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=OMID\SQLEXPRESS;'
                      'Database=AdventureWorks2012;'
                      'Trusted_Connection=yes;')

# Create a cursor object
cursor = conn.cursor()


# Execute the BULK INSERT command
bulk_insert_query = "BULK INSERT [AdventureWorks2012].[dbo].[irancell] FROM 'C:\\Users\\Lion\\Desktop\\Test.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2)"
cursor.execute(bulk_insert_query)

# Commit the transaction
conn.commit()

# Close the cursor and connection objects
cursor.close()
conn.close()