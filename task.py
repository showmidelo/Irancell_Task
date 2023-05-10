import pandas as pd
import requests
import datetime
import urllib.parse
from datetime import datetime
import pyodbc
import logging



logging.basicConfig(filename=r"C:\Users\Lion\Desktop\scraper.log",level=logging.DEBUG, format = '%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
logging.info("Extracting information about Justin Trudeau for sysdate-1")



# define parameters for our request to guardian api for sysdate-1
parameters={
    'from-date' : '2020-01-01',
    'to-date' : str(datetime.today())[:10],
    'q' : 'Justin%20AND%20Trudeau%20OR%20JustinTrudeau',
    'format' : 'json',
    'page' : 1,
    'page-size' : 10,
    'api-key' : "30da9971-7eab-48e8-8525-213e200d921b"
            }
main_url='https://content.guardianapis.com/search?'


# checking request status and extract data
def extractData(params, main_urladdress):
    url=main_urladdress+ urllib.parse.urlencode(params).replace('%2520', '%20')
    data= requests.get(url).json()
    if data['response']['status']=='ok':
        return data

fullData = extractData(parameters,main_url)
numberofResults= fullData['response']['total']
numberofPages= fullData['response']['pages']

# add all information about justin traduaue to fullData
def appendfullData(params, numPages, fullData):
    for currentpage in range(2, numPages+1):
        parameters['page']= currentpage
        currentData = extractData(parameters,main_url)
        if currentData['response']['results'] is not None:
            fullData['response']['results'].extend(currentData['response']['results'])
        else:
            continue

# extract result of our responses and add them to a list to make a dataframe later
appendfullData(parameters, numberofPages, fullData)

# just made a dataframe out of created list
df = pd.DataFrame(fullData['response']['results'])
logging.info("make a dataframe out of the json information we have exctracted")

df = df.drop(["sectionId",'apiUrl','isHosted','pillarId','pillarName'],axis=1)
df['webPublicationDate'] = pd.to_datetime(df['webPublicationDate'])
df.rename({"webPublicationDate":"Date"},axis=1,inplace=True)

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
try:
    create_table_query = "CREATE TABLE irancell(column1 int, id ntext,type ntext, sectionName ntext, Date datetime, webTitle ntext, webUrl ntext )"
    truncate_query = "Truncate Table irancell"
    bulk_insert_query = "BULK INSERT [AdventureWorks2012].[dbo].[irancell] FROM 'C:\\Users\\Lion\\Desktop\\Test.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2)"
    cursor.execute(create_table_query)
    cursor.execute(truncate_query)
    cursor.execute(bulk_insert_query)
# if the try raise an error the except would handle it, cause of error is the table have been created before so we just do the bulk insert
except:
    bulk_insert_query = "BULK INSERT [AdventureWorks2012].[dbo].[irancell] FROM 'C:\\Users\\Lion\\Desktop\\Test.csv' WITH (FORMAT = 'CSV', FIRSTROW = 2)"
    truncate_query = "Truncate Table irancell"
    cursor.execute(truncate_query)
    cursor.execute(bulk_insert_query)

# Commit the transaction
conn.commit()

# Close the cursor and connection objects
cursor.close()
conn.close()
logging.info("WE HAVE DONE IT SUCESSFULLY")