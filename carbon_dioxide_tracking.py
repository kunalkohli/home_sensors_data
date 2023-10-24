import mh_z19
import pandas as pd
import requests as re
import datetime
from datetime import timedelta
import json
from dotenv import load_dotenv
import os
import MySQLdb
import time

load_dotenv()

###############
## Database connection details ->
## I am using https://planetscale.com/ as my database which provides a free Vitess based database which is amazingly fast and resilient
## Head over to vitess website to create your free database.
##alternatively, you can other fav (https://www.cockroachlabs.com/) for a free distributed database.

HOST= 'host_config'
USERNAME= 'username'
PASSWORD= 'password_config'
DATABASE= 'name_database'

connection = MySQLdb.connect(
  host= HOST,
  user=USERNAME,
  passwd= PASSWORD,
  db= DATABASE,
  ssl_mode = "VERIFY_IDENTITY",
  ssl      = {
    "ca": "/etc/ssl/certs/ca-certificates.crt"
  }
)

# Create cursor and use it to execute SQL command. This is needed for planetscale db to upload dataframe without failing. Not sure if you need this for other mysql vendors.
cursor = connection.cursor()
cursor.execute('SET autocommit = true')

## Continue running the code forever
## Make sure to run the code with sudo command to give serial port access to raspberry pi
while(1):
  
    metric_dict = {}

    try:
        metric_dict['carbon_dioxide_reading'] = mh_z19.read()['co2']
        metric_dict['timestamp'] = datetime.datetime.now()
        df = pd.DataFrame(metric_dict, index=[0])

        df['timestamp'] = pd.to_datetime(df['timestamp']) #convert to datetime pandas

        cols = ",".join([str(i) for i in df.columns.tolist()])
    
        # create dataframe from dictionary
        df = pd.DataFrame(metric_dict, index=[0])

        df['timestamp'] = pd.to_datetime(df['timestamp']) #convert to datetime pandas

        cols = ",".join([str(i) for i in df.columns.tolist()])

        # insert data to table
        for row in df.values:
            value = "','".join([str(i) for i in row])
            sql = "INSERT INTO home_status_dashboard.pi_co2_readings (" +cols + ") VALUES ('" +value+ "')"
            cursor.execute(sql)

    except ValueError:
        print('Oops. Wrong data')
    
    except Exception as e:
        print(f'An error occurred: {e}')


    time.sleep(60*5) #sleep for 5 mins. Change the frequency of data collection here