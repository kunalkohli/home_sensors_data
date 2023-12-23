# 'CREATE TABLE home_status_dashboard.pi_humidity_temperature_reading ( timestamp varchar(255) NOT NULL, humidity varchar(255) NOT NULL, temperature varchar(255) NOT NULL, PRIMARY KEY (timestamp))'


import Adafruit_DHT
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

## Select the sensor you are using DHT11 or DHT22
DHT_SENSOR = Adafruit_DHT.DHT22

## Select the rpio pin the sensor is connected on 
DHT_PIN = 4


###############
## Database connection details ->
## I am using Vitess as my database which provides a free database and is amazingly fast and resilient
## Head over to vitess website to create your free database

HOST= 'host_config'
USERNAME= 'username'
PASSWORD= 'password_config'
DATABASE= 'name_database'

# Create cursor and use it to execute SQL command
def connect_to_mysql():
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
  cursor = connection.cursor()
  time.sleep(10)

  print('connecting to mysql using cursor')
  cursor.execute('SET autocommit = true')

  return connection, cursor


connection, cursor = connect_to_mysql()

## Continue running the code forever
## Make sure to run the code with sudo command to give serial port access to raspberry pi
while(1):
    metric_dict = {}

    try:

        humidity, temperature = Adafruit_DHT.read(DHT_SENSOR, DHT_PIN)

        metric_dict['humidity'] = humidity
        metric_dict['temperature'] = temperature
        metric_dict['timestamp'] = datetime.datetime.now()

        ## Convert to a dataframe
        df = pd.DataFrame(metric_dict, index=[0])

        #convert to datetime pandas
        df['timestamp'] = pd.to_datetime(df['timestamp']) 

        cols = ",".join([str(i) for i in df.columns.tolist()])

        # create dataframe from dictionary
        df = pd.DataFrame(metric_dict, index=[0])

        df['timestamp'] = pd.to_datetime(df['timestamp']) #convert to datetime pandas

        cols = ",".join([str(i) for i in df.columns.tolist()])

        # insert data to table
        for row in df.values:
                value = "','".join([str(i) for i in row])
                sql = "INSERT INTO home_status_dashboard.pi_humidity_temperature_reading (" +cols + ") VALUES ('" +value+ "')"
                cursor.execute(sql)

    except ValueError:
        print('Oops. Wrong data')

    except MySQLdb.OperationalError as e:
        if e.args[0] == 2006:  # MySQL server has gone away
            print("Reconnecting to MySQL...")
            connection, cursor = connect_to_mysql()
            # Retry your data insertion logic with the new connection

    except Exception as e:
        print(f'An error occurred: {e}')


    time.sleep(60*5) #sleep for 5 mins. Change the frequency of data collection here