# --------------------------------------------------------------
# Author : Stefan Ignat
# Date Created : 11/01/2020
# Date Modified : 11/23/2020
# Description : ETL of probe data to SQL Server through proxy
# Required Modules: pyodbc, requests
# --------------------------------------------------------------

import os, sys, time
import json
import logging
import time
import pyodbc
import requests

# 1a. Connect to customer DB
# 2. Extract information into an object and store object into a dic
# 3. Connect to SQL-Server DB
# 4. Executemany into Oracle

#-----------------------
# Connect to customer DB
#-----------------------
# Define authentication variables

aUser = os.environ['USERNAME']
aPass = os.environ['mypass']
username = 'web_api@website.com'
password = 'p4ssw0rd!'
grant_type = 'password'
proxyDict = {
    "http": f"http://{aUser}:{aPass}@proxy:1234",
    "https": f"https://{aUser}:{aPass}@proxy:1234"
}

# Authentication; expires in 1 hour. Gets new token every time this script is ran.
res = requests.get("https://data-website.com/authenticate", data={'grant_type': grant_type, 'username': username, 'password': password}, proxies=proxyDict)
print ("Authentication token acquired. Expires in 1 hour.")
# Turn response into json
data = res.json()
# Extract access token
token = data['access_token']
headers = {'Authorization': 'Bearer ' + token}


#-----------------------
# Extract Information
#-----------------------
# Define probe dict
def probe():
    return {
        'SerialNo': '',
        'ReceivedTime': '',
        'Thickness': '',
        'Uac': '',
        'Iac': '',
        'Jac': '',
        'Rs': '',
        'Idc': '',
        'Jdc': '',
        'Edc': '',
        'Rr': '',
        'Rc': '',
        'Eoff': ''
    }


# Get probes
response = requests.get("https://data-website.com/api/probes", headers=headers, proxies=proxyDict)
print ("Queuing for probes.")
# Turn probes response into json
probes = response.json()
#data = [] # To store the probes


#-----------------------
# Connect to Sql-Server
#-----------------------
#Connection string
conn_str = (
    r'Driver={SQL Server};'
    r'Server=SQL;'
    r'Database=myDB;'
    r'Trusted_Connection=yes;' #For windows authentication
)
try:
    cnxn = pyodbc.connect(conn_str)
except Exception as e:
    print ("ERROR during connection: " + str(e))
cursor = cnxn.cursor()

#-----------------------
# Iterate and load
#-----------------------
# Navigate through each probe and make a dict, then insert it to the db
numErrors = 0
for p in probes:
    # Create new probe instance
    newProbe = probe()
    # Get basic probe info 
    newProbe['SerialNo'] = p["SerialNo"]
    LastTimeStamp = p["LastTimeStamp"]
    # Get probe logger lines 
    payload = {'SerialNo': newProbe['SerialNo'], 'from': LastTimeStamp, 'to': LastTimeStamp}
    try:
        Qlogs = requests.get("https://data-website.com/api/Probeloggerlines", headers=headers, params=payload, proxies=proxyDict)
    except Exception as e:
        print ("ERROR getting logs for probe: " + newProbe['SerialNo'])
    logs = Qlogs.json()
    # Make sure logs is not null
    if logs:
        newProbe['ReceivedTime'] = p["LastTimeStamp"]
        newProbe['Thickness'] = logs[0]["Thickness"]
        newProbe['Uac'] = logs[0]["Uac"]
        newProbe['Iac'] = logs[0]["Iac"]
        newProbe['Jac'] = logs[0]["Jac"]
        newProbe['Rs'] = logs[0]["Rs"]
        newProbe['Idc'] = logs[0]["Idc"]
        newProbe['Jdc'] = logs[0]["Jdc"]
        newProbe['Edc'] = logs[0]["Edc"]
        newProbe['Rr'] = logs[0]["Rr"]
        newProbe['Rc'] = logs[0]["Rc"]
        Eoff = logs[0]["Eoff"]
        if (Eoff):
            Eoff = round(Eoff, 17)
        newProbe['Eoff'] = f'{Eoff}'
    
    # Side note: Could instead save all probes in list, then execute many. But, because
    # the api response is slow, this would not save any time, only reduce trips to the
    # database. Also couldn't get SQL to play nice... I think.

    # Write the probe to the DB
    try:
        cursor.execute("INSERT INTO dbo.ProbeData (SerialNo, ReceivedTime, Thickness, Uac, Iac, Jac, Rs, Idc, Jdc, Edc, Rr, Rc, Eoff) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(newProbe.values()))
        print ("Wrote to DB.")
    except Exception as e:
        numErrors = numErrors + 1
        print ("ERROR during Execute:" + str(e))
print("Errors: " + numErrors)
cursor.commit()
cursor.close()
print("---DONE---")
