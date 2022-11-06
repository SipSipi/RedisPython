""" Module Name: 
   
   API for uploading postgress based on create_tbl_lambda_personnel in MySQL

   Created By: Asif asif@schinkels.com.my 
   Created: 31/7/2022 
   Framework: Python 3.10 on production database to postgres
   
   Last Edited: 
    Reason Edited: 
  
"""

# source : library
# uasge : import required module
import sys
import mysql.connector

# source : library
# usage : json parsing
import json

# source : libarary
# allows http request to be sent
import requests

# source : library
# usage : setup path
import os.path
from os import path
from this import d, s

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcGetDB, funcReadJson, funcWriteJSON, funcRedisUpdate, funcGetColname

# source : library
# usage : log message to log file
import logging

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
#from funcFile import funcGetDBPersonnel
# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = ".\\log\\personnel2PostgresAPI.log",
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

# ensuring path existence
if os.path.exists(path) is False:    
    logging.info('env dir not found. Ensure env file in the right directory')    
    raise SystemExit  
logging.info('Initiating personnel2MayanInitLoad.py')

# ensuring the env file is JSON file
filename = ('{}/env/safwa2.env'.format(path))
logging.info('Testing env file existence') 
with open(filename, "r") as f:
    try:
        json.load(f)
    except:
        logging.info('file not in JSON Format. Please load only JSON format file')
        raise SystemExit

# read JSON file
dictEnv = funcReadJson(filename)  
dictRuntime = dictEnv['runtime']
strRuntimeMode = dictRuntime['runtimeMode']
dictRuntime = dictEnv[strRuntimeMode]
dictPostgres = dictRuntime[2]
dictMySQL = dictRuntime[0]
dictAPI = dictRuntime[5]
dictMySQL = dictMySQL['MySQL8.0']
dictPostgres = dictPostgres['postgres']
dictAPI = dictAPI['usersAPI']

def funcLoadDB2Postgres():
    #1st connection for SELECT statement
    connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
    #2nd connection for UPDATE statement
    connection2 = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))                                             
    cursor = connection.cursor()
    cursor2 = connection2.cursor()
    cursor.execute("SELECT * FROM safwa_tender_dev.personnel WHERE is_safwa = 1", multi = True)
    dictResult = cursor.fetchone()
    while dictResult is not None:
        dictData.update({'username':dictResult[17],'first_name' : dictResult[1],'lastname':dictResult[2],'email':dictResult[3]})
        #jsonData = json.dumps(dictData)
        url = 'http://alb-dangabay-1a-702507269.ap-southeast-1.elb.amazonaws.com:81/api/v4/users/'
        #use the 'auth' parameter to send requests with HTTP Basic Auth:
        x = requests.post(url, data=dictData, auth= (dictAPI['username'], dictAPI['password']))
        try : 
            cursor2.execute("UPDATE safwa_tender_dev.personnel SET access_to_dl = 1 WHERE uuid = '%s'" %(dictResult[0]), multi = True)
            connection2.commit()
            logging.info('{} is updated in safwa_tender_dev.personnel'.format(dictResult[17]))
        except Exception as strErr:
            logging.error(strErr)
        logging.info(dictResult[17] + x.status_code)    
        dictData = {}
        dictResult = cursor.fetchone()
    connection.close()
    connection2.close()

        #logging.info(strErr)
if __name__ == '__main__':
    logging.info('env test pass. loadDB2Redis.py system START')
    funcLoadDB2Postgres()