""" Module Name: 
   
    Updating mayan based on changes in create_tbl_lambda_personnel in MySQL

   Created By: Asif asif@schinkels.com.my 
   Created: 31/7/2022 
   Framework: Python 3.10 on production database to postgres
   
   Last Edited: 03/10/2022
    Reason Edited: change interval time from 1 second to 5 minutes
  
"""

# source : library
# uasge : import required module
import sys

# source : library
# usage : json parsing
import json

# source : library
# usage : setup path
import os.path
from os import path

# source : libarary
# allows http request to be sent
import requests

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcReadJson

# source : library
# usage : log message to log file
import logging

import mysql.connector

# source : library
# usage : Run Python functions (or any other callable) periodically using a friendly syntax.
import schedule 

# source : library
# usage : handle time-related tasks
import time

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcGetDB, funcReadJson, funcWriteJSON, funcRedisUpdate, funcGetColname

# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/lambdaPersonnel.log".format(parPath),
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

# ensuring path existence
if os.path.exists(path) is False:    
    logging.info('env dir not found. Ensure env file in the right directory')    
    raise SystemExit  
logging.info('Initiating updateRedis.py')

# ensuring the env file is JSON file
filename = ('{}/env/safwa2.env'.format(parPath))
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

def funcPersonnelLambda():
    connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
    connection2 = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))                                            
    cursor = connection.cursor()
    cursor2 = connection2.cursor()

    #
    cursor.execute("SELECT * FROM dangabay.tbl_lambda_personnel" )
    dictResult = cursor.fetchone()
    while True:
        if dictResult[1] != '0000' and dictResult != None:

            # actionItem = insert
            if dictResult[2] == 'insert':
                
                dictData = {}
                cursor2.execute("SELECT COUNT(uuid) FROM safwa_tender_dev.personnel WHERE uuid = '%s'" %(str(dictResult[0])))
                dictCount = cursor2.fetchone()
                if dictCount[0] > 0 :
                    
                    # fetch data from tbl_lambda_personnel 03c169aa-acbd-11eb-b7b0-06264d286b0a
                    cursor2.execute("SELECT * FROM safwa_tender_dev.personnel WHERE uuid = '%s'" %(str(dictResult[0])))
                    dictDataResult = cursor2.fetchall()
                    dictData.update({'username':dictDataResult[17],'first_name' : dictDataResult[1],'lastname':dictDataResult[2],'email':dictDataResult[3]})
                    url = 'http://alb-dangabay-1a-702507269.ap-southeast-1.elb.amazonaws.com:81/api/v4/users/'
                    
                    #use the 'auth' parameter to send requests with HTTP Basic Auth:
                    x = requests.post(url, data = dictData, auth = (dictAPI['username'], dictAPI['password']) )
                    logging.info("{} inserted in mayan : {}".format(dictResult[17],x.status_code))
                    
                    # delete data in tbl_lambda_personnel
                    cursor2.execute("DELETE FROM dangabay.tbl_lambda_personnel WHERE uuid='%s',actionItem='%s'" %( dictResult[0],dictResult[2]))
                    connection2.commit()
                    connection2.close()
                elif dictCount[0] == 0:
                    
                    # delete data in tbl_lambda_personnel
                    cursor2.execute("DELETE FROM dangabay.tbl_lambda_personnel WHERE uuid='%s' AND actionItem='%s'" %( dictResult[0],dictResult[2]))
                    connection2.commit()
                    connection2.close()
                    continue   

            # actionItem = update
            elif dictResult[2] == 'update':
                
                dictData = {}
                cursor2.execute("SELECT COUNT(uuid) FROM safwa_tender_dev.personnel WHERE uuid = '%s'" %(str(dictResult[0])))
                dictCount = cursor2.fetchone()
                if dictCount[0] > 0 :
                    # fetch data from tbl_lambda_personnel
                    cursor2.execute("SELECT * FROM safwa_tender_dev.personnel WHERE uuid = '%s'" %(dictResult[0]) )
                    dictDataResult = cursor2.fetchone()

                    # get id using the api based on username
                    url = 'http://alb-dangabay-1a-702507269.ap-southeast-1.elb.amazonaws.com:81/usersID/{}'.format(dictDataResult[17])
                    i = requests.get(url, data = dictData, auth = (dictAPI['username'], dictAPI['password']) )

                    # data need to be updated in postgres
                    dictData.update({'username':dictDataResult[17],'first_name' : dictDataResult[1],'lastname':dictDataResult[2],'email':dictDataResult[3]})
                    url2 = 'http://alb-dangabay-1a-702507269.ap-southeast-1.elb.amazonaws.com:81/api/v4/users/{}'.format(i)

                    # use the 'auth' parameter to send requests with HTTP Basic Auth:
                    x = requests.put(url2, data = dictData, auth = (dictAPI['username'], dictAPI['password']) )
                    logging.info("{} updated in mayan : {}".format(dictResult[17],x.status_code)) 

                    # delete data in tbl_lambda_personnel
                    cursor2.execute("DELETE FROM dangabay.tbl_lambda_personnel WHERE uuid='%s', actionItem='%s'" %( dictResult[0],dictResult[2]))
                    connection2.commit()
                    connection2.close()   

                elif dictCount[0] == 0:
                    cursor2.execute("DELETE FROM dangabay.tbl_lambda_personnel WHERE uuid='%s' AND actionItem='%s'" %( dictResult[0],dictResult[2]))
                    connection2.commit()
                    connection2.close()
                    continue   

            # actionItem = delete
            elif dictResult[2] == 'delete':

                # data need to be delete in postgres based on user_id in tbl_lambda_personnel table
                url = 'http://alb-dangabay-1a-702507269.ap-southeast-1.elb.amazonaws.com:81/api/v4/users/{}'.format(dictResult[1])

                #use the 'auth' parameter to send requests with HTTP Basic Auth:
                x = requests.delete(url, auth = (dictAPI['username'], dictAPI['password']) )
                logging.info("{} deleted in mayan : {}".format(dictResult[17],x.status_code))

                # delete data in tbl_lambda_personnel 
                cursor2.execute("DELETE FROM dangabay.tbl_lambda_personnel WHERE uuid='%s', actionItem='%s'" %( dictResult[0],dictResult[2]))
                connection2.commit()
                connection2.close()   

        elif dictResult == None:
            continue
        elif dictResult[1] == '0000':
            break
        dictResult = cursor.fetchone()
    connection.close()
    logging.info('database is 0000. System exit')
    logging.info('funcPersonnelLambda.py system STOP') 
    raise SystemExit

if __name__ == '__main__':
    # code run every 5 minutes
    schedule.every(5).minutes.do(funcPersonnelLambda)
    logging.info('env test pass. funcPersonnelLambda.py system START.')
    print('funcPersonnelLambda.py system START. Please Wait. Activate 00000 to stop')
    while True:
        schedule.run_pending()
        # interval between each code run : 1 second
        time.sleep(1)