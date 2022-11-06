""" Module Name: 
   funcLoadRedisMain python file to load dangabay database to redeis
   Created By: Asif asif@schinkels.com.my 
   Created: 25/6/2022 
   Framework: Python 3.10 on production database to redis
   Last Edited: 26/7/2022
    Reason Edited: Env path adjustment for 10.2.1.64 .
   Last Edited: 7/7/2022
    Reason Edited: Reconstruct the code. Cut dict memory relation.
   Last Edited: 3/7/2022
    Reason Edited: completing the code
"""
dictDBTblNme    = {}  # dicts of database
colName = {}        # dicts of column names
listColName = []    # column name list
listdictDBTblNme = []     # list of dicts of database
dictEnv = {}        # dicts of JSON file
dictRuntime = {}    # dicts of runtime variable
strRuntimeMode = '' # string of runtime = 'localhost/dangabay/qa/production'

# source : library
# uasge : import required module
import sys

# source : library
# uasge : setup path
import os.path
from os import path

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcGetColname, funcGetDB, funcGetDBTbl, funcReadJson, funcWriteJSON2, funcRedisUpdate

# source : library
# usage : log message to log file
import logging

# source : library
# usage : json parsing
import json

# source : library
# usage : Run Python functions (or any other callable) periodically using a friendly syntax.
import schedule 

# source : library
# usage : handle time-related tasks
import time

# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/updateRedis.log".format(parPath),
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

def funcLoadRedisMain():
    while True:
        # Get database for tablename from lambdaFred4       
        dictDBTblNme = funcGetDBTbl(dictRuntime[0])
        if dictDBTblNme != '0000' and dictDBTblNme != None :
            # Change data from MySQL to JSON format
            dictDBData = funcGetDB(dictDBTblNme, dictRuntime[0])
            dictColName = funcGetColname (dictDBTblNme, dictRuntime[0])             
            dictDataJson = funcWriteJSON2(dictColName, dictDBData)
            # Put data from MySQL in JSON format to Redis
            bFuncRedisUpdate = funcRedisUpdate(dictDBTblNme, dictRuntime[1], dictDataJson)
            if bFuncRedisUpdate == True:
                logging.info('{} updated in redis'.format(dictDBTblNme))
        elif dictDBTblNme == None:
            continue
        elif dictDBTblNme == '0000' :
            break
    logging.info('database is 0000. System exit')
    logging.info('updateRedis.py system STOP') 
    raise SystemExit

# first test ensuring function call as funcLoadRedisMain
if __name__ == '__main__':
    # code run every 5 minutes
    schedule.every(5).minutes.do(funcLoadRedisMain)
    logging.info('env test pass. updateRedis.py system START.')
    print('updateRedis.py system START. Please Wait. Activate 00000 to stop')
    while True:
        schedule.run_pending()
        # interval between each code run : 1 second
        time.sleep(1)
else: 
    logging.info('Illegal function calling. Only run at funcLoadRedisMain level')
    raise SystemExit