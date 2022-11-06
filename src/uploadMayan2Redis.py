""" Module Name: 
   Postgres data to Redis

   Created By: Asif asif@schinkels.com.my 
   Created: 17/8/2022 
   Framework: Python 3.10 on production database to redis
   
   Last Edited: 
    Reason Edited: 
   
"""
listColName = []    # column name list
listDictDBData = []     # list of dicts of database
dictEnv = {}        # dicts of JSON file
dictRuntime = {}    # dicts of runtime variable
strRuntimeMode = '' # string of runtime = 'localhost/dangabay/qa/production'

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

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcGetPostgresDB, funcReadJson, funcWriteJSON, funcRedisUpdate

# source : library
# usage : log message to log file
import logging

# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = ".\\log\\uploadMayan2Redis.log",
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

# ensuring path existence
if os.path.exists(path) is False:    
    logging.info('env dir not found. Ensure env file in the right directory')    
    raise SystemExit  

logging.info('Initiating uploadMayan2Redis.py')
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

# source : library
# usage : logging
import logging

# table documents_documettype
def funcLoadRedisMain(strTblName,strQuery):
    try:
        tplData = {}
        listData = []
        # connect to postgres and get data
        dictDBData = funcGetPostgresDB(strQuery,dictRuntime[2])
        for row in dictDBData:
            tplData.update({"id":str(row[0]),"label":str(row[1])})
            dictUpdatedData = tplData.copy()     
            listData.append(dictUpdatedData)
        # Change data from postgres to JSON format
        strDataJson = json.dumps(listData, indent=None, sort_keys=False)
        # Put data from postgres in JSON format to Redis
        bFuncRedisUpdate = funcRedisUpdate(strTblName,dictRuntime[1],strDataJson)
        if bFuncRedisUpdate is True:
            logging.info('{} updated in redis'.format(strTblName))
        else:
            logging.info('{} NOT updated in redis'.format(strTblName))
    except Exception as strSysErr:
        print (strSysErr)
    finally:
        #listColName.clear()
        #listDictDBData.clear()
        pass

# table documents_documettype
def funcLoadRedisMain1(strTblName,strQuery):
    try:
        tplData = {}
        listData = []
        # connect to postgres and get data
        dictDBData = funcGetPostgresDB(strQuery,dictRuntime[2])
        for row in dictDBData:
            tplData.update({"id":str(row[0]),"label":str(row[1]),"color":str(row[2])})     
            dictUpdatedData = tplData.copy()     
            listData.append(dictUpdatedData)
        # Change data from postgres to JSON format
        strDataJson = json.dumps(listData, indent=None, sort_keys=False)
        # Put data from postgres in JSON format to Redis
        bFuncRedisUpdate = funcRedisUpdate(strTblName,dictRuntime[1],strDataJson)
        if bFuncRedisUpdate is True:
            logging.info('{} updated in redis'.format(strTblName))
        else:
            logging.info('{} NOT updated in redis'.format(strTblName))
    except Exception as strSysErr:
        print (strSysErr)
    finally:
        #listColName.clear()
        #listDictDBData.clear()
        pass
    # table documents_documettype
def funcLoadRedisMain2(strTblName,strQuery):
    try:
        tplData = {}
        listData = []
        # connect to postgres and get data
        dictDBData = funcGetPostgresDB(strQuery,dictRuntime[2])
        for row in dictDBData:
            tplData.update({"id":str(row[0]),"label":str(row[1]),"parent_id":str(row[2])})     
            dictUpdatedData = tplData.copy()     
            listData.append(dictUpdatedData)
        # Change data from postgres to JSON format
        strDataJson = json.dumps(listData, indent=None, sort_keys=False)
        # Put data from postgres in JSON format to Redis
        bFuncRedisUpdate = funcRedisUpdate(strTblName,dictRuntime[1],strDataJson)
        if bFuncRedisUpdate is True:
            logging.info('{} updated in redis'.format(strTblName))
        else:
            logging.info('{} NOT updated in redis'.format(strTblName))
    except Exception as strSysErr:
        print (strSysErr)
    finally:
        #listColName.clear()
        #listDictDBData.clear()
        pass
if __name__ == '__main__':
    logging.info('env test pass. uploadMayan2Redis.py system START')
    funcLoadRedisMain('public.documents_documenttype','SELECT id, label FROM documents_documenttype')
    logging.info('{} uploaded to redis'.format('public.documents_documenttype'))
    funcLoadRedisMain1('public.tags_tag','SELECT id, label, color FROM tags_tag')
    logging.info('{} uploaded to redis'.format('public.tags_tag'))
    funcLoadRedisMain2('public.cabinets_cabinet','SELECT id, label, parent_id FROM cabinets_cabinet')
    logging.info('{} uploaded to redis'.format('public.cabinets_cabinet'))
    logging.info('loadDB2Redis.py system STOP')
else: 
    logging.info('Illegal function calling. Only run at uploadMayan2Redis.py level')
    raise SystemExit
