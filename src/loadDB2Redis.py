""" Module Name: 
   funcLoadRedisMain python file to load dangabay database to redeis

   Created By: Asif asif@schinkels.com.my 
   Created: 25/6/2022 
   Framework: Python 3.10 on production database to redis
   
   Last Edited: 26/7/2022
    Reason Edited: Env path adjustment for 10.2.1.64 .
   Last Edited: 7/7/2022
    Reason Edited: Reconstruct the code. Clear list memory. Komen
   Last Edited: 28/6/2022
    Reason Edited: completing the code
   
"""
dictDBData    = {}  # dicts of database
colName = {}        # dicts of column names
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
from funcFile import funcGetDB, funcReadJson, funcWriteJSON, funcRedisUpdate, funcGetColname

# source : library
# usage : log message to log file
import logging

# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/loadDB2Redis.log".format(parPath),
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

logging.info('Initiating loadDB2Redis.py')

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

# source : library
# usage : logging
import logging

def funcLoadRedisMain(strTblName):
    try:
        listColName = []
        listDictDBData = []
        # connect to MySQL and get column name 
        tplColName = funcGetColname(strTblName,dictRuntime[0])
        #change column name from tuple to string
        for row in tplColName:
            listColName.append(row[0])
        # connect to MySQL and get data
        dictDBData = funcGetDB(strTblName,dictRuntime[0])
        for row in dictDBData:     
            listDictDBData.append(row)
        # Change data from MySQL to JSON format
        dictDataJson = funcWriteJSON(listColName,listDictDBData)
        # Put data from MySQL in JSON format to Redis
        bFuncRedisUpdate = funcRedisUpdate(strTblName,dictRuntime[1],dictDataJson)
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

# first test ensuring function call as funcLoadRedisMain
if __name__ == '__main__':
    logging.info('env test pass. loadDB2Redis.py system START')
    # lookup tables list that will be uploaded in Redis
    #funcLoadRedisMain('infracit_pbtdb.t_pbt_routelist')
    #funcLoadRedisMain('infracit_sharedb.tref_route_treatment')
    funcLoadRedisMain('infracit_sharedb.tref_pac_zone')
    funcLoadRedisMain('infracit_sharedb.tref_bahanbakar')
    funcLoadRedisMain('infracit_sharedb.tref_buatan')
    funcLoadRedisMain('infracit_sharedb.tref_empdesignation')
    funcLoadRedisMain('infracit_pbtdb.pbt_sor')
    funcLoadRedisMain('infracit_sharedb.tref_state')
    funcLoadRedisMain('infracit_sharedb.tref_district')
    funcLoadRedisMain('infracit_sharedb.tref_rgo')
    funcLoadRedisMain('infracit_sharedb.tref_pbt')
    funcLoadRedisMain('infracit_sharedb.tref_taman')
    funcLoadRedisMain('infracit_sharedb.tref_mukim')
    funcLoadRedisMain('infracit_pbtdb.pbt_sor_item')
    funcLoadRedisMain('infracit_pbtdb.pbt_sor_detailsitem')
    funcLoadRedisMain('infracit_sharedb.tref_department')
    funcLoadRedisMain('infracit_sharedb.tref_fin_year')
    funcLoadRedisMain('infracit_sharedb.tref_pac_cate')
    funcLoadRedisMain('infracit_sharedb.tref_pac_subcate')
    funcLoadRedisMain('infracit_sharedb.tref_pac_status')
    funcLoadRedisMain('infracit_sharedb.tref_asl_comstatus')
    funcLoadRedisMain('infracit_sharedb.tref_asl_empdesignation')
    funcLoadRedisMain('infracit_sharedb.tref_asl_plant')
    funcLoadRedisMain('infracit_sharedb.tref_asl_regbody')
    funcLoadRedisMain('infracit_sharedb.tref_asl_scopeofwork')
    funcLoadRedisMain('infracit_sharedb.tref_asl_specialization')
    funcLoadRedisMain('infracit_sharedb.tref_asl_swdesignation')
    funcLoadRedisMain('infracit_sharedb.tref_asl_typeofcompany')
    funcLoadRedisMain('infracit_sharedb.tref_nationality')
    funcLoadRedisMain('infracit_sharedb.tref_jenis_aduan')
    funcLoadRedisMain('infracit_sharedb.tref_dun')
    funcLoadRedisMain('infracit_sharedb.tref_parliament')
    funcLoadRedisMain('infracit_sharedb.tref_office_location')
    funcLoadRedisMain('infracit_sharedb.tref_asset_code')
    funcLoadRedisMain('infracit_sharedb.tref_asset_model')
    funcLoadRedisMain('infracit_sharedb.tref_category_assets')
    funcLoadRedisMain('infracit_sharedb.tref_itemlocations')
    funcLoadRedisMain('infracit_sharedb.tref_model')
    funcLoadRedisMain('infracit_sharedb.tref_jenisbadan')
    funcLoadRedisMain('infracit_sharedb.tref_kelaskenderaan')
    funcLoadRedisMain('infracit_sharedb.tref_enjin')
    funcLoadRedisMain('infracit_sharedb.tref_status_kenderaan')
    funcLoadRedisMain('infracit_sharedb.tref_emp_level')
    funcLoadRedisMain('infracit_sharedb.tref_emp_levels')
    funcLoadRedisMain('infracit_sharedb.tref_workstatus')
    funcLoadRedisMain('infracit_sharedb.tref_filling_color')
    funcLoadRedisMain('infracit_sharedb.tref_project')
    funcLoadRedisMain('dangabay.tbl_claims_process')
    funcLoadRedisMain('dangabay.tbl_claims_process_main')
    
    logging.info('loadDB2Redis.py system STOP')
else: 
    logging.info('Illegal function calling. Only run at funcLoadRedisMain level')
    raise SystemExit
