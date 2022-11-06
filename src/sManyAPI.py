""" Module Name: 
   
   return actor, date, text, cabinet, color
   personnel validation 

   Created By: Asif asif@schinkels.com.my 
   Created: 24/8/2022 
   Framework: Python 3.10 on postgres

   Last Edited: 5/10/2022
    Reason Edited: edit table checker api, add:close Mysql connection
   Last Edited: 29/9/2022
    Reason Edited: Add next stage, prev stage, checkTable
   Last Edited: 8/9/2022
    Reason Edited: Reconstruct stage API
   Last Edited: 6/9/2022
    Reason Edited: Add stage API
   

"""

from email import header
import os
import sys

# source : library
# usage : setup path
import os.path
from os import path

# source : library
# usage : read json file
import json

# source : library
# usage : log message to log file
import logging
from types import NoneType

# source : library
# usage : log message to log file
import psycopg2

# source : library
# usage : web application framework
from flask import Flask, jsonify, request, make_response

# source : library
# usage : connect to redis
import redis

# source : libary
# usage : The datetime module supplies classes for manipulating dates and times
from datetime import datetime

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcGetDB2, funcReadJson, funcWriteJSON, funcWriteJSONforAllClaimAPI, funcGetColname

# source : self
# usage : call custom error
from customError import customError,customError2

# source : library
# uasge : MySQL cennection module
import mysql.connector

# adding env folder to the system path
path = os.getcwd()
parPath = os.path.dirname(path)
sys.path.insert(0,path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/sManyAPI.log".format(parPath),
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

filename1 = ('{}/env/safwa2.env'.format(parPath))

with open(filename1, "r") as f:
    try:
        #ensuring env file is in JSON format
        json.load(f)
    except Exception as e:
        logging.info('file not in JSON Format. Please load only JSON format file')
        raise SystemExit

# read JSON file
dictEnv = funcReadJson(filename1)   
dictRuntime = dictEnv['runtime']
strRuntimeMode = dictRuntime['runtimeMode']
dictRuntime = dictEnv[strRuntimeMode]
dictPostgres = dictRuntime[2]
dictPostgres = dictPostgres['postgres']
dictRedis = dictRuntime[1]
dictRedisVar = dictRedis['redis']
dictAPI = dictRuntime[5]
strAuth= dictAPI['usersAPI']
dictMySQL = dictRuntime[0]
dictMySQL = dictMySQL['MySQL8.0']

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = True

# testing variables
bLocalhostTest = False

@app.route("/DocumentInfo/<DocumentID>")
def funcUsers(DocumentID):
    strResponse = {}
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            conn = psycopg2.connect(host=dictPostgres['host'],
                                    database=dictPostgres['database'],
                                    user=dictPostgres['user'],
                                    password=dictPostgres['password'])
            try:
                # 1st fetch : get verb, document id, date, actor
                strResponse = {}
                cur = conn.cursor()
                strSql = """select actstream_action.target_object_id, actstream_action.verb, actstream_action.timestamp, CONCAT(auth_user.first_name,' ',auth_user.last_name) AS "full_name" from actstream_action join auth_user on actstream_action.actor_object_id = cast(auth_user.id as varchar) join documents_document on actstream_action.target_object_id = cast(documents_document.id as varchar) WHERE (verb = 'documents.document_create') AND (documents_document.id = {} )""".format(DocumentID)
                cur.execute(strSql)
                tplResponse = cur.fetchone()
                strResponse.update({'document_id':str(tplResponse[0]),'verb':str(tplResponse[1]),'date':str(tplResponse[2]),'actor':str(tplResponse[3])})
            except Exception as strError:
                logging.error('fetch 1: {}'.format(strError))
            
            try:
                # 2nd fetch : get cabinet
                strSql2 = "select cabinets_cabinet.label from cabinets_cabinet_documents join cabinets_cabinet on cabinets_cabinet_documents.cabinet_id = cabinets_cabinet.id where cabinets_cabinet_documents.document_id = {}".format(DocumentID)
                cur.execute(strSql2)
                tplResponse2 = cur.fetchall()
                logging.info(tplResponse2)
                listResponse = []
                strResponse2 = {}
                if tplResponse2 == None:
                    strResponse2.update({'cabinet':[]})
                elif len(tplResponse2) == 1:
                    tplResponse2 = tplResponse2[0]
                    strResponse2.update({'cabinet':[str(tplResponse2[0])]})
                else :
                    for a in tplResponse2:
                        listResponse.append(str(a[0]))
                    strResponse2.update({'cabinet':listResponse})
                    listResponse = ""
            except Exception as strError:
                logging.error('fetch 2: {}'.format(strError))

            try:
                # 3rd fetch : get tags (label & color)
                strSql3 = "select tags_tag.label, tags_tag.color from tags_tag_documents join tags_tag on tags_tag_documents.tag_id = tags_tag.id where tags_tag_documents.document_id = {}".format(DocumentID)
                cur.execute(strSql3)
                tplResponse3 =  cur.fetchall()
                logging.info(tplResponse3)
                strResponse3 = {}
                strTempResponse3 = []
                if tplResponse3 == None:
                    strResponse3.update({'tags':[]})
                elif len(tplResponse3) == 1:
                    tplResponse3 = tplResponse3[0]
                    strTempResponse3.append({'label':str(tplResponse3[0]),'color':str(tplResponse3[1])})
                    strResponse3.update({'tag':strTempResponse3})
                else :
                    dictTemp = {}
                    dictTempResponse3 = []
                    for a in tplResponse3:
                        dictTemp.update({'label':str(a[0]),'color':str(a[1])})
                        dictTempResponse3.append(dictTemp)
                        dictTemp = {}
                    strResponse3.update({'tags':dictTempResponse3})
            except Exception as strError:
                logging.error('fetch 3: {}'.format(strError))

            # append all responses to return    
            strResponse.update(strResponse2)
            strResponse.update(strResponse3)
            conn.close()
            return jsonify(strResponse)  

        except Exception as strError:
            logging.error(strError)
            return jsonify({'message':str(strError)})

    return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

# get user list
@app.route("/users")
def funcUser():
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            dictStrResult = ''
            dictResult = {}
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            cursor.execute("SELECT user_id FROM safwa_tender_dev.personnel")
            tplResult = cursor.fetchall()
            for row in tplResult:
                dictStrResult.update({"id":row})
                dictUpdated = dictStrResult.copy()
                dictResult.append(dictUpdated)
                dictResult = {}
            dictData =json.dumps(dictResult)
            connection.close()
            return jsonify(dictData)

        except Exception as strSysErr:
            logging.error(strSysErr)

        pass
    return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/users/<id>")
def funcUsersID(id):
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            dictStrData = {}
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM safwa_tender_dev.personnel WHERE user_id = '%s'" %(id))
            dictResult = cursor.fetchone()
            dictStrData.update({'username':dictResult[17],'first_name' : dictResult[1],'lastname':dictResult[2],'email':dictResult[3]})
            jsonData = json.dumps(dictStrData)
            connection.close()
            return jsonify (jsonData)
            
        except Exception as strSysErr:
            logging.info(strSysErr)
        
        pass
    return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/validate")
def funcValidate():
    auth = request.authorization
    try:
        connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
        cursor = connection.cursor()
        cursor.execute( "SELECT COUNT(*) FROM  safwa_tender_dev.personnel WHERE (is_safwa = TRUE AND  is_active =TRUE AND is_claims = TRUE) AND (user_id = '%s' AND claims_pwd = '%s')" %(auth.username,auth.password))
        dictResult = cursor.fetchone()

        # for user_id and claims_pwd checking
        cursor.execute("SELECT COUNT(*) FROM safwa_tender_dev.personnel WHERE user_id = '%s' OR claims_pwd = '%s'"%(auth.username,auth.password))
        tplWrongInput = cursor.fetchone()
        # for user_id checking
        cursor.execute("SELECT COUNT(user_id) FROM safwa_tender_dev.personnel WHERE user_id = '%s'"%(auth.username))
        tplWrongInput2 = cursor.fetchone()
        # for claims_pwd checking
        cursor.execute("SELECT COUNT(claims_pwd) FROM safwa_tender_dev.personnel WHERE claims_pwd = '%s'"%(auth.password))
        tplWrongInput3 = cursor.fetchone()
        connection.close()

        if dictResult[0] == 1:
            return jsonify ({"message":"Valid User. Approved!"})
            
        elif dictResult[0] == 0:
            if tplWrongInput[0] ==0:
                return make_response('Cannot Verify Username and Password!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })
            elif tplWrongInput2[0] ==0:
                return make_response('Cannot Verify Username!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })
            if tplWrongInput3[0] ==0:
                return make_response('Cannot Verify Password!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

        else :
            return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })
        
    except Exception as strSysErr:
        logging.error(strSysErr)
        return jsonify ({"message":str(strSysErr)})


@app.route("/timeline")
def funcTimeline():
    claim_uuid  = request.args.get('claim_uuid', None)

    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        strTblName = "dangabay.tbl_timeline_claims"
        try:
            listColName = []
            listDictDBData = []
            dictDataJson = []
            # connect to MySQL and get column name 
            tplColName = funcGetColname(strTblName,dictRuntime[0])
            #change column name from tuple to string
            for row in tplColName:
                listColName.append(row[0])
            # connect to MySQL and get data
            dictDBData = funcGetDB2(strTblName, claim_uuid, dictRuntime[0])
            dictDataJson = []
            #if no row for the claim_id
            if len(dictDBData) == 0:
                dictDataJson ={"message":"no id"}
                # return json data
                return dictDataJson
            # if uuid row = 1
            elif len(dictDBData) == 1:
                for row in dictDBData:
                    listDictDBData.append(row)
                # Change data from MySQL to JSON format
                dictDataJson = funcWriteJSON(listColName,listDictDBData)
                # return json data
                return dictDataJson
            #if uuid more than 1
            elif len(dictDBData) > 1:
                for row in dictDBData:
                    listDictDBData.append(row)
                # Change data from MySQL to JSON format
                dictDataJson = funcWriteJSON(listColName,listDictDBData)
                # return json data
                return dictDataJson

        except Exception as strSysErr:
            logging.error(strSysErr)
            return jsonify({"message":strSysErr,"code" : 500})
        pass

    return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/stage")
def funcStage():

    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:

        # if authorized
        try:
            dictStrData = []
            # connect to redis 
            
            if bLocalhostTest == False:
                r = redis.Redis(host=str(dictRedisVar['host']),
                            port=str(dictRedisVar['port']),
                            db=str(dictRedisVar['database']))

            elif bLocalhostTest == True:
                r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
            
            # get data from redis
            redisReply = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])

            # get only stage id and stage name
            for row in redisReply:
                strStageId = row['process_stage_id']
                strStageName = row['process_claim_stage']
                dictStrData.append({'process_stage_id':strStageId,'process_claim_stage':strStageName})

            # return all stage id and stage name 
            return jsonify(dictStrData)
            
        except Exception as strSysErr:
            logging.info(strSysErr)


    # if not authorized
    return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/nextstage")
def funcNextStage():
    uuid  = request.args.get('uuid', None)
    claim_id  = request.args.get('claim_id', None)
    claim_stage  = request.args.get('claim_stage', None)
    claim_stage_uuid  = request.args.get('claim_stage_uuid', None)
    #authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            # connect to redis
            
            if bLocalhostTest == False:
                r = redis.Redis(host=str(dictRedisVar['host']),
                            port=str(dictRedisVar['port']),
                            db=str(dictRedisVar['database']))

            elif bLocalhostTest == True:
                r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
            
            # get data from redis
            redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
            redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
            
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            sqlstmt = "SELECT uuid, claim_id, claim_stage, claim_stage_uuid FROM dangabay.tbl_pbt_pac_claim WHERE uuid = '{}'".format(uuid)
            cursor.execute(sqlstmt)
            tplResult = cursor.fetchone()
            connection.close()
            if tplResult == None:
                 return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })

            # validate parameters with sql data
            elif str(uuid) == str(tplResult[0]) and str(claim_id) == str(tplResult[1]) and str(claim_stage) == str(tplResult[2]) and str(claim_stage_uuid) == str(tplResult[3]):

                # search for current stage
                for rowA in redisReply:
                    # compare current stage uuid from redis with uuid in parameter
                    if rowA['curr_stage_uuid'] == claim_stage_uuid:
                        # if next stage from redis in None
                        if rowA['next_stage_uuid'] == None:
                         
                            return jsonify ({'message' : 'no next stage','uuid':tplResult[0],'claim_id':tplResult[1]})
                            
                        # if next stage from redis is exist    
                        else:
                            for rowB in redisReply2:
                                
                                if rowA['next_stage_uuid'] == rowB['uuid']:

                                    dictReturn = {  
                                                    'uuid':tplResult[0],
                                                    'claim_id':tplResult[1],
                                                    'next stage name' :rowB['process_stage_name'],
                                                    'next stage uuid': rowB['uuid'],
                                                    'error':'0',
                                                    'message' : 'to be determined'
                                                }
                                    return jsonify (dictReturn) 

            # claim uuid from parameter differ from MySQL        
            elif uuid is not tplResult[0]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim uuid"' })
            
            # claim id from parameter differ from MySQL        
            elif claim_id is not tplResult[1]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim id"' })
            
            # claim id from parameter differ from MySQL        
            elif claim_stage is not tplResult[2]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct stage name"' })
            
            # claim id from parameter differ from MySQL        
            elif claim_stage_uuid is not tplResult[3]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct stage uuid"' })    

        except Exception as strError:
            logging.error(str(strError))
            return str(strError)
    else:
        return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/revertstage")
def funcPrevStage():
    uuid  = request.args.get('uuid', None)
    claim_id  = request.args.get('claim_id', None)
    claim_stage  = request.args.get('claim_stage', None)
    claim_stage_uuid  = request.args.get('claim_stage_uuid', None)

    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try :
            # connect to redis
            if bLocalhostTest == False:
                r = redis.Redis(host=str(dictRedisVar['host']),
                            port=str(dictRedisVar['port']),
                            db=str(dictRedisVar['database']))

            elif bLocalhostTest == True:
                r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
            
            # get data from redis
            redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
            redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])

            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            sql = "SELECT uuid, claim_id, claim_stage, claim_stage_uuid FROM dangabay.tbl_pbt_pac_claim WHERE uuid = '{}'".format(uuid)
            cursor.execute(sql)
            tplResult = cursor.fetchone()
            connection.close()
            if tplResult == None:
                 return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })

            # validate parameters with sql data
            elif str(uuid) == str(tplResult[0]) and str(claim_id) == str(tplResult[1]) and str(claim_stage) == str(tplResult[2]) and str(claim_stage_uuid) == str(tplResult[3]):
            
                # search for current stage
                for rowA in redisReply:
                    # compare current stage uuid from redis with uuid in parameter
                    if rowA['curr_stage_uuid'] == claim_stage_uuid:
                        # if previous stage from redis in None
                        if rowA['prev_stage_uuid'] == None:
                            return jsonify ({'message' : 'no previous stage','uuid':tplResult[0],'claim_id':tplResult[1]})
                        # if previous stage from redis is exist
                        else:
                            for rowB in redisReply2:
                                if rowA['prev_stage_uuid'] == rowB['uuid']:
                                    dictReturn = {  
                                                    'uuid':tplResult[0],
                                                    'claim_id':tplResult[1],
                                                    'stage name' :rowB['process_stage_name'],
                                                    'uuid': rowB['uuid'],'error':'0',
                                                    'message' : 'to be determined'
                                                }
                                    return jsonify (dictReturn)
            
            # claim uuid from parameter differ from MySQL        
            elif uuid is not tplResult[0]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })
            
            # claim id from parameter differ from MySQL        
            elif claim_id is not tplResult[1]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct claim id' })
            
            # claim id from parameter differ from MySQL        
            elif claim_stage is not tplResult[2]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct stage name' })
            
            # claim id from parameter differ from MySQL        
            elif claim_stage_uuid is not tplResult[3]:
                return make_response('Unprocessable Entity!',422,{'InvalidArgumentException' : 'Please insert the correct stage uuid' })
                

        except Exception as strError:
            logging.error(str(strError))
            return str(strError)
    # if not authorized
    else :
        return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

@app.route("/draft2Claims")
def funcDraft2Claims():
    stage_uuid  = request.args.get('stage_uuid', None)
    stage_id  = request.args.get('stage_id', None)

    #authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            # connect to redis

            if bLocalhostTest == False:
                r = redis.Redis(host=str(dictRedisVar['host']),
                            port=str(dictRedisVar['port']),
                            db=str(dictRedisVar['database']))

            elif bLocalhostTest == True:
                r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
            
            # get data from redis
            redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
            redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])

            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            sql = "SELECT uuid, process_stage_id FROM dangabay.tbl_claims_process WHERE uuid = '{}'".format(stage_uuid)
            cursor.execute(sql)
            tplResult = cursor.fetchone()
            connection.close()
            if tplResult == None:
                dictReturn = {
                            'error': '1',
                            'error message':'ERR_0002-Please insert the correct claim uuid',
                            'uuid':'',
                            'stage name' :''
                }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })

            # validate parameters with sql data
            elif str(stage_uuid) == str(tplResult[0]) and str(stage_id) == str(tplResult[1]) :
                # search for current stage
                for rowA in redisReply:
                    # compare current stage uuid from redis with uuid in parameter
                    if rowA['curr_stage_uuid'] == stage_uuid:

                        # if previous stage from redis in None
                        if rowA['next_stage_uuid'] == None:
                            return jsonify ({'message' : 'no next stage','uuid':tplResult[0],'claim_id':tplResult[1]})
                        # if previous stage from redis is exist
                        else: 
                            for rowB in redisReply2:
                                if rowA['next_stage_uuid'] == rowB['uuid']:
                                    dictReturn = {  
                                                        'error':'0',
                                                        'error message':'',
                                                        'uuid': rowB['uuid'],
                                                        'stage name' :rowB['process_stage_name']
                                                    }
                                    return jsonify (dictReturn)
            
            # claim uuid from parameter differ from MySQL        
            elif (str(stage_uuid) != str(tplResult[0])) and (str(stage_id) == str(tplResult[1])) :
                dictReturn = {
                            'error': '1',
                            'error message':'ERR0001 - Unprocessable Entity!',
                            'uuid':'',
                            'stage name' :''
                }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })
            
            # process stage id from parameter differ from MySQL        
            elif (str(stage_id) != str(tplResult[1])) and (str(stage_uuid) == str(tplResult[0])):
                dictReturn = {
                            'error': '1',
                            'error message':'Unprocessable Entity!',
                            'uuid':'',
                            'stage name' :''
                }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct process stage id' })

            # claim uuid  and process stage idfrom parameter differ from MySQL
            elif (str(stage_id) != str(tplResult[1])) and (str(stage_uuid) != str(tplResult[0])):
                dictReturn = {
                            'error': '1',
                            'error message':'ERR0002 -  Unprocessable Entity!',
                            'uuid':'',
                            'stage name' :''
                }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct uuid and process stage id'})

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

    # if not authorized
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })       

# start of saveNewClaims API
@app.route("/saveNewClaims")
def funcSaveNewClaims():
    stage_uuid  = request.args.get('stage_uuid', None)
    stage_id  = request.args.get('stage_id', None)
    claim_uuid  = request.args.get('claim_uuid', None)
    source_stage = request.args.get('source_stage', None)

    if source_stage != None:
        source_stage = source_stage.upper()

    #authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        
        try:
            # direct to RGO1
            if (claim_uuid == None or claim_uuid == ''):
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 - Claim UUID required!',
                                'uuid':'',
                                'stage name' :''
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert claim_uuid' })
            
            elif ((stage_uuid == None or stage_uuid == '') and (stage_id == None or stage_id == '') and (claim_uuid != None or claim_uuid != '')): 
                if (source_stage == 'DIRECT' or source_stage == '' or source_stage == None):
            
                    # connect to DB
                    connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                    cursor = connection.cursor()
                    sqlstmnt = "SELECT uuid FROM dangabay.tbl_pbt_pac_claim WHERE uuid = '{}'".format(str(claim_uuid))
                    cursor.execute(sqlstmnt)
                    tplResult = cursor.fetchone()
                    
                    if tplResult != None:
                        dictReturn = {  
                                        'error':'0',
                                        'error message':'',
                                        'uuid': '11e82e2e-2e64-11ed-9819-02c9dcd6ed1e',
                                        'stage name' :'RGO-01-01 CONTRACT EXECUTIVE'
                                        }
                        return jsonify (dictReturn)
                        
                    else:
                        dictReturn = {
                                    'error': '1',
                                    'error message':'ERR_0002 - Unprocessable Entity!Please insert the correct claim uuid',
                                    'uuid':'',
                                    'stage name' :''
                        }
                        return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })
                elif (source_stage== 'DRAFT'):
                    dictReturn = {
                                    'error': '1',
                                    'error message':'ERR_0005 - stage_uuid and stage name_needed',
                                    'uuid':'',
                                    'stage name' :''
                        }
                    return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })

            # claim from draft
            elif (stage_uuid != None or stage_uuid != '') and (stage_id != None or stage_id != '') and (claim_uuid != None or claim_uuid != ''):
                if (source_stage == 'DRAFT' or source_stage == '' or source_stage == None):
                    # connect to redis
                    if bLocalhostTest == False:
                        r = redis.Redis(host=str(dictRedisVar['host']),
                                    port=str(dictRedisVar['port']),
                                    db=str(dictRedisVar['database']))

                    elif bLocalhostTest == True:
                        r = redis.Redis(host='localhost',
                                    port='6380',
                                    db='1')
                    
                    # get data from redis
                    redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
                    redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
                    
                    connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                    cursor = connection.cursor()
                    sql = "SELECT uuid, process_stage_id FROM dangabay.tbl_claims_process WHERE uuid = '{}'".format(stage_uuid)
                    cursor.execute(sql)
                    tplResult = cursor.fetchone()
                    
                    if tplResult == None:
                        dictReturn = {
                                    'error': 'ERR_0001',
                                    'error message':'Unprocessable Entity!',
                                    'uuid':'',
                                    'stage name' :''
                        }
                        return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })
                
                    # validate parameters with sql data
                    if (str(stage_uuid) == 'c80fcd3a-2f17-11ed-9819-02c9dcd6ed1e') and (str(stage_id) == '0000') :
                        
                        # search for current stage
                        for rowA in redisReply:
                            # compare current stage uuid from redis with uuid in parameter
                            if rowA['curr_stage_uuid'] == stage_uuid:

                                # if previous stage from redis in None
                                if rowA['next_stage_uuid'] == None:
                                    return jsonify ({'message' : 'no next stage'})
                                # if previous stage from redis is exist
                                else: 
                                    for rowB in redisReply2:
                                        if rowA['next_stage_uuid'] == rowB['uuid']:
                                            dictReturn = {  
                                                                'error':'0',
                                                                'error message':'',
                                                                'uuid': rowB['uuid'],
                                                                'stage name' :rowB['process_stage_name']
                                                            }
                                            return jsonify (dictReturn)
                    
                    # claim uuid from parameter differ from MySQL        
                    elif (str(stage_uuid) != str(tplResult[0])) and (str(stage_id) == str(tplResult[1])) :
                        dictReturn = {
                                    'error': '1',
                                    'error message':'ERR0001 - Unprocessable Entity!',
                                    'uuid':'',
                                    'stage name' :''
                        }
                        return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })
                    
                    # process stage id from parameter differ from MySQL        
                    elif (str(stage_id) != str(tplResult[1])) and (str(stage_uuid) == str(tplResult[0])):
                        dictReturn = {
                                    'error': '1',
                                    'error message':'Unprocessable Entity!',
                                    'uuid':'',
                                    'stage name' :''
                        }
                        return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct process stage id' })

                    # claim uuid  and process stage idfrom parameter differ from MySQL
                    elif (str(stage_id) != str(tplResult[1])) and (str(stage_uuid) != str(tplResult[0])):
                        dictReturn = {
                                    'error': '1',
                                    'error message':'ERR0002 -  Unprocessable Entity!',
                                    'uuid':'',
                                    'stage name' :''
                        }
                        return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct uuid and process stage id'})

                elif (source_stage == 'DIRECT'):
                    dictReturn = {
                                    'error': '1',
                                    'error message':'ERR_0005 - Incorrect source_stage input',
                                    'uuid':'',
                                    'stage name' :''
                        }
                    return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct claim uuid' })

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

    # if not authorized
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })       

# start of initialinsert2timeline
@app.route("/initialinsert2timeline")
def funcinsertTimeline():
    claim_uuid  = request.args.get('claim_uuid', None)
    stage_id  = request.args.get('stage_id', None)
    stage_name  = request.args.get('stage_name', None)
    claim_stage_uuid = request.args.get('claim_stage_uuid', None)
    user_id = request.args.get('user_id', None)

    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            # connect to DB
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                    database=str(dictMySQL["database"]),
                                                    user=str(dictMySQL["user"]),
                                                    password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            sqlstmnt = "SELECT uuid,claim_stage,claim_id FROM dangabay.tbl_pbt_pac_claim WHERE uuid = '{}'".format(claim_uuid)
            cursor.execute(sqlstmnt)
            tplResult = cursor.fetchone()

            sqlstmnt = "SELECT COUNT(claim_uuid) FROM dangabay.tbl_timeline_claims WHERE claim_uuid = '{}'".format(claim_uuid) 
            cursor.execute(sqlstmnt)
            tplResultTimeline = cursor.fetchone()

            if claim_uuid == None or claim_uuid == '' or claim_uuid == NoneType:
                dictReturn = {
                                        'error': '1',
                                        'error message':'ERR0003 -  No Entity Given!',
                            }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert claim_uuid'})
            
            elif tplResultTimeline[0] > 0 :
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0006 - Data already exist in tbl_timeline_claims!',
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct stage id or stage uuid ' })        

            elif tplResult == None:
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 -  Unprocessable Entity!',
                                'uuid':'',
                                'stage name' :''
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert valid claim_uuid'})

            # check stage_id,stage_name parameter and table pac_claim's stage_id, stage_name
            elif (str(tplResult[0]) == str(claim_uuid)) or (str(tplResult[1]) == str(stage_name))  or (str(tplResult[2]) == str(stage_id)): 
                # connect to redis
                if bLocalhostTest == False:
                    r = redis.Redis(host=str(dictRedisVar['host']),
                                port=str(dictRedisVar['port']),
                                db=str(dictRedisVar['database']))

                elif bLocalhostTest == True:
                    r = redis.Redis(host='localhost',
                                port='6380',
                                db='1')
                    
                # get data from redis
                redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
                redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
                    
                try :
                        # search for current stage
                    for rowA in redisReply:

                        # compare current stage uuid from redis with uuid in parameter
                        if rowA['curr_stage_uuid'] == claim_stage_uuid:

                            for rowB in redisReply2:
                                if rowA['next_stage_uuid'] == rowB['uuid']:
                                 
                                    sqlstmnt2 = "INSERT INTO dangabay.tbl_timeline_claims (claim_uuid,userid,prev_stage,prev_stage_uuid,current_stage_uuid,current_stage,next_stage,next_stage_uuid) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(str(claim_uuid),str(user_id),'DRAFT','c80fcd3a-2f17-11ed-9819-02c9dcd6ed1e',str(claim_stage_uuid),str(stage_name),rowA['next_stage_uuid'],rowB['process_stage_name']  )
                                    cursor.execute(sqlstmnt2)
                                    connection.commit()

                            return jsonify ({'status' : 'success','message':'updated in timeline'})

                except Exception as error:
                    strError = str(error)
                    logging.error(strError)
                    return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

            # check stage_id,stage_name parameter and table pac_claim's stage_id, stage_name
            elif (str(tplResult[0]) != str(claim_uuid)) or (str(tplResult[1]) != str(stage_name))  or (str(tplResult[2]) != str(stage_id)):
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 - Moving first-level stage failed due to wrong or invalid parameters!',
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert another claim' })

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    # if not authorized
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })       

# start of claim/save
# claim will save to pac_claim & timeline
@app.route("/claim/saved",methods=['POST'])
def funcSaveClaims():
    
    write = request.args.get('write', None)
    if write == None or write == '':
        dictError = customError2('err1','write is required!')
        return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })
    if write.upper() == 'INSERT':
        source_stage = request.args.get('source_stage', None)
        if source_stage == None or source_stage == '':
            dictError = customError2('err1','source_stage is required!')
            return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })
        elif source_stage.upper() == 'DRAFT':
            draft_uuid = request.args.get('draft_uuid', None)
            if draft_uuid == None or draft_uuid == '':
                dictError = customError2('err1','draft_uuid is required!')
                return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })
    dictData = request.get_json()
    
    # check authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            # connect to DB
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                database=str(dictMySQL["database"]),
                                                user=str(dictMySQL["user"]),
                                                password=str(dictMySQL["password"]))
            cursor = connection.cursor()

            # write and source_stage parameters required
            #if (write == None or write == '') and (source_stage == None or source_stage ==''):
             #   dictError = customError2('err1','write and source_stage are required!')
              #  return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })

            
            if (write.upper() == 'INSERT') and (source_stage.upper() == 'DRAFT'):
                try:
                    sqlstmnt = "SELECT is_saved FROM dangabay.tbl_pbt_pac_claim_draft WHERE uuid = '{}'".format(draft_uuid)
                    cursor.execute(sqlstmnt)
                    checkResult = cursor.fetchone()
                    
                    if checkResult[0] == 1 :
                        dictError = customError('err4')
                        return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })

                    elif checkResult[0] == 0 :
                        cursor.execute('SELECT UUID()')
                        strUUID = cursor.fetchone()

                        sqlstmnt = "INSERT INTO dangabay.tbl_pbt_pac_claim ( uuid, claim_id,claim_creation_status,rgo_id,pbt_id,pac_cate,pac_id,asl_id,wd_prev,wd_cur,reten_prev,reten_cur,paid_prev,paid_cur,onbehalf_prev,onbehalf_cur,lad_prev,lad_cur,amt_due,date_cop,date_due,date_pe,date_val_prev,claim_stage_uuid,claim_stage,created_userid,last_edited_userid,is_saved) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(str(strUUID[0]),dictData['claim_id'],dictData['claim_creation_status'],dictData['rgo_id'],dictData['pbt_id'],dictData['pac_cate'],dictData['pac_id'],dictData['asl_id'],dictData['wd_prev'],dictData['wd_cur'],dictData['reten_prev'],dictData['reten_cur'],dictData['paid_prev'],dictData['paid_cur'],dictData['onbehalf_prev'],dictData['onbehalf_cur'],dictData['lad_prev'],dictData['lad_cur'],dictData['amt_due'],dictData['date_cop'],dictData['date_due'],dictData['date_pe'],dictData['date_val_prev'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['user_id'],dictData['user_id'],'1')
                        cursor.execute(sqlstmnt)
                        connection.commit()

                        # connect to redis
                        if bLocalhostTest == False:
                            r = redis.Redis(host=str(dictRedisVar['host']),
                                        port=str(dictRedisVar['port']),
                                        db=str(dictRedisVar['database']))

                        elif bLocalhostTest == True:
                            r = redis.Redis(host='localhost',
                                        port='6380',
                                        db='1')
                            
                        # get data from redis
                        redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
                        redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
                        
                            # search for current stage
                        for rowA in redisReply:

                            # compare current stage uuid from redis with uuid in parameter
                            if rowA['curr_stage_uuid'] == dictData['claim_stage_uuid']:

                                for rowB in redisReply2:
                                    if rowA['next_stage_uuid'] == rowB['uuid']:

                                    
                                        sqlstmnt2 = "INSERT INTO dangabay.tbl_timeline_claims (claim_uuid,userid,prev_stage,prev_stage_uuid,current_stage_uuid,current_stage,next_stage,next_stage_uuid) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(str(strUUID[0]),dictData['user_id'],'DRAFT','c80fcd3a-2f17-11ed-9819-02c9dcd6ed1e',dictData['claim_stage_uuid'],dictData['claim_stage'],rowB['process_stage_name'],rowB['uuid'])
                                        cursor.execute(sqlstmnt2)
                                        connection.commit()
                                        
                                        sqlstmnt3 = "UPDATE dangabay.tbl_pbt_pac_claim_draft SET is_saved = '1' WHERE uuid = '{}'".format(str(draft_uuid))
                                        cursor.execute(sqlstmnt3)
                                        connection.commit() 
                                        
                                        return jsonify ({'error':'0','error message' : '','uuid':strUUID[0],'next_stage_uuid':rowB['uuid'],'next_stage':rowB['process_stage_name'],'status':'ok','message' : 'claim created successfully!'})

                except Exception as error:
                    strError = str(error)
                    logging.error(strError)
                    return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

            elif (write.upper() == 'INSERT') and (source_stage.upper() == 'DIRECT'):
                try:
                    cursor.execute('SELECT UUID()')
                    strUUID = cursor.fetchone()

                    sqlstmnt = "INSERT INTO dangabay.tbl_pbt_pac_claim ( uuid,claim_id,claim_creation_status,rgo_id,pbt_id,pac_cate,pac_id,asl_id,wd_prev,wd_cur,reten_prev,reten_cur,paid_prev,paid_cur,onbehalf_prev,onbehalf_cur,lad_prev,lad_cur,amt_due,date_cop,date_due,date_pe,date_val_prev,claim_stage_uuid,claim_stage,created_userid,last_edited_userid,is_saved ) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(str(strUUID[0]),dictData['claim_id'],dictData['claim_creation_status'],dictData['rgo_id'],dictData['pbt_id'],dictData['pac_cate'],dictData['pac_id'],dictData['asl_id'],dictData['wd_prev'],dictData['wd_cur'],dictData['reten_prev'],dictData['reten_cur'],dictData['paid_prev'],dictData['paid_cur'],dictData['onbehalf_prev'],dictData['onbehalf_cur'],dictData['lad_prev'],dictData['lad_cur'],dictData['amt_due'],dictData['date_cop'],dictData['date_due'],dictData['date_pe'],dictData['date_val_prev'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['user_id'],dictData['user_id'],'1')
                    cursor.execute(sqlstmnt)
                    connection.commit()
                    
                    # connect to redis
                    if bLocalhostTest == False:
                        r = redis.Redis(host=str(dictRedisVar['host']),
                                    port=str(dictRedisVar['port']),
                                    db=str(dictRedisVar['database']))

                    elif bLocalhostTest == True:
                        r = redis.Redis(host='localhost',
                                    port='6380',
                                    db='1')
                        
                    # get data from redis
                    redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
                    redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
                    
                
                        # search for current stage
                    for rowA in redisReply:

                        # compare current stage uuid from redis with uuid in parameter
                        if rowA['curr_stage_uuid'] == dictData['claim_stage_uuid']:

                            for rowB in redisReply2:
                                if rowA['next_stage_uuid'] == rowB['uuid']:
                    
                                    sqlstmnt2 = "INSERT INTO dangabay.tbl_timeline_claims (claim_uuid,userid,prev_stage,prev_stage_uuid,current_stage_uuid,current_stage,next_stage,next_stage_uuid) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(str(strUUID[0]),dictData['user_id'],None,None,dictData['claim_stage_uuid'],dictData['claim_stage'],rowB['process_stage_name'],rowB['uuid'])

                                    print (sqlstmnt2)
                                    cursor.execute(sqlstmnt2)
                                    connection.commit()

                                    return jsonify ({'error':'0','error message' : '','uuid':strUUID[0],'next_stage_uuid':rowB['uuid'],'next_stage':rowB['process_stage_name'],'status':'ok','message' : 'claim created successfully!'})

                except Exception as error:
                    strError = str(error)
                    logging.error(strError)
                    return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

            elif (write.upper() == 'UPDATE') :
                if (dictData['claim_id'] != None) or (dictData['claim_id'] != ''):
                    try:
                        
                        sqlstmnt = "UPDATE dangabay.tbl_pbt_pac_claim SET uuid ='{}', claim_id = '{}',claim_creation_status = '{}' ,rgo_id = '{}' ,pbt_id = '{}' ,pac_cate = '{}' ,pac_id = '{}' ,asl_id = '{}' ,wd_prev= '{}' ,wd_cur = '{}' ,reten_prev = '{}' ,reten_cur = '{}' ,paid_prev = '{}' ,paid_cur = '{}' ,onbehalf_prev = '{}' ,onbehalf_cur = '{}',lad_prev = '{}',lad_cur = '{}' ,amt_due = '{}' ,date_cop = '{}' ,date_due = '{}' ,date_pe = '{}' ,date_val_prev = '{}' ,claim_stage_uuid= '{}' ,claim_stage = '{}' ,last_edited_userid = '{}' ,is_saved = '{}' WHERE uuid = '{}'".format(dictData['uuid'],dictData['claim_id'],dictData['claim_creation_status'],dictData['rgo_id'],dictData['pbt_id'],dictData['pac_cate'],dictData['pac_id'],dictData['asl_id'],dictData['wd_prev'],dictData['wd_cur'],dictData['reten_prev'],dictData['reten_cur'],dictData['paid_prev'],dictData['paid_cur'],dictData['onbehalf_prev'],dictData['onbehalf_cur'],dictData['lad_prev'],dictData['lad_cur'],dictData['amt_due'],dictData['date_cop'],dictData['date_due'],dictData['date_pe'],dictData['date_val_prev'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['user_id'],'1',dictData['uuid'])

                        cursor.execute(sqlstmnt)
                        connection.commit()

                         # connect to redis
                        if bLocalhostTest == False:
                            r = redis.Redis(host=str(dictRedisVar['host']),
                                        port=str(dictRedisVar['port']),
                                        db=str(dictRedisVar['database']))

                        elif bLocalhostTest == True:
                            r = redis.Redis(host='localhost',
                                        port='6380',
                                        db='1')
                            
                        # get data from redis
                        redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
                        redisReply2 = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
                        
                    
                            # search for current stage
                        for rowA in redisReply:

                            # compare current stage uuid from redis with uuid in parameter
                            if rowA['curr_stage_uuid'] == dictData['claim_stage_uuid']:

                                for rowB in redisReply2:
                                    if rowA['next_stage_uuid'] == rowB['uuid']:


                                        sqlstmnt2 = "INSERT INTO dangabay.tbl_timeline_claims (claim_uuid,userid,prev_stage,prev_stage_uuid,current_stage_uuid,current_stage,next_stage,next_stage_uuid) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(dictData['uuid'],dictData['user_id'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['claim_stage_uuid'],dictData['claim_stage'],rowB['process_stage_name']  ,rowB['uuid'])
                                        cursor.execute(sqlstmnt2)
                                        connection.commit()

                                        return jsonify ({'error':'0','error message' : '','next_stage_uuid':rowB['uuid'],'next_stage':rowB['process_stage_name'] ,'status':'ok','message' : 'claim updated successfully!'})

                    except Exception as error:
                        strError = str(error)
                        logging.error(strError)
                        return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

                else :
                    dictError = customError2('err1','claim_id required!')
                    return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })

# start of claim/draft
# claim will save to pac_claim
@app.route("/claim/draft",methods=['POST'])
def funcSaveDraft():
    
    write = request.args.get('write', None)
    dictData = request.get_json()
    
    # check authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            # connect to DB
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                    database=str(dictMySQL["database"]),
                                                    user=str(dictMySQL["user"]),
                                                    password=str(dictMySQL["password"]))
            cursor = connection.cursor()
            
            # write and source_stage parameters required
            if (write == (None or '')) :
                dictError = customError2('err1','write and source_stage are required!')
                return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })

            elif (write.upper() == 'UPDATE') :
                if (dictData['uuid'] != None) or (dictData['uuid'] != ''):
                    try:
                        sqlstmnt = "UPDATE dangabay.tbl_pbt_pac_claim_draft SET uuid ='{}', claim_id = '{}',claim_creation_status = '{}' ,rgo_id ='{}',pbt_id = '{}' ,pac_cate = '{}' ,pac_id = '{}' ,asl_id = '{}' ,wd_prev= '{}' ,wd_cur = '{}' ,reten_prev = '{}' ,reten_cur = '{}' ,paid_prev = '{}' ,paid_cur = '{}' ,onbehalf_prev = '{}' ,onbehalf_cur = '{}',lad_prev = '{}',lad_cur = '{}' ,amt_due = '{}' ,date_cop = '{}' ,date_due = '{}' ,date_pe = '{}' ,date_val_prev = '{}' ,claim_stage_uuid= '{}' ,claim_stage = '{}' ,last_edited_userid = '{}' ,is_saved = '{}' WHERE uuid = '{}'".format(dictData['uuid'],None,dictData['claim_creation_status'],dictData['rgo_id'],dictData['pbt_id'],dictData['pac_cate'],dictData['pac_id'],dictData['asl_id'],dictData['wd_prev'],dictData['wd_cur'],dictData['reten_prev'],dictData['reten_cur'],dictData['paid_prev'],dictData['paid_cur'],dictData['onbehalf_prev'],dictData['onbehalf_cur'],dictData['lad_prev'],dictData['lad_cur'],dictData['amt_due'],dictData['date_cop'],dictData['date_due'],dictData['date_pe'],dictData['date_val_prev'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['last_edited_userid'],'0',dictData['uuid'])

                        cursor.execute(sqlstmnt)
                        connection.commit()

                        return jsonify ({'error':'0','error message' : '','status':'ok','message' : 'claim updated successfully!'})

                    except Exception as error:
                        strError = str(error)
                        logging.error(strError)
                        return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
                        
                else :
                    dictError = customError2('err1','claim_id required!')
                    return make_response(dictError,422,{'InvalidArgumentException' : 'Please insert the correct parameter/s ' })

            elif (write.upper() == 'INSERT') :
                try:
                    cursor.execute('SELECT UUID()')
                    strUUID = cursor.fetchone()

                    sqlstmnt = "INSERT INTO dangabay.tbl_pbt_pac_claim_draft ( uuid, claim_id,claim_creation_status,rgo_id,pbt_id,pac_cate,pac_id,asl_id,wd_prev,wd_cur,reten_prev,reten_cur,paid_prev,paid_cur,onbehalf_prev,onbehalf_cur,lad_prev,lad_cur,amt_due,date_cop,date_due,date_pe,date_val_prev,claim_stage_uuid,claim_stage,created_userid,last_edited_userid,is_saved) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(str(strUUID[0]),None,dictData['claim_creation_status'],dictData['rgo_id'],dictData['pbt_id'],dictData['pac_cate'],dictData['pac_id'],dictData['asl_id'],dictData['wd_prev'],dictData['wd_cur'],dictData['reten_prev'],dictData['reten_cur'],dictData['paid_prev'],dictData['paid_cur'],dictData['onbehalf_prev'],dictData['onbehalf_cur'],dictData['lad_prev'],dictData['lad_cur'],dictData['amt_due'],dictData['date_cop'],dictData['date_due'],dictData['date_pe'],dictData['date_val_prev'],dictData['claim_stage'],dictData['claim_stage_uuid'],dictData['user_id'],dictData['last_edited_userid'],'0')
                    cursor.execute(sqlstmnt)
                    connection.commit()
                
                    return jsonify ({'error':'0','error message' : '','uuid':strUUID[0],'status':'ok','message' : 'claim created successfully!'})

                except Exception as error:
                    strError = str(error)
                    logging.error(strError)
                    return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })

@app.route('/allClaim')
def funcAllClaim():
    start_date  = request.args.get('start_date',None)
    end_date  = request.args.get('end_date',None)
    
    # initializing format
    format = "%Y-%m-%d"
 
    # checking if format matches the date
    res = True

    if start_date != None :
        try:
            res = bool(datetime.strptime(start_date, format))
        except ValueError:
            return jsonify ({"message":"please insert correct date"})

    if end_date != None :
        try:
            res = bool(datetime.strptime(end_date, format))
        except ValueError:
            return jsonify ({"message":"please insert correct date"})

    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            listStrDataJson = []
            listColName = []
            # connect to DB
            connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                    database=str(dictMySQL["database"]),
                                                    user=str(dictMySQL["user"]),
                                                    password=str(dictMySQL["password"]))
            cursor = connection.cursor()

            # if no date given
            if ((start_date == '' or start_date == None) and (end_date == '' or end_date == None)):

                sqlstmnt = "SELECT * FROM dangabay.tbl_pbt_pac_claim "
                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                
                for row in tplColname:
                    listColName.append(str(row[0]))
                
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)

            
            # if only start date given
            elif ((start_date != '' or start_date != None) and (end_date == '' or end_date == None)):

                sqlstmnt = "SELECT uuid, claim_creation_status, claim_id, rgo_id, pbt_id, pac_id, asl_id, wd_prev, wd_cur, reten_prev, reten_cur, deduct_prev, deduct_cur, paid_prev, paid_cur, pac_cate, pac_subcate, claim_status, interim_no, onbehalf_prev, onbehalf_cur, lad_prev, lad_cur, sst, date_created, date_update, date_val_prev, date_pe, date_cop, opn_ce, opn_ce_date, opn_ce_by, opn_eng, opn_eng_date, opn_eng_by, opn_am, opn_am_date, opn_am_by, opnhq_jeng, opnhq_jeng_by, opnhq_jeng_date, opnhq_eng, opnhq_eng_by, opnhq_eng_date, cpd_aqs, cpd_aqs_by, cpd_aqs_date, cpd_sqs, cpd_sqs_by, cpd_sqs_date, cpd_hod, cpd_hod_by, cpd_hod_date, ed, ed_by, ed_date, pending, reten, sst1, amt_sub_vo_add, amt_sub_vo_omit, amt_estm_prev, amt_due, amt_sub, amt_sub_fm, date_due, amt_estm_cur, recommended, is_deleted, is_saved, sst_pre, claim_stage, claim_stage_uuid, created_userid, DATE(created_datetime) AS date, last_edited_userid, last_edited_datetime FROM dangabay.tbl_pbt_pac_claim WHERE DATE(created_datetime) > '{}'".format(str(start_date))

                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                
                for row in tplColname:
                    listColName.append(str(row[0]))
                
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)

            # if only end date given
            elif ((start_date == None or start_date == '') and (end_date != None or end_date != '')):
                
                sqlstmnt = "SELECT uuid, claim_creation_status, claim_id, rgo_id, pbt_id, pac_id, asl_id, wd_prev, wd_cur, reten_prev, reten_cur, deduct_prev, deduct_cur, paid_prev, paid_cur, pac_cate, pac_subcate, claim_status, interim_no, onbehalf_prev, onbehalf_cur, lad_prev, lad_cur, sst, date_created, date_update, date_val_prev, date_pe, date_cop, opn_ce, opn_ce_date, opn_ce_by, opn_eng, opn_eng_date, opn_eng_by, opn_am, opn_am_date, opn_am_by, opnhq_jeng, opnhq_jeng_by, opnhq_jeng_date, opnhq_eng, opnhq_eng_by, opnhq_eng_date, cpd_aqs, cpd_aqs_by, cpd_aqs_date, cpd_sqs, cpd_sqs_by, cpd_sqs_date, cpd_hod, cpd_hod_by, cpd_hod_date, ed, ed_by, ed_date, pending, reten, sst1, amt_sub_vo_add, amt_sub_vo_omit, amt_estm_prev, amt_due, amt_sub, amt_sub_fm, date_due, amt_estm_cur, recommended, is_deleted, is_saved, sst_pre, claim_stage, claim_stage_uuid, created_userid, DATE(created_datetime) AS date, last_edited_userid, last_edited_datetime FROM dangabay.tbl_pbt_pac_claim WHERE DATE(created_datetime) < '{}'".format(str(end_date))

                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                
                for row in tplColname:
                    listColName.append(str(row[0]))
                
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)

            # if start date and end date is given
            elif ((start_date != '' or start_date != None) and (end_date != '' or end_date != None)):

                sqlstmnt = "SELECT uuid, claim_creation_status, claim_id, rgo_id, pbt_id, pac_id, asl_id, wd_prev, wd_cur, reten_prev, reten_cur, deduct_prev, deduct_cur, paid_prev, paid_cur, pac_cate, pac_subcate, claim_status, interim_no, onbehalf_prev, onbehalf_cur, lad_prev, lad_cur, sst, date_created, date_update, date_val_prev, date_pe, date_cop, opn_ce, opn_ce_date, opn_ce_by, opn_eng, opn_eng_date, opn_eng_by, opn_am, opn_am_date, opn_am_by, opnhq_jeng, opnhq_jeng_by, opnhq_jeng_date, opnhq_eng, opnhq_eng_by, opnhq_eng_date, cpd_aqs, cpd_aqs_by, cpd_aqs_date, cpd_sqs, cpd_sqs_by, cpd_sqs_date, cpd_hod, cpd_hod_by, cpd_hod_date, ed, ed_by, ed_date, pending, reten, sst1, amt_sub_vo_add, amt_sub_vo_omit, amt_estm_prev, amt_due, amt_sub, amt_sub_fm, date_due, amt_estm_cur, recommended, is_deleted, is_saved, sst_pre, claim_stage, claim_stage_uuid, created_userid, DATE(created_datetime) AS date, last_edited_userid, last_edited_datetime FROM dangabay.tbl_pbt_pac_claim WHERE DATE(created_datetime) BETWEEN '{}' AND '{}'".format(str(start_date),str(end_date))

                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                
                for row in tplColname:
                    listColName.append(str(row[0]))
                
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()


    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })     

# get claim infos based on claim id
@app.route('/claimInfo')
def funcClaimInfo():
    claim_id  = request.args.get('claim_id',None)
    
    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            if (claim_id == None or claim_id == ''):
                dictReturn = customError('eer1')
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct stage id or stage uuid ' })

            elif (claim_id != None or claim_id != ''):
                listStrDataJson = []
                listColName = []
                # connect to DB
                connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                cursor = connection.cursor()
                
                sqlstmnt = "SELECT * FROM dangabay.tbl_pbt_pac_claim WHERE claim_id = '{}'".format(claim_id)
                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                    
                for row in tplColname:
                    listColName.append(str(row[0]))
                    
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)

        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })

# get claim stage based on claim_id
@app.route('/claimStageInfo')
def funcClaimStageInfo():
    claim_id  = request.args.get('claim_id',None)
    
    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            if (claim_id != None or claim_id != ''):
                # connect to DB
                connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                cursor = connection.cursor()
                
                sqlstmnt = "SELECT claim_stage FROM dangabay.tbl_pbt_pac_claim WHERE claim_id = '{}'".format(claim_id)
                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchone()
                return jsonify ({'claim_stage': tplResult[0]})

            elif (claim_id == None or claim_id == ''):
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 - Please insert claim uuid!',
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct stage id or stage uuid ' })

            
                
        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })

# get claim based on package id
@app.route('/claimOfPac')
def funcClaimOfPac():
    pac_id  = request.args.get('pac_id',None)
    
    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            if (pac_id != None or pac_id != ''):
                listStrDataJson = []
                listColName = []
                # connect to DB
                connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                cursor = connection.cursor()
                
                sqlstmnt = "SELECT * FROM dangabay.tbl_pbt_pac_claim WHERE pac_id = '{}'".format(pac_id)
                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim',dictRuntime[0])
                    
                for row in tplColname:
                    listColName.append(str(row[0]))
                    
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)
                
            elif (pac_id == None or pac_id == ''):
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 - Please insert package id!',
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct stage id or stage uuid ' })

            
                
        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })

# get claim SOQ based on claim id
@app.route('/claimSOQ')
def funcClaimSOQ():
    claim_id  = request.args.get('claim_id',None)
    
    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try:
            if (claim_id != None or claim_id != ''):
                listStrDataJson = []
                listColName = []
                # connect to DB
                connection = mysql.connector.connect(host=str(dictMySQL["host"]),
                                                        database=str(dictMySQL["database"]),
                                                        user=str(dictMySQL["user"]),
                                                        password=str(dictMySQL["password"]))
                cursor = connection.cursor()
                
                sqlstmnt = "SELECT * FROM dangabay.tbl_pbt_pac_claim_soq WHERE claim_id = '{}'".format(claim_id)
                cursor.execute(sqlstmnt)
                tplResult = cursor.fetchall()

                tplColname = funcGetColname('dangabay.tbl_pbt_pac_claim_soq',dictRuntime[0])
                    
                for row in tplColname:
                    listColName.append(str(row[0]))
                    
                strDataJson = funcWriteJSONforAllClaimAPI(listColName,tplResult)
                listStrDataJson.append(strDataJson)
                return jsonify (listStrDataJson)
                
            elif (claim_id == None or claim_id == ''):
                dictReturn = {
                                'error': '1',
                                'error message':'ERR0001 - Please insert package id!',
                    }
                return make_response(dictReturn,422,{'InvalidArgumentException' : 'Please insert the correct stage id or stage uuid ' })

            
                
        except Exception as error:
            strError = str(error)
            logging.error(strError)
            return make_response(strError,500,{'InvalidArgumentException' : 'server encountered an unexpected condition that prevented it from fulfilling the request'})
        
        finally:
            cursor.close()
            connection.close()
    else :
        return make_response('Could not verify!',401,{'Internal Server Error' : 'Basic realm ="Login Required"' })


@app.route('/tableChecker')
def TableCheckerV2():

    # authorization
    auth = request.authorization
    if auth and auth.username == strAuth['username'] and auth.password == strAuth['password']:
        try :
            # name of tables that need to be checked 
            listTotalTable = ['postcode_cust', 'postcode_cust_activities', 'postcode_cust_payment', 'tbl_audit_trail', 'tbl_claims_process', 'tbl_claims_process_main', 'tbl_lambda_personnel', 'tbl_lambdafred_infracit', 'tbl_lambdafred_infracit_failed', 'tbl_pbt_pac_claim_cop', 'tbl_pbt_pac_claim_cop_mirror', 'tbl_pbt_pac_claim', 'tbl_pbt_pac_claim_draft', 'tbl_pbt_pac_claim_draft_mirror', 'tbl_pbt_pac_claim_mirror', 'tbl_pbt_pac_claim_route', 'tbl_pbt_pac_claim_route_draft', 'tbl_pbt_pac_claim_route_draft_mirror', 'tbl_pbt_pac_claim_route_mirror', 'tbl_pbt_pac_claim_routebq', 'tbl_pbt_pac_claim_routebq_draft', 'tbl_pbt_pac_claim_routebq_draft_mirror', 'tbl_pbt_pac_claim_routebq_mirror', 'tbl_pbt_pac_claim_soq', 'tbl_pbt_pac_claim_soq_draft', 'tbl_pbt_pac_claim_soq_draft_mirror', 'tbl_pbt_pac_claim_soq_mirror', 'tbl_pbt_pac_claim_wd', 'tbl_pbt_pac_claim_wd_draft', 'tbl_pbt_pac_claim_wd_draft_mirror', 'tbl_pbt_pac_claim_wd_mirror','postcode','postcode_state','tbl_lambdafred','tbl_pbt_pac_claim_soq_mirror','tbl_pbt_pac_claim_wd','tbl_pbt_pac_claim_wd_draft','tbl_pbt_pac_claim_wd_draft_mirror','tbl_pbt_pac_claim_wd_mirror','tbl_timeline_claims']
            
            listStrTableNames = []
            listMissingTable = []   # create list to store missing table if avail 

            # build connection to production database (QA MySQL for now )
            conn = mysql.connector.connect(host=str(dictMySQL["host"]),
                                            database=str(dictMySQL["database"]),
                                            user=str(dictMySQL["user"]),
                                            password=str(dictMySQL["password"]))

            # create a cursor 
            cursor = conn.cursor()

            # get table names of database
            #====dangabay====
            cursor.execute("SHOW TABLES")
            tplTableNames = cursor.fetchall()
            for row in tplTableNames:
                listStrTableNames.append(row[0])

            # append table name of the database that differ from the list on top
            for row in listTotalTable:
                if row not in listStrTableNames:
                    listMissingTable.append(row)
                elif row in listStrTableNames:
                    continue
                    
            # if all tables from both list matches
            if len(listMissingTable) < 1:            
                return jsonify ({"Message": "All table is available", "Status" : True})

            # if there is any table that did not match
            else:
                #HTTP 409 error status: The HTTP 409 status code (Conflict) indicates that the request could not be processed because of conflict in the request, such as the requested resource is not in the expected state, or the result of processing the request would create a conflict within the resource
                dictResponse = {
                    "Message": "Missing Tables",
                    "Missing Tables" : listMissingTable,
                    "Status" : False
                     }
                return make_response(dictResponse,409,{'Conflict' : 'request could not be processed because of conflict in the request' })
    
        except Exception as strError:
            logging.error(strError)
            return str(strError)  

        finally:
            conn.close()
    # if not authorized
    else :
        return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })

if __file__ == '__main__':
    # 0.0.0.0 indicate global value for server.  This allow for startup at Windows and Ubuntu servers
    logging.info('env test pass. sAPI.py system START.')
    app.run( host= "0.0.0.0" )