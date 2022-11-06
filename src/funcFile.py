""" Module Name: 
   Connect to strDatabase
   Parse string
   DB to JSON file
   Redis Conn
   JSON to Redis

   Created By: Asif asif@schinkels.com.my 
   Created: 6/6/2022 
   Framework: Python 3.10 on dangabay
   Last Edited: 19/6/2022

   Reason Edited: Clean the code
"""
# source : library
# usage : strDatabase connection
import logging
import mysql.connector

# source : library
# usage : read json file
import json

# source : library
# usage : connect to redis
import redis

# source : library
# usage :   Store the object in Redis using the json().set() method. 
#           The first argument, person:1 is the name of the key that will reference the JSON.
#           The second argument is a JSON path. We use Path.root_path(), as this is a new object
from redis.commands.json.path import Path

# source : library
# usage : PostgreSQL adapter for the Python programming language
import psycopg2

# open JSON file
def funcReadJson (strFilename):
    with open(strFilename, "r") as f:
        jsonFile = json.load(f)
    return jsonFile

# testing variables
bLocalhostTest = True

# write strDatabase strstrData in JSON file
def funcWriteJSON(listCol ,listStrData):  
    try:
        listUpdatedDictData =[]
        dictStrData = {}
        for aRow in listStrData:
            j=0           
            for bRow in listCol:
                if j >= len(aRow) :
                    value = 'None'
                else:
                    value = str(aRow[j])
                dictStrData.update({str(bRow):value})
                j+=1
            dictUpdatedData = dictStrData.copy()
            listUpdatedDictData.append(dictUpdatedData)
        
        strDataJson = json.dumps(listUpdatedDictData, indent=0 , sort_keys=False)
        listUpdatedDictData.clear()
        return strDataJson
       
    except Exception as strSysError:             
        print ('Error Code conversion from DB to JSON')
        print (strSysError)
        logging.error('Error Code conversion from DB to JSON : {}'.format(strSysError))

#listStrData came in list        
def funcWriteJSON2(listCol ,listStrData):  
    try:
        listUpdatedDictData =[]
        dictStrData = {}
        for aRow in listStrData:
            j=0           
            for bRow in listCol:
                key = bRow[0]
                if j >= len(aRow):
                    value = 'None'
                else:
                    value = str(aRow[j])
                dictStrData.update({key:value})
                j+=1
            dictUpdatedData = dictStrData.copy()
            listUpdatedDictData.append(dictUpdatedData)
        
        strDataJson = json.dumps(listUpdatedDictData, indent=None, sort_keys=False)
        return strDataJson
       
    except Exception as strSysError:             
        print ('Error Code conversion from DB to JSON')
        print (strSysError)
        logging.error('Error Code conversion from DB to JSON'+ strSysError)

#listStrData came in list of tuple
def funcWriteJSON3(listCol ,listStrData):  
    try:
        listUpdatedDictData =[]
        dictStrData = {}
        for aRow in listStrData:
            j=0           
            for bRow in listCol:
                key = bRow[0]
                if j >= len(aRow):
                    value = 'None'
                else:
                    value = str(aRow)
                dictStrData.update({key:value})
                j+=1
            dictUpdatedData = dictStrData.copy()
            listUpdatedDictData.append(dictUpdatedData)
        
        strDataJson = json.dumps(listUpdatedDictData, indent=None, sort_keys=False)
        return strDataJson
       
    except Exception as strSysError:             
        print ('Error Code conversion from DB to JSON')
        print (strSysError)
        logging.error('Error Code conversion from DB to JSON'+ strSysError)

# write strDatabase strstrData in JSON file
def funcWriteJSONforAllClaimAPI(listCol ,listStrData):  
    try:
        listUpdatedDictData =[]
        dictStrData = {}
        for aRow in listStrData:
            j=0           
            for bRow in listCol:
                if j >= len(aRow) :
                    value = 'None'
                else:
                    value = str(aRow[j])
                dictStrData.update({str(bRow):value})
                j+=1
            dictUpdatedData = dictStrData.copy()
            listUpdatedDictData.append(dictUpdatedData)

        return listUpdatedDictData
       
    except Exception as strSysError:             
        print ('Error Code conversion from DB to JSON')
        print (strSysError)
        logging.error('Error Code conversion from DB to JSON : {}'.format(strSysError))

# Connect to Redis and set JSON file in Redis
def funcRedisUpdate(strFilename,strData,jsonFile):
    strData = strData['redis']
    try:
        if bLocalhostTest == False:
            r = redis.Redis(host=str(strData['host']),
                        port=str(strData['port']),
                        db=str(strData['database']))

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
        r.json().set(strFilename,Path.root_path(), jsonFile)
        print("{} updated in redis".format(strFilename))
        return True
    except Exception as strSysErr:
        print (strSysErr)
        print ("{} not updated in redis".format(strFilename))
        return False
    
        
# get table names from strDatabase
def funcGetColname(strTablename,strData):
    strData = strData["MySQL8.0"]
    tplResult = {}
    try:
        connection = mysql.connector.connect(host=str(strData["host"]),
                                                database=str(strData["database"]),
                                                user=str(strData["user"]),
                                                password=str(strData["password"]))
        cursor = connection.cursor()
        cursor.execute("SHOW COLUMNS FROM {}".format(strTablename))
        tplResult = cursor.fetchall()

    except Exception as strSysError :
        print (strSysError)
        print ("cannot get column names from {}".format(strTablename))
        logging.error()
    
    finally:
        connection.close()
        return tplResult

# get strstrData from strDatabase
def funcGetDB(strTablename,strData):
    strData = strData["MySQL8.0"]
    try:
        connection = mysql.connector.connect(host=str(strData["host"]),
                                                database=str(strData["database"]),
                                                user=str(strData["user"]),
                                                password=str(strData["password"]))
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM {}".format(strTablename))
        dictResult = cursor.fetchall()
        return dictResult

    except Exception as strSysError :
        print (strSysError)
        print ("cannot get database from {}".format(strTablename))
        logging.error("cannot get database from {}".format(strTablename) + strSysError)
    
    finally:
        connection.close()

# get strstrData from strDatabase
def funcGetDB2(strTablename, claim_uuid, strData):
    strData = strData["MySQL8.0"]
    try:
        connection = mysql.connector.connect(host=str(strData["host"]),
                                                database=str(strData["database"]),
                                                user=str(strData["user"]),
                                                password=str(strData["password"]))
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM {} where claim_uuid = {} " .format(strTablename, claim_uuid))
        dictResult = cursor.fetchall()
        return dictResult

    except Exception as strSysError :
        print (strSysError)
        print ("cannot get database from {}".format(strTablename))
        logging.error("cannot get database from {}".format(strTablename) + strSysError)
    
    finally:
        connection.close()

# get Data from postgres
def funcGetPostgresDB(strQry,strData):
    strData = strData["postgres"]
    try:
        connection = psycopg2.connect(host = strData['host'],
                        port = strData['port'],
                        database = '',
                        user = strData['user'],
                        password = strData['password'])
        cursor = connection.cursor()
        cursor.execute(strQry)
        dictResult = cursor.fetchall()
        return dictResult

    except Exception as strSysError :
        print (strSysError)
        print ("cannot perform {}".format(strQry))
        logging.error("cannot perform {}".format(strQry) + strSysError)
    
    finally:
        connection.close()


# get data from lambdafred table
def funcGetDBTbl(strData):
    strData = strData["MySQL8.0"]
    dictResult = None
    try:
        connection = mysql.connector.connect(host=str(strData["host"]),
                                                database=str(strData["database"]),
                                                user=str(strData["user"]),
                                                password=str(strData["password"]))
        cursor = connection.cursor()
        cursor.execute("SELECT uuid, tableName FROM infracit_sharedb.tbl_lambdafred ORDER BY updated ASC LIMIT 1")
        dictResult = cursor.fetchall()
        strDictResult = dictResult[0]
        if dictResult != None and strDictResult[1] != '0000':
            cursor.execute("DELETE FROM infracit_sharedb.tbl_lambdafred WHERE uuid='%s'" %(strDictResult[0]))
            connection.commit()
            return strDictResult[1]
        else :
            if dictResult == None:
                return dictResult
            else:
                return strDictResult[1]
                
    except Exception :
        print ('.')
    
    finally:
        connection.close()

#check wether data already exist or not
def funcLoadPostgress(strData,listDBData):
    conn = psycopg2.connect(host = strData['host'],
                        port = strData['port'],
                        database = '',
                        user = strData['user'],
                        password = strData['password'])
    
    pass