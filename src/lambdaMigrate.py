""" Module Name: 
   
    this module will update data from MySQL5.7 to MySQL8.0 based on data changes in MySQL5.7 that is stored in dangabay.tbl_lambdafred_infracit

   Created By: Asif asif@schinkels.com.my 
   Created: 13/9/2022 
   Framework: Python 3.10 on production MySQL5.7 to MySQL8.0
   
   Last Edited: 18/10/2022
    Reason Edited: Error checking. Insert update and delete to uppercase
   Last Edited: 03/10/2022
    Reason Edited: Swap env mysql 5.7 to MySQL8.0 in code where tplResult[2] == 'delete', and change interval time from 1 second to 5 minutes
   Last Edited: 02/10/2022
    Reason Edited: Add new error handling for moving row from tbl_lambdafred_infracit to tbl_lambdafred_infracit_failed
   Last Edited: 01/10/2022
    Reason Edited: Loop correction
   Last Edited: 19/09/2022
    Reason Edited: Test, and make env file ready for production
   Last Edited: 27/09/2022
    Reason Edited: Check data existance in MySQL5.7, Shorten code by using function to connect to MySQL
  
"""

# source : library
# usage : import required module
import sys
import os

# source : library
# uasge : setup path
import os.path
from os import path

# source : library
# usage : json parsing
import json

# source : library
# usage : log message to log file
import logging

# source : self
# usage : Get DB, Change DB to JSON format, send JSON to Redis
from funcFile import funcReadJson

# source : library
# usage : strDatabase connection
import mysql.connector

# source : library
# usage : Run Python functions (or any other callable) periodically using a friendly syntax.
import schedule 

# source : library
# usage : handle time-related tasks
import time
import datetime

# adding folder to the system path
path = os.getcwd()
sys.path.insert(0,path)
parPath = os.path.dirname(path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/lambdaMigrate.log".format(parPath),
                        level = logging.INFO,
                        format= logFormat,
                    )
logger = logging.getLogger

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
dictRuntime = dictEnv[strRuntimeMode]       # dictRuntime[0] for MySQL8.0 connection, dictRuntime[6] for MySQL5.7 connection
dictData1 = dictRuntime[6]                   
dictData1 = dictData1['MySQL5.7']           # dictData1 for MySQL5.7 connection
dictData2 = dictRuntime[0]                   
dictData2 = dictData2['MySQL8.0']           # dictData2 for MySQL8.0 connection



# main function for lambdaMigrate
def funcLambda():
    while True:
        tempListColName = []
        # connection to MySQL5.7
        connection = mysql.connector.connect(host=str(dictData1["host"]),
                                                database=str(dictData1["database"]),
                                                user=str(dictData1["user"]),
                                                password=str(dictData1["password"]))
        # fetch one row from tbl_lambdafred_infracit based on FIFO
        sql1 = "SELECT * FROM dangabay.tbl_lambdafred_infracit ORDER BY updated ASC LIMIT 1"
        cursor = connection.cursor()
        cursor.execute(sql1)
        tplResult = cursor.fetchone()
        connection.close()
        strtplResult = tplResult[1]
        if tplResult != None and str(strtplResult) != '0000':
            try:

                # get column names
                listColname = funcGetColname(tplResult[1],dictData1)
                tplColName = tuple(listColname)
                # put back tick to column name
                for row in tplColName:
                    temp = '`'+row+'`'
                    tempListColName.append(temp)
                tplColname = tuple(tempListColName)
                strtplColname = str(tplColname)
                strColname = strtplColname.replace("'","")
                
                # actionItem = insert
                if str(tplResult[2]).upper() == 'INSERT':
                    tempTuple = []
                    sql2 = "SELECT COUNT(*) FROM {} WHERE uuid = '{}'".format(tplResult[1],tplResult[0])
                    listExist = funcGetDB(sql2,dictData1)
                    IntExist = listExist[0]
                    #if data not exit
                    if IntExist[0] == 0:
                        # Delete row from tbl_lambdafred_infracit
                        sql5 = ("DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0]))
                        funcDelDB(sql5,dictData1)
                        
                    elif IntExist[0] > 0 :
                        # get data from MySQL5.7 based on uuid
                        sql3 = "SELECT * FROM {} WHERE uuid = '{}'".format(tplResult[1],tplResult[0])
                        temptupleResult2 = funcGetDB(sql3,dictData1)

                        # change from python date function to string if exist in the data
                        for row in temptupleResult2[0]:
                            if type(row) is datetime.datetime :
                                temp = row.strftime('%Y-%m-%d %H:%M:%S')
                                tempTuple.append(temp)
                            else :
                                tempTuple.append(str(row))
                        tplResult2 = tuple(tempTuple)

                        #Insert data from MySQL5.7 to MySQL8.0
                        sql4 = "INSERT INTO {} {} VALUES {}".format(tplResult[1],strColname,str(tplResult2))
                        boolReturn = funcInsertDB(sql4,dictData2)
                        
                        # boolReturn == True , insert to MySQL above is success// boolReturn == False ,insert to MySQL above is success
                        if boolReturn == True:
                            # Delete row from tbl_lambdafred_infracit
                            sql5 = ("DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0]))
                            funcDelDB(sql5,dictData1)
                            logging.info("uuid ='{}' inserted".format(tplResult[0]))
                        elif boolReturn == False:
                            tempTuple = []
                            # Delete row from lambdafred
                            sql5 = "DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                            funcDelDB(sql5,dictData1)
                            for row in tplResult:
                                if type(row) is datetime.datetime :
                                    tempResult = row.strftime('%Y-%m-%d %H:%M:%S')
                                    tempTuple.append(tempResult)
                                else :
                                    tempTuple.append(row)
                                updatedTplResult = tuple(tempTuple)
                            
                            # check wether there is any similar data in tbl_lambdafred_infracit_failed
                            sql10 = "SELECT COUNT(*) FROM dangabay.tbl_lambdafred_infracit_failed WHERE uuid = '{}'".format(updatedTplResult[0])
                            listDelExist = funcGetDB(sql10,dictData1)
                            IntDelExist = listDelExist[0]
                            
                            # if there no similar data in tbl_lambdafred_infracit_failed, the failed row in tbl_lambdafred_infracit will not be moved. 
                            if listDelExist == None or IntDelExist == 0 :
                                sql9 ="INSERT INTO dangabay.tbl_lambdafred_infracit_failed (uuid, tableName, actionItem, created, updated) VALUES {}".format(str(updatedTplResult))
                                funcInsertDB(sql9,dictData1)
                            else:
                                continue
                        
                # actionItem = update
                elif str(tplResult[2]).upper() == 'UPDATE':
                    tempTuple = []
                    sql2 = "SELECT COUNT(*) FROM {} WHERE uuid = '{}'".format(tplResult[1],tplResult[0])
                    listExist = funcGetDB(sql2,dictData1)
                    IntExist = listExist[0]

                    # if data not exist
                    if IntExist[0] == 0 :
                        # Delete row from tbl_lambdafred_infracit
                        sql5 = ("DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0]))
                        funcDelDB(sql5,dictData1)
                    
                    elif IntExist[0] > 0 :
                        tempListColName = []
                        updatedlistColname = []
                        # get data from MySQL5.7 based on uuid
                        sql6 ="SELECT * FROM {} WHERE uuid = '{}'".format(str(tplResult[1]),str(tplResult[0]))
                        temptupleResult2 = funcGetDB(sql6,dictData1)

                        # change from python date function to string if exist in the data
                        for row in temptupleResult2[0]:
                            if type(row) is datetime.datetime :
                                temp = row.strftime('%Y-%m-%d %H:%M:%S')
                                tempTuple.append(temp)
                            else :
                                tempTuple.append(row)
                        tplResult2 = tuple(tempTuple)

                        # Create SQL statement
                        for row in tplColName:
                            temp = '`'+row+'`='
                            tempListColName.append(temp)
                        tplColname = tuple(tempListColName)
                        strtplColname = str(tplColname)
                        strColname = strtplColname.replace("'","")
                        listSplit = strColname.split(",")
                        
                        i = 0
                        for row2 in listSplit:
                            updatedlistColname.append(row2 + "'" + str(tplResult2[i]) + "'")
                            i += 1
                        strupdatedlistColname = str(updatedlistColname)
                        updatedStrColName = strupdatedlistColname.replace("[","").replace("]","").replace("/","").replace('"','').replace(')','').replace('(','')
                         
                        #Insert data from MySQL5.7 to MySQL8.0[]
                        sql7 = "UPDATE {} SET {} WHERE uuid = '{}'".format(tplResult[1],updatedStrColName,str(tplResult2[0]))
                        boolReturn = funcInsertDB(sql7,dictData2)

                        # boolReturn == True , insert to MySQL above is success// boolReturn == False ,insert to MySQL above is success
                        if boolReturn == True :
                            # Delete row from lambdafred
                            sql5 = "DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                            funcDelDB(sql5,dictData1)
                            logging.info("uuid ='{}' updated".format(tplResult[0]))
                        elif boolReturn == False :
                            tempTuple = []
                            # Delete row from lambdafred
                            sql5 = "DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                            funcDelDB(sql5,dictData1)
                            for row in tplResult:
                                if type(row) is datetime.datetime :
                                    tempResult = row.strftime('%Y-%m-%d %H:%M:%S')
                                    tempTuple.append(tempResult)
                                else :
                                    tempTuple.append(row)
                                updatedTplResult = tuple(tempTuple)
                            
                            # check wether there is any similar data in tbl_lambdafred_infracit_failed
                            sql10 = "SELECT COUNT(*) FROM dangabay.tbl_lambdafred_infracit_failed WHERE uuid = '{}'".format(updatedTplResult[0])
                            listDelExist = funcGetDB(sql10,dictData1)
                            IntDelExist = listDelExist[0][0]
                            
                            # if there no similar data in tbl_lambdafred_infracit_failed, the failed row in tbl_lambdafred_infracit will not be moved. 
                            if listDelExist == None or IntDelExist == 0 :
                                sql9 ="INSERT INTO dangabay.tbl_lambdafred_infracit_failed (uuid, tableName, actionItem, created, updated) VALUES {}".format(str(updatedTplResult))
                                funcInsertDB(sql9,dictData1)
                            else:
                                continue
                                                
                # actionItem = delete
                elif str(tplResult[2]).upper() == 'DELETE':
                    
                    sql2 = "SELECT COUNT(*) FROM {} WHERE uuid = '{}'".format(tplResult[1],tplResult[0])
                    listExist = funcGetDB(sql2,dictData2)
                    IntExist = listExist[0]
                    
                    #if data not exist in mySQL8.0
                    if IntExist[0] == 0 or listExist == None :
                        # Delete row from lambdafred
                        sql5 = "DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                        print(sql5)
                        boolReturn = funcDelDB(sql5,dictData1)
                        if boolReturn == True:
                            logging.info("uuid ='{}' deleted".format(tplResult[0]))

                    elif IntExist[0] > 0 :
                        # delete data in MySQL8.0 
                        sql8 = "DELETE FROM {} WHERE uuid ='{}'".format(tplResult[1],tplResult[0])
                        boolReturn = funcDelDB(sql8,dictData2)

                        # Delete row from lambdafred
                        if boolReturn == True:
                            sql5 = "DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                            funcDelDB(sql5,dictData1)
                            logging.info("uuid ='{}' deleted".format(tplResult[0]))
                    

            # if there is problem in fetch data from 5.7 or load insert data in MySQL8.0 , the row in tbl_lambdafred_infracit will be moved to tbl_lambdafred_infracit_failed 
            except Exception as strError:

                # insert failed row in tbl_lambdafred_infracit to tbl_lambdafred_infracit_failed
                tempTuple = []
                for row in tplResult:
                    if type(row) is datetime.datetime :
                        tempResult = row.strftime('%Y-%m-%d %H:%M:%S')
                        tempTuple.append(tempResult)
                    else :
                        tempTuple.append(row)
                    updatedTplResult = tuple(tempTuple)
                
                # check wether there is any similar data in tbl_lambdafred_infracit_failed
                sql10 = "SELECT COUNT(*) FROM dangabay.tbl_lambdafred_infracit_failed WHERE uuid = '{}'".format(updatedTplResult[0])
                listDelExist = funcGetDB(sql10,dictData1)
                IntDelExist = listDelExist[0]
                
                # if there no similar data in tbl_lambdafred_infracit_failed, the failed row in tbl_lambdafred_infracit will not be moved. 
                if listDelExist == None or IntDelExist == 0 :
                    sql9 ="INSERT INTO dangabay.tbl_lambdafred_infracit_failed (uuid, tableName, actionItem, created, updated) VALUES {}".format(str(updatedTplResult))
                    funcInsertDB(sql9,dictData1)
                else:
                    continue

                # delete failed row in tbl_lambdafred_infracit
                sql10 ="DELETE FROM dangabay.tbl_lambdafred_infracit WHERE uuid ='{}'".format(tplResult[0])
                funcDelDB(sql10,dictData1)
                logging.error(strError)

        elif tplResult == None:
            continue
        
        # break condition
        elif tplResult[1] == '0000':
            break
    logging.info('database is 0000. System exit')
    logging.info('lambdaMigrate.py system STOP') 
    raise SystemExit

def funcGetColname(strTablename,strData):
    tplResult = {}
    listColName = []
    try:
        # connect to MySQL5.7
        connection = mysql.connector.connect(host=str(strData["host"]),
                                                database=str(strData["database"]),
                                                user=str(strData["user"]),
                                                password=str(strData["password"]))
        cursor = connection.cursor(buffered=True)

        # get column names from table
        cursor.execute("SHOW COLUMNS FROM {}".format(strTablename))
        tplResult = cursor.fetchall()
        for row in tplResult:
            listColName.append(row[0])
        return listColName

    except Exception as strSysError :
        logging.error(strSysError)
        logging.error("cannot get column names from {}".format(strTablename))
    
    finally:
        connection.close()
        

def funcGetDB(strSql, strData):
    try:
        connection1 = mysql.connector.connect(host=str(strData["host"]),
                                            database=str(strData["database"]),
                                            user=str(strData["user"]),
                                            password=str(strData["password"]))
        cursor1 = connection1.cursor()
        cursor1.execute(strSql)
        dictResult = cursor1.fetchall()
        return dictResult

    except Exception as strSysError :
        logging.error(strSysError)
        return False
    
    finally:
        connection1.close()
         

def funcInsertDB (strSQL,strData):
    try:
        connection2 = mysql.connector.connect(host=str(strData["host"]),
                                            database=str(strData["database"]),
                                            user=str(strData["user"]),
                                            password=str(strData["password"]))
        cursor2 = connection2.cursor()
        cursor2.execute(strSQL)
        connection2.commit()
        return True
        
    except Exception as strSysError :
        logging.error(strSysError)
        return False
    finally:
        connection2.close

def funcDelDB (strSQL2,strData):
    try:
        connection3 = mysql.connector.connect(host=str(strData["host"]),
                                            database=str(strData["database"]),
                                            user=str(strData["user"]),
                                            password=str(strData["password"]))
        cursor3 = connection3.cursor()
        cursor3.execute(strSQL2)
        connection3.commit()
        return True

    except Exception as strSysError :
        logging.error(strSysError)
        return False
    finally:
        connection3.close

if __name__ == '__main__':
    # code run every 5 minutes
    schedule.every(5).minutes.do(funcLambda)
    print('lambdaMigrate.py system START. Please Wait. Activate 00000 to stop')
    while True:
        schedule.run_pending()
        # interval between each code run : 1 second
        time.sleep(1)