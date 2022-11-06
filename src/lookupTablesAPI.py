""" Module Name: 
   
   REST API
   PUT row from redis

   Created By: Asif asif@schinkels.com.my 
   Created: 18/7/2022 
   Framework: Python 3.10 on redis

   Last Edited: 5/9/2022
    Reason Edited: Add claim API, add claim process API, add simplify lookuptablesAPI
   Last Edited: 28/7/2022
    Reason Edited: JSON format correction  
   Last Edited: 27/7/2022
    Reason Edited: Add logging system. infracit_sharedb.tref_buatan & infracit_sharedb.tref_asl_empdesignation correction. Change token validation for 30 days
   Last Edited: 26/7/2022
    Reason Edited: Change hardcoded to running from env parameters
   Last Edited: 24/7/2022
    Reason Edited: adding encripting and decrypting API
"""
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
# usage : functions that act on or return other functions
from functools import wraps

# source : library
# usage : connect to redis
import redis

# source : library
# usage : log message to log file
import logging

# source : library
# usage : web application framework
from flask import Flask, jsonify, request, make_response

# source : library
# usage : encode and decode JSON Web Tokens 
import jwt

# source : library
# usage : display the current date
import datetime

# source : library
# usage :   Store the object in Redis using the json().set() method. 
#           The first argument, person:1 is the name of the key that will reference the JSON.
#           The second argument is a JSON path. We use Path.root_path(), as this is a new object
from redis.commands.json.path import Path

# source : library
# usage :  encrypting/decrypting function 
from cryptography.fernet import Fernet

# source : self
# usage : JSON format
from funcFile import funcReadJson


# adding env folder to the system path
path = os.getcwd()
parPath = os.path.dirname(path)
sys.path.insert(0,path)

# create and configure logger
logFormat = "%(asctime)s:%(levelname)s:%(filename)s - %(message)s"
logging.basicConfig(    filename = "{}/log/lookupTablesAPI.log".format(parPath),
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
strSecret = dictRuntime["SECRET_KEY"]
dictRuntime = dictEnv[strRuntimeMode]
dictRedis = dictRuntime[1]
dictRedisVar = dictRedis['redis']
dictAPI = dictRuntime[4]
strToken= dictAPI['lookupTablesAPI']

app = Flask(__name__)

app.config['SECRET_KEY'] = str(strSecret)

key = Fernet.generate_key()
fernet = Fernet(key)

# testing variables
bLocalhostTest = True

#function for recognizing token
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token')

        if not token:
            return jsonify({'message':'token is missing'}),403
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'])
            
        except:
            
            return jsonify({'message':'token is invalid'})
            
        return f(*args, **kwargs)
    
    return decorated

# Default @app.route http method is GET, otherwise stated
# This is true for all API generator
@app.route("/token")
def token():
    auth = request.authorization
    try:
        # check wether user is authorize or not
        if auth and auth.username == strToken['username'] and auth.password == strToken['password']:
            #creating token
            token = jwt.encode({'user': auth.username, 'exp' : datetime.datetime.utcnow() + datetime.timedelta(days=360)}, app.config['SECRET_KEY'] )
            #return token to user
            return jsonify({'token' : token.decode('UTF-8') })
        return make_response('Could not verify!',401,{'WWW-Authentication' : 'Basic realm ="Login Required"' })
    except Exception as strError:
        logging.error(strError)
        return make_response(str(strError),500,{'Internal Server Error' : 'The server encountered an internal error and was unable to complete your request. Either the server is overloaded or there is an error in the application' })

@app.route("/infracit_pbtdb.pbt_sor" )
@token_required
def pullFromRedis():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_pbtdb.pbt_sor', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_state" )
@token_required
def pullFromRedis1():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_state', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_district" )
@token_required
def pullFromRedis2():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_district', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_rgo" )
@token_required
def pullFromRedis3():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_rgo', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_pbt" )
@token_required
def pullFromRedis4():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_pbt', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_taman" )
@token_required
def pullFromRedis5():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_taman', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_mukim" )
@token_required
def pullFromRedis6():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_mukim', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_pbtdb.pbt_sor_item" )
@token_required
def pullFromRedis7():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_pbtdb.pbt_sor_item', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_pbtdb.pbt_sor_detailsitem" )
@token_required
def pullFromRedis8():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_pbtdb.pbt_sor_detailsitem', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_department" )
@token_required
def pullFromRedis9():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_department', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_fin_year" )
@token_required
def pullFromRedis10():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_fin_year', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_pac_cate" )
@token_required
def pullFromRedis11():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_pac_cate', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_pac_subcate" )
@token_required
def pullFromRedis12():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_pac_subcate', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_pac_status" )
@token_required
def pullFromRedis13():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_pac_status', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_comstatus" )
@token_required
def pullFromRedis14():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_comstatus', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_empdesignation" )
@token_required
def pullFromRedis15():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_empdesignation', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_plant" )
@token_required
def pullFromRedis16():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_plant', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_regbody" )
@token_required
def pullFromRedis17():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_regbody', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_scopeofwork" )
@token_required
def pullFromRedis18():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_scopeofwork', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_specialization" )
@token_required
def pullFromRedis19():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_specialization', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_swdesignation" )
@token_required
def pullFromRedis20():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_swdesignation', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asl_typeofcompany" )
@token_required
def pullFromRedis21():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asl_typeofcompany', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_nationality" )
@token_required
def pullFromRedis22():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_nationality', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_jenis_aduan" )
@token_required
def pullFromRedis23():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_jenis_aduan', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_dun" )
@token_required
def pullFromRedis24():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_dun', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_parliament" )
@token_required
def pullFromRedis25():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_parliament', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_office_location" )
@token_required
def pullFromRedis26():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_office_location', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asset_code" )
@token_required
def pullFromRedis27():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asset_code', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_asset_model" )
@token_required
def pullFromRedis28():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_asset_model', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_category_assets" )
@token_required
def pullFromRedis29():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_category_assets', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_itemlocations" )
@token_required
def pullFromRedis30():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_itemlocations', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_buatan" )
@token_required
def pullFromRedis31():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_buatan', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_model" )
@token_required
def pullFromRedis32():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_model', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_jenisbadan" )
@token_required
def pullFromRedis33():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_jenisbadan', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_kelaskenderaan" )
@token_required
def pullFromRedis34():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_kelaskenderaan', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_enjin" )
@token_required
def pullFromRedis35():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_enjin', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_bahanbakar" )
@token_required
def pullFromRedis36():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_bahanbakar', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_status_kenderaan" )
@token_required
def pullFromRedis37():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_status_kenderaan', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_empdesignation" )
@token_required
def pullFromRedis38():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_empdesignation', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_emp_level" )
@token_required
def pullFromRedis39():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_emp_level', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_emp_levels" )
@token_required
def pullFromRedis40():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_emp_levels', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_workstatus" )
@token_required
def pullFromRedis41():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_workstatus', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_filling_color" )
@token_required
def pullFromRedis42():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_filling_color', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_project" )
@token_required
def pullFromRedis43():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('infracit_sharedb.tref_project', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/public.cabinets_cabinet" )
@token_required
def pullFromRedis44():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('public.cabinets_cabinet', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/public.documents_documenttype" )
@token_required
def pullFromRedis45():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('public.documents_documenttype', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/public.tags_tag" )
@token_required
def pullFromRedis46():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        redisReply = json.loads(r.json().get('public.tags_tag', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/infracit_sharedb.tref_pac_zone" )
@token_required
def pullFromRedis47():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        

        redisReply = json.loads(r.json().get('public.tags_tag', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/dangabay.tbl_claims_process_main" )
@token_required
def pullFromRedis48():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        

        redisReply = json.loads(r.json().get('dangabay.tbl_claims_process_main', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

@app.route("/dangabay.tbl_claims_process" )
@token_required
def pullFromRedis49():
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        

        redisReply = json.loads(r.json().get('dangabay.tbl_claims_process', '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

# get data from redis based on parameter as the key in redis
@app.route("/lookuptables/<tablename>" )
@token_required
def pullFromRedis50(tablename):
    try:
        
        if bLocalhostTest == False:
            r = redis.Redis(host=dictRedisVar['host'],
                        port=dictRedisVar['port'],
                        db=dictRedisVar['database'])

        elif bLocalhostTest == True:
            r = redis.Redis(host='localhost',
                            port='6380',
                            db='1')
        
                        
        
        redisReply = json.loads(r.json().get(tablename, '$')[0])
        return jsonify(redisReply)
        
    except Exception as strError:
        logging.error(strError)
        return jsonify({'message':strError})
    
    pass

#encrypting password from client
@app.route('/encrypt', methods=['POST'])
def encrypt():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'): 
        jsonData = request.get_json()
        strPassword = jsonData['password']
        encMessage = fernet.encrypt(strPassword.encode())
        strEncMessage = str(encMessage,'utf-8')
        dctData = {}
        dctData.__setitem__('password',strEncMessage)
        #return encrypted json
        return jsonify(dctData)
    else :
        return 'Data type not in JSON'

#decrypting password from client
@app.route("/decrypt", methods=['POST'])
def decrypt():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'): 
        jsonData = request.get_json()
        strPassword = jsonData['password']
        bytesStrPassword = bytes(strPassword,'utf-8')
        decMessage = fernet.decrypt(bytesStrPassword).decode()
        dctData = {}
        dctData.__setitem__("password",decMessage)
        #return encrypted json
        return jsonify(dctData)
    else:
        return 'Wrong datatype'

if __file__ == '__main__':
    # 0.0.0.0 indicate global value for server.  This allow for startup at Windows and Ubuntu servers
    logging.info('env test pass. updateRedis.py system START.')
    app.run( host= "0.0.0.0" )
