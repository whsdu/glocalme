#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
import schedule, time
import numpy as np
import copy
import json
from kits import getdbinfo
import requests
from datetime import timedelta
from flask import Flask, render_template, request, url_for
import string
from flask import json
from flask import jsonify

app = Flask(__name__)
recordedSet = set()
reload(sys)
sys.setdefaultencoding("utf-8")

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                charset = 'utf8')
    return dbconnection

def getMysqlCon(dbhost,dbname):
    DBinfo = getdbinfo(dbhost)
    DBname = dbname

    try:
        DBconnection = getDBconnection(DBinfo,DBname)
    except Exception as e:
        time.sleep(20)
        return getMysqlCon(dbhost,dbname)

    dbCur = DBconnection.cursor()

    return [DBconnection,dbCur]

@app.route('/')
def form():
    return render_template('form_submit.html')

# Define a route for the action of the form, for example '/hello/'
# We are also defining which type of requests this route is
# accepting: POST requests in this case
@app.route('/partnerhis/', methods=['POST'])
def partnerhist():
    daterange=request.form['daterange']
    partner=request.form['partner']
    iso2 = request.form['iso2']

    print daterange
    print partner
    print iso2

    rjson = jsonify({"test":[
                            {'iso2':'cn1','num':10},
                            {'iso2':'cn2','num':20},
                            {'iso2':'cn3','num':30},
                            {'iso2':'cn4','num':40},
                             ]})
    return rjson

if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=int("8082")
    )
