# -*- coding: utf-8 -*-
import sys
import MySQLdb
import paramiko
import os
import StringIO
import pymongo
from pymongo import MongoClient
from kits import getdbinfo
from os import walk
import datetime
import  time
import requests
import json

reload(sys)
sys.setdefaultencoding("utf-8")

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                    charset="utf8")
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

def get4Cors():

    Con,Cur = getMysqlCon("REMOTE","login_history")
    gtbuquery = """
    SELECT mcc,mnc,lac,cellid,rat,sQ
    FROM t_sidelist
    WHERE sQ<100
    ORDER BY RAND()
    LIMIT 200
    """
    Cur.execute(gtbuquery)
    gtburesult = dict()
    queryresult = Cur.fetchall()
    Con.close()

    return queryresult

def initiate():

    print "start getting 4 cords:"
    corrods = get4Cors()
    print " finished with:"
    print len(corrods)
    print " reocrds: "

    r = 1
    f = 1
    for cor in corrods:
        mcc = str(cor[0])
        mnc = str(cor[1])
        lac = str(cor[2])
        cid = str(cor[3])
        rat = str(cor[4])
        sQ = str(cor[5])

        payload = """
        {
    "token": "977c2913193b94",
    "radio": "gsm",
    "mcc": """+mcc+""",
    "mnc": """+mnc+""",
    "cells": [{
        "lac": """+lac+""",
        "cid": """+cid+"""
    }]
    }
        """
        url = "https://ap1.unwiredlabs.com/v2/process.php"
        response = requests.request("POST", url, data=payload)
        jd = json.loads(response.text)
        if jd["status"] != "ok":continue

        print str(r)
        r+=1

        
        lat = jd["lat"]
        lon = jd["lon"]
        jd["sq"]=sQ
        jd["mcc"] = mcc
        jd["mnc"] = mnc
        jd["lac"] = lac
        jd["cid"] = cid

        client = MongoClient()
        db = client['login_history']
        col = db['col_gps']

        col.update({'lat':lat,'lon':lon,'sq':sQ,'mcc':mcc,'mnc':mnc,'lac':lac,'cid':cid},jd,upsert = True)
        client.close()

if __name__=="__main__":

    #initiate()
    while True:
        initiate()
        time.sleep(86700)
    # while True:
    #     intoFolder()
    #     time.sleep(60)
    #
    # schedule.every().day.at("0:45").do(initiate)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
