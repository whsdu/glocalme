#!/usr/bionpython
#coding:utf-8
import os
import MySQLdb
import pymongo
import datetime
from pymongo import MongoClient
from datetime import timedelta
from xlsxwriter.workbook import Workbook
import schedule
import time
import json
import csv
import operator
import xlrd
from kits import getdbinfo

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

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
def serilizeResult(bssDict):

    thedaybefore = datetime.datetime.now().date()-timedelta(days=1)
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,val in bssDict.iteritems():
        imei = val[0]
        code = val[1]
        iso2 = val[2]
        up = val[3]
        down = val[4]

        if (up + down) > ( 1024 * 1024 * 1024):
            remoteCur.execute('''insert into t_flowsummary_small(epochTime,iso2,imei,up,down,belonging,ucode)
            values(%s,%s,%s,%s,%s,%s,%s)''',
        (thedaybefore,iso2,imei,up,down,'gtbu',code))

    remoteCon.commit()
    remoteCon.close()

def getBss():
    yesterday = datetime.datetime.now().date()-timedelta(days=0)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=1)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    bssurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55161/bss'
    bssCon = MongoClient(bssurl)
    bssDB= bssCon['bss']

    thatdays = thedaybefore.strftime("%m%d")
    colname = ""

#this is for test
    print thatdays
    for c in bssDB.collection_names():
        print c
# this is for test

    for col in bssDB.collection_names():
        ts = col[-4:]
        if ts == thatdays:
            colname = col

    bssCol = bssDB[colname]

#this is for test
    print colname
# this is for test

    bssCur = bssCol.aggregate(
        [
            {"$group":{
                    "_id":{"imei":"$imei","code":"$userName","iso2":"$visitCountry"},
                    "up":{"$sum":"$upstreamFlow"},
                    "down":{"$sum":"$downFlow"}
                    }
            }
        ]
    )

    bssDict = dict()

    for doc in bssCur:
        imei = str(doc['_id']['imei'])
        iso2 = str(doc['_id'].get('iso2','not_exsits'))
        code = str(doc['_id'].get('code','unknown'))
        up = doc['up']
        down = doc['down']

        if (up + down) > (1024 * 1024 * 1024):
            bssDict[imei]=[imei,code,iso2,up,down]


    bssCon.close()

    return bssDict

def getTask1():
    bssDict = getBss()
    print bssDict

    serilizeResult(bssDict)


if __name__ == "__main__":
#    getTask1()
    schedule.every().day.at("02:30").do(getTask1)
    while 1:
        schedule.run_pending()
        time.sleep(1)
