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

accDict = dict()
yesterday = ''

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
                                  db = dbname)
    return dbconnection

def getRemoteCon():
    remoteDBinfo = getdbinfo("REMOTE")
    remoteDBname = "login_history"

    try:
        remoteDBconnection = getDBconnection(remoteDBinfo,remoteDBname)
    except Exception as e:
        time.sleep(20)
        return getRemoteCon()

    remoteCur = remoteDBconnection.cursor()

    return [remoteDBconnection,remoteCur]

def getOss(collection):

    try:
        ossurl = 'mongodb://mongoquery:DJyBjiyzmqB2e95@52.74.132.61:55075/oss_system'
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['oss_system']
        ossCol = ossDB[collection]
    except Exception as e:
        time.sleep(30)
        return getOss()

    return [ossCon,ossCol]

def accOSS(accDict,startdate,enddate,collection):
    ossCon,ossCol = getOss(collection)
    ossCur = ossCol.aggregate(
    [
        {
            "$match":{
                    "$and":
                        [
                            {"softversion":{"$exists":"true"}},
                            {"logindatetime":{"$gte":startdate,"$lte":enddate}}
                        ]
                     }
        },
        {
            "$project":
                    {"_id":0,"imei":1,"softversion":1}
        },
        {
            "$group":
                    {"_id":{"type":"$softversion","imei":"$imei"},"total":{"$sum":1}}
        },
        {
            "$group":
                    {"_id":"$_id.type","total":{"$sum":1}}
        }
     ]
    )

    for doc in ossCur:
        print doc
        version = doc['_id']
        cnt = doc['total']
        if len(version) == 0:
            version = "unknown"
        elif version[1]=='2' or version[2]=='T':
            version = version[0:2]
        else:
            version =version[0:3]
        acc = accDict.get(version,0)
        accDict[version]=acc+cnt

    ossCon.close()

def readDenominator():
    remoteCon,remoteCur = getRemoteCon()
    typeDict = dict()
    query = """
    SELECT devtype,totalnumber FROM
    t_activedenominator
    """
    remoteCur.execute(query)
    deResult = remoteCur.fetchall()
    for row in deResult:
        type = row[0]
        num = row[1]
        typeDict[type]=num

    remoteCon.close()
    return typeDict

def computeResult(accDict,typeDict):

    resultList = list()
    ratioDict = dict()

    totalnu = 0
    activetotal = 0
    for kay,val in typeDict.iteritems():
        totalnu += val

    for key,val in accDict.iteritems():
        nu = val
        de = typeDict.get(key,0)
        activetotal +=nu
        if de !=0:
            ratioDict[key] = 1. * nu/de
        else:
            ratioDict[key] = 0
        print ""

    resultList.append(1. * activetotal/totalnu)
    resultList.append(activetotal)
    resultList.append(totalnu)

    return [resultList,ratioDict]


def onlineACC(n):
    global accDict

    today = datetime.datetime.now().strftime("%d")
    recordtime = datetime.datetime.now().date()-timedelta(days=n+1)
    recorddate = recordtime.strftime("%D")

    yesterday = datetime.datetime.now().date()-timedelta(days=n)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=n+1)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    for collection in ['t_monitor_term_history']:
        accOSS(accDict,secondBef,secondYes,collection)
    typeDict = readDenominator()

    remoteCon,remoteCur = getRemoteCon()
    resultlist,ratioDict = computeResult(accDict,typeDict)


    recorddate = recordtime.strftime("%Y-%m-%d")
    remoteCur.execute('''insert into t_onlinedev_tmp(recordtime,activeratio,activetotal,total,g2ratio,g2active,g2total,
    g1sratio,g1sactive,g1stotal,g1pratio,g1pactive,g1ptotal)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
    (recorddate,resultlist[0],resultlist[1],resultlist[2],ratioDict.get('G2',0),accDict.get('G2',0),typeDict.get('G2',0),
     ratioDict.get('G1S',0),accDict.get('G1S',0),typeDict.get('G1S',0),
     ratioDict.get('G1P',0),accDict.get('G1P',0),typeDict.get('G1P',0)))
    remoteCon.commit()
    remoteCon.close()

    accDict.clear()

if __name__ == "__main__":
    for i in range(12):
        onlineACC(i+1)
