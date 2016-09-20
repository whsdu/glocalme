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

floderName =""
fileRoot = "huzhilei"
def createDailyFloder():
    global floderName
    global fileRoot

    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/"+fileRoot

    if not os.path.exists(tmpFloderName):
        os.makedirs(tmpFloderName)

    floderName = tmpFloderName

def accumulator(row,recordDict):

    start = row[0]
    interval = row[1]+1
    imei = row[2]
    butype = row[3]

    keylist = []
    for i in range(0,interval):
        thatday = start + timedelta(days=i)
        key=(thatday,butype)
        keylist.append(key)

    for key in keylist:
        imeiset = recordDict.get(key,set())
        imeiset.add(imei)
        recordDict[key]=imeiset

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

def getAss():
    assDBinfo = getdbinfo("ASS_GTBU")
    assDBname = "glocalme_ass"
    assDBCon = getDBconnection(assDBinfo,assDBname)
    assCur = assDBCon.cursor()

    assquery = """
    SELECT imei,COUNT(1)
    FROM
    (
    SELECT imei,sessionid
    FROM t_usmguserloginlog
    WHERE logoutdatetime BETWEEN  DATE_SUB(DATE(NOW()),INTERVAL 80 HOUR) AND  DATE_SUB(DATE(NOW()),INTERVAL 8 HOUR)

    UNION ALL

    SELECT imei,sessionid
    FROM t_usmguserloginonline
    WHERE logindatetime BETWEEN  DATE_SUB(DATE(NOW()),INTERVAL 80 HOUR) AND  DATE_SUB(DATE(NOW()),INTERVAL 8 HOUR)
    ) AS z
    GROUP BY imei
    """
    assDict = dict()
    assCur.execute(assquery)
    assGenerator = fetchsome(assCur,20000)
    for row in assGenerator:
        assDict[str(row[0])]=row[1]

    assDBCon.close()

    return assDict

def getBss(assDict):
    imeilist = assDict.keys()
    yesterday = datetime.datetime.now().date()-timedelta(days=0)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=3)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    bssurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55065/bss'
    bssCon = MongoClient(bssurl) 
    bssDB= bssCon['bss']
    bssCol = bssDB['AccountDeductionDetailRecord']

    bssCur = bssCol.aggregate(
        [
            {"$match":{
                        "$and":
                        [
                        {"startTime":{"$gte":secondBef,"$lte":secondYes}}
                        ]
                        }
            },
            {"$group":{
                    "_id":"$imei",
                    "total":{"$sum":"$flowSize"}
                    }
            }
        ]
    )

    bssDict = dict()
    print imeilist
    print secondBef
    print secondYes

    for doc in bssCur:
        imei = str(doc['_id'])
        flowsize = doc['total']
        cnt = assDict.get(imei,0)
        bssDict[imei]=[flowsize,cnt]

    bssCon.close()

    return bssDict

def getTask1():
    global floderName
    global fileRoot

    assDict=getAss()
    bssDict = getBss(assDict)

    print bssDict

    y = datetime.datetime.now().date()-timedelta(days=1)
    dateStr = str(y)
    filename = fileRoot + "_"+dateStr+".xlsx"

    workbook = Workbook(floderName+"/"+filename)
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"imei")
    sheet.write(0,1,"flowsize")
    sheet.write(0,2,"cnt")

    r =1
    for key,val in bssDict.iteritems():
        sheet.write(r,0,key)
        sheet.write(r,1,val[0])
        sheet.write(r,2,val[1])
        r += 1

    workbook.close()



if __name__ == "__main__":
    createDailyFloder()
    getTask1()
    schedule.every(3).day.at("02:30").do(getTask1)
    schedule.every(3).day.at("00:02").do(createDailyFloder)
    while 1:
        schedule.run_pending()
        time.sleep(1)
