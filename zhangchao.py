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
fileRoot = "zhangchao"
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
    SELECT u2.iso2,u1.imei,u1.cnt
    FROM
    (
    SELECT mcc,imei,COUNT(1) AS cnt
    FROM
    (
    SELECT imei,sessionid,mcc
    FROM t_usmguserloginlog
    WHERE DATE(logoutdatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)

    UNION ALL

    SELECT imei,sessionid,mcc
    FROM t_usmguserloginonline
    WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    ) AS z
    GROUP BY mcc,imei
    ) AS u1
    LEFT JOIN
    glocalme_css.t_css_mcc_country_map AS u2
    ON u1.mcc = u2.mcc
    """
    imeiList = list()
    assDict = dict()
    assCur.execute(assquery)
    assGenerator = fetchsome(assCur,20000)
    for row in assGenerator:
        key = row[0]+str(row[1])
        assDict[key]=row[2]
        imeiList.append(str(row[1]))

    assDBCon.close()

    return [imeiList,assDict]

def getBss():
    # imeilist = returnList[0]
    # assDict = returnList[1]
    yesterday = datetime.datetime.now().date()-timedelta(days=0)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=1)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    bssurl = 'mongodb://mongoquery:DJyBjiyzmqB2e95@52.74.132.61:55065/bss'
    bssCon = MongoClient(bssurl)
    bssDB = bssCon['bss']
    bssCol = bssDB['AccountDeductionDetailRecord']

    print secondBef
    print secondYes

    bssCur = bssCol.aggregate(
        [
            {"$match":{
                        "$and":
                        [
                            {"startTime":{"$gte":secondBef,"$lte":secondYes}},
                            {"tradeType":"1"}
                        ]
                        }
            },
            {"$group":{
                    "_id":{"imei":"$imei","iso2":"$visitCountry"},
                    "total":{"$sum":"$flowSize"}
                    }
            }
        ]
    )

    bssList = list()
    print bssCur
    
    for doc in bssCur:
        iso2 = doc['_id']['iso2']
        imei = str(doc['_id']['imei'])
        flowsize = doc['total']
        bssList.append([iso2,imei,flowsize])
    bssCon.close()

    return bssList

def getTask1():
    global floderName
    global fileRoot

    # returnList=getAss()
    bssList = getBss()

    print bssList

    y = datetime.datetime.now().date()-timedelta(days=1)
    dateStr = str(y)
    filename = fileRoot + "_"+dateStr+".xlsx"

    workbook = Workbook(floderName+"/"+filename)
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"visitcountry")
    sheet.write(0,1,"imei")
    sheet.write(0,2,"flowsize")

    r =1
    for bssResutle in bssList:
        sheet.write(r,0,bssResutle[0])
        sheet.write(r,1,bssResutle[1])
        sheet.write(r,2,bssResutle[2])
        r += 1

    workbook.close()



if __name__ == "__main__":
    createDailyFloder()
    getTask1()

    schedule.every().day.at("00:30").do(getTask1)
    schedule.every().day.at("00:02").do(createDailyFloder)
    while 1:
        schedule.run_pending()
        time.sleep(1)
