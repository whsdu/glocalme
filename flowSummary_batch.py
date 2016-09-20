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
def serilizeResult(bssList):

    thedaybefore = datetime.datetime.now().date()-timedelta(days=1)
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for row in bssList:
        iso2 = row[0]
        cnt = row[1]
        totalflow = row[2]

        remoteCur.execute('''insert into t_flowsummary_iso(epochTime,iso2,cnt,totalflow,belonging)
            values(%s,%s,%s,%s,%s)''',
        (thedaybefore,iso2,cnt,totalflow,'gtbu'))

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
    if colname == "": return list()
    bssCol = bssDB[colname]

#this is for test
    print colname
# this is for test

    bssCur = bssCol.aggregate(
        [
            {"$match":
                {"$and":
                    [
                        {"flowSize":{"$ne":0}}
                    ]
                }
            },
            {"$group":{
                    "_id":{"imei":"$imei","iso2":"$visitCountry"},
                    "flow":{"$sum":"$flowSize"}
                    }
            },
            {
                "$group":{
                    "_id":"$_id.iso2",
                    "num":{"$sum":1},
                    "totalflow":{"$sum":"$flow"}
                }
            }
        ]
    )


    bssList = list()

    for doc in bssCur:
        iso2 = str(doc['_id'])
        cnt = doc['num']
        totalflow = doc['totalflow']

        bssList.append([iso2,cnt,totalflow])


    bssCon.close()

    return bssList

def getTask1():
    bssList = getBss()
    print bssList

    serilizeResult(bssList)


if __name__ == "__main__":
    getTask1()
    schedule.every().day.at("01:43").do(getTask1)
    while 1:
        schedule.run_pending()
        time.sleep(1)
