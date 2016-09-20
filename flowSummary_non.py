# -*- coding: utf-8 -*-
import sys
import MySQLdb
import schedule, time
import datetime
from kits import getdbinfo
from pymongo import MongoClient
import copy
import numpy as np


reload(sys)
sys.setdefaultencoding("utf-8")
lastMax = 0
today = 0
serilizeDate=0
preDict = dict()
reserveCntDict = dict()
freeCntDict = dict()
idcodeDict = dict()
imeitypeDict = dict()

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

def accumulate(curflowList):
    global preDict

    nonquery = """
        SELECT t1.imei,
        CASE WHEN t2.butype =1 THEN 'UPBU'
         WHEN t2.butype = 2 THEN 'GTBU'
         WHEN t2.butype = 3 THEN 'GCBU'
         WHEN t2.butype = 4 THEN 'GEBU'
         WHEN t2.butype = 5 THEN 'internal user'
        ELSE 'other' END AS butype
        FROM
        t_usmguserloginonline AS t1
        LEFT JOIN
        t_usmguser AS t2
        ON t1.uid = t2.uid
    """

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    nonCur.execute(nonquery)
    r = nonCur.fetchall()
    nonCon.close()

    buDict = dict()

    for row in r:
        buDict[row[0]] = row[1]

    for flowdetail in curflowList:
        imei = flowdetail[0]
        flow = flowdetail[1]
        iso2 = flowdetail[2]

        bu = buDict.get(imei,0)
        if bu == 0: continue

        tmpkey = str(iso2)+str(bu)
        tmpSet = set()

        totalflow,imeiset,tmpiso2,tmpbu = preDict.get(tmpkey,[0,tmpSet,'iso2','bu'])
        totalflow += flow
        imeiset.add(imei)
        preDict[tmpkey] = [totalflow,imeiset,iso2,bu]

def serilizeResult():
    global preDict
    global serilizeDate
    #this is the begin f test part
    print preDict
    #this is the end of test part
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,val in preDict.iteritems():
        totalflow = val[0]
        cnt = len(val[1])
        iso2 = val[2]
        bu = val[3]

        remoteCur.execute('''insert into t_flowsummary_iso(epochTime,iso2,cnt,totalflow,belonging)
            values(%s,%s,%s,%s,%s)''',(serilizeDate,iso2,cnt,totalflow,bu))

    remoteCon.commit()
    remoteCon.close()

    preDict.clear()

    serilizeDate = datetime.datetime.now().date()

def queryandinsert():
    global lastMax
    global today

    nowdate = datetime.datetime.now().strftime("%d")

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    queryNonMax="""
    SELECT MAX(itemid) FROM
    t_chgflowlog001    """

    queryNonCurrent="""
    SELECT imei,flowSize,visitcountry as iso2 FROM
    t_chgflowlog001
    WHERE itemid >= %s AND itemid < %s AND flowSize != 0
    ORDER BY  itemid ASC
    """

    nonCur.execute(queryNonMax)
    r = nonCur.fetchall()
    curMax=0
    for row in r:
        curMax=row[0]

    if curMax==lastMax:
        time.sleep(2)
        return

    curflowList = list()
    nonCur.execute(queryNonCurrent,(lastMax,curMax))
    r = nonCur.fetchall()
    for row in r:
        curflowList.append(row)

    nonCon.close()

    accumulate(curflowList)
    lastMax=curMax


# this is the beging of test part
#    serilizeResult()
# this is the end of test part

    if today == nowdate:
        return
    else:
        serilizeResult()

        today = nowdate


def loadDictionaries():
    global idcodeDict
    global imeitypeDict

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")

    imeitypeQuery="""
    SELECT imei, CASE WHEN devsoftversion IS NULL THEN 'unknown' ELSE devsoftversion END AS devsoftversion
    FROM
    t_tmlterminal
    """

    idcodeQuery="""
    SELECT uid,CODE FROM
    t_usmguser
    """

    nonCur.execute(idcodeQuery)
    r = nonCur.fetchall()
    curMax=0
    for row in r:
        idcodeDict[row[0]]=row[1]

    nonCur.execute(imeitypeQuery)
    r = nonCur.fetchall()
    for row in r:
        imeitypeDict[row[0]]=row[1]

    nonCon.close()

def initiate():
    global lastMax
    global today
    global serilizeDate

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    queryNon="""
    SELECT MAX(itemid) FROM
    t_chgflowlog001    """

    nonCur.execute(queryNon)
    r = nonCur.fetchall()
    for row in r:
        lastMax=row[0]
    nonCon.close()

    today = datetime.datetime.now().strftime("%d")
    serilizeDate = datetime.datetime.now().date()

if __name__=="__main__":
    initiate()
    time.sleep(2)
    flag = True
    while flag:
        queryandinsert()

