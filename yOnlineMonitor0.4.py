#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
import schedule, time
import numpy as np
from plotly.offline import plot
import plotly.graph_objs as go
import copy
import json
from kits import getdbinfo
import requests

postMinute = 0
postHour = 0
xrecord = list()
yrecord = list()

reload(sys)
sys.setdefaultencoding("utf-8")


def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def getdata():

    gtbuDBinfo = getdbinfo("ASS_GTBU")
    gtbuDBname = "glocalme_ass"

    nonDBinfo = getdbinfo('NON_GTBU')
    nonDBname = 'ucloudplatform'

    try:
        gtbuDBconnection = getDBconnection(gtbuDBinfo,gtbuDBname)
        nonDBconnection = getDBconnection(nonDBinfo,nonDBname)
    except Exception as e:
        time.sleep(20)
        return getdata()

    gtbuCur = gtbuDBconnection.cursor()
    nonCur = nonDBconnection.cursor()

    queryGTBU ="""
    SELECT COUNT(1)
    FROM t_usmguserloginonline
    """

    queryNon = """
    SELECT COUNT(1)
    FROM t_usmguserloginonline
    """

    gtbuCur.execute(queryGTBU)
    nonCur.execute(queryNon)

    gtbuResult = gtbuCur.fetchall()
    nonResult = nonCur.fetchall()

    gtbuDBconnection.close()
    nonDBconnection.close()

    gtbuNum = gtbuResult[0][0]
    nonNum = nonResult[0][0]

    return gtbuNum+nonNum

def getPastRecord(curH,curM):
    remoteDBinfo = getdbinfo("REMOTE")
    remoteDBname = "login_history"

    try:
        remoteDBconnection = getDBconnection(remoteDBinfo,remoteDBname)
    except Exception as e:
        time.sleep(50)
        return getPassRecord()

    remoteCur = remoteDBconnection.cursor()

    queryREMOTE ="""
    SELECT DATE(recordtime),AVG(average),MAX(STD),MIN(xymin),MAX(xymax)
    FROM
    t_xymonitor
    WHERE hourposition = %s AND ( minuteposition BETWEEN %s AND %s)
    GROUP BY DATE(recordtime)
    ORDER BY recordtime DESC
    """

    startM = curM-5
    endM = curM + 5

    remoteCur.execute(queryREMOTE,(curH,startM,endM))
    remoteResult = remoteCur.fetchall()

    pastRecord = list()
    i = 0
    for row in remoteResult:
        pastRecord.append([row[0],row[1],row[2],row[3],row[4]])
        i += 1
        if i >=10:break

    return pastRecord

def getPrediction(curAve,pastRecord):

    accList = list()
    for index,re in enumerate(pastRecord):
        accList.append(np.abs(pastRecord[index+1]-re))
        if index+1 == len(pastRecord): break

    aveAcc = np.average(accList)
    return [pastRecord[0][1] + aveAcc,curAve]

def verifyRecord(curRecord,pastRecord):

    verifyResult = list()

    if len(pastRecord) == 0:
        return verifyResult

    curAve = curRecord[0]
    curStd = curRecord[1]
    curMin = curRecord[2]
    curMax = curRecord[3]
    curMinute = curRecord[4]

    print pastRecord
    print curRecord

    i = 0
    for re in pastRecord:
        if curAve>re[1]: i = 1

    if i == 0:
        verifyResult= getPrediction(curAve,pastRecord)

    return verifyResult


def xysentinel(curNum):
    global postMinute
    global postHour
    global xrecord
    global yrecord

    dnow = datetime.datetime.now()
    curMinute = dnow.strftime("%M")
    curMinute = int(curMinute)
    curHour = dnow.strftime("%H")

    xlist = list()

    print postMinute
    print curMinute
    print postHour
    print curHour
    if np.abs((curMinute-postMinute+60)%60) >= 5:
        pastRecord = getPastRecord(int(curHour),curMinute)
        xavg = np.average(xrecord)
        xstd = np.std(xrecord)
        xmax = np.max(xrecord)
        xmin = np.min(xrecord)
        verifyResult = verifyRecord([xavg,xstd,xmin,xmax,curMinute],pastRecord)

        if len(verifyResult) == 0:
            xlist = [postHour,postMinute,xavg,0,xstd,xmin,xmax]
        else:
            xlist = [postHour,postMinute,verifyResult[0],1,0,0,0]

        xrecord = list()
        postMinute = curMinute

    if curHour != postHour:
        postHour = curHour

    xrecord.append(curNum)
    return xlist

def presist2DB(xlist):
    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'
    insertdb = getDBconnection(insertdbinfo,insertdbname)
    insertCur = insertdb.cursor()

    recordtime = datetime.datetime.now()
    hourposition = int(xlist[0])
    minuteposition = xlist[1]
    average = xlist[2]
    prediction = xlist[3]
    xstd = xlist[4]
    xymin = xlist[5]
    xymax = xlist[6]
    insertCur.execute('''insert into t_yonlinemonitor(recordtime,hourposition,minuteposition,prediction,average,std,xymin,xymax) values(%s,%s,%s,%s,%s,%s,%s,%s)''',
                             (recordtime,hourposition,minuteposition,prediction,average,xstd,xymin,xymax))

    insertdb.commit()
    insertdb.close()

def sentinel():

    curNum = getdata()
    xlist = xysentinel(curNum)
    print xlist
    if len(xlist)!= 0:
        presist2DB(xlist)

def initiateNum():
    global postMinute
    global postHour
    global xrecord
    global yrecord

    now = datetime.datetime.now()
    curNum = getdata()
    postMinute = int(now.strftime("%M"))
    postHour = now.strftime("%H")
    xrecord.append(curNum)
    yrecord.append(curNum)

if __name__ == "__main__":
    initiateNum()
    while(1):
        sentinel()
        time.sleep(20)
