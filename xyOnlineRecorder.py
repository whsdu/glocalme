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
    ylist = list()

    print postMinute
    print curMinute
    print postHour
    print curHour
    if np.abs((curMinute-postMinute+60)%60) >= 2:
        xavg = np.average(xrecord)
        xstd = np.std(xrecord)
        xmax = np.max(xrecord)
        xmin = np.min(xrecord)
        xlist=[postHour,postMinute,xavg,xstd,xmin,xmax]
        xrecord = list()
        postMinute = curMinute

    if curHour != postHour:
        yavg = np.average(yrecord)
        ystd = np.std(yrecord)
        ymax = np.max(yrecord)
        ymin = np.min(yrecord)
        ylist=[postHour,postMinute,yavg,ystd,ymin,ymax]
        yrecord = list()
        postHour = curHour

    xrecord.append(curNum)
    yrecord.append(curNum)

    return [xlist,ylist]

def presist2DB(xylist):
    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'
    insertdb = getDBconnection(insertdbinfo,insertdbname)
    insertCur = insertdb.cursor()

    recordtime = datetime.datetime.now()
    for index,list in enumerate(xylist):
        if len(list)!= 0:
            hourposition = int(list[0])
            minuteposition = list[1]
            if index == 0:
                xory = "x"
            else:
                xory = "y"
                insertCur.execute("""DELETE FROM t_xymonitor
                                    WHERE recordtime <=  DATE_SUB(NOW(),INTERVAL 2 DAY""")
                insertdb.commit()
            average = list[2]
            std = list[3]
            xymin = list[4]
            xymax = list[5]
            insertCur.execute('''insert into t_xymonitor(recordtime,hourposition,minuteposition,xory,average,std,xymin,xymax) values(%s,%s,%s,%s,%s,%s,%s,%s)''',
                             (recordtime,hourposition,minuteposition,xory,average,std,xymin,xymax))

    insertdb.commit()
    insertdb.close()

def sentinel():

    curTime = datetime.datetime.now()
    curNum = getdata()

    xylist = xysentinel(curNum)

    print xylist
    if len(xylist[0])!=0 or len(xylist[1])!=0:
        presist2DB(xylist)


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
        time.sleep(10)
