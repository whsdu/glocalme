#!/usr/bionpython
#coding:utf-8

import sys
import MySQLdb
import datetime
import time
import logging
from kits import getdbinfo
from datetime import timedelta

reload(sys)
sys.setdefaultencoding("utf-8")

today = 0
dateBatchQuery = """
SELECT epochDate,epochHour,epochMin,COUNT(*)
FROM
(
SELECT DATE(epochTime) AS epochDate,HOUR(epochTime) AS epochHour,MINUTE(epochTime)AS epochMin,imei,visitcountry
FROM t_login_history_backup
WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()),INTERVAL %s DAY)
) AS z
GROUP BY epochDate,epochHour,epochMin
"""

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

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def dateRange(step,hostname,dbname):
    global dateBatchQuery
    remoteCon,remoteCur = getMysqlCon(hostname,dbname)
    remoteCur.execute(dateBatchQuery,(step,step,step))
    datelist = remoteCur.fetchall()
    non = datelist[0][0]
    remoteCon.close()

    return non

def getRealtime(hostname,dbname):
    global realtimeQuery
    remoteCon,remoteCur = getMysqlCon(hostname,dbname)
    remoteCur.execute(realtimeQuery)
    datelist = remoteCur.fetchall()
    non = datelist[0][0]
    remoteCon.close()

    return non

def dataInsert(targetdate,non,new,non_max,new_max):

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    remoteCur.execute('''insert into tmp_saiban_daily(recorddate,non,gtbu,non_max,gtbu_max)values(%s,%s,%s,%s,%s)''',
                          (targetdate,non,new,non_max,new_max))
    remoteCon.commit()
    remoteCon.close()

def startBatchInsert(step,non_max,new_max):

    td = datetime.datetime.now().date()
    for d in range(1,step):
        non = dateRange(d,"NON_GTBU","ucloudplatform")
        new = dateRange(d,"ASS_GTBU","glocalme_ass")

        targetday = td-timedelta(days = d)
        dataInsert(targetday,non,new,non_max,new_max)

        print non
        print new
        print targetday


def partnerMonitor(non,new):
    step = 2
    startBatchInsert(step,non,new)

def initiate():
    global today

    today = datetime.datetime.now().strftime("%d")
    non = getRealtime("NON_GTBU","ucloudplatform")
    new = getRealtime("ASS_GTBU","glocalme_ass")

    return [today,non,new]

def queryInsert(d):
    global dateBatchQuery
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    remoteCur.execute(dateBatchQuery,(d,))

    resultList = remoteCur.fetchall()
    print len(resultList)
    for row in resultList:
         remoteCur.execute('''insert into tmp_onlinecnt_old(epochDate,epochHour,epochMin,cnt)values(%s,%s,%s,%s)''',
                          tuple(row))

    remoteCon.commit()
    remoteCon.close()

def realtimeMonitor(datelist):
    print datelist
    for d in datelist:
        queryInsert(d)

if __name__ == "__main__":
    datelist = range(17,60)
    realtimeMonitor(datelist)
