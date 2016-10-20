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


dateBatchQuery = """
SELECT COUNT(DISTINCT imei)
FROM
(
SELECT imei
FROM t_usmguserloginonline
WHERE DATE(logindatetime) <= DATE_SUB(DATE(NOW()),INTERVAL %s DAY)
	AND mcc = 310 AND mnc IN (140,370,470)

UNION

SELECT imei
FROM t_usmguserloginlog
WHERE
	(
		DATE(logindatetime) <= DATE_SUB(DATE(NOW()),INTERVAL %s DAY)
		AND
		DATE(logoutdatetime) >= DATE_SUB(DATE(NOW()),INTERVAL %s DAY)
	)
	AND mcc = 310 AND mnc IN (140,370,470)
) AS z
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

def dataInsert(targetdate,non,new):

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    remoteCur.execute('''insert into tmp_saiban(recorddate,non,gtbu)values(%s,%s,%s)''',
                          (targetdate,non,new))
    remoteCon.commit()
    remoteCon.close()

def startBatchInsert(step):

    td = datetime.datetime.now().date()
    for d in range(1,step):
        non = dateRange(d,"NON_GTBU","ucloudplatform")
        new = dateRange(d,"ASS_GTBU","glocalme_ass")

        targetday = td-timedelta(days = d)
        dataInsert(targetday,non,new)

        print non
        print new
        print targetday


def partnerMonitor():
    step = 130
    startBatchInsert(step)

if __name__ == "__main__":
    # schedule.every(2).second.do(getTask1)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
    partnerMonitor()

    # while 1:
    #     print "starts at:" + str( datetime.datetime.now())
    #     getTask1()
    #     print "ends at:" + str( datetime.datetime.now())
    #     print "...."
    #     time.sleep(10)
