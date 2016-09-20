#!/usr/bionpython
#coding:utf-8
import MySQLdb
import datetime
import schedule, time
from kits import getdbinfo

lastEpochGtbu = 0
lastEpochNon = 0
epoch =0
lastDay = 0
querydbname = "ucloudplatform"

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def getTask1():

    querydbinfoGTBU = getdbinfo("ASS_GTBU")
    querydbnameGTBU = "glocalme_ass"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    # queryGTBU = MySQLdb.connect(user = mysqlGTBUinfo['usr'],passwd = mysqlGTBUinfo['pwd'], host = mysqlGTBUinfo['host'], port = mysqlGTBUinfo['port'], db = 'carecookies')
    # queryNonGTBU =  MySQLdb.connect(user = mysqlNonGTBUinfo['usr'],passwd = mysqlNonGTBUinfo['pwd'], host = mysqlNonGTBUinfo['host'], port = mysqlNonGTBUinfo['port'], db = querydbname)

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    gtbuCur = querydbGTBU.cursor()
    insertCur = insertdb.cursor()

    ccQuery = """
SELECT iso2,cnt
FROM
(
SELECT mcc,COUNT(1) AS cnt
FROM carecookies.work_order
WHERE DATE(occurs_time) = DATE(NOW())
GROUP BY mcc
) AS t1
LEFT JOIN
ucloudplatformgtbu.t_diccountries AS t2
ON t1.mcc = t2.id
    """
    gtbuQuery ="""
SELECT t2.iso2,t1.cnt
FROM
(
SELECT mcc, COUNT(DISTINCT(imei)) AS cnt
FROM
(
SELECT mcc,imei,sessionid
FROM
t_usmguserloginlog
WHERE DATE(logoutdatetime) = DATE(NOW())

UNION

SELECT mcc,imei,sessionid
FROM
t_usmguserloginonline
) AS z
GROUP BY mcc
) AS t1
LEFT JOIN
glocalme_css.t_css_mcc_country_map AS t2
ON t1.mcc = t2.mcc
    """
    insertCur.execute(ccQuery)
    queryR = insertCur.fetchall()
    ccDict = dict()
    for row in queryR:
        ccDict[row[0]]=row[1]

    gtbuCur.execute(gtbuQuery)
    queryR = gtbuCur.fetchall()
    countryDict = dict()
    for row in queryR:
        countryDict[row[0]]=row[1]

    recordtime = datetime.datetime.now()
    for iso2,useramount in countryDict.iteritems():
        cnt = ccDict.get(iso2,0)
        insertCur.execute('''insert into t_compliant_realtime(recordtime,iso2,cnt,useramount,belongs) values(%s,%s,%s,%s,%s)''',
                             (recordtime,iso2,cnt,useramount,"GTBU"))

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()

if __name__ == "__main__":
    schedule.every(5).minutes.do(getTask1)
    getTask1()
    while 1:
        schedule.run_pending()
        time.sleep(1)
