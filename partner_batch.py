#!/usr/bionpython
#coding:utf-8

import sys
import MySQLdb
import datetime
import time
import logging
from kits import getdbinfo

reload(sys)
sys.setdefaultencoding("utf-8")

def getQueryStatment(startPoint,endPoint):
    dateBatchQuery = """
SELECT logid,partner,uid,butype,imei,registersource,NAME AS parentname,parentcode,logindatetime,logoutdatetime,visitcountry,dif AS datedif
FROM
(
	SELECT
	CASE WHEN c.name LIKE '%商用——青岛微易得%'  THEN '微易得'
      WHEN  c.name LIKE '%商用——力新%'  THEN '力新'
      WHEN c.name LIKE '%商用——香港博胜%'  THEN '博胜'
      WHEN c.name LIKE '%商用——乐云互联%'  THEN '乐云'
         WHEN c.name LIKE 'ING%'  THEN 'ING'
      WHEN  c.parentcode LIKE '%@pocwifi%'  THEN 'Pocwifi'
       WHEN c.name LIKE '%商用——连连科技%'  THEN '连连'
	WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = '大将军') THEN '大将军'
        WHEN c.name LIKE '%商用——Visondata%'  THEN 'visondata'
       WHEN c.name LIKE '%商用——新世代GWIFI%'  THEN '新世代GWIFI'
       WHEN c.parentcode LIKE '%simlocal%'  OR c.registersource=7  THEN 'simlocal'
       WHEN c.parentcode LIKE '%AVIS%'  THEN 'AVIS'
	WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = 'WT&T') OR c.name LIKE '%wttadmin@ecmefi.com%' THEN 'WT&T'
        WHEN c.name LIKE '%Royal Eagle%'  THEN 'Royal Eagle'
       WHEN c.name LIKE '%商用——台湾大晏%'  THEN '台湾大晏'
       WHEN c.name LIKE '%商用——欧威网游%'  THEN '欧威网游'
       WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = '和记') OR c.name LIKE '%商用——香港和记%'  THEN '和记'
       WHEN c.ritesimuser_id  IN  (SELECT id FROM ucloudplatform211.t_usmgritesimuser  WHERE   username LIKE '%NTC%')  THEN 'NTC'
       WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = 'Qool') THEN 'Qool'
	WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = 'NTC') THEN 'NTC'
	WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = 'Royal Eagle') THEN 'Royal Eagle'
       WHEN c.name LIKE '%商用——香港Wifi world%'  THEN 'WIFI World'
       WHEN c.name LIKE '%商用——香港亚升%'  THEN '亚升'
	WHEN c.imei IN (SELECT imei FROM t_newimeilist WHERE partner = '环球漫游') OR c.name LIKE '%商用——环球漫游%' THEN '环球漫游'
        WHEN c.name LIKE '%商用——世界邦%'  THEN '世界邦'
        WHEN c.name LIKE '%商用——桔豐科技%' THEN '桔豐科技'
        WHEN c.name LIKE '%商用——Crazyegg%' THEN 'Crazyegg'
        WHEN c.name LIKE '%商用——CMI%' THEN '上海中远'
        WHEN c.name LIKE '%商用——ING%' THEN 'ING'
        WHEN c.name LIKE '%商用——新加坡AsiaCloud%' THEN 'AsiaCloud'
        WHEN c.name LIKE '%商用——新浓包年套餐%' THEN '新浓'
       ELSE 'others' END AS partner,c.*
       FROM
       (
	SELECT t1.logid,t1.uid,t2.butype,t1.imei,t2.registersource,t3.name,t3.code AS parentcode,t3.ritesimuser_id,logindatetime,logoutdatetime,visitcountry,DATEDIFF(logoutdatetime,logindatetime) AS dif
	FROM
	ucloudplatform211.t_usmguserloginlog AS t1
	LEFT JOIN
	ucloudplatform211.t_usmguser AS t2 ON t1.uid = t2.uid
	LEFT JOIN
	ucloudplatform211.t_usmguser_parent AS t3 ON t2.parentid = t3.uid
	WHERE logid BETWEEN """ +str(startPoint)+""" AND """ +str(endPoint)+"""
	) AS c
) AS z
"""
    return dateBatchQuery
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

def dateRange(startPoint,endPoint):
    dateBatchQuery = getQueryStatment(startPoint,endPoint)
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    remoteCur.execute(dateBatchQuery)
    datelist = remoteCur.fetchall()

    remoteCon.close()

    return datelist

def dataInsert(datelist):

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for row in datelist:
        try:
            remoteCur.execute('''insert into tmp_partner_onlinedetail(logid,partner,uid,butype,imei,registersource,
                        parentname,parentcode,logindatetime,logoutdatetime,visitcountry, datedif)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                          (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]))
        except Exception as e:
            print e
            print row
    remoteCon.commit()
    remoteCon.close()

def startBatchInsert(start,end,step):
    startpoint = end-step
    if startpoint<=start: startpoint = start
    endpoint = end

    datelist = dateRange(startpoint,endpoint)
    dataInsert(datelist)

    print startpoint

    if startpoint == start: return
    startBatchInsert(start,startpoint-1,step)

def partnerMonitor():
    start = 9863552
    end = 11042523
    testend = 13008487
    step = 1000

    startBatchInsert(start,end,step)


def monitorSupport(datelist):
    for rdate in datelist:
        start(rdate)

def accumulatOnlineNumber(records):

    accDict = dict()

    for r in records:
        partner = r[0]
        iso2 = r[1]
        imei = r[2]

        tmpkey = str(partner.encode('utf-8'))+str(iso2)
        tmpimeiset = set()
        tmplist = accDict.get(tmpkey,[partner,iso2,tmpimeiset])
        imeiset=tmplist[2]
        imeiset.add(imei)
        accDict[tmpkey] = [partner,iso2,imeiset]

    return accDict

def submit2DB(queryDate,accDict):
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,value in accDict.iteritems():
        partner = value[0]
        iso2 = value[1]
        imeiset = value[2]
        onlinecnt = len(imeiset)
        remoteCur.execute('''insert into t_partnerOnline_summary(recordtime,partner,iso2,onlinecnt) values(%s,%s,%s,%s)''',
                              (queryDate,partner,iso2,onlinecnt))
    remoteCon.commit()
    remoteCon.close()

def start(queryDate):
    global queryStatementOnline

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    remoteCur.execute(queryStatementOnline,(queryDate,queryDate))

    records = remoteCur.fetchall()
    newAccDict = accumulatOnlineNumber(records)

    remoteCon.close()

    submit2DB(queryDate,newAccDict)

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
