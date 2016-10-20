#!/usr/bionpython
#coding:utf-8
import MySQLdb
import datetime
import time
import logging
import sys
from datetime import  timedelta

from kits import getdbinfo

reload(sys)
sys.setdefaultencoding("utf-8")

def getquerystatement (querydate):
    queryStatementOnline = """
SELECT partner,visitcountry,COUNT(1) as cnt
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
	WHEN c.name LIKE "%global wifi 私有%" THEN 'global wifi 私有卡池'
	WHEN c.name LIKE "%global wifi 流量%" THEN "%global wifi 流量%"
	WHEN c.name LIKE "%商用——NUU%" THEN 'NUU'
	WHEN c.name LIKE "%商用——和记%" THEN '和记'
	WHEN c.name LIKE "%商用——QOOL%" THEN 'Qool'
	WHEN c.name LIKE "%WTT%" THEN 'WT&T'
	WHEN c.name LIKE "%商用——泰国GDD%" THEN '泰国GDD'
	WHEN c.name LIKE "%商用——Cello Mobile%" THEN 'Cello Mobile'
	WHEN c.name LIKE "%新浓%" THEN '新浓'
	WHEN c.name LIKE "%国航%" THEN '国航'
	WHEN c.name LIKE "%Onesystem%" THEN 'Onesystem'
	WHEN c.name LIKE "%ABCO PAYG%" THEN  'ABCO PAYG'
	WHEN c.name LIKE "%商用—易游网%" THEN '易游网'
	WHEN c.name LIKE "%Gwifi%" THEN '新世代GWIFI'
       ELSE 'others' END AS partner,c.visitcountry
       FROM
       (
	SELECT t1.uid,t2.butype,t1.imei,t2.registersource,t3.name,t3.code AS parentcode,t3.ritesimuser_id,visitcountry
	FROM
	(
		SELECT uid,imei,visitcountry FROM ucloudplatform211.t_usmguserloginlog
			WHERE
				DATE(logindatetime) <= """ + querydate+ """ AND DATE(logoutdatetime) >= """ + querydate+ """ """+" " + """
		UNION

		SELECT uid,imei,visitcountry FROM ucloudplatform211.t_usmguserloginonline
		WHERE
				DATE(logindatetime) <= """ + querydate+ """ """+" " + """
	) AS t1
	LEFT JOIN
	ucloudplatform211.t_usmguser AS t2 ON t1.uid = t2.uid
	LEFT JOIN
	ucloudplatform211.t_usmguser_parent AS t3 ON t2.parentid = t3.uid
	) AS c
) AS z
GROUP BY partner, visitcountry
"""
    return queryStatementOnline

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

def dateRange(startdate,intervals):
    td = datetime.datetime.now().date()
    sd = td-timedelta(days = startdate)
    dayrange = list()
    for d in range(intervals):
        targetday = sd-timedelta(days = d+1)
        dayrange.append(targetday)
    return dayrange

def partnerMonitor(startdate,dayinterval):
    datelist = dateRange(startdate,dayinterval)
    print datelist
    monitorSupport(datelist)

def monitorSupport(datelist):
    for row in datelist:
        print row
        start(row)

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

def submit2DB(queryDate,records):
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for row in records:
        partner = row[0]
        iso2 = row[1]
        onlinecnt = row[2]
        remoteCur.execute('''insert into t_partnerOnline_summary(recordtime,partner,iso2,onlinecnt) values(%s,%s,%s,%s)''',
                              (queryDate,partner,iso2,onlinecnt))
    remoteCon.commit()
    remoteCon.close()

def start(queryDate):
    qdate = queryDate.strftime('%Y-%m-%d %H:%M:%S')
    queryStatementOnline = getquerystatement("'"+qdate+"'")
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    remoteCur.execute(queryStatementOnline)

    records = remoteCur.fetchall()

    print queryDate
    print len(records)

    remoteCon.close()

    submit2DB(queryDate,records)

if __name__ == "__main__":
    # schedule.every(2).second.do(getTask1)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
    startdate = 45
    dayinterval = 62
    partnerMonitor(startdate,dayinterval)

    # while 1:
    #     print "starts at:" + str( datetime.datetime.now())
    #     getTask1()
    #     print "ends at:" + str( datetime.datetime.now())
    #     print "...."
    #     time.sleep(10)
