#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
from datetime import timedelta
import datetime
import schedule, time
from kits import getdbinfo

epoch = 0

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                   charset = 'utf8')
    return dbconnection

def getNonCon():

    gtbuDBinfo = getdbinfo("NON_GTBU")
    gtbuDBname = "ucloudplatform"

    try:
        gtbuDBconnection = getDBconnection(gtbuDBinfo,gtbuDBname)
    except Exception as e:
        time.sleep(20)
        return getAssCon()

    gtbuCur = gtbuDBconnection.cursor()

    return [gtbuDBconnection,gtbuCur]

def getTask1():
    reload(sys)
    sys.setdefaultencoding("utf-8")
    global epoch
    global lastEpochGtbu
    global lastEpochNon
    global querydbname
    global preDictGtbu
    global preDictNon
    global lastDay

    querydbinfoGTBU = getdbinfo("CRM")
    querydbnameGTBU = "vtigercrm"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    insertCur = insertdb.cursor()

    testquery = """
    select * from vtiger_ticketcf
    """
    pullQuery ="""
    SELECT cf_807,cf_809,left(cf_1109,4),COUNT(1) FROM
    vtiger_crmentity AS t1
    LEFT JOIN
    vtiger_ticketcf AS t2
    ON t1.crmid = t2.ticketid
    WHERE
	setype='HelpDesk'
	AND
	DATE(createdtime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)\
    GROUP BY cf_807,cf_1091,cf_1109
    """

    amountquery ="""
    SELECT t2.country_code2,t1.cnt
FROM
(
SELECT mcc, COUNT(DISTINCT(imei)) AS cnt
FROM
(
SELECT mcc,imei,sessionid
FROM
t_usmguserloginlog
WHERE DATE(logoutdatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
	OR
	DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)

UNION

SELECT mcc,imei,sessionid
FROM
t_usmguserloginonline
WHERE DATE(logoutdatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
) AS z
GROUP BY mcc
) AS t1
LEFT JOIN
t_dicmcc AS t2
ON t1.mcc = t2.mcc
    """
    nonCon,nonCur = getNonCon()
    nonCur.execute(amountquery)
    nonResult = nonCur.fetchall()
    countryCntDict = dict()
    for row in nonResult:
        countryCntDict[row[0]]=row[1]


    queryCountryISO2 = """
    SELECT country,iso2
    FROM t_diccountries
     """
    nonCur.execute(queryCountryISO2)
    nonResult = nonCur.fetchall()
    countryDict = dict()
    for row in nonResult:
        countryDict[row[0]]=row[1]

    queryCurGTBU.execute(pullQuery)
    queryGtbu = queryCurGTBU.fetchall()

    print " ....epoch "+ str(epoch)+" has  been finished!"
    print ""
    print len(queryGtbu)

    recordtime = datetime.datetime.now().date()-timedelta(days=1)
    countryMaxDict,buCntDict,buMaxDict = remoteSupply()
    for row in queryGtbu:
        countryname = row[0]
        iso2 = countryDict.get(countryname,"NULL")
        problem_type = str(row[1]).encode('utf-8')
        belongs = row[2]
        cnt = row[3]
        useramount = countryCntDict.get(iso2,0)
        countryMax = countryMaxDict.get(iso2,0)
        buCnt = buCntDict.get(belongs,0)
        buMax = buMaxDict.get(belongs,0)
        period = "daily"
        insertCur.execute('''insert into t_compliant_old(recordtime,iso2,problem_type,cnt,belongs,period,onlineuser,countrymax,bucnt,bumax)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                             (recordtime,iso2,problem_type,cnt,belongs,period,useramount,countryMax,buCnt,buMax))
        print iso2
        print cnt
        print useramount
        print "...."
        print " "

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()
    nonCon.close()

    epoch +=1

def remoteSupply():
    countryMaxState = """
    SELECT visitcountry,MAX(onlinenum)
    FROM
    t_fordemo
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()), INTERVAL 1 DAY) AND butype !=2
    GROUP BY visitcountry
    """
    butypeCntState = """
    SELECT CASE
	WHEN butype = 1 THEN 'GPBU'
	WHEN butype = 3 THEN 'GCBU'
	WHEN butype = 4 THEN 'GEBU'
	WHEN butype = 5 THEN 'others'
	END AS 'butype'
	,COUNT(imei)
FROM
(
SELECT imei,t2.buType FROM
ucloudplatform211.t_usmguserloginlog AS t1
LEFT JOIN
ucloudplatform211.t_usmguser AS t2
ON t1.uid = t2.uid
WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY) AND butype != 2

UNION

SELECT imei,t2.buType FROM
ucloudplatform211.t_usmguserloginonline AS t1
LEFT JOIN
ucloudplatform211.t_usmguser AS t2
ON t1.uid = t2.uid
WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY) AND butype != 2
) AS z
GROUP BY butype
    """

    butypeMaxState = """
    SELECT CASE
	WHEN butype = 1 THEN 'GPBU'
	WHEN butype = 3 THEN 'GCBU'
	WHEN butype = 4 THEN 'GEBU'
	WHEN butype = 5 THEN 'others'
	END AS 'butype',MAX(totalnum)
    FROM
    (
    SELECT epochTime,butype,SUM(onlinenum) AS totalnum
    FROM
    t_fordemo
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()), INTERVAL 1 DAY) AND butype != 2
    GROUP BY epochTime,butype
    ) AS z
    GROUP BY butype
    """

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'
    insertdb = getDBconnection(insertdbinfo,insertdbname)
    insertCur = insertdb.cursor()

    insertCur.execute(countryMaxState)
    countryMax = insertCur.fetchall()
    countryMaxDict = dict()
    for row in countryMax:
        countryMaxDict[row[0]]=row[1]

    insertCur.execute(butypeCntState)
    buCntResule = insertCur.fetchall()
    buCntDict = dict()
    for row in buCntResule:
        buCntDict[row[0]]=row[1]

    insertCur.execute(butypeMaxState)
    buMaxResule = insertCur.fetchall()
    buMaxDict = dict()
    for row in buMaxResule:
        buMaxDict[row[0]]=row[1]

    print countryMaxDict
    print buCntDict
    print buMaxDict

    insertdb.close()

    return [countryMaxDict,buCntDict,buMaxDict]

if __name__ == "__main__":
    getTask1()
    schedule.every().day.at("00:18").do(getTask1)
    while 1:
        schedule.run_pending()
        time.sleep(1)
