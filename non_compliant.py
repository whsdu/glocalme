#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
import schedule, time
from kits import getdbinfo

epoch = 0

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
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
    SELECT cf_807,cf_1071,COUNT(1) FROM
    vtiger_crmentity AS t1
    LEFT JOIN
    vtiger_ticketcf AS t2
    ON t1.crmid = t2.ticketid
    WHERE
	setype='HelpDesk'
	AND
	DATE(createdtime) = DATE(NOW())
    GROUP BY cf_807,cf_1071
    """

    nonCon,nonCur = getNonCon()

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
WHERE DATE(logoutdatetime) = DATE(NOW())

UNION

SELECT mcc,imei,sessionid
FROM
t_usmguserloginonline
) AS z
GROUP BY mcc
) AS t1
LEFT JOIN
t_dicmcc AS t2
ON t1.mcc = t2.mcc
    """
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


    recordtime = datetime.datetime.now()

    for row in queryGtbu:
        countryname = row[0]
        iso2 = countryDict.get(countryname,"NULL")
        cnt = row[2]
        useramount = countryCntDict.get(iso2,0)
        insertCur.execute('''insert into t_compliant_realtime(recordtime,iso2,cnt,useramount,belongs) values(%s,%s,%s,%s,%s)''',
                             (recordtime,iso2,cnt,useramount,'GCBU'))
        print iso2
        print cnt
        print useramount
        print""
        print "...."

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()
    nonCon.close()

    epoch +=1

if __name__ == "__main__":
    schedule.every(30).minutes.do(getTask1)
    getTask1()
    while 1:
        schedule.run_pending()
        time.sleep(1)
