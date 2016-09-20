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

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    queryCurGTBU = querydbGTBU.cursor()

    testquery = """
    select * from vtiger_ticketcf
    """
    pullQuery ="""
    SELECT cf_807,cf_1091,cf_1109,COUNT(1) FROM
    vtiger_crmentity AS t1
    LEFT JOIN
    vtiger_ticketcf AS t2
    ON t1.crmid = t2.ticketid
    WHERE
	setype='HelpDesk'
	AND
	DATE(createdtime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
	AND cf_809=N'"""+u'故障类'.encode("utf-8")+"""'
    GROUP BY cf_807,cf_1091,cf_1109
    """
    queryCurGTBU.execute(pullQuery)
    queryGtbu = queryCurGTBU.fetchall()

    for row in queryGtbu:
        print row

    querydbGTBU.close()

if __name__ == "__main__":
    getTask1()
