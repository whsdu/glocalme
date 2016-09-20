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

def getAssCon():

    gtbuDBinfo = getdbinfo("ASS_GTBU")
    gtbuDBname = "glocalme_ass"

    try:
        gtbuDBconnection = getDBconnection(gtbuDBinfo,gtbuDBname)
    except Exception as e:
        time.sleep(20)
        return getAssCon()

    gtbuCur = gtbuDBconnection.cursor()

    return [gtbuDBconnection,gtbuCur]

def getRemoteCon():
    remoteDBinfo = getdbinfo("REMOTE")
    remoteDBname = "login_history"

    try:
        remoteDBconnection = getDBconnection(remoteDBinfo,remoteDBname)
    except Exception as e:
        time.sleep(20)
        return getRemoteCon()

    remoteCur = remoteDBconnection.cursor()

    return [remoteDBconnection,remoteCur]

def queryAss(dnum=1):
    assCon,assCur = getAssCon()
    remoteCon,remoteCur = getRemoteCon()

    assQuery ="""
    SELECT YEAR(DATE_SUB(DATE(NOW()),INTERVAL %s DAY)) AS 'Year',WEEK(DATE_SUB(DATE(NOW()),INTERVAL %s DAY)) AS 'week','daily' AS flag,
    DATE(DATE_SUB(DATE(NOW()),INTERVAL %s DAY)) AS 'startdate',DATE(DATE_SUB(DATE(NOW()),INTERVAL %s DAY)) AS 'enddate',
    zz3.iso2 AS countrycode,zz3.en_US AS countryname,
    butype,COUNT(DISTINCT imei) AS onlinedevnum,MONTH(DATE_SUB(DATE(NOW()),INTERVAL %s DAY)) AS 'month', partner,SUM(flowsize) AS 'flowsize', COUNT(DISTINCT usercode) onlineuser

    FROM
    (
    SELECT logindatetime, usercode,"2" AS butype,imei,visitcountry,0 AS flowsize,'others' AS partner
    FROM
    t_usmguserloginlog
    WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL %s DAY)
    ) AS zz1
    LEFT JOIN
    glocalme_css.t_css_mcc_country_map AS zz2 ON zz1.visitcountry = zz2.mcc
    LEFT JOIN
    glocalme_css.t_css_country AS zz3 ON zz2.iso2 = zz3.iso2
    GROUP BY visitcountry,butype,partner;
    """
    assCur.execute(assQuery,(dnum,dnum,dnum,dnum,dnum,dnum))
    assResult = assCur.fetchall()

    print "finished query of day: " + str(dnum)
    for row in assResult:
        Year,week,flag,startdate,enddate,countrycode,countryname,butype,onlinedevnum,month,partner,flowsize,onlineuser = row
        remoteCur.execute('''insert into t_onlinedevnum_test(Year,week,flag,startdate,enddate,countrycode,countryname,butype,onlinedevnum,month,partner,flowsize,onlineuser)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (Year,week,flag,startdate,enddate,countrycode,countryname,butype,onlinedevnum,month,partner,flowsize,onlineuser))

    print "finished insert of day: " + str(dnum)

    remoteCon.commit()
    remoteCon.close()
    assCon.close()

if __name__ == "__main__":
    """
    The following code is used for fix the past data.
    """
    # for i in range(3):
    #     print "start operation of day: " + str(i+1)
    #     queryAss(i+1)

    schedule.every().day.at("00:23").do(queryAss)
    while 1:
        schedule.run_pending()
        time.sleep(1)
