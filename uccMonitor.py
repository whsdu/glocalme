# -*- coding: utf-8 -*-
import schedule
import sys
import MySQLdb
import paramiko
import os
import StringIO
import pymongo
from pymongo import MongoClient
from kits import getdbinfo
from os import walk
import datetime
import  time


reload(sys)
sys.setdefaultencoding("utf-8")

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                    charset="utf8")
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

def getUCCdaily():
    Con,Cur = getMysqlCon("UCC","ucloudcc")
    gtbuquery = """
    select happenedtime,iso2,problem_type,belongs,count(*)
    from
    (
    select t2.iso2, cf_251 as 'problem_type', LEFT(cf_256,4) as 'belongs', DATE(cf_264) as 'happenedtime'
    from  t_helpdesk as t1
    left join
    t_country as t2 on t1.cf_262 = t2.id
    where date(cf_264) = date_sub(date(now()), interval 1 day)
    ) as z
    group by iso2,problem_type,belongs
    """
    Cur.execute(gtbuquery)
    gtburesult = dict()
    queryresult = Cur.fetchall()

    return queryresult

def getCountryMax():

    countryMaxDict = dict()

    Con,Cur = getMysqlCon("REMOTE","login_history")
    gtbuquery = """
    SELECT visitcountry,MAX(onlinenum)
    FROM
    t_fordemo_new
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()), INTERVAL 1 DAY)
    GROUP BY visitcountry
    """
    Cur.execute(gtbuquery)
    gtburesult = dict()
    queryresult = Cur.fetchall()
    for r in queryresult:
        countryMaxDict[r[0]]=r[1]
    Con.close()

    return countryMaxDict

def getTotalMax():
    Con,Cur = getMysqlCon("REMOTE","login_history")
    gtbuquery = """
    SELECT MAX(totalnum)
    FROM
    (
    SELECT epochTime,SUM(onlinenum) AS totalnum
    FROM
    t_fordemo_new
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()), INTERVAL 1 DAY)
    GROUP BY epochTime
    ) AS z
    """
    Cur.execute(gtbuquery)
    gtburesult = dict()
    queryresult = Cur.fetchall()
    totalmax = queryresult[0][0]
    Con.close()

    return totalmax

def getonlineuser():
    countOnlineDict = dict()

    Con,Cur = getMysqlCon("ASS_GTBU","glocalme_ass")
    gtbuquery = """
SELECT iso2,COUNT(imei)
FROM
(
SELECT imei,t2.iso2 FROM
t_usmguserloginlog AS t1
LEFT JOIN
glocalme_css.t_css_mcc_country_map AS t2
ON t1.visitcountry = t2.mcc
WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)

UNION

SELECT imei,t2.iso2 FROM
t_usmguserloginonline AS t1
LEFT JOIN
glocalme_css.t_css_mcc_country_map AS t2
ON t1.visitcountry = t2.mcc
WHERE DATE(logindatetime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
) AS z
GROUP BY iso2
    """
    Cur.execute(gtbuquery)
    gtburesult = dict()
    queryresult = Cur.fetchall()
    for r in queryresult:
        countOnlineDict[r[0]]=r[1]
    Con.close()

    return countOnlineDict

def initiate():

    insertList = list()

    uccResult = getUCCdaily()
    countryMaxDict = getCountryMax()
    totalMax = getTotalMax()
    onlineDict = getonlineuser()
    totalonline = sum(onlineDict.values())
    for row in uccResult:
        countrymax = countryMaxDict.get(row[1],0)
        onlinenum = onlineDict.get(row[1],0)
        maxlist = [countrymax,totalMax,onlinenum]
        insertList.append(list(row) + maxlist)

    Con,Cur = getMysqlCon("REMOTE","login_history")
    for r in insertList:
        if r[3] != 'GTBU': continue
        Cur.execute('''insert into t_compliant_new(recordtime,iso2,problem_type,belongs,cnt,maxonlineuser,maxtotaluser,loginuser,totalonline)
         values(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                             (r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],totalonline))

    Con.commit()
    Con.close()

if __name__=="__main__":

    #initiate()
    # while True:
    #     intoFolder()
    #     time.sleep(60)
    #
    schedule.every().day.at("0:45").do(initiate)
    while True:
        schedule.run_pending()
        time.sleep(1)
