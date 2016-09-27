#!/usr/bionpython
#coding:utf-8
import os
import MySQLdb
import pymongo
import datetime
from pymongo import MongoClient
from datetime import timedelta
from xlsxwriter.workbook import Workbook
import schedule
import time
import json
import csv
import operator
import xlrd
from kits import getdbinfo

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

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
def serilizeResult(diff=1):

    thedaybefore = datetime.datetime.now().date()-timedelta(days=diff)
    Con,Cur = getMysqlCon("ASS_GTBU","glocalme_ass")

    query = """

SELECT zz2.iso2,cnt
FROM
(
SELECT visitcountry AS mcc,COUNT(DISTINCT usercode) AS cnt
FROM
(
SELECT usercode,visitcountry
FROM t_usmguserloginlog
WHERE logindatetime <= %s AND logoutdatetime >= %s

UNION

SELECT usercode,visitcountry
FROM t_usmguserloginonline
WHERE logindatetime <= %s
) AS z
GROUP BY visitcountry
) AS zz1
LEFT JOIN
glocalme_css.t_css_mcc_country_map AS zz2
ON zz1.mcc = zz2.mcc

    """
    Cur.execute(query,(thedaybefore,thedaybefore,thedaybefore))

    res = Cur.fetchall()

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for row in res:
        iso2 = row[0]
        cnt = row[1]
        remoteCur.execute('''insert into t_gtbu_iso2_daily_cnt(recorddate,iso2,cnt)
            values(%s,%s,%s)''',(thedaybefore,iso2,cnt))
        print row

    print thedaybefore

    Con.close()
    remoteCon.commit()
    remoteCon.close()

if __name__ == "__main__":
    # for i in range(30):
    #     serilizeResult(i+1)

    schedule.every().day.at("01:10").do(serilizeResult)
    while 1:
        schedule.run_pending()
        time.sleep(1)
