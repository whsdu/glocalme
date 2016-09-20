# -*- coding: utf-8 -*-
import os
import string
import smtplib
import MySQLdb
from pymongo import MongoClient
import datetime
import schedule
import time
import json
from pytz import timezone
from kits import getdbinfo
from xlsxwriter.workbook import Workbook
import xlrd

post_month = 0

def createDailyFloder(subfolder):

    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/zhongguihua/"+subfolder

    if not os.path.exists(tmpFloderName):
        os.makedirs(tmpFloderName)

    floderName = tmpFloderName
    return floderName

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                    charset="utf8")
    return dbconnection

def getCon(dbcode,dbname):
    DBinfo = getdbinfo(dbcode)
    DBname = dbname

    try:
        DBconnection = getDBconnection(DBinfo,DBname)
    except Exception as e:
        print e
        time.sleep(20)
        return getCon(dbcode,dbname)

    Cur = DBconnection.cursor()

    return [DBconnection,Cur]

def publishQuery(partner,pcode):
    queryFilter = """
SELECT t1.createtime,t1.imei,t1.level,t1.status,t2.name,t2.code,t3.name,t3.code FROM
t_usmglimitspeed_his AS t1
LEFT JOIN
t_usmguser AS t2 ON t1.uid = t2.uid
LEFT JOIN
t_usmguser_parent AS t3 ON t2.parentid = t3.uid
WHERE
	DATE(NOW()) = DATE(t1.createtime)
	AND
	t3.uid = %s
        AND t1.level != 15
    """
    rList = list()
    con,cur = getCon("NON_GTBU","ucloudplatform")
    for code in pcode:
        cur.execute(queryFilter,(code,))
        tm = cur.fetchall()
        for t in tm:
            rList.append(t)

    con.close()

    foldername = createDailyFloder("limit")
    y = datetime.datetime.now().date()
    dateStr = str(y)
    filename = partner + "_"+dateStr+".xlsx"

    workbook = Workbook(foldername+"/"+filename)
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"createtime")
    sheet.write(0,1,"imei")
    sheet.write(0,2,"level")
    sheet.write(0,3,"status")
    sheet.write(0,4,"username")
    sheet.write(0,5,"usercode")
    sheet.write(0,6,"partnername")
    sheet.write(0,7,"partnercode")

    r =1
    for row in rList:
        sheet.write(r,0,row[0])
        sheet.write(r,1,row[1])
        sheet.write(r,2,row[2])
        sheet.write(r,3,row[3])
        sheet.write(r,4,row[4])
        sheet.write(r,5,row[5])
        sheet.write(r,6,row[6])
        sheet.write(r,7,row[7])

        r += 1

    workbook.close()


def initiate():
    today = datetime.date.today()
    weekday = today.weekday()
    now_month = today.strftime("%d")

    with open("zhongguihua_limitspeed_conf.json") as json_file:
        jdata = json.load(json_file)

    for key,val in jdata.iteritems():
        for k,v in val.iteritems():
            partner = k
            pcode = v

            publishQuery(partner,pcode)


if __name__ == "__main__":
    initiate()

    schedule.every().day.at("8:00").do(initiate)
    while True:
        schedule.run_pending()
        time.sleep(1)
