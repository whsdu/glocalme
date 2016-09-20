#coding:utf-8
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

def createDailyFloder(partner):

    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/zhongguihua/"+partner

    if not os.path.exists(tmpFloderName):
        os.makedirs(tmpFloderName)

    floderName = tmpFloderName
    return floderName

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
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

def publishQuery(partner,pcode,period):
    queryMonthlyFilter = """
SELECT t2.code,t1.logindatetime,t1.logoutdatetime,t1.imei,t1.imsi,t1.visitcountry,t1.flowsize,t1.up,t1.down,t1.money,t1.cellid,t1.lac,t1.mcc,t1.mnc,t1.devicetype,t3.iccid FROM
t_usmguserloginlog AS t1
LEFT JOIN
t_usmguser AS t2
ON t1.uid = t2.uid
LEFT JOIN
t_resvsim AS t3
ON t1.imsi = t3.imsi
WHERE DATE(t1.logindatetime) BETWEEN DATE_ADD(LAST_DAY(NOW() - INTERVAL 2 MONTH), INTERVAL 1 DAY) AND LAST_DAY(NOW() - INTERVAL 1 MONTH)
AND t2.parentid = %s
    """
    queryWeeklyFilter = """
SELECT t2.code,t1.logindatetime,t1.logoutdatetime,t1.imei,t1.imsi,t1.visitcountry,t1.flowsize,t1.up,t1.down,t1.money,t1.cellid,t1.lac,t1.mcc,t1.mnc,t1.devicetype,t3.iccid FROM
t_usmguserloginlog AS t1
LEFT JOIN
t_usmguser AS t2
ON t1.uid = t2.uid
LEFT JOIN
t_resvsim AS t3
ON t1.imsi = t3.imsi
WHERE DATE(t1.logindatetime) BETWEEN CURDATE() - INTERVAL DAYOFWEEK(NOW()) + 2 DAY AND DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
AND t2.parentid = %s
    """
    rList = list()
    con,cur = getCon("NON_GTBU","ucloudplatform")
    for code in pcode:
        if period == 'weekly':
            cur.execute(queryWeeklyFilter,(code,))
        else:
            cur.execute(queryMonthlyFilter,(code,))

        tm = cur.fetchall()
        for t in tm:
            rList.append(t)

    con.close()

    foldername = createDailyFloder(partner)
    y = datetime.datetime.now().date()
    dateStr = str(y)
    filename = partner + "_"+dateStr+".xlsx"

    workbook = Workbook(foldername+"/"+filename)
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"code")
    sheet.write(0,1,"logindatetime")
    sheet.write(0,2,"logoutdatetime")
    sheet.write(0,3,"imei")
    sheet.write(0,4,"imsi")
    sheet.write(0,5,"visitcountry")
    sheet.write(0,6,"flowsize")
    sheet.write(0,7,"up")
    sheet.write(0,8,"down")
    sheet.write(0,9,"money")
    sheet.write(0,10,"cellid")
    sheet.write(0,11,"lac")
    sheet.write(0,12,"mcc")
    sheet.write(0,13,"mnc")
    sheet.write(0,14,"devicetype")
    sheet.write(0,15,"iccid")

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
        sheet.write(r,8,row[8])
        sheet.write(r,9,row[9])
        sheet.write(r,10,row[10])
        sheet.write(r,11,row[11])
        sheet.write(r,12,row[12])
        sheet.write(r,13,row[13])
        sheet.write(r,14,row[14])
        sheet.write(r,15,row[15])

        r += 1

    workbook.close()


def initiate():
    global post_month

    today = datetime.date.today()
    weekday = today.weekday()
    now_month = today.strftime("%d")

    with open("zhongguihua_conf.json") as json_file:
        jdata = json.load(json_file)

    for key,val in jdata.iteritems():
        for k,v in val.iteritems():
            partner = k
            pcode = v['codelist']
            period = v['period']

            print partner
            print pcode
            print period
            print post_month
            print now_month

            if period == 'weekly' and weekday ==4:
                publishQuery(partner,pcode,'weekly')

            if period == 'monthly' and now_month != post_month:
                publishQuery(partner,pcode,'monthly')
                post_month = now_month



if __name__ == "__main__":
    global post_month
    today = datetime.date.today()
    weekday = today.weekday()
    post_month = today.strftime("%d")


    schedule.every().day.at("8:30").do(initiate)
    while True:
        schedule.run_pending()
        time.sleep(1)
