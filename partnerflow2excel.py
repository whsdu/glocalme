#coding:utf-8
import os
import sys
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
reload(sys)
sys.setdefaultencoding("utf-8")

def createDailyFloder(partner):

    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/weekdata/"+partner

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
                                   charset = 'utf8')
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

def publishQuery(period):
    yesterQuery = """
    SELECT * FROM
    t_flowsummary_partner_adv
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    """
    weeklyQuery = """
    SELECT WEEK(NOW()) AS weeknum,iso2,partner,SUM(reserveup),SUM(reservedown),SUM(freeup),SUM(freedown),SUM(reservecnt),SUM(freecnt) FROM
    t_flowsummary_partner_adv
    WHERE DATE(epochTime) BETWEEN DATE_SUB(DATE(NOW()),INTERVAL 7 DAY) AND DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    GROUP BY iso2,partner
    """
    rList = list()
    con,cur = getCon("REMOTE","login_history")
    if period == 'weekly':
        cur.execute(weeklyQuery)
    else:
        cur.execute(yesterQuery)

    tm = cur.fetchall()
    for t in tm:
        rList.append(t)

    con.close()

    foldername = createDailyFloder(period)
    y = datetime.datetime.now().date()
    dateStr = str(y)
    filename = period + "_"+dateStr+".xlsx"

    workbook = Workbook(foldername+"/"+filename)
    sheet = workbook.add_worksheet()
    date_format = workbook.add_format({'num_format': 'mmmm d yyyy'})
    if period == "daily":
        sheet.write(0,0,u"日期".encode('utf-8'),date_format)
    else:
        sheet.write(0,0,u"周".encode('utf-8'))
    sheet.write(0,1,u"国家".encode('utf-8'))
    sheet.write(0,2,"partner")
    sheet.write(0,3,u"包卡上行".encode('utf-8'))
    sheet.write(0,4,u"包卡下行".encode('utf-8'))
    sheet.write(0,5,u"非包卡上行".encode('utf-8'))
    sheet.write(0,6,u"非包卡下行".encode('utf-8'))
    sheet.write(0,7,u"包卡用户数".encode('utf-8'))
    sheet.write(0,8,u"非包卡用户数".encode('utf-8'))

    r =1
    for row in rList:
        sheet.write(r,0,row[0])
        sheet.write(r,1,row[1])
        sheet.write(r,2,row[2])
        sheet.write(r,3,row[3])
        sheet.write(r,4,row[4])
        sheet.write(r,5,row[6])
        sheet.write(r,6,row[5])
        sheet.write(r,7,row[7])
        sheet.write(r,8,row[8])
        r += 1

    workbook.close()


def initiate():
    today = datetime.date.today()
    weekday = today.weekday()
    now_month = today.strftime("%d")

    publishQuery("daily")

    if weekday == 4:
        publishQuery("weekly")



if __name__ == "__main__":

    today = datetime.date.today()
    weekday = today.weekday()
    post_month = today.strftime("%d")

    createDailyFloder("daily")
    createDailyFloder("weekly")
    initiate()
    schedule.every().day.at("1:20").do(initiate)
    while True:
        schedule.run_pending()
        time.sleep(1)
