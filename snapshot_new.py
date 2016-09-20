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

    querydbinfoGTBU = getdbinfo("ASS_GTBU")
    querydbnameGTBU = "glocalme_ass"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    insertCur = insertdb.cursor()

    pullQuery ="""
    SELECT usercode,logindatetime,imei,iso2 FROM
    t_usmguserloginonline AS t1
    LEFT JOIN
    glocalme_css.t_css_mcc_country_map AS t2
    ON t1.mcc = t2.mcc
    """

    queryCurGTBU.execute(pullQuery)

    queryGtbu = queryCurGTBU.fetchall()

    recordtime = datetime.datetime.now()

    for row in queryGtbu:
        code = row[0]
        logindatetime = row[1]
        imei = row[2]
        visitcountry =row[3]
        insertCur.execute('''insert into t_login_history_new(epochtime,CODE,imei,visitcountry,logindatetime) values(%s,%s,%s,%s,%s)''',
                             (recordtime,code,imei,visitcountry,logindatetime))

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()

    epoch +=1

if __name__ == "__main__":
    schedule.every(5).minutes.do(getTask1)
    getTask1()
    while 1:
        schedule.run_pending()
        time.sleep(1)
