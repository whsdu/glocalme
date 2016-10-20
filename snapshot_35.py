#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
import schedule, time
from kits import getdbinfo
from pymongo import MongoClient

epoch = 0

reload(sys)
sys.setdefaultencoding("utf-8")
def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                charset = 'utf8')
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

def getOss():
    try:
        ossurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55075/oss_system'
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['oss_system']
        ossCol = ossDB['t_monitor_term_online']
    except Exception as e:
        time.sleep(30)
        print e
        return getOss()

    print "get oss mongo connection"
    return [ossCon,ossCol]

def getAssOnline():
    querystatment = """
    SELECT imei FROM
    t_usmguserloginonline
    """

    assCon,assCur = getMysqlCon("ASS_GTBU","glocalme_ass")
    assCur.execute(querystatment)
    onlineSet = set()

    res = assCur.fetchall()
    for row in res:
        onlineSet.add(row[0])

    assCon.close()
    return onlineSet

def getTask1():
    ossCon,ossCol = getOss()

    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    querydict={"mvnoId":"57dfa3ad1ffdba479a12a1a9"}
    ossCur = ossCol.find(querydict)

    onlineSet = getAssOnline()
    recordtime = datetime.datetime.now()

    for doc in ossCur:
        code = doc.get("userCode","unknown")
        imei = doc.get("imei","unknown")
        
        if str(imei) not in onlineSet: continue

        visitcountry = doc.get("iso2","unknown")
        ms = doc.get("logindatetime",0)
        logindatetime = datetime.datetime.fromtimestamp(ms/1000.0)
        remoteCur.execute('''insert into t_login_history_35(epochtime,CODE,imei,visitcountry,logindatetime) values(%s,%s,%s,%s,%s)''',
                             (recordtime,code,imei,visitcountry,logindatetime))


    print "finished this round snapshot!"
    remoteCon.commit()
    remoteCon.close()
    ossCon.close()

if __name__ == "__main__":
    schedule.every(5).minutes.do(getTask1)
    getTask1()
    while 1:
        schedule.run_pending()
        time.sleep(1)
