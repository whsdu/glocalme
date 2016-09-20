# -*- coding: utf-8 -*-
import sys
import MySQLdb
import schedule, time
import datetime
from kits import getdbinfo
from pymongo import MongoClient
import copy
import numpy as np
import vsimloadrate


reload(sys)
sys.setdefaultencoding("utf-8")
lastMax = 0
today = 0
serilizeDate=0
preDict = dict()

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

def merge(resultDict):
    returnDict = dict()

    for key,result in resultDict.iteritems():
        for row in result:
            if row[1]==0 and row[2]==0:
                continue
            tmplist=returnDict.get(row[0],[0,0,0,0,0,0])
            if key =="gtbu":
                returnDict[row[0]]=[tmplist[0]+row[1],tmplist[1]+row[2],tmplist[2]+row[1],tmplist[3]+row[2],tmplist[4],tmplist[5]]
            else:
                returnDict[row[0]]=[tmplist[0]+row[1],tmplist[1]+row[2],tmplist[2],tmplist[3],tmplist[4]+row[1],tmplist[5]+row[2]]


    return returnDict

def serilizeResult():

    gtburesult = vsimloadrate.getGTBU()
    nonresult = vsimloadrate.getNON()

    resultDict = merge({"gtbu":gtburesult,"non":nonresult})

    nowtime = datetime.datetime.now()
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,val in resultDict.iteritems():
        remoteCur.execute('''insert into t_vsimloadnumsnapshot(recordtime,iso2,total,inuse,gtbutotal,gtbuinuse,nontotal,noninuse)
            values(%s,%s,%s,%s,%s,%s,%s,%s)''',
        (nowtime,key,val[0],val[1],val[2],val[3],val[4],val[5]))

    remoteCon.commit()
    remoteCon.close()


if __name__=="__main__":
    serilizeResult()
    schedule.every(5).minutes.do(serilizeResult)
    while True:
        schedule.run_pending()
        time.sleep(1)

