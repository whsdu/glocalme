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

def merge(resultlist):
    returnDict = dict()

    for result in resultlist:
        for row in result:
            if row[1]==0 and row[2]==0:
                continue
            tmplist=returnDict.get(row[0],[0,0])
            returnDict[row[0]]=[tmplist[0]+row[1],tmplist[1]+row[2]]

    return returnDict

def serilizeResult():

    gtburesult = vsimloadrate.getGTBU()
    nonresult = vsimloadrate.getNON()

    resultDict = merge([gtburesult,nonresult])

    nowtime = datetime.datetime.now()
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,val in resultDict.iteritems():
        remoteCur.execute('''insert into t_vsimloadnumsnapshot(recordtime,iso2,total,inuse)
            values(%s,%s,%s,%s)''',
        (nowtime,key,val[0],val[1]))

    remoteCon.commit()
    remoteCon.close()


if __name__=="__main__":
    serilizeResult()
    schedule.every(5).minutes.do(serilizeResult)
    while True:
        schedule.run_pending()
        time.sleep(1)

