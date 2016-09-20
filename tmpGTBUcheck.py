from datetime import timedelta
import MySQLdb
import schedule, time
import datetime
from kits import getdbinfo
from pymongo import MongoClient
import copy
import numpy as np
from xlsxwriter.workbook import Workbook


presisDict = dict()
presisSurvey = dict()
presisPro = dict()
proMark = 0


def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def getMysqlCon(dbhost,dbname):
    DBinfo = getdbinfo(dbhost)
    DBname = dbname

    try:
        DBconnection = getDBconnection(DBinfo,DBname)
    except Exception as e:
        time.sleep(20)
        return getRemoteCon()

    dbCur = DBconnection.cursor()

    return [DBconnection,dbCur]

def getOss():
    try:
        ossurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55075/oss_system'
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['oss_system']
        ossCol = ossDB['t_monitor_term_history']
    except Exception as e:
        time.sleep(30)
        return getOss()

    return [ossCon,ossCol]

def readOSS(startmc,endmc):
    ossDict = dict()
    ossCon,ossCol = getOss()
    ossCur = ossCol.aggregate(
    [
        {"$match":
            {"$and":
                [
                    {"logindatetime":
                        {"$gte":startmc,
                         "$lte":endmc}
                    },
                    {"iso2":{"$exists":1}}
                ]
            }
        },
        {"$project":
            {
                "sessionid":1,"logindatetime":1,"sumFlow":1,"iso2":1,"imei":1
            }
        }
    ]
)

    for doc in ossCur:
        ms = doc['logindatetime']
        sessionid = doc['sessionid']
        flow = doc['sumFlow']
        iso2 = doc['iso2']
        imei = doc['imei']
        ts = datetime.datetime.fromtimestamp(ms/1000.0)
        td = ts.date()

        ossDict[sessionid]=[sessionid,td,flow,iso2,imei]

    ossCon.close()
    return ossDict

def aggregate():
    yesterday = datetime.datetime.now().date()-timedelta(days=0)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=18)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    aggResult = dict()
    ossDict = readOSS(secondBef,secondYes)
    for record in ossDict.values():
        td = record[1]
        iso2 = record[3]
        key = str(td)+str(iso2)
        flow = record[2]
        imei = record[4]
        tmplist = aggResult.get(key,[0,0,0,set()])
        tmpflow = flow + tmplist[2]
        tmpcnt = tmplist[3]
        tmpcnt.add(imei)
        aggResult[key]=[iso2,td,tmpflow,tmpcnt]

    print aggResult

    workbook = Workbook("./"+"flowcheck.xlsx")
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"iso2")
    sheet.write(0,1,"datetime")
    sheet.write(0,2,"flowsize")
    sheet.write(0,3,"cnt")

    r = 1
    for record in aggResult.values():
        iso2 = record[0]
        dt = record[1]
        flowsize = record[2]
        cntSet = record[3]

        sheet.write(r,0,iso2)
        sheet.write(r,1,dt)
        sheet.write(r,2,flowsize)
        sheet.write(r,3,len(cntSet))
        r += 1

    workbook.close()

if __name__=="__main__":
    aggregate()

