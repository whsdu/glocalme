#!/usr/bionpython
#coding:utf-8
import os
from datetime import timedelta
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

floderName =""
fileRoot = "adsummary"

def createDailyFloder():
    global floderName
    global fileRoot

    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/"+fileRoot

    if not os.path.exists(tmpFloderName):
        os.makedirs(tmpFloderName)

    floderName = tmpFloderName

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                   charset='utf8')
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

def getSeperateBalance():
    queryFilter = """
    SELECT u1.uid,u1.code, u1.amount, u1.parentid, u1.parentcode,IFNULL(u3.money,0) AS parentamount,IFNULL(u2.usmguser_pid,-9999) AS dailypck
    FROM
    (
	    SELECT t2.uid,t2.code,t1.amount,t3.uid AS parentid,t3.code AS parentcode
	    FROM t_usmguseraccount AS t1
	    LEFT JOIN
	    t_usmguser AS t2 ON t1.uid = t2.uid
	    LEFT JOIN
	    t_usmguser_parent AS t3 ON t2.parentid = t3.uid
	    WHERE (t2.isdeleted+1) = 1 AND t3.uid IS NOT NULL
    ) AS u1
    LEFT JOIN
    (
	    SELECT DISTINCT usmguser_pid
	    FROM
	    t_gpdaypackagedetail
    )AS u2
    ON u1.parentid = u2.usmguser_pid
    LEFT JOIN
    t_gpusmguseraccount AS u3
    ON u1.parentid = u3.uid
    """
    balanceDict = dict()
    con,cur = getMysqlCon("NON_GTBU","ucloudplatform")
    cur.execute(queryFilter)
    tm = cur.fetchall()
    for r in tm:
        balanceDict[r[0]]={'usercode':r[1],'userbalance':r[2],'parentcode':r[4],'parentbalance':r[5],'dailypack':r[6]}

    con.close
    return balanceDict


def publishQuery(individual_threshold,partner_threshold,vipDict,maillist):
    balanceDict = getSeperateBalance()

    indivlist = list()
    partnerlist = list()
    partnerDict = dict()

    n =1
    for uid,val in vipDict.iteritems():
        print n
        n+=1
        userBalanceDetail = balanceDict.get(uid)
        if userBalanceDetail is None: continue

        # print str(uid) +" "+str(userBalanceDetail.get('usercode'))+" " +str(userBalanceDetail.get('userbalance'))+ " "+\
        #       str(userBalanceDetail.get('parentcode'))+ " "+str(userBalanceDetail.get('parentbalance')) +" : "+str(userBalanceDetail.get('dailypack'))

        if userBalanceDetail.get('dailypack') == -9999 and userBalanceDetail.get('userbalance')<=individual_threshold:
            indivlist.append([userBalanceDetail.get('usercode'),int(userBalanceDetail.get('userbalance')),'individual'])

        if userBalanceDetail.get('dailypack') != -9999 and userBalanceDetail.get('parentbalance')<=partner_threshold:
            partnerDict[userBalanceDetail.get('parentcode')]=userBalanceDetail.get('parentbalance')

    for key,val in partnerDict.iteritems():
        partnerlist.append([key,int(val),'partner'])
    print indivlist
    print partnerlist
    print "  "
    print partnerDict

    if len(indivlist)!=0:
        mailboday = dailyoutput.getbodytext(indivlist,'VIP Individual Account',len(indivlist))
        sendmail("VIP individual account",maillist,mailboday)
    if len(partnerlist)!=0:
        mailboday = dailyoutput.getbodytext(partnerlist,'VIP partner Account',len(partnerlist))
        sendmail("VIP partner account",maillist,mailboday)

def advanceVip(vipR):
    Con,Cur = getMysqlCon("NON_GTBU","ucloudplatform")
    onlinequery = """
    SELECT imei, uid
    FROM t_usmguserloginonline
    """
    vipquery = """
    SELECT t1.imei,t2.uid
    FROM
    (
    SELECT imei,MAX(logindatetime) AS logindatetime
    FROM t_usmguserloginlog
    WHERE DATE(logindatetime) >= DATE_SUB(DATE(NOW()),INTERVAL 3 DAY)
    GROUP BY imei

    ) AS t1
    LEFT JOIN
    (

    SELECT uid,imei,logindatetime
    FROM t_usmguserloginlog
    WHERE DATE(logindatetime) >= DATE_SUB(DATE(NOW()),INTERVAL 3 DAY)
    ) AS t2
    ON t1.imei = t2.imei AND t1.logindatetime = t2.logindatetime
    """
    loginDict = dict()
    Cur.execute(vipquery)
    res = Cur.fetchall()
    for r in res:
        loginDict[r[0]]=r[1]

    resDict = dict()
    for r in vipR:
        imei = 0
        print r
        if len(r[0]) > 15:
            imei = r[0][:16]
        else: imei = r[0]
        uid = loginDict.get(imei,0)
        if uid == 0: continue
        resDict[imei]=uid

    print resDict
    print "  " + str(len(resDict))
    print "...."

    onlineDict = dict()
    Cur.execute(onlinequery)
    res = Cur.fetchall()
    for r in res:
        onlineDict[r[0]]=r[1]

    for r in vipR:
        imei = 0
        if len(r[0]) > 15:
            imei = r[0][:16]
        else: imei = r[0]
        uid = onlineDict.get(imei,0)
        if uid == 0: continue
        resDict[imei]=uid

    print resDict
    print "  " + str(len(resDict))

    Con.close()

    return resDict

def getAD():
    vipquery = """
    SELECT t1.imei, t2.fValue FROM
    t_imp_data AS t1
    LEFT JOIN
    t_loginhistory_dic AS t2
    ON t1.state = t2.fkey
    WHERE t2.ftype = 'group_type'
    """
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    remoteCur.execute(vipquery)
    adR = remoteCur.fetchall()
    remoteCon.close()
    return adR

def getlogdict():
    yesterday = datetime.datetime.now().date()-timedelta(days=1)
    thedaybefore = datetime.datetime.now().date()-timedelta(days=2)
    secondYes = int((yesterday-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000
    secondBef = int((thedaybefore-datetime.date(1970,1,1)).total_seconds()-8*3600)*1000

    ossurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55075/oss_system'
    ossCon = MongoClient(ossurl)
    ossDB= ossCon['oss_system']
    ossCol = ossDB['t_monitor_term_history']

    ossCur = ossCol.aggregate(
        [
            {"$match":{
                        "$and":
                        [
                        {"logindatetime":{"$gte":secondBef,"$lte":secondYes}}
                        ]
                        }
            },
            {"$group":{
                    "_id":"$imei",
                    "total":{"$sum":"$sumFlow"},
                    "cnt":{"$sum":1}
                    }
            }
        ]
    )

    flowDict = dict()
    for doc in ossCur:
        flowDict[str(doc['_id'])]=[doc['total'],doc['cnt']]

    zeroCur = ossCol.aggregate(
        [
            {"$match":{
                        "$and":
                        [
                        {"logindatetime":{"$gte":secondBef,"$lte":secondYes}},
                        {"userUpFlow":0},
                        {"userDownFlow":0}
                        ]
                        }
            },
            {"$group":{
                    "_id":"$imei",
                    "cnt":{"$sum":1}
                    }
            }
        ]
    )
    zeroDict = dict()
    for doc in zeroDict:
        zeroDict[str(doc['_id'])]=doc['cnt']

    countryDict = dict()
    countryCur = ossCol.aggregate(
        [
            {"$match":{
                        "$and":
                        [
                        {"logindatetime":{"$gte":secondBef,"$lte":secondYes}},
                        {"softversion":{"$exists":1}}
                        ]
                        }
            },
            {"$project":{
                    "_id":-1,
                    "imei":1,
                    "iso2":1,
                    "softversion":1
                    }
            }
        ]
    )
    for doc in countryCur:
        countryDict[str(doc['imei'])]=[doc['iso2'],doc['softversion']]

    ossCon.close()

    return [flowDict,zeroDict,countryDict]

def createXML(type,record):
    global floderName

    y = datetime.datetime.now().date()-timedelta(days=1)
    dateStr = str(y)
    filename = type + "_"+dateStr+".xlsx"

    workbook = Workbook(floderName+"/"+filename)
    sheet = workbook.add_worksheet()

    sheet.write(0,0,"imei")
    sheet.write(0,1,"loginCnt")
    sheet.write(0,2,"zeroCnt")
    sheet.write(0,3,"flowsize")
    sheet.write(0,4,"visitcountry")
    sheet.write(0,5,"version")

    r =1
    for summary in record:
        sheet.write(r,0,summary[0])
        sheet.write(r,1,summary[1])
        sheet.write(r,2,summary[2])
        sheet.write(r,3,summary[3])
        sheet.write(r,4,summary[4])
        sheet.write(r,5,summary[5])
        r += 1

    workbook.close()

def initiate():
    today = datetime.date.today()
    weekday = today.weekday()

    adR = getAD()
    flowDict,zeroDict,countryDict = getlogdict()

    print flowDict
    # print zeroDict
    # print countryDict

    print ""
    print len(flowDict)
    print len(zeroDict)
    print len(countryDict)

    adSumamry = dict()
    for row in adR:
        imei = row[0]
        type = row[1]
        userflow,cnt = flowDict.get(imei,[None,None])
        if userflow is None: continue

        tmprecord = adSumamry.get(type,list())
        iso2, version = countryDict.get(imei,["unknown","unknown"])
        tmprecord.append([imei,cnt,zeroDict.get(imei,0),userflow,iso2,version])
        adSumamry[type]=tmprecord

    r = 1
    for type,record in adSumamry.iteritems():
        print type
        print record
        print ""
        r+=1
        createXML(str(r),record)

    print "finished"

if __name__ == "__main__":

    createDailyFloder()
    initiate()

  #  schedule.every().day.at("1:08").do(initiate)
  #  while True:
  #      schedule.run_pending()
  #      time.sleep(1)
