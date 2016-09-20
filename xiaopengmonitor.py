# -*- coding: utf-8 -*-
import sys
import MySQLdb
import schedule, time
import datetime
from kits import getdbinfo
from pymongo import MongoClient
import copy
import numpy as np


reload(sys)
sys.setdefaultencoding("utf-8")
lastMax = 0
today = 0
serilizeDate=0
preDict = dict()

onlineIntervalDict = dict()
vsimChangeDict = dict()
smallFlowDict = dict()

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


def getVIP():
    vipquery = """
    SELECT imei
    FROM
    t_vip_data
    """
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    remoteCur.execute(vipquery)
    rows = remoteCur.fetchall()
    vipList = list()
    for r in rows:
        vipList.append(r[0])
    remoteCon.close()

    return vipList

def isNotVip(imei,viplist):
    if imei in viplist:
        return 0
    else: return 1


def accumulate(parent,curflowDict):
    global preDict

    nonquery = """
    SELECT imsi,visitcountry,butype,ownerid
    FROM
    (
        SELECT t1.imsi,t1.visitcountry,t2.butype,
	    CASE WHEN t3.uid IS NULL THEN 'others'
	    ELSE t3.name END AS NAME,
	    IFNULL(t4.ownerid,-99) AS ownerid
        FROM
        t_usmguserloginonline AS t1
        LEFT JOIN
        t_usmguser AS t2
        ON t1.uid = t2.uid
        LEFT JOIN
        t_usmguser_parent AS t3
        ON t2.parentid = t3.uid
        LEFT JOIN
        t_resvsimowner AS t4
        ON t1.imsi= t4.sourceid
    )AS z
    WHERE name LIKE '%"""+parent.encode('utf-8')+"""%'"""
    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    nonCur.execute(nonquery)
    r = nonCur.fetchall()
    nonCon.close()

    for row in r:
        tmpflow = curflowDict.get(row[0],list())
        if len(tmpflow)==0:
            continue
        country = row[1]
        bu = row[2]
        type = row[3]

        up = tmpflow[0]
        down = tmpflow[1]

        key = str(country)+str(bu)

        tmpList = preDict.get(key,[country,bu,0,0,0,0])
        if type == -99:
            preDict[key]=[tmpList[0],tmpList[1],tmpList[2],tmpList[3],tmpList[4]+up,tmpList[5]+down]
        else:
            preDict[key]=[tmpList[0],tmpList[1],tmpList[2]+up,tmpList[3]+down,tmpList[4],tmpList[5]]

def getIMSIinfo():
    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")

    query="""
    SELECT imsi,rat,country_code2 AS iso2
    FROM
    t_resvsim
    """
    nonCur.execute(query)
    rows = nonCur.fetchall()
    imsiDict = dict()
    for r in rows:
        imsiDict[r[0]]=[r[1],r[2]]

    nonCon.close()

    return imsiDict

def monitorOnlineInterval(curFlowList):
    global onlineIntervalDict
    client = MongoClient()
    db = client['login_history']
    col = db['col_viperrormonitor']

    curTime = datetime.datetime.now()

    for row in curFlowList:
        imei = row[0]
        session = row[2]
        country = row[4]
        sessionset, timelist, countrylist= onlineIntervalDict.get(imei,[set(),list(),list()])
        if session not in sessionset:
            timelist.append(curTime)
            sessionset.add(session)
            countrylist.append(country)
        onlineIntervalDict[imei]=[sessionset,timelist,countrylist]

    print "In onlineIntervalDict, size is: " + str(len(onlineIntervalDict))
    print onlineIntervalDict
    print ""

    for imei,records in onlineIntervalDict.iteritems():
        sessionset,timelist, countrylist = records
        if len(timelist) == 6:
            onlineIntervalDict[imei]=[sessionset,timelist[-5:],countrylist[-5:]]


    for imei,records in onlineIntervalDict.iteritems():
        sessionset,timelist, countrylist = records
        if len(timelist)<5: continue

        gap = (timelist[4]-timelist[0]).total_seconds()

        if gap<600:
            iso2 = countrylist[-1]
            col.update({'imei':imei,'recordtime':curTime},
                               {'$set':{'imei':imei,'recordtime':curTime,'iso2':iso2,'belonging':'non','errtype':'multilogin','vip':1}},
                               upsert = True)
            onlineIntervalDict[imei]=[set(),timelist[-1:], countrylist[-1:]]

    client.close()

def monitorBestVsim(curFlowList):
    imsiDict = getIMSIinfo()

    client = MongoClient()
    db = client['login_history']
    col = db['col_viperrormonitor']

    curTime = datetime.datetime.now()

    for row in curFlowList:
        imei = row[0]
        imsi = row[1]
        iso2 = row[4]
        imsiDetail = imsiDict.get(imsi,list())

        if len(imsiDetail) == 0 : continue

        rat = imsiDetail[0]
        imsiISO2 = imsiDetail[1]

        if (rat == 2 or rat == 3) and iso2 == imsiISO2:
            col.update({'imei':imei,'recordtime':curTime},
                               {'$set':{'imei':imei,'recordtime':curTime,'iso2':iso2,'belonging':'non','errtype':'badvsim','vip':1}},
                               upsert = True)

    client.close()

def monitorVsimChange(curFlowList):
    global vsimChangeDict

    client = MongoClient()
    db = client['login_history']
    col = db['col_viperrormonitor']

    curTime = datetime.datetime.now()

    for row in curFlowList:
        imei = row[0]
        imsi = row[1]
        country = row[4]
        imsiset, timelist, countrylist= vsimChangeDict.get(imei,[set(),list(),list()])
        if imsi not in imsiset:
            timelist.append(curTime)
            imsiset.add(imsi)
            countrylist.append(country)
        vsimChangeDict[imei]=[imsiset,timelist,countrylist]

    print "In vsimChangeDict, size is: " + str(len(vsimChangeDict))

    for imei,records in vsimChangeDict.iteritems():
        imsiset, timelist, countrylist = records
        if len(timelist) == 6:
            vsimChangeDict[imei]=[imsiset,timelist[-5:],countrylist[-5:]]

    for imei,records in vsimChangeDict.iteritems():
        sessionset,timelist, countrylist = records
        if len(timelist)<5: continue

        gap = (timelist[4]-timelist[0]).total_seconds()

        if gap<600:
            iso2 = countrylist[-1]
            col.update({'imei':imei,'recordtime':curTime},
                               {'$set':{'imei':imei,'recordtime':curTime,'iso2':iso2,'belonging':'non','errtype':'vsimchange','vip':1}},
                               upsert = True)
            vsimChangeDict[imei]=[set(),timelist[-1:], countrylist[-1:]]

    client.close()

def monitorSmallFlow(curFlowList):
    global smallFlowDict

    client = MongoClient()
    db = client['login_history']
    col = db['col_viperrormonitor']

    curTime = datetime.datetime.now()

    for row in curFlowList:
        imsi = row[1]
        flow = row[3]
        country = row[4]
        imei = row[0]
        flowlist, timelist, countrylist, imeilist= smallFlowDict.get(imsi,[list(),list(),list(),list()])

        flowlist.append(flow)
        timelist.append(curTime)
        countrylist.append(country)
        imeilist.append(imei)
        smallFlowDict[imsi]=[flowlist,timelist,countrylist,imeilist]

    print "In smallFlowDict, size is: " + str(len(smallFlowDict))

    for imsi,records in smallFlowDict.iteritems():
        flowlist, timelist, countrylist, imeilist = records
        if (timelist[-1]-timelist[0]).total_seconds() > 600:
            p = len(flowlist)/2
            smallFlowDict[imsi]=[flowlist[-p:],timelist[-p:],countrylist[-p:],imeilist[-p:]]

    for imsi,records in smallFlowDict.iteritems():
        flowlist, timelist, countrylist, imeilist = records
        if (timelist[-1]-timelist[0]).total_seconds()<60: continue

        gap = (timelist[-1]-timelist[0]).total_seconds()
        totalflow = sum(flowlist)
        aveflow = 1.0 * float(totalflow)/float(gap)

        if aveflow < (200*1024)/60:
            print "smallFlowDict---->>>>>>"
            print imsi
            print gap
            print totalflow
            print aveflow
            print records
            print "<<<<<------"
            print ""

        if aveflow<(100*1024)/60:
            iso2 = countrylist[-1]
            imei = imeilist[-1]
            col.update({'imei':imei,'recordtime':curTime},
                               {'$set':{'imei':imei,'imsi':imsi,'recordtime':curTime,'iso2':iso2,'belonging':'non','errtype':'smallflow','vip':1}},
                               upsert = True)
            smallFlowDict[imsi]=[flowlist[-1:], timelist[-1:], countrylist[-1:], imeilist[-1:]]

    client.close()

def tensorFlow(curFlowList):
    monitorOnlineInterval(curFlowList)
    monitorBestVsim(curFlowList)
    monitorVsimChange(curFlowList)
    monitorSmallFlow(curFlowList)

def queryandinsert():
    global lastMax
    global today

    nowdate = datetime.datetime.now().strftime("%d")

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    queryNonMax="""
    SELECT MAX(itemid) FROM
    t_chgflowlog001    """

    queryNonCurrent="""
    SELECT imei,vsimimsi AS imsi,sessionid,flowsize,visitcountry FROM
    t_chgflowlog001
    WHERE itemid >= %s AND itemid < %s
    ORDER BY  itemid ASC
    """

    nonCur.execute(queryNonMax)
    r = nonCur.fetchall()
    curMax=0
    for row in r:
        curMax=row[0]

    if curMax==lastMax:
        time.sleep(2)
        return

    vipList = getVIP()
    curFlowList = list()
    nonCur.execute(queryNonCurrent,(lastMax,curMax))
    r = nonCur.fetchall()
    for row in r:
        if isNotVip(row[0],vipList):continue
        curFlowList.append(row)
    nonCon.close()

    tensorFlow(curFlowList)

    lastMax=curMax

def initiate():
    global lastMax
    global today
    global serilizeDate

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    queryNon="""
    SELECT MAX(itemid) FROM
    t_chgflowlog001    """

    nonCur.execute(queryNon)
    r = nonCur.fetchall()
    for row in r:
        lastMax=row[0]
    nonCon.close()

    today = datetime.datetime.now().strftime("%d")
    serilizeDate = datetime.datetime.now().date()


if __name__=="__main__":
    initiate()
    time.sleep(2)
    flag = True
    while flag:
        queryandinsert()

