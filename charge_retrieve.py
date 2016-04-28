#!/usr/bionpython
#coding:utf-8
import MySQLdb
import datetime
import time
import logging
import json
import numpy as np
from kits import getdbinfo
from collections import defaultdict
from pymongo import MongoClient

lastEpochGtbu = 0
lastEpochNon = 0
epoch =0
querydbname = "ucloudplatform"
step = 52428800
minN = 0
maxN = 10737400000

preDictGtbu = defaultdict(int)
preDictNon = defaultdict(int)
daterecord = datetime.datetime.now().date()
recordDates = {"GTBU":daterecord,"NON-GTBU":daterecord}

def retrieveByDay(row,preDict,belong):
    global recordDates

    key1 = row[0]
    flowsize = float(row[1])
    up = float(row[2])
    down = float(row[3])
    day = row[4]
    key2 = row[5]
    key = (key1,key2)

    flowDict = preDict.get(key,{"flowsize":0,"up":0,"down":0})
    flowDict["flowsize"] += flowsize
    flowDict["up"] += up
    flowDict["down"] += down
    preDict[key] = flowDict

    day= day.date()

    if recordDates[belong] != day:
        seriDict(recordDates,preDict,belong)
        recordDates[belong] = day
        preDict.clear()

def seriDict(recordtime,preDict,belong):
    client = MongoClient()
    db = client['login_history']
    col = db['col_flowPro']

    countrySet = set()
    countryListDict = dict()
    dateCountryDict = dict()
    dt = recordtime[belong]
    ds = datetime.datetime(*(dt.timetuple()[:6]))

    for key in preDict:
        countrySet.add(key[1])

    for country in list(countrySet):
        countryListDict[country]=[list(),list(),list()]

    for key,value in preDict.iteritems():
        flowSizeList = countryListDict.get(key[1])[0]
        upList = countryListDict.get(key[1])[1]
        downList = countryListDict.get(key[1])[2]

        flowSizeList.append(value.get("flowsize"))
        upList.append(value.get("up"))
        downList.append(value.get("down"))

    for key,value in countryListDict.iteritems():
        flowSizeList = value[0]
        upList = value[1]
        downList = value[2]

        flowSizeJSON = getJSONpdf(flowSizeList)
        upJSON = getJSONpdf(upList)
        downJSON = getJSONpdf(downList)

        proDict = {"flowSize":flowSizeJSON,"up":upJSON,"down":downJSON}

        # countryTmpDict = dateCountryDict.get(datetime,dict())

        post = {"datetime":ds,"country":key,"distribution":proDict,"butype":belong}
        col.insert_one(post)
    #
    #     countryTmpDict[key]=proDict
    #     dateCountryDict[datetime]=countryTmpDict
    #
    # countryDict = dateCountryDict.get(datetime)
    # post = {"datetime":ds,"countryDetail":countryDict,"butype":belong}
    #
    # col.insert_one(post)

    client.close()

    # insertDBinfo = getdbinfo("REMOTE")
    # insertRemote =  MySQLdb.connect(user = insertDBinfo['usr'],passwd = insertDBinfo['pwd'], host = insertDBinfo['host'], port = insertDBinfo['port'], db = "login_history")
    #
    # insertSQL = "INSERT INTO t_charge_count(ip_address, soft_data) VALUES (%s, %s)"
    # cursor.execute(sql, ("192.xxx.xx.xx", json.dumps(dic)))
    # cursor.commit()

def getJSONpdf(dataList):
    global step
    global minN
    global maxN

    maxL = int(max(dataList)+1)
    if maxL > maxN:
        maxN = maxL

    binsList = range(minN,maxN+step,step)
    count,bins = np.histogram(dataList,bins = binsList)
    pro = 1. * count/np.sum(count)
    pdfJSON = dict()
    pdfJSON['pro'] = list(pro)
    pdfJSON['bin'] = list(bins)

    return pdfJSON


def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row
def retrieve(numberOfDays):
    global epoch
    global lastEpochGtbu
    global lastEpochNon
    global querydbname
    global preDictGtbu
    global preDictNon

    mysqlNonGTBUinfo = getdbinfo("NON_GTBU")
    mysqlGTBUinfo = getdbinfo("GTBU")

    queryGTBU = MySQLdb.connect(user = mysqlGTBUinfo['usr'],passwd = mysqlGTBUinfo['pwd'], host = mysqlGTBUinfo['host'], port = mysqlGTBUinfo['port'], db = querydbname)
    queryNonGTBU =  MySQLdb.connect(user = mysqlNonGTBUinfo['usr'],passwd = mysqlNonGTBUinfo['pwd'], host = mysqlNonGTBUinfo['host'], port = mysqlNonGTBUinfo['port'], db = querydbname)

    realtimeQuery = """
    SELECT MAX(itemid)
    FROM t_chgflowlog001
    """

    pullQuery ="""
    SELECT uid,flowSize,businessSizeup,businessSizedown,sessionid FROM
    t_chgflowlog001
    WHERE itemid > %s AND itemid <=%s
    """
    retrieveQuery ="""
    SELECT imei,flowSize,up,down,beginTime,visitcountry FROM
    t_chgflowlog001
    WHERE itemid > %s AND itemid <=%s
    ORDER BY itemid DESC
    """

    myGTBUcursor = queryGTBU.cursor()
    myNonGTBUcursor = queryNonGTBU.cursor()

    logging.info("Fire mysql queries...")

    if lastEpochNon == 0:
        myGTBUcursor.execute(realtimeQuery)
        queryR = myGTBUcursor.fetchall()
        for row in queryR:
            currentMaxIDgtbu = row[0]

        myNonGTBUcursor.execute(realtimeQuery)
        queryR = myNonGTBUcursor.fetchall()
        for row in queryR:
            currentMaxIDnon = row[0]
    else:
        currentMaxIDgtbu = lastEpochGtbu
        currentMaxIDnon = lastEpochNon

    logging.info("mysql queries finished...")

    lastEpochGtbu = currentMaxIDgtbu - 5000
    lastEpochNon = currentMaxIDnon - 5000

    gtbuGap = currentMaxIDgtbu - lastEpochGtbu
    nonGap = currentMaxIDnon - lastEpochNon
    epoch +=1

    myGTBUcursor.execute(retrieveQuery,(lastEpochGtbu,currentMaxIDgtbu))
    myNonGTBUcursor.execute(retrieveQuery,(lastEpochNon,currentMaxIDnon))

    gtbuGenerator = fetchsome(myGTBUcursor,500)
    for row in gtbuGenerator:
        retrieveByDay(row,preDictGtbu,"GTBU")

    nonGenerator = fetchsome(myNonGTBUcursor,500)
    for row in nonGenerator:
        retrieveByDay(row,preDictNon,"NON-GTBU")

    queryGTBU.close()
    queryNonGTBU.close()

if __name__ == "__main__":
    # schedule.every(2).second.do(getTask1)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)

    while 1:
#        print "starts at:" + str( datetime.datetime.now())
        retrieve(10)
#        print "ends at:" + str( datetime.datetime.now())
#        print "...."
