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

        key = str(country)+str(parent)

        tmpList = preDict.get(key,[country,parent,0,0,0,0])
        if type == -99:
            preDict[key]=[tmpList[0],tmpList[1],tmpList[2],tmpList[3],tmpList[4]+up,tmpList[5]+down]
        else:
            preDict[key]=[tmpList[0],tmpList[1],tmpList[2]+up,tmpList[3]+down,tmpList[4],tmpList[5]]

def serilizeResult():
    global preDict
    global serilizeDate

    #this is the begin of test part
    print preDict
    #this is the end of test part

    nowdate = datetime.datetime.now().strftime("%d")
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")

    for key,val in preDict.iteritems():
        remoteCur.execute('''insert into t_flowsummary_partner(epochTime,iso2,partner,reserveup,reservedown,freeup,freedown)
            values(%s,%s,%s,%s,%s,%s,%s)''',
        (serilizeDate,val[0],val[1],val[2],val[3],val[4],val[5]))

    remoteCon.commit()
    remoteCon.close()

    preDict.clear()
    serilizeDate = datetime.datetime.now().date()

def queryandinsert():
    global lastMax
    global today

    nowdate = datetime.datetime.now().strftime("%d")
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    query="""
    SELECT fValue FROM t_loginhistory_dic WHERE ftype = 'partner'
    """
    remoteCur.execute(query)
    result = remoteCur.fetchall()
    parentlist = list()

    for row in result:
        parentlist.append(row[0])

    remoteCon.close()

    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")
    queryNonMax="""
    SELECT MAX(itemid) FROM
    t_chgflowlog001    """

    queryNonCurrent="""
    SELECT vsimimsi AS imsi,up,down FROM
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

    curflowDict = dict()
    nonCur.execute(queryNonCurrent,(lastMax,curMax))
    r = nonCur.fetchall()
    for row in r:
        curflowDict[row[0]]=[row[1],row[2]]

    nonCon.close()

    for parent in parentlist:
        accumulate(parent,curflowDict)
    lastMax=curMax


# # this is the beging of test part
#     serilizeResult()
# # this is the end of test part

    if today == nowdate:
        return
    else:
        serilizeResult()

        today = nowdate

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

