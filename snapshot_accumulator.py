import MySQLdb
import schedule, time
import datetime
import json
from kits import getdbinfo
import numpy as np

accSet = set()

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def queryandinsert():
    global accSet

    querydbinfoGTBU = getdbinfo("ASS_GTBU")
    querydbnameGTBU = "glocalme_ass"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    insertCur = insertdb.cursor()

    pullQuery ="""
    SELECT imei,sessionid
    FROM
    t_usmguserloginonline
    """

    queryCurGTBU.execute(pullQuery)
    onlineSetGenerator = fetchsome(queryCurGTBU,3000)
    for row in onlineSetGenerator:
        imei = str(row[0])
        sessionid = row[1]
        accSet.add(imei)

    print len(accSet)

    insertCur.execute("delete from t_newacc")
    insertdb.commit()

    recordtime = datetime.datetime.now()
    accNum = len(accSet)

    print accNum

    insertCur.execute('''insert into t_newacc(recordtime,accNum) values(%s,%s)''',(recordtime,accNum))

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()

def initialAdjustDict():
    global accSet

    querydbinfoGTBU = getdbinfo("ASS_GTBU")
    querydbnameGTBU = "glocalme_ass"

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    queryCurGTBU = querydbGTBU.cursor()

    pullQuery ="""
    SELECT imei
    FROM
    t_usmguserloginlog
    GROUP BY imei
    """
    queryCurGTBU.execute(pullQuery)
    onlineSetGenerator = fetchsome(queryCurGTBU,3000)
    for row in onlineSetGenerator:
        imei = str(row[0])
        accSet.add(imei)

if __name__=="__main__":
    # initialAdjustDict()
    # queryandinsert()
    # schedule.every(1).minutes.do(queryandinsert)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

    initialAdjustDict()
    while True:
        queryandinsert()
        time.sleep(5)