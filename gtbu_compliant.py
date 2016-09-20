#!/usr/bionpython
#coding:utf-8
import MySQLdb
import datetime
import schedule, time
from kits import getdbinfo

lastEpochGtbu = 0
lastEpochNon = 0
epoch =0
lastDay = 0
querydbname = "ucloudplatform"

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def getTask1():
    global epoch
    global lastEpochGtbu
    global lastEpochNon
    global querydbname
    global preDictGtbu
    global preDictNon
    global lastDay

    querydbinfoGTBU = getdbinfo("REMOTE")
    querydbnameGTBU = "carecookies"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    # queryGTBU = MySQLdb.connect(user = mysqlGTBUinfo['usr'],passwd = mysqlGTBUinfo['pwd'], host = mysqlGTBUinfo['host'], port = mysqlGTBUinfo['port'], db = 'carecookies')
    # queryNonGTBU =  MySQLdb.connect(user = mysqlNonGTBUinfo['usr'],passwd = mysqlNonGTBUinfo['pwd'], host = mysqlNonGTBUinfo['host'], port = mysqlNonGTBUinfo['port'], db = querydbname)

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    insertCur = insertdb.cursor()

    realtimeQuery = """
    SELECT MAX(id)
    FROM
    work_order
    """

    # pullQuery ="""
    # SELECT mcc,problem_type,COUNT(1)
    # FROM
    # (
    # SELECT id,user_code,imei,create_time,mcc,sys_version,problem_type
    # FROM
    # work_order
    # WHERE
	 #    id BETWEEN %s AND %s
	 #    AND
	 #    (
		#     fault_type IS NOT NULL OR
		#     js_person IS NOT NULL OR
		#     problem_type IS NOT NULL OR
		#     kf_occur_scene = 009
	 #    )
	 #    AND problem_type != 05
    # ) AS z
    # GROUP BY mcc,problem_type
    # """
    pullQuery ="""
    SELECT mcc,problem_type,COUNT(1)
    FROM
    (
    SELECT id,user_code,imei,create_time,mcc,sys_version,problem_type
    FROM
    work_order
    WHERE
	    id BETWEEN %s AND %s
	) as z
    GROUP BY mcc,problem_type
    """
    queryCurGTBU.execute(realtimeQuery)
    queryR = queryCurGTBU.fetchall()
    for row in queryR:
        currentMaxIDgtbu = row[0]

    today = datetime.datetime.now().date()
    if lastDay != today:
        lastDay = today
        lastEpochGtbu = currentMaxIDgtbu
        return

    gtbuGap = currentMaxIDgtbu - lastEpochGtbu

    queryCurGTBU.execute(pullQuery,(lastEpochGtbu,currentMaxIDgtbu))

    queryGtbu = queryCurGTBU.fetchall()
    print ">>>>>"
    print lastEpochGtbu
    print currentMaxIDgtbu
    print len(queryGtbu)

    print gtbuGap

    print " ....epoch "+ str(epoch)+" has  been finished!"
    print ""

    recordtime = datetime.datetime.now()

    for row in queryGtbu:
        mcc = row[0]
        problem_type = row[1]
        cnt = row[2]
        insertCur.execute('''insert into t_compliant_realtime(recordtime,mcc,problem_type,cnt,belongs) values(%s,%s,%s,%s,%s)''',
                             (recordtime,mcc,problem_type,cnt,'GTBU'))

    querydbGTBU.close()
    insertdb.commit()
    insertdb.close()

    epoch +=1

if __name__ == "__main__":
    schedule.every(30).minutes.do(getTask1)
    getTask1()
    while 1:
        schedule.run_pending()
        time.sleep(1)
