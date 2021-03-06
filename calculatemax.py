#coding:utf-8
import sys
import MySQLdb
import schedule, time
import datetime
from datetime import timedelta
from kits import getdbinfo
import copy

reload(sys)
sys.setdefaultencoding('utf-8')

presisDict = dict()
counter = 1
def accumulator(row,recordDict):
    global counter
    start = row[0]
    interval = row[2]+1
    keylist = []
    for i in range(0,interval):
        keylist.append(start+timedelta(days = i))

    pck = row[4].encode('utf-8')

    for key in keylist:
        detail = recordDict.get(key,{"packages":[],"max":[]})
        pckList = detail.get("packages")
        maxList = detail.get("max")

        if pck not in pckList:
            pckList.append(pck)
            maxList.append(1)
        else:
            i = pckList.index(pck)
            maxList[i] +=1

        recordDict[key] = detail
    counter +=1


def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def queryandinsert():


    starttime = datetime.datetime.now()
    global presisDict

    print len(presisDict)
    print "connect to databae!"

    # connect to the database use my own toolkits
    querydbinfo = getdbinfo('OMS')
    querydbname = "wifi_data"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    # print the database information for verification
    for key, value in querydbinfo.iteritems():
        print key + " : " + str(value)

    querystatement = """
    SELECT DATE(date_goabroad),DATE(date_repatriate),DATEDIFF(date_repatriate,date_goabroad),imei,package_name FROM tbl_order_basic
    WHERE
	  data_status = 0 						AND
	(
		DATE(date_goabroad) BETWEEN DATE(NOW()) AND DATE_ADD(NOW(),INTERVAL 3 MONTH) OR
		(
		DATE(date_goabroad) < DATE(NOW()) AND DATE(date_repatriate) >= DATE(NOW())
		)
	)
    """
    querydb = MySQLdb.connect(user = querydbinfo['usr'],passwd = querydbinfo['pwd'], host = querydbinfo['host'], port = querydbinfo['port'], db = querydbname,charset='utf8')
    insertdb = MySQLdb.connect(user = insertdbinfo['usr'],passwd = insertdbinfo['pwd'], host = insertdbinfo['host'], port = insertdbinfo['port'], db = insertdbname,charset='utf8')
    queryCur = querydb.cursor()
    insertCur = insertdb.cursor()

    print "executing query!!!"
    queryCur.execute(querystatement)
    omsGenerator = fetchsome(queryCur,5000)

    print " fetching all record in mysql"

    for row in omsGenerator:
        accumulator(row,presisDict)

    insertCur.execute("delete from t_maxcalculate")
    insertdb.commit()

    insertStart = datetime.datetime.now()
    n = datetime.datetime.now()
    for key,detail in presisDict.iteritems():
        pckList = detail.get("packages")
        maxList = detail.get("max")

        for index,pckage in enumerate(pckList):
            now = n
            date = key
            pck = pckage.encode('utf-8')
            max = maxList[index]

            print "package is: " + str(pck)
            insertCur.execute('''insert into t_maxcalculate(recordtime,date,pck,max) values(%s,%s,%s,%s)''',(now,date,pck,max))

    insertend = datetime.datetime.now()

    insertdb.commit()
    insertdb.close()
    querydb.commit()
    querydb.close()
    finishtime = datetime.datetime.now()

    print "insert start at: "+str(insertStart)+ " and finish at: " + str(insertend)
    print "start at: " +str(starttime)+" and finish at: "+str(finishtime)
if __name__=="__main__":
    queryandinsert()
    schedule.every().day.do(queryandinsert)
    while True:
        schedule.run_pending()
        time.sleep(1)

