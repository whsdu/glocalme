
import os
import MySQLdb
from xlsxwriter.workbook import Workbook
import datetime
import schedule
import time
import sys
import json
import xlrd
from kits import getdbinfo

floderName =""
fileRoot = "ABmonitor"

def createDailyFloder():
    global floderName
    global fileRoot

    y = datetime.datetime.now()
    dateStr = datetime.datetime.strftime(y,'%Y-%m-%d')
    tmpFloderName ="/ukl/apache-tomcat-7.0.67/webapps/"+fileRoot

    if not os.path.exists(tmpFloderName):
        os.makedirs(tmpFloderName)

    floderName = tmpFloderName

def check_abuser():
    global floderName
    global fileRoot
    #
    # reload(sys)
    # sys.setdefaultencoding('utf-8')

    mysqlNonGTBUinfo = getdbinfo("NON_GTBU")
    mysqlGTBUinfo = getdbinfo("GTBU")

    querydbname = "ucloudplatform"
    print "Initiating connections to mysql..."
    queryGTBU = MySQLdb.connect(user = mysqlGTBUinfo['usr'],
                                passwd = mysqlGTBUinfo['pwd'],
                                host = mysqlGTBUinfo['host'],
                                port = mysqlGTBUinfo['port'],
                                db = querydbname)

    queryNonGTBU =  MySQLdb.connect(user = mysqlNonGTBUinfo['usr'],
                                    passwd = mysqlNonGTBUinfo['pwd'],
                                    host = mysqlNonGTBUinfo['host'],
                                    port = mysqlNonGTBUinfo['port'],
                                    db = querydbname)

    print "MySQL connection established.."

    queryABconsume = """
    SELECT z1.*,DATE(NOW()) AS recordtime,z2.code AS usercode,z2.name AS username,z3.code AS parentid,z3.name AS parentname
    FROM
    (
    SELECT uid,SUM(money) totalcost,MIN(logindatetime) AS starttime,SUM(flowsize) totalflowsize,SUM(up) totalup,
	SUM(down) AS totaldown,SUM(businessSizeup) AS totalbusinessup,SUM(businessSizedown) AS totalbusinessdown,
	COUNT(sessionid) AS sessioncounter,
	GROUP_CONCAT(CONCAT(visitcountry,":",TRUNCATE (money,2))
			ORDER BY logindatetime ASC
			SEPARATOR '--> ') AS summary
    FROM
    (
    SELECT uid,logindatetime,logoutdatetime,sessionid,visitcountry,flowsize,up,down,businessSizeup,businessSizedown,money
    FROM t_usmguserloginlog
    WHERE DATE(logindatetime) BETWEEN DATE_SUB(DATE(NOW()),INTERVAL 3 DAY) AND DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    UNION ALL
    SELECT uid,logindatetime,logoutdatetime,sessionid,visitcountry,flowsize,up,down,businessSizeup,businessSizedown,money
    FROM t_usmguserloginonline
    WHERE DATE(logindatetime) BETWEEN DATE_SUB(DATE(NOW()),INTERVAL 3 DAY) AND DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    ) AS z
    GROUP BY uid
    HAVING totalcost > 100
    ) AS z1
    LEFT JOIN
    t_usmguser AS z2
    ON z1.uid = z2.uid
    LEFT JOIN
    t_usmguser_parent AS z3
    ON z2.parentid = z3.uid
    """

    queryABbalance = """
    SELECT z1.*,z2.code AS usercode,z2.name AS username,z3.code AS parentid,z3.name AS parentname
    FROM
    (
    SELECT uid,amount FROM
    t_usmguseraccount
    WHERE amount > 5000
    ) AS z1
    LEFT JOIN
    t_usmguser AS z2
    ON z1.uid = z2.uid
    LEFT JOIN
    t_usmguser_parent AS z3
    ON z2.parentid = z3.uid
    """

    queryABtopup = """
    SELECT z1.*,DATE(NOW()) AS recordtime,z2.code AS usercode,z2.name AS username,z3.code AS parentid,z3.name AS parentname
    FROM
    (
    SELECT uid,DATE(DATE) AS DATETIME,SUM(amount) totaltopup,COUNT(1) AS topupcounter
    FROM
    (
    SELECT uid,amount,TYPE,packageid,paytype,DATE
    FROM t_usmgrecharge
    WHERE state = 2
    	and packageid is null
    	AND
    	DATE(DATE) BETWEEN DATE_SUB(DATE(NOW()),INTERVAL 3 DAY) AND DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    ) AS z
    GROUP BY uid,DATE(DATE)
    HAVING totaltopup > 500
    ) AS z1
    LEFT JOIN
    t_usmguser AS z2
    ON z1.uid = z2.uid
    LEFT JOIN
    t_usmguser_parent AS z3
    ON z2.parentid = z3.uid
    """
    productCursor = queryNonGTBU.cursor()

    print "Fire mysql queries..."


    y = datetime.datetime.now()
    dateStr = datetime.datetime.strftime(y,'%Y-%m-%d_%H_%M')
    filename = fileRoot + "_"+dateStr+".xlsx"

    workbook = Workbook(floderName+"/"+filename)
    #


    sheet1 = workbook.add_worksheet('abnormal PAYG')
    sheet2 = workbook.add_worksheet('abnormal topup')
    sheet3 = workbook.add_worksheet('abnormal balance')

    productCursor.execute(queryABconsume)
    consumeResult = productCursor.fetchall()

    sheet1.write(0,0,'recordtime')
    sheet1.write(0,1,'uid')
    sheet1.write(0,2,'user code')
    sheet1.write(0,3,'user name')
    sheet1.write(0,4,'parent code')
    sheet1.write(0,5,'parent name')
    # sheet1.write(0,6,u'总消费'.encode('utf-8'))
    # sheet1.write(0,7,u'起始时间'.encode('utf-8'))
    # sheet1.write(0,8,u'总流量'.encode('utf-8'))
    # sheet1.write(0,9,u'总上行'.encode('utf-8'))
    # sheet1.write(0,10,u'总下行'.encode('utf-8'))
    # sheet1.write(0,11,u'总业务上行'.encode('utf-8'))
    # sheet1.write(0,12,u'总业务下行'.encode('utf-8'))
    # sheet1.write(0,13,u'session数'.encode('utf-8'))
    # sheet1.write(0,14,u'简述'.encode('utf-8'))

    r=1
    for row in consumeResult:
        sheet1.write(r,1,row[0])
        # sheet1.write(r,6,row[1])
        # sheet1.write(r,7,row[2])
        # sheet1.write(r,8,row[3])
        # sheet1.write(r,9,row[4])
        # sheet1.write(r,10,row[5])
        # sheet1.write(r,11,row[6])
        # sheet1.write(r,12,row[7])
        # sheet1.write(r,13,row[8])
        # sheet1.write(r,14,row[9])

        sheet1.write(r,0,row[10])
        sheet1.write(r,1,row[11])
        sheet1.write(r,2,row[12])
        sheet1.write(r,3,row[13])
        sheet1.write(r,4,row[14])

        r+=1



    productCursor.execute(queryABtopup)
    topupResult = productCursor.fetchall()

    sheet2.write(0,0,'uid')
    # sheet2.write(0,1,u'异常发生日'.encode('utf-8'))
    # sheet2.write(0,2,u'总充值金额'.encode('utf-8'))
    # sheet2.write(0,3,u'充值次数'.encode('utf-8'))
    # sheet2.write(0,4,u'记录时间'.encode('utf-8'))
    sheet2.write(0,5,'user code')
    sheet2.write(0,6,'user name')
    sheet2.write(0,7,'parent code')
    sheet2.write(0,8,'parent name')

    r=1
    for row in topupResult:
        sheet2.write(r,0,row[0])
        # sheet2.write(r,1,row[1])
        # sheet2.write(r,2,row[2])
        # sheet2.write(r,3,row[3])
        # sheet2.write(r,4,row[4])
        sheet2.write(r,5,row[5])
        sheet2.write(r,6,row[6])
        sheet2.write(r,7,row[7])
        sheet2.write(r,8,row[8])

        r+=1

    productCursor.execute(queryABbalance)
    balanceResult = productCursor.fetchall()

    sheet3.write(0,0,'uid')
    # sheet3.write(0,1,u'余额'.encode('utf-8'))
    # sheet3.write(0,2,u'记录时间'.encode('utf-8'))
    sheet3.write(0,3,'user code')
    sheet3.write(0,4,'user name')
    sheet3.write(0,5,'parent code')
    sheet3.write(0,6,'parent name')

    r=1
    for row in balanceResult:
        sheet3.write(r,0,row[0])
        # sheet3.write(r,1,row[1])
        # sheet3.write(r,2,row[2])
        sheet3.write(r,3,row[3])
        sheet3.write(r,4,row[4])
        sheet3.write(r,5,row[5])
        sheet3.write(r,6,row[6])

    print "mysql queries finished..."

    workbook.close()

if __name__ == "__main__":
    createDailyFloder()
    check_abuser()

    schedule.every().day.at("01:10").do(check_abuser)
    schedule.every().day.at("00:02").do(createDailyFloder)
    while 1:
        schedule.run_pending()
        time.sleep(1)
