#!/usr/bionpython
#coding:utf-8

import sys
import MySQLdb
import datetime
import time
import logging
from kits import getdbinfo

reload(sys)
sys.setdefaultencoding("utf-8")

gsvcQuery = """
SELECT plmn,operator,person_gsvc,COUNT(imsi),IFNULL(package_type,'unknown999')
FROM
(
	SELECT LEFT(imsi,5) AS plmn,person_gsvc,operator,imsi,package_type FROM vsim_manual_infor
	vsim_manual_infor
) AS z
GROUP BY plmn,operator,person_gsvc,package_type
"""
cssIMSIQuery = """
SELECT iso2,LEFT(plmn,5),
	SUM(
		CASE WHEN available_status = 0 THEN 1 ELSE 0 END
	) AS available,
	SUM(
		CASE WHEN occupy_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS inuse,
	SUM(
		CASE WHEN bam_status = 0 THEN 1 ELSE 0 END
	) AS bamok,
	SUM(
		CASE WHEN bam_status != 0 THEN 1 ELSE 0 END
	) AS bamerr,
	SUM(
		CASE WHEN business_status = 0 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_ok,
	SUM(
		CASE WHEN business_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_stop,
	SUM(
		CASE WHEN business_status = 2 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_goingstop,
	SUM(
		CASE WHEN business_status = 3 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_ineff,
	SUM(
		CASE WHEN business_status = 4 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_pending,
	SUM(
		CASE WHEN business_status = 5 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_test,
	SUM(
		CASE WHEN business_status = 6 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_goingdown,
	SUM(
		CASE WHEN business_status = 7 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_overlimitf,
	SUM(
		CASE WHEN activate_status = 0 THEN 1 ELSE 0 END
	) AS active_on,
	SUM(
		CASE WHEN activate_status = 1  AND bam_status = 0 THEN 1 ELSE 0 END
	) AS active_off,
	SUM(
		CASE WHEN slot_status = 0  AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_in,
	SUM(
		CASE WHEN slot_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_out,
	SUM(
		CASE WHEN slot_status = 2 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_err,
	'local' AS flag,
	IFNULL(package_type_name,'unknown') AS package_type_name
FROM
(
	SELECT imsi,mcc,iso2,plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status,package_type_name
	FROM
	(
	SELECT
		t1.imsi,t1.mcc,t4.iso2,plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status,t2.package_type_name
	FROM
		t_css_vsim AS t1
	LEFT JOIN t_css_vsim_packages AS t2 ON t1.imsi = t2.imsi
	LEFT JOIN t_css_v_pool_map AS t3 ON t1.id = t3.vsim_id
	LEFT JOIN t_css_mcc_country_map AS t4 ON t1.mcc = t4.mcc
	WHERE (LENGTH(t1.plmnset_id) < 10 OR t1.plmnset_id IS NULL)
	) AS tmp
	GROUP BY imsi,mcc,iso2,plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status
) AS u1
GROUP BY iso2,plmn,package_type_name
"""
cssPckQuery = """SELECT package_type_name,LEFT(plmn,5),
	SUM(
		CASE WHEN available_status = 0 THEN 1 ELSE 0 END
	) AS available,
	SUM(
		CASE WHEN occupy_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS inuse,
	SUM(
		CASE WHEN bam_status = 0 THEN 1 ELSE 0 END
	) AS bamok,
	SUM(
		CASE WHEN bam_status != 0 THEN 1 ELSE 0 END
	) AS bamerr,
	SUM(
		CASE WHEN business_status = 0 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_ok,
	SUM(
		CASE WHEN business_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_stop,
	SUM(
		CASE WHEN business_status = 2 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_goingstop,
	SUM(
		CASE WHEN business_status = 3 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_ineff,
	SUM(
		CASE WHEN business_status = 4 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_pending,
	SUM(
		CASE WHEN business_status = 5 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_test,
	SUM(
		CASE WHEN business_status = 6 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_goingdown,
	SUM(
		CASE WHEN business_status = 7 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS business_overlimitf,
	SUM(
		CASE WHEN activate_status = 0 THEN 1 ELSE 0 END
	) AS active_on,
	SUM(
		CASE WHEN activate_status = 1  AND bam_status = 0 THEN 1 ELSE 0 END
	) AS active_off,
	SUM(
		CASE WHEN slot_status = 0  AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_in,
	SUM(
		CASE WHEN slot_status = 1 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_out,
	SUM(
		CASE WHEN slot_status = 2 AND bam_status = 0 THEN 1 ELSE 0 END
	) AS slot_err,
	flag,
	package_type_name
	FROM
	(

		SELECT imsi,package_type_name,
		CASE
			WHEN localflag IS NOT NULL THEN localiso2
			WHEN muliso2 IS NOT NULL THEN muliso2
		ELSE NULL END AS 'iso2',
		CASE
			WHEN localflag IS NOT NULL THEN 'local'
			WHEN muliso2 IS NOT NULL THEN 'multi'
		ELSE 'wrong' END AS 'flag',
		plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status
		FROM
		(
			SELECT
			u1.imsi,u1.package_type_name,u1.plmnset_id,u1.localflag,u2.iso2 AS 'localiso2',u3.iso2 AS 'muliso2',
			plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status
			FROM
			(
				SELECT t1.imsi,t1.package_type_name,t2.plmnset_id,
				plmn,bam_status,business_status,activate_status,slot_status,available_status,occupy_status,
				CASE WHEN LENGTH(t2.plmnset_id) > 10 THEN NULL
				ELSE t2.mcc END AS 'localflag'
				FROM
				t_css_vsim_packages AS t1
				LEFT JOIN
				t_css_vsim AS t2
				ON t1.imsi = t2.imsi
				WHERE
					package_type_name NOT LIKE "%140%"
					AND t2.bam_status != 1
			) AS u1
			LEFT JOIN
			t_css_mcc_country_map AS u2
			ON u1.localflag = u2.mcc
			LEFT JOIN
			(
				SELECT plmnset_id,iso2
				FROM
				t_css_plmn AS u2
				GROUP BY plmnset_id,iso2
			) AS u3
			ON u1.plmnset_id = u3.plmnset_id
		) AS v
	) AS w
	GROUP BY package_type_name,plmn
	HAVING flag = 'multi'
"""
def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname,
                                charset = 'utf8')
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

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def getGSVC():
    global gsvcQuery
    gsvcPckDict = dict()
    gsvcPlmnDict = dict()

    remoteCon,remoteCur = getMysqlCon("GSVC","gsvcdatabase")
    remoteCur.execute(gsvcQuery)
    gsvclist = remoteCur.fetchall()
    remoteCon.close()

    for row in gsvclist:
        pck = row[-1]
        plmn = row[0]

        gsvcPckDict[pck] = row
        gsvcPlmnDict[plmn] = row

    return (gsvcPckDict,gsvcPlmnDict)

def getCssIMSI():
    global cssIMSIQuery
    remoteCon,remoteCur = getMysqlCon("ASS_GTBU","glocalme_css")
    remoteCur.execute(cssIMSIQuery)
    cssimsilist = remoteCur.fetchall()

    remoteCon.close()
    return cssimsilist

def getCssPck():
    global cssPckQuery
    remoteCon,remoteCur = getMysqlCon("ASS_GTBU","glocalme_css")
    remoteCur.execute(cssPckQuery)
    csspcklist = remoteCur.fetchall()

    remoteCon.close()
    return csspcklist

def mergeList(gsvcPckDict,gsvcPlmnDict,cssimsilist,csspcklist):
    mergelist = list()

    for row in cssimsilist:
        pck = row[-1]
        plmn = row[1]

        gsvcPck = gsvcPckDict.get(pck,list())
        gsvcPlmn = gsvcPlmnDict.get(plmn,list())

        if len(gsvcPck) == 0 and len(gsvcPlmn) == 0:
            operator = "unknown"
            person = "unknown"
            maintain = 99999
            pckname = "unknown"

        else:
            operator = gsvcPlmn[1]
            person = gsvcPlmn[2]
            maintain = gsvcPlmn[3]

        if  len(gsvcPck) == 0 and len(gsvcPlmn) != 0:
            pckname = gsvcPlmn[4]

        if len(gsvcPck) != 0:
            pckname = pck

        tmpres = list(row)[:-1]+ [operator,person,maintain,pckname]
        mergelist.append(tmpres)

    for row in csspcklist:
        pck = row[-1]
        plmn = row[1]

        gsvcPck = gsvcPckDict.get(pck,list())
        gsvcPlmn = gsvcPlmnDict.get(plmn,list())

        if len(gsvcPck) == 0 and len(gsvcPlmn) == 0:
            operator = "unknown"
            person = "unknown"
            maintain = 99999
            pckname = "unknown"

        else:
            operator = gsvcPlmn[1]
            person = gsvcPlmn[2]
            maintain = gsvcPlmn[3]

        if  len(gsvcPck) == 0 and len(gsvcPlmn) != 0:
            pckname = gsvcPlmn[4]

        if len(gsvcPck) != 0:
            pckname = pck

        tmpres = list(row)[:-1]+ [operator,person,maintain,pckname]
        mergelist.append(tmpres)

    return mergelist

def partnerMonitor():
    gsvcPckDict,gsvcPlmnDict = getGSVC()
    cssimsilist = getCssIMSI()
    csspcklist = getCssPck()

    mergelist = mergeList(gsvcPckDict,gsvcPlmnDict,cssimsilist,csspcklist)

    print len(mergelist)

    submit2DB(mergelist)
    print mergelist

def submit2DB(mergelist):
    today = datetime.datetime.now().date()
    remoteCon,remoteCur = getMysqlCon("REMOTE","login_history")
    for row in mergelist:
        remoteCur.execute('''insert into tmp_iso_plmn(isopck,plmn,available,inusing,bamok,bamerr,business_ok,business_stop,business_goingstop,business_ineff,business_pending,
 business_test,business_goingdown,business_overlimitf,active_on,active_off,slot_in,slot_out,slot_err,flag,operator,person_gsvc,cnt,pckname,recorddate)
 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',tuple(row)+(today,))
    remoteCon.commit()
    remoteCon.close()

if __name__ == "__main__":
    # schedule.every(2).second.do(getTask1)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
    partnerMonitor()

    # while 1:
    #     print "starts at:" + str( datetime.datetime.now())
    #     getTask1()
    #     print "ends at:" + str( datetime.datetime.now())
    #     print "...."
    #     time.sleep(10)
