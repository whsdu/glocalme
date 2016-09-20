# -*- coding: utf-8 -*-
import sys
import MySQLdb
import schedule, time
import datetime
from kits import getdbinfo
from pymongo import MongoClient
import copy
import numpy as np

gtbuquery="""
SELECT v1.iso2,IFNULL(v1.total,0)+IFNULL(v2.mtotal,0) AS TOTAL,IFNULL(v1.inuse,0)+IFNULL(v2.minuse,0) AS online
FROM
(
SELECT u1.iso2,SUM(u2.total) AS total,SUM(u2.inuse) AS inuse
FROM
t_css_mcc_country_map AS u1
LEFT JOIN
(
SELECT
	t1.mcc,
	SUM(1) AS total,
	SUM(t1.occupy_status) AS inuse
FROM
	t_css_vsim t1
LEFT JOIN t_css_vsim_packages t2 ON t1.imsi = t2.imsi
LEFT JOIN t_css_v_pool_map AS t3 ON t1.id = t3.vsim_id
WHERE
	t1.available_status = 0
	AND t1.business_status IN (0,4,5)
	AND (LENGTH(t1.plmnset_id) < 10 OR t1.plmnset_id IS NULL)
	AND t2.package_status IN (0,1)
	AND t2.expire_time >= NOW()
	AND t3.pool_id IS NOT NULL
	AND t1.expire_time >= NOW()
	AND t1.validity_period IS NOT NULL
GROUP BY
	t1.mcc
) AS u2
ON u1.mcc = u2.mcc
GROUP BY iso2
) AS v1

LEFT JOIN
(

SELECT
	iso2,
	SUM(1) AS mtotal,
	SUM(occupy_status) AS minuse
FROM
(
	SELECT * FROM
	(
	SELECT u1.imsi,u1.business_status,u1.occupy_status,u2.iso2
	FROM
	(
	SELECT t1.imsi,t1.rat,t1.plmnset_id,t1.business_status,t1.occupy_status
	FROM
		t_css_vsim AS t1
		LEFT JOIN t_css_vsim_packages AS t2 ON t1.imsi = t2.imsi
		LEFT JOIN t_css_plmnset AS t4 ON t1.plmnset_id = t4.id
		LEFT JOIN t_css_v_pool_map AS t3 ON t1.id = t3.vsim_id
	WHERE
		t1.available_status = 0
		AND t1.business_status  IN (0,4,5)
		AND t4.id IS NOT NULL
		AND t2.package_status = 0
		AND t2.expire_time >= NOW()
		AND t3.pool_id IS NOT NULL
		AND t1.expire_time >= NOW()
		AND t1.validity_period IS NOT NULL
	) AS u1
	LEFT JOIN
	t_css_plmn AS u2
	ON u1.plmnset_id = u2.plmnset_id
	) AS tz
	GROUP BY imsi,business_status,occupy_status,iso2
) AS zz
GROUP BY iso2
) AS v2
ON v1.iso2 = v2.iso2
    """

nonquery = """
			SELECT iso2,
					total+pending+pendinginuse+multitotal+mulpending+mpendinginuse AS total,
					inuse+pendinginuse+minuse+mpendinginuse AS inuse
				FROM
				(
				SELECT
					country_code2 AS iso2,
					country,
					IFNULL(SUM(available),0) AS available,
					IFNULL(SUM(total),0) AS total,
					IFNULL(SUM(pending),0) AS pending,
					IFNULL(SUM(inuse),0) AS inuse,
					IFNULL(SUM(pendinginuse),0) AS pendinginuse,
					IFNULL(SUM(multicountries),0) AS multicountries,
					IFNULL(SUM(multitotal),0) AS multitotal,
					IFNULL(SUM(mulpending),0) AS mulpending,
					IFNULL(SUM(minuse),0) AS minuse,
					IFNULL(SUM(mpendinginuse),0) AS mpendinginuse
				FROM
					(
						SELECT
							b1.country_code2,
							b1.country,
							b2.available,
							b2.total,
							b2.pending,
							b2.inuse,
							b2.pendinginuse,
							b1.mcc
						FROM
							(
								SELECT
									a1.iso2 AS country_code2,
									a1.country,
									a2.mcc
								FROM
									t_diccountries AS a1
								LEFT JOIN t_dicmcc AS a2 ON a2.country_code2 = a1.iso2
								WHERE
									a1.isshow = 1
							) AS b1
						LEFT JOIN (
							SELECT
								mcc,
								COUNT(*) summaryall,
								SUM(
									CASE
									WHEN state = '00000' THEN
										1
									ELSE
										0
									END
								) available,
								SUM(
									CASE
									WHEN state = '10000' THEN
										1
									WHEN state = '00000' THEN
										1
									ELSE
										0
									END
								) total,
								SUM(
									CASE
									WHEN state = '00400' THEN
										1
									ELSE
										0
									END
								) pending,
								SUM(
									CASE
									WHEN state = '10000' THEN
										1
									ELSE
										0
									END
								) AS inuse,
								SUM(
									CASE
									WHEN state = '10400' THEN
										1
									ELSE
										0
									END
								) AS pendinginuse
							FROM
								t_resvsim AS f1
							WHERE
								NOT EXISTS (
									SELECT
										NULL
									FROM
										(
											SELECT
												sourceid AS imsi
											FROM
												t_resvsimowner
											UNION
												SELECT
													imsi
												FROM
													t_resvsimbinduser
												UNION
													SELECT
														imsi
													FROM
														t_resimsigid
										) AS tt
									WHERE
										f1.imsi = tt.imsi
								)
							AND nouselocal != 1
							GROUP BY
								mcc
						) AS b2 ON b1.mcc = b2.mcc
					) AS c1
				LEFT JOIN (
					SELECT
						g0.mcc,
						multicountries,
						multitotal,
						mulpending,
						minuse,
						mpendinginuse
					FROM
						(
							SELECT
								a1.iso2 AS country_code2,
								a1.country,
								a2.mcc
							FROM
								t_diccountries AS a1
							LEFT JOIN t_dicmcc AS a2 ON a2.country_code2 = a1.iso2
							WHERE
								a1.isshow = 1
						) AS g0
					LEFT JOIN (
						SELECT
							f2.mcc,
							COUNT(DISTINCT f1.imsi) AS multicountries
						FROM
							t_resimsigid AS f1
						LEFT JOIN (
							SELECT
								t2.gid,
								t2.eplmn,
								t1.mcc
							FROM
								t_dicmcc AS t1
							LEFT JOIN t_resgidplmn AS t2 ON POSITION(t1.mcc IN t2.eplmn) = 1
						) AS f2 ON f1.gid = f2.gid
						WHERE
							f1.imsi IN (
								SELECT
									f1.imsi
								FROM
									t_resvsim AS f1
								WHERE
									f1.imsi IN (SELECT imsi FROM t_resimsigid)
								AND f1.imsi NOT IN (
									SELECT
										sourceid AS imsi
									FROM
										t_resvsimowner
									UNION
										SELECT
											imsi
										FROM
											t_resvsimbinduser
								)
								AND f1.state = '00000'
							)
						GROUP BY
							f2.mcc
					) AS g1 ON g0.mcc = g1.mcc
					LEFT JOIN (
						SELECT
							f2.mcc,
							COUNT(DISTINCT f1.imsi) AS multitotal
						FROM
							t_resimsigid AS f1
						LEFT JOIN (
							SELECT
								t2.gid,
								t2.eplmn,
								t1.mcc
							FROM
								t_dicmcc AS t1
							LEFT JOIN t_resgidplmn AS t2 ON POSITION(t1.mcc IN t2.eplmn) = 1
						) AS f2 ON f1.gid = f2.gid
						WHERE
							f1.imsi IN (
								SELECT
									f1.imsi
								FROM
									t_resvsim AS f1
								WHERE
									f1.imsi IN (SELECT imsi FROM t_resimsigid)
								AND f1.imsi NOT IN (
									SELECT
										sourceid AS imsi
									FROM
										t_resvsimowner
									UNION
										SELECT
											imsi
										FROM
											t_resvsimbinduser
								)
								AND (
									f1.state = '00000'
									OR f1.state = '10000'
								)
							)
						GROUP BY
							f2.mcc
					) AS g2 ON g0.mcc = g2.mcc
					LEFT JOIN (
						SELECT
							f2.mcc,
							COUNT(DISTINCT f1.imsi) AS minuse
						FROM
							t_resimsigid AS f1
						LEFT JOIN (
							SELECT
								t2.gid,
								t2.eplmn,
								t1.mcc
							FROM
								t_dicmcc AS t1
							LEFT JOIN t_resgidplmn AS t2 ON POSITION(t1.mcc IN t2.eplmn) = 1
						) AS f2 ON f1.gid = f2.gid
						WHERE
							f1.imsi IN (
								SELECT
									f1.imsi
								FROM
									t_resvsim AS f1
								WHERE
									f1.imsi IN (SELECT imsi FROM t_resimsigid)
								AND f1.imsi NOT IN (
									SELECT
										sourceid AS imsi
									FROM
										t_resvsimowner
									UNION
										SELECT
											imsi
										FROM
											t_resvsimbinduser
								)
								AND (f1.state = '10000')
							)
						GROUP BY
							f2.mcc
					) AS g4 ON g0.mcc = g4.mcc
					LEFT JOIN (
						SELECT
							f2.mcc,
							COUNT(DISTINCT f1.imsi) AS mpendinginuse
						FROM
							t_resimsigid AS f1
						LEFT JOIN (
							SELECT
								t2.gid,
								t2.eplmn,
								t1.mcc
							FROM
								t_dicmcc AS t1
							LEFT JOIN t_resgidplmn AS t2 ON POSITION(t1.mcc IN t2.eplmn) = 1
						) AS f2 ON f1.gid = f2.gid
						WHERE
							f1.imsi IN (
								SELECT
									f1.imsi
								FROM
									t_resvsim AS f1
								WHERE
									f1.imsi IN (SELECT imsi FROM t_resimsigid)
								AND f1.imsi NOT IN (
									SELECT
										sourceid AS imsi
									FROM
										t_resvsimowner
									UNION
										SELECT
											imsi
										FROM
											t_resvsimbinduser
								)
								AND (f1.state = '10400')
							)
						GROUP BY
							f2.mcc
					) AS g5 ON g0.mcc = g5.mcc
					LEFT JOIN (
						SELECT
							f2.mcc,
							COUNT(DISTINCT f1.imsi) AS mulpending
						FROM
							t_resimsigid AS f1
						LEFT JOIN (
							SELECT
								t2.gid,
								t2.eplmn,
								t1.mcc
							FROM
								t_dicmcc AS t1
							LEFT JOIN t_resgidplmn AS t2 ON POSITION(t1.mcc IN t2.eplmn) = 1
						) AS f2 ON f1.gid = f2.gid
						WHERE
							f1.imsi IN (
								SELECT
									f1.imsi
								FROM
									t_resvsim AS f1
								WHERE
									f1.imsi IN (SELECT imsi FROM t_resimsigid)
								AND f1.imsi NOT IN (
									SELECT
										sourceid AS imsi
									FROM
										t_resvsimowner
									UNION
										SELECT
											imsi
										FROM
											t_resvsimbinduser
								)
								AND (f1.state = '00400')
							)
						GROUP BY
							f2.mcc
					) AS g3 ON g0.mcc = g3.mcc
				) AS c2 ON c1.mcc = c2.mcc
				GROUP BY
					iso2
			)AS z
"""
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

def getGTBU():
    global gtbuquery
    Con,Cur = getMysqlCon("ASS_GTBU","glocalme_css")

    Cur.execute(gtbuquery)
    gtburesult = Cur.fetchall()
    Con.close()

    return gtburesult

def getNON():
    global nonquery
    nonCon,nonCur = getMysqlCon("NON_GTBU","ucloudplatform")

    nonCur.execute(nonquery)
    nonresult = nonCur.fetchall()
    nonCon.close()

    return nonresult

if __name__=="__main__":
    gtburesult = getGTBU()
    nonresult = getNON()

    print gtburesult
    print ""
    print "..."
    print len(gtburesult)

