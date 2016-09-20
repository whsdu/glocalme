#!/usr/bionpython
# -*- coding: utf-8 -*-
import MySQLdb
import sys
import datetime
import schedule, time
import numpy as np
from plotly.offline import plot
import plotly.graph_objs as go
import copy
from kits import getdbinfo
import dailyoutput
import mailman

postNum = 0
postTime = 0
record = list()

testStd = list()
testAve = list()
testPost = list()

host = "localhost"
subject = "Abnormal states summary"
to_addr = ["hao.wu@ucloudlink.com"]
from_addr = "service_support_2@ucloudlink.com"
body_test = ""

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def getdata():

    gtbuDBinfo = getdbinfo("ASS_GTBU")
    gtbuDBname = "glocalme_ass"

    nonDBinfo = getdbinfo('NON_GTBU')
    nonDBname = 'ucloudplatform'

    gtbuDBconnection = getDBconnection(gtbuDBinfo,gtbuDBname)
    nonDBconnection = getDBconnection(nonDBinfo,nonDBname)

    gtbuCur = gtbuDBconnection.cursor()
    nonCur = nonDBconnection.cursor()

    queryGTBU ="""
    SELECT COUNT(1)
    FROM t_usmguserloginonline
    """

    queryNon = """
    SELECT COUNT(1)
    FROM t_usmguserloginonline
    """

    gtbuCur.execute(queryGTBU)
    nonCur.execute(queryNon)

    gtbuResult = gtbuCur.fetchall()
    nonResult = nonCur.fetchall()

    gtbuDBconnection.close()
    nonDBconnection.close()

    gtbuNum = gtbuResult[0][0]
    nonNum = nonResult[0][0]

    return gtbuNum+nonNum

def checkCurNum(curTime,curNum):
    global record
    global postNum
    global postTime

    global testPost
    global testAve
    global testStd

    std = np.std(record[-120:])
    ave = np.average(record[-120:])
    returnlist = list()

    testStd.append(std)
    testAve.append(np.abs(curNum-ave))
    testPost.append(np.abs(curNum-postNum))

    if len(record) > 200:
        record = record[-120:]
        testStd = list()
        testAve = list()
        testPost = list()

    if len(record) < 120:
        record.append(curNum)
        postNum = curNum
        postTime = curTime
    elif np.abs(curNum-postNum)<2*std:
        record.append(curNum)
        postNum = curNum
        postTime = curTime
    elif curNum<postNum:
        returnlist = [postTime,postNum]
        record = record[-120:]

    return returnlist

def sentinel():

    global testPost
    global testAve
    global testStd

    global host
    global subject
    global to_addr
    global from_addr

    curTime = datetime.datetime.now()
    curNum = getdata()

    resList = checkCurNum(curTime,curNum)

    if len(resList) != 0:
        postTime =resList[0]
        postNum = resList[1]

        body_text = dailyoutput.getbodytext(postTime,postNum,curTime,curNum)
        mailman.send_mail(host,subject,to_addr,from_addr,body_text)
        print "!!!!!!!!!!"
        print "At time: " + str(postTime)+ " the online number is: " + str(postNum)
        print "At time: " + str(curTime)+ " the online number is: " + str(curNum)
        print " online number decrised by : " + str(postNum-curNum)
        print "................"

    lstd = copy.copy(testStd)
    ustd = copy.copy(testStd)

    for index,i in enumerate(lstd):
        lstd[index] = i - 2*i

    for index,i in enumerate(ustd):
        ustd[index] = 2*i

    x_axis = range(len(testStd))
    y_axis = testStd

    std=go.Scatter(
        x = x_axis,
        y = y_axis,
        mode = 'lines+markers',
        name = "std"
    )

    lowboundstd=go.Scatter(
        x = x_axis,
        y = lstd,
        mode = 'lines+markers',
        name = "lowerboundstd"
    )

    upboundstd=go.Scatter(
        x = x_axis,
        y = ustd,
        mode = 'lines+markers',
        name = "upboundstd"
    )

    aveDiff=go.Scatter(
        x = x_axis,
        y = testAve,
        mode = 'lines+markers',
        name = "aveDiff"
    )
    postDiff=go.Scatter(
        x = x_axis,
        y = testPost,
        mode = 'lines+markers',
        name = "postDiff"
    )

    layout = dict(title = 'predicewma',
              xaxis = dict(title = 'Date'),
              yaxis = dict(title = 'online Number'),
              )

    data = [std,lowboundstd,upboundstd,aveDiff,postDiff]
    fig = dict(data=data, layout=layout)

    plot(fig,filename ="/ukl/apache-tomcat-7.0.67/webapps/demoplotly/stddiff.html",auto_open=False)

def initiateNum():
    global postNum
    global postTime

    postNum = getdata()
    postTime = datetime.datetime.now()
    record.append(postNum)

if __name__ == "__main__":
    initiateNum()
    while(1):
        sentinel()
        time.sleep(2)
