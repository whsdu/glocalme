import MySQLdb
import schedule, time
import datetime
from datetime import timedelta
from kits import getdbinfo
import numpy as np
import pandas
import logging
from sklearn import svm
from sklearn import linear_model
from plotly.offline import plot
from pandas import DataFrame
from pandas import Series
import pandas as pd
import plotly.graph_objs as go
from sklearn.ensemble import GradientBoostingRegressor
from statsmodels.tsa.stattools import acf,pacf
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.arima_model import _arma_predict_out_of_sample

gtbuDict = dict()
omsDict = dict()
presisDict = dict()
testingDict = dict()
onlineDict = dict()

counter = 1
logging.basicConfig(filename = 'imsierror.log',format = '%(asctime)s %(message)s',level = logging.DEBUG)
def accumulatOnlineNumber(row,onlineDict):

    day = row[0]
    country = row[1]
    num = row[2]

    detail = onlineDict.get(day,{"countries":[],"max":[]})
    countryList = detail.get("countries")
    maxList = detail.get("max")

    if country not in countryList:
        countryList.append(country)
        maxList.append(num)

    onlineDict[day] = detail

    print "This is onlnie number of day: " + str(day)

def transfromIndex(twodArray):
    tList = list()

    inner = len(twodArray[0])
    outter = len(twodArray)

    for i in range(inner):
        tmplist = list()
        for o in range(outter):
            tmplist.append(twodArray[o][i])
        tList.append(tmplist)
    return tList

def accumulator(row,recordDict):
    '''
    A 2D array is indexed by two parameters,therefore, each of these two parameters will come from a dimension.
    Therefore, if we need to accumulate ( or process data indexed by two parameters), the cartesian product of these two
    dimensions are possible key which can be used in a dictionary....
    What we have will be 1.a start date (1st part of the key). 2.an interval 3.another (2nd part of the key)
    :param row: contains 1,2 and 3
    :param recordDict: {key1:{key2:[],accumulate:[]}} by doin so we could iterate over all possible records and accumulate them
    :return: No return value
    '''
    global counter
    start = row[0]
    interval = row[2]+1
    country = row[4]
    keylist = []
    for i in range(0,interval):
        keylist.append(start+timedelta(days = i))

    for key in keylist:
        detail = recordDict.get(key,{"countries":[],"max":[]})
        countryList = detail.get("countries")
        maxList = detail.get("max")

        if country not in countryList:
            countryList.append(country)
            maxList.append(1)
        else:
            i = countryList.index(country)
            maxList[i] +=1

        recordDict[key] = detail

    print "processing record...:" + str(counter)
    counter +=1


def getTestingList(dcDict):
    '''
    Originally,, the structure of dcDict is: {key1:{key2:[],acc:[]}}
    We would like to assemble all acc:[] into a 2D array(Matrix) under two constrains:
    constrain 1: Key1 should be sorted, so the corresponding vale in acc needs to be carefully rearranged as well.
    constrain 2: From the perspective of key2, the corresponding key1 may not be existed continuously, so a default value (in this case 0)
                need to be filled in for the missing key1.
    :param dcDict: The input dict which has structure {key1:{key2:[],acc:[]}}
    :return: resultList=[list1,list2,2darray] ,list of key1, list of key2 , 2D array with the shape (len(key2),len(key1))
    '''
    dateList = dcDict.keys()        # datelist is the dimension of key1 and it should be sorted
    dateList.sort()
    print dateList

    resultList = list()
    countryList = list()
    numberList = list()

    listIndicator = 1               # being used to record the max iteration of date list.

    for pdate,date in enumerate(dateList):
        detail = dcDict.get(date)
        countries = detail.get("countries")
        numbers = detail.get("max")

        nonZeroUpdateList = list()
        for place,c in enumerate(countries):
            i = countryList.index(c) if c in countryList else -1
            if i == -1:
                countryList.append(c)
                tmpList = [0] * listIndicator
                tmpList[-1] = numbers[place]
                numberList.append(tmpList)

                t = countryList.index(c)
                nonZeroUpdateList.append(t)
                logging.info( "insert into country: " + str(c) + " number: " + str(numbers[place]) + " at day: " + str(date))
            else:
                numberList[i].append(numbers[place])
                nonZeroUpdateList.append(i)
                logging.info( "update country: " + str(c) + " number: " + str(numbers[place]) + " at day: " + str(date))

        for k in range(0,len(numberList)):
            if k not in nonZeroUpdateList:
                numberList[k].append(0)
                logging.info( "insert zero country: "+countryList[k] +"  at day: " + str(date))

        listIndicator +=1

    resultList.append(dateList)
    resultList.append(countryList)
    resultList.append(numberList)

    return resultList

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def queryandinsert():
    """ This is the main function which will be call by main... it integrate several other functions.
    Please do not call this function in other pack, otherwise it will cause unexpected result!!!!"""
    global gtbuDict             # gtbuDict, being used to store query data from gtbu database.....
    global omsDict              # being used to store query data from OMS database.....
    global presisDict
    global counter
    global testingDict

    starttime = datetime.datetime.now()

    print len(presisDict)
    print "connect to databae!"

    # connect to the database use my own toolkits
    querydbinfoOMS = getdbinfo('OMS')
    querydbnameOMS = "wifi_data"

    querydbinfoGTBU = getdbinfo("GTBU")
    querydbnameGTBU = "ucloudplatform"

    insertdbinfo = getdbinfo('REMOTE')
    insertdbname = 'login_history'

    # print the database information for verification
    for key, value in querydbinfoOMS.iteritems():
        print key + " : " + str(value)

    queryStatementRemote = """
    SELECT epochTime,visitcountry,onlinenum
    FROM t_fordemo
    WHERE butype =2 AND visitcountry IN ('JP','DE','TR') AND epochTime BETWEEN DATE_SUB(NOW(),INTERVAL 2 DAY) AND NOW()
    ORDER BY epochTime ASC
    """
    # get the online data which will be used to calculate the daily uer number ( Daily user number is bigger than the max number...
    # and the max number is actually what being used in this scenario
    queryStatementTraining = """
    SELECT t1,t2,DATEDIFF(t2,t1) AS dif,imei,visitcountry FROM
    (
    SELECT DATE(logindatetime) AS t1,DATE(logoutdatetime) AS t2, imei,visitcountry
    FROM t_usmguserloginlog
    WHERE visitcountry IN ('JP','DE','TR')
    ) AS z
    GROUP BY t1,t2,imei
    """

    # (output data) get the max online number for each of these countries every day ( this record is incomplete due to the constant network partition
    # therefore a lot of corresponding operation is necessary for aligning the input and output date by day!...
    queryStatementOnline ="""
    SELECT epochTime,visitcountry,MAX(onlinenum)
    FROM
    (
    SELECT DATE(epochTime) AS epochTime,visitcountry,onlinenum
    FROM t_fordemo
    WHERE butype =2 and visitcountry IN ('JP','DE','TR')
    ) AS z
    GROUP BY epochTime,visitcountry
    """

    # (input data) get the order number information which will be used to calculate the daily maximum number for each country...
    # this number could be ridiculously large with respect to the real number for some specific countries.
    querystatementOMS = """
    SELECT DATE(date_goabroad),DATE(date_repatriate),DATEDIFF(date_repatriate,date_goabroad),imei,package_id FROM tbl_order_basic
    WHERE imei IS NOT NULL AND (DATE(date_repatriate)) > '2016-01-01' AND DATE(date_goabroad) < DATE(NOW())
    ORDER BY date_repatriate ASC
    """

    querystatementOMSCount = """
    SELECT  date_goabroad,date_repatriate,DATEDIFF(date_repatriate,date_goabroad),t1.package_id,t3.iso2 FROM tbl_order_basic AS t1
    LEFT JOIN tbl_package_countries AS t2
    ON t1.package_id = t2.package_id
    LEFT JOIN tbl_country AS t3
    ON t2.country_id = t3.pk_global_id
    WHERE t1.data_status = 0 AND DATE(date_goabroad) BETWEEN DATE(NOW()) AND DATE_ADD(NOW(),INTERVAL 3 MONTH) OR
    (
    DATE(date_repatriate) >= DATE(NOW())
    )
    """

    # establish connection to the mysql databases................
    querydbGTBU = MySQLdb.connect(user = querydbinfoGTBU['usr'],
                                  passwd = querydbinfoGTBU['pwd'],
                                  host = querydbinfoGTBU['host'],
                                  port = querydbinfoGTBU['port'],
                                  db = querydbnameGTBU)
    querydbOMS = MySQLdb.connect(user = querydbinfoOMS['usr'],
                                 passwd = querydbinfoOMS['pwd'],
                                 host = querydbinfoOMS['host'],
                                 port = querydbinfoOMS['port'],
                                 db = querydbnameOMS)
    insertdb = MySQLdb.connect(user = insertdbinfo['usr'],
                               passwd = insertdbinfo['pwd'],
                               host = insertdbinfo['host'],
                               port = insertdbinfo['port'],
                               db = insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    queryCurOMS = querydbOMS.cursor()
    insertCur = insertdb.cursor()


    print "executing query!!! By using generator!!!"
    insertCur.execute(queryStatementRemote)
    remoteGenerator = fetchsome(insertCur,100) #fetchsome is a generator which will fetch a certain number of query each time.

    for row in remoteGenerator:
        accumulatOnlineNumber(row,testingDict)

    onlineList = getTestingList(testingDict)

    countryList = onlineList[1]
    jpIndex = countryList.index('JP')
    datalist = onlineList[2][jpIndex]
    timelist = onlineList[0]

    tsJP = Series(datalist,index = timelist)
    df = DataFrame()
    df['JP'] = tsJP

    print df.index
    print df.columns

    print df

    tsJP_log = np.log(tsJP)
    lag_acf = acf(tsJP_log,nlags=200)
    lag_pacf = pacf(tsJP_log,nlags=200,method='ols')

    # model = ARIMA(tsJP_log,order=(2,1,2))
    model = ARMA(tsJP_log,(5,2))
    res = model.fit(disp=-1)


    print "Here is the fit result"
    print res

    params = res.params
    residuals = res.resid
    p = res.k_ar
    q = res.k_ma
    k_exog = res.k_exog
    k_trend = res.k_trend
    steps = 300

    newP = _arma_predict_out_of_sample(params, steps, residuals, p, q, k_trend, k_exog, endog=tsJP_log, exog=None, start=len(tsJP_log))
    newF,stdF,confiF = res.forecast(steps)

    print newP
    newP = np.exp(newP)
    print newP

    print " Forecast below!!"
    print newF
    newF = np.exp(newF)
    print newF
    print stdF
    stdF = np.exp(stdF)
    print stdF

    x_axis = range(len(lag_acf))
    y_axis = lag_acf

    onlineEWMA=go.Scatter(
        x = x_axis,
        y = y_axis,
        mode = 'lines+markers',
        name = "lag_acf"
    )

    onlinePre=go.Scatter(
        x = x_axis,
        y = newP,
        mode = 'lines+markers',
        name = "predictJP"
    )

    layout = dict(title = 'predicewma',
              xaxis = dict(title = 'Date'),
              yaxis = dict(title = 'online Number'),
              )

    data = [onlineEWMA,onlinePre]
    fig = dict(data=data, layout=layout)

    plot(fig,filename ="/ukl/apache-tomcat-7.0.67/webapps/demoplotly/EWMAprediction.html",auto_open=False)


if __name__=="__main__":
    queryandinsert()
    schedule.every(60).minutes.do(queryandinsert)
    while True:
        schedule.run_pending()
        time.sleep(1)

