import MySQLdb
import schedule, time
import datetime
from datetime import timedelta
from kits import getdbinfo
import numpy as np
import logging
import pymongo
from pymongo import MongoClient

from sklearn.ensemble import GradientBoostingRegressor

gtbuDict = dict()
omsDict = dict()
presisDict = dict()
testingDict = dict()
onlineDict = dict()
omsFutureDict= dict()
dailyMax = dict()
adjustDict = dict()
countryAdjust = dict()

adjustTimeInterval = 30             # how many days will be included in the adjust dailyMaxMatrix

counter = 1
logging.basicConfig(filename = 'imsierror.log',format = '%(asctime)s %(message)s',level = logging.DEBUG)
processDate = datetime.datetime.now().date()

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
    So, if we need to accumulate ( or process data indexed by two parameters), the cartesian product of these two
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

    # print "processing record...:" + str(counter)
    counter +=1

def getRatio(countries,Ratios):
    cr = dict()
    for i,country in enumerate(countries):
        cr[country] = max(Ratios[i])

    return cr
def dealAdjustDict(analyDict):
    s = 0
    for country,num in analyDict.iteritems():
        s += num

    for country,num in analyDict.iteritems():
        analyDict[country] = float(num)/s

def adjustRatio(result):
    adjuctDict = dict()
    for pck,details in resultDict.iteritems():
        analyInnerDict = dict()
        for country,list in details.iteritems():
            cSum = np.sum(list)
            analyInnerDict[country] = cSum
        dealAdjustDict(analyInnerDict)
        adjuctDict[pck] = analyInnerDict

    return adjuctDict

def dealAnalyDict(analyDict):

    topsum = 0
    minsum =0
    for country,value in analyDict.iteritems():
        topsum += value[0]
        minsum += value[2]

    for country,value in analyDict.iteritems():
        value[0] = float(value[0])/minsum
        value[2] = float(value[2])/topsum


def getRanges(resultDict):

    analyDict = dict()

    for pck,details in resultDict.iteritems():
        analyInnerDict = dict()
        for country,list in details.iteritems():
            top = max(list)
            mid = float(np.sum(list))/len(list)
            min = top
            for i in list:
                if i !=0 and i < min:
                    min = i
            analyInnerDict[country] = [top,mid,min]
        dealAnalyDict(analyInnerDict)
        analyDict[pck] = analyInnerDict

    return analyDict

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
                # logging.info( "insert into country: " + str(c) + " number: " + str(numbers[place]) + " at day: " + str(date))
            else:
                numberList[i].append(numbers[place])
                nonZeroUpdateList.append(i)
                # logging.info( "update country: " + str(c) + " number: " + str(numbers[place]) + " at day: " + str(date))

        for k in range(0,len(numberList)):
            if k not in nonZeroUpdateList:
                numberList[k].append(0)
                # logging.info( "insert zero country: "+countryList[k] +"  at day: " + str(date))

        listIndicator +=1

    resultList.append(dateList)
    resultList.append(countryList)
    resultList.append(numberList)

    return resultList

def getNumber(pck,country,analyDict):
    countryDetail = analyDict.get(pck,dict())

    if len(countryDetail.keys()) != 0:
        numberList = countryDetail.get(country,list())
        if len(numberList) != 0:
            co = numberList[1]
        else:
            co =0
    else:
        co =0
    return co

def adjustNumber(pck,country,adjustDict):
    countryDetail = analyDict.get(pck,dict())

    if len(countryDetail.keys()) != 0:
        co = countryDetail.get(country,0)
    else:
        co =0
    return co

def omsCounter(row,recordDict,analyDict):
    global counter
    start = row[0]
    interval = row[2]+1
    keylist = []
    for i in range(0,interval):
        keylist.append(start+timedelta(days = i))

    pck = row[3]
    country = row[4]


    for key in keylist:
        detail = recordDict.get(key,{"countries":[],"max":[]})
        countryList = detail.get("countries")
        maxList = detail.get("max")


        # num = getNumber(pck,country,analyDict)
        num = adjustNumber(pck,country,analyDict)
        if num == 0 and len(country)!=0:
            num = float(1)/len(countryList)

            print num

        if country not in countryList:
            countryList.append(country)
            maxList.append(num)
        else:
            i = countryList.index(country)
            maxList[i] +=num

        recordDict[key] = detail
    #
    # print "processing record...:" + str(counter)
    # counter +=1

def mapReduce(row,dcDict):
    global counter
    start = row[0]
    interval = row[2]+1
    dayList = []
    imei = row[3]
    value = row[4]

    for i in range(0,interval):
        dayList.append(start+timedelta(days = i))
        # logging.info(str(start+timedelta(days = i)))

    for day in dayList:
        tmpkey = (day,imei)
        dcDict[tmpkey] = value

    # print "processing record...:" + str(counter)
    counter +=1

def affineTransform(rDict,lDict):
    global counter

    pckDayDict = dict()
    resultDict = dict()

    for key,country in rDict.iteritems():
        day = key[0]
        imei =key[1]

        pck = lDict.get(key,0)
        if pck == 0:
            continue
        print " got a key: " + str(counter)
        counter+=1

        daylist = pckDayDict.get(pck,list())
        if day not in daylist:
            daylist.append(day)
        pckDayDict[pck] = daylist

        length = len(pckDayDict[pck])
        place = pckDayDict[pck].index(day)

        pckDetail = resultDict.get(pck,dict())

        if country not in pckDetail:
            pckDetail[country] = [0] * length
            pckDetail[country][place] = 1
        else:
            if len(pckDetail[country]) < length:
                pckDetail[country].append(1)
            else:
                pckDetail[country][place] += 1

        for key,value in pckDetail.iteritems():
            if key != country:
                if len(pckDetail[key])<length:
                    pckDetail[key].append(0)


        resultDict[pck]=pckDetail

    return resultDict

def fetchsome(cursor, some=1000):
    fetch = cursor.fetchmany
    while True:
        rows = fetch(some)
        if not rows: break
        for row in rows:
            yield row

def partitionList(inputList,outputList,country):
    cDayList = outputList[0]
    countryList = outputList[1]
    jpIndex = countryList.index(country)
    outputdataList = outputList[2][jpIndex]

    print "output data is: "
    print outputdataList

    inputDataList = inputList[2]
    inputDayList = inputList[0]

    maxcindex = len(cDayList)
    trainingStart = 12
    trainingend = int(maxcindex*1)

    testStart = trainingStart
    testend = trainingend

    xTrainingSet = list()
    yTrainingSet = list()

    for i in range(trainingStart,trainingend):
        day = cDayList[i]
        if day not in inputDayList:
            continue

        # print "Training at day: " + str(day)
        yTrainingSet.append(outputdataList[i])
        # print "Training output is: " + str(outputdataList[i])

        index = inputDayList.index(day)
        tmpinput =inputDataList[index]
        xTrainingSet.append(tmpinput)

        # print " Training input is: "
        # print tmpinput

    xTestSet = list()
    yTestSet = list()
    testDayList = list()

    for i in range(testStart,testend):
        day = cDayList[i]
        if day not in inputDayList:
            continue
        testDayList.append(day)
        yTestSet.append(outputdataList[i])

        index = inputDayList.index(day)
        tmpinput =inputDataList[index]
        xTestSet.append(tmpinput)

    return(testDayList,xTrainingSet,yTrainingSet,xTestSet,yTestSet)

def doNormalization(inputSet):
    avgList = np.average(inputSet,axis = 1)
    stdList = np.std(inputSet,axis = 1)

    for i,input in enumerate(inputSet):
        input = input - avgList[i]
        input = input/float(stdList[i])
        inputSet[i] = input

def batchTraining(orderList,onlineList):
    fittingDict = dict()
    countryList = onlineList[1]

    for country in countryList:
        testDaylist,xTrainingSet,yTrainingSet,xTestSet,yTestSet = partitionList(orderList,onlineList,country)
        doNormalization(xTrainingSet)
        doNormalization(xTestSet)

        # clf = svm.SVR(kernel='rbf', C=1e3, gamma=0.1)
        # clf.fit(xTrainingSet,yTrainingSet)

        est = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,max_depth=1, random_state=0, loss='ls').fit(xTrainingSet, yTrainingSet)
        fittingDict[country] = est

    return fittingDict

def reformTrainingSet(orderList,futureOrderList):
    trainingMatrix = list()
    standardPckList = orderList[1]
    futurePckList = futureOrderList[1]
    futureNumberMatrix = futureOrderList[2]

    tmpMatrix = list()
    for futureNumberList in futureNumberMatrix:
        tmplist = list()
        for standpck in standardPckList:
            if standpck in futurePckList:
                index = futurePckList.index(standpck)
                tmplist.append(futureNumberList[index])
            else:
                tmplist.append(0)

        tmpMatrix.append(tmplist)

    futureOrderList[1] = standardPckList
    futureOrderList[2] = tmpMatrix

    trainingMatrix = futureOrderList
    return trainingMatrix

def batchForecasting(fiitingDict,futureOrderList):
    forecastMatrix = futureOrderList[2]
    doNormalization(forecastMatrix)

    resultDict = dict()
    for country,model in fiitingDict.iteritems():
        tmpResult = model.predict(forecastMatrix)
        resultDict[country]=tmpResult

    return resultDict

def fillDailyMaxMatrix(dailyMax,futureOrderList,forecastDict):
    global adjustTimeInterval

    datelist = futureOrderList[0][0:adjustTimeInterval]

    for dateindex,futuredate in enumerate(datelist):
        for country,forecastList in forecastDict.iteritems():
            forcast = forecastList[dateindex]
            curMax = dailyMax.get((country,futuredate),0)
            if forcast > curMax:
                    dailyMax[(country,futuredate)] = forcast

    print dailyMax

def storeDailyMax (processDate,dailyMax):
    print " the porcess date is : "
    print processDate

    print dailyMax

    client = MongoClient()
    db = client['login_history']
    col = db['col_dailymax']

    yesterdayPredict = dict()
    pdate = datetime.datetime(*(processDate.timetuple()[:6]))

    for key,value in dailyMax.iteritems():
        country = key[0]
        date = key[1]
        forecastDateList = list()
        forecastNumList = list()

        date = datetime.datetime(*(date.timetuple()[:6]))
        tmpDoc = col.find_one({'targetDate':date,'targetCountry':country},{'forecastDate':1,'forecastNum':1})
        if tmpDoc is None:
            forecastDateList.append(pdate)
            forecastNumList.append(value)
        else:
            forecastDateList = tmpDoc['forecastDate']
            forecastNumList = tmpDoc['forecastNum']
            forecastDateList.append(pdate)
            forecastNumList.append(value)

        tmpResult = col.update({'targetDate':date,'targetCountry':country},
                               {'$set':{'forecastDate':forecastDateList,'forecastNum':forecastNumList}},
                               upsert = True)

        print tmpResult
        print date
        print country
        print "....."

    cursor = col.find({'targetDate':pdate})

    for doc in cursor:
        country = doc['targetCountry']
        dateList = doc['forecastDate']
        numList = doc['forecastNum']
        yesterdayPredict[country]=[dateList,numList]

    print "this is processdate:"
    print pdate

    col.delete_many({'targetDate':{'$lt':pdate}})

    dailyMax.clear()
    client.close()

    return yesterdayPredict

def adjustForecast(processDate,yesterdayPredict,theTruth):
    countryThisRoundAdjust = dict()
    for country,value in yesterdayPredict.iteritems():
        numlist = value[1]
        datelist = value[0]
        anumlist = np.asarray(numlist)
        truevalue = theTruth.get(country,np.max(anumlist))
        anumlist = anumlist - truevalue
        numlist = list(anumlist)

        print ".....<<.>>...."

        intervalList = list()
        for day in datelist:
            tmpDate = day.date()
            intervalList.append((processDate-tmpDate).days)

        numlist.reverse()
        intervalList.reverse()
        print numlist
        print intervalList

        countryThisRoundAdjust[country] = [intervalList,numlist]

    print processDate
    print yesterdayPredict
    print theTruth
    print countryThisRoundAdjust

    countryAdjust = processAdjustCube(countryThisRoundAdjust)


    return countryAdjust

def processAdjustCube(countryThisRoundAdjust):
    global adjustDict
    global countryAdjust

    client = MongoClient()
    db = client['login_history']
    col = db['col_adjust']
    today = datetime.datetime.now().date()
    ds = datetime.datetime(*(today.timetuple()[:6]))

    for country,adjustList in countryThisRoundAdjust.iteritems():
        intervallist = adjustList[0]
        numlist = adjustList[1]
        tmpResult = col.update({'recordtime':ds,'targetCountry':country},
                               {'$set':{'dateintervalList':intervallist,'numlist':numlist}},
                               upsert = True)

        print tmpResult

    for country,adjustList in countryThisRoundAdjust.iteritems():
        countrydetail = adjustDict.get(country,dict())
        intervallist = adjustList[0]
        numlist = adjustList[1]
        for index,interval in enumerate(intervallist):
            tmplist = countrydetail.get(interval,list())
            tmplist.append(numlist[index])
            countrydetail[interval] = tmplist
        adjustDict[country] = countrydetail

    getCountryAdjust()
    client.close()

def initialAdjustDict(adjustDict):

    client = MongoClient()
    db = client['login_history']
    col = db['col_adjust']

    tmpCur = col.find({},sort=[('recordtime',pymongo.ASCENDING)])
    for doc in tmpCur:
        country = doc['targetCountry']
        countrydetail = adjustDict.get(country,dict())
        tmpIntervallist = doc['dateintervalList']
        tmpNumList = doc['numlist']
        for index,interval in enumerate(tmpIntervallist):
            tmplist = countrydetail.get(interval,list())
            tmplist.append(tmpNumList[index])
            countrydetail[interval] = tmplist
        adjustDict[country] = countrydetail

    getCountryAdjust()
    client.close()

def getCountryAdjust():
    global adjustDict
    global countryAdjust

    for country,countryDetail in adjustDict.iteritems():
        tmpDict = dict()
        for interval,numlistlist in countryDetail.iteritems():
            sv = np.average(numlistlist)
            std = np.std(numlistlist)
            tmpDict[interval] = (sv,std)
        countryAdjust[country]=tmpDict

def trimFutureDict(processDate,reDict):

    for key in reDict.keys():
        if key < processDate:
            del reDict[key]

def getDBconnection(dbinfor,dbname):
    dbconnection = MySQLdb.connect(user = dbinfor['usr'],
                                  passwd = dbinfor['pwd'],
                                  host = dbinfor['host'],
                                  port = dbinfor['port'],
                                  db = dbname)
    return dbconnection

def queryandinsert():
    """ This is the main function which will be call by main... it integrate several other functions.
    Please do not call this function in other pack, otherwise it will cause unexpected result!!!!"""
    global gtbuDict             # gtbuDict, being used to store query data from gtbu database.....
    global omsDict              # being used to store query data from OMS database.....
    global presisDict
    global counter
    global testingDict
    global omsFutureDict
    global processDate
    global dailyMax
    global countryAdjust


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

    # get the online data which will be used to calculate the daily uer number ( Daily user number is bigger than the max number...
    # and the max number is actually what being used in this scenario
    queryStatementTraining = """
    SELECT t1,t2,DATEDIFF(t2,t1) AS dif,imei,visitcountry FROM
    (
    SELECT DATE(logindatetime) AS t1,DATE(logoutdatetime) AS t2, imei,visitcountry
    FROM t_usmguserloginlog
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
    WHERE butype =2
    ) AS z
    GROUP BY epochTime,visitcountry
    """

    # (input data) get the order number information which will be used to calculate the daily maximum number for each country...
    # this number could be ridiculously large with respect to the real amount for some specific countries.
    querystatementOMS = """
    SELECT DATE(date_goabroad),DATE(date_repatriate),DATEDIFF(date_repatriate,date_goabroad),imei,package_id FROM tbl_order_basic
    WHERE imei IS NOT NULL AND (DATE(date_repatriate)) > '2016-01-01' AND DATE(date_goabroad) < DATE(NOW())
    ORDER BY date_repatriate ASC
    """

    querystatementOMSFuture = """
    SELECT DATE(date_goabroad),DATE(date_repatriate),DATEDIFF(date_repatriate,date_goabroad),imei,package_id FROM tbl_order_basic
    WHERE data_status = 0 AND DATE(date_goabroad) BETWEEN DATE(NOW()) AND DATE_ADD(NOW(),INTERVAL 3 MONTH) OR
    (
    DATE(date_repatriate) >= DATE(NOW())
    )
    """

    querystatementYesterday = """
    SELECT DATE(epochTime),visitcountry,MAX(onlinenum)
    FROM t_fordemo
    WHERE DATE(epochTime) = DATE_SUB(DATE(NOW()),INTERVAL 1 DAY)
    GROUP BY visitcountry
    """
    # establish connection to the mysql databases................
    # querydbGTBU = MySQLdb.connect(user = querydbinfoGTBU['usr'],
    #                               passwd = querydbinfoGTBU['pwd'],
    #                               host = querydbinfoGTBU['host'],
    #                               port = querydbinfoGTBU['port'],
    #                               db = querydbnameGTBU)
    # querydbOMS = MySQLdb.connect(user = querydbinfoOMS['usr'],
    #                              passwd = querydbinfoOMS['pwd'],
    #                              host = querydbinfoOMS['host'],
    #                              port = querydbinfoOMS['port'],
    #                              db = querydbnameOMS)
    # insertdb = MySQLdb.connect(user = insertdbinfo['usr'],
    #                            passwd = insertdbinfo['pwd'],
    #                            host = insertdbinfo['host'],
    #                            port = insertdbinfo['port'],
    #                            db = insertdbname)

    querydbGTBU = getDBconnection(querydbinfoGTBU,querydbnameGTBU)
    querydbOMS = getDBconnection(querydbinfoOMS,querydbnameOMS)
    insertdb = getDBconnection(insertdbinfo,insertdbname)

    queryCurGTBU = querydbGTBU.cursor()
    queryCurOMS = querydbOMS.cursor()
    insertCur = insertdb.cursor()

    # print "executing query!!! By using generator!!!"
    # queryCurGTBU.execute(queryStatementTraining)
    # testingSetGenerator = fetchsome(queryCurGTBU,30000) #fetchsome is a generator which will fetch a certain number of query each time.
    #
    # for row in testingSetGenerator:
    #     accumulator(row,testingDict)
    #
    # testList = getTestingList(testingDict)      # testList is [daylist,countrylist,2dArray with shape (len(countrylist),len(daylist))

    today = datetime.datetime.now().date()

    print "today is: "
    print today

    print "...."
    print "process date is: "
    print processDate

    if today != processDate:
        print " start doing std"
        countryAdjust.clear()
        yesterdayPredict = storeDailyMax(processDate,dailyMax)
        insertCur.execute(querystatementYesterday)
        yesterDayGenerator = fetchsome(insertCur,10)
        trueValue = dict()
        for row in yesterDayGenerator:
            trueValue[row[1]] = row[2]
        adjustForecast(processDate,yesterdayPredict,trueValue)

        processDate = today


    insertCur.execute(queryStatementOnline)
    onlineSetGenerator = fetchsome(insertCur,3000)
    for row in onlineSetGenerator:
        accumulatOnlineNumber(row,onlineDict)


    onlineList = getTestingList(onlineDict)     # online is [daylist,countrylist,2dArray with shape (len(countrylist),len(daylist))

    queryCurOMS.execute(querystatementOMS)
    omsOrderGenerator = fetchsome(queryCurOMS,3000)
    for row in omsOrderGenerator:
        accumulator(row,omsDict)

    orderList = getTestingList(omsDict)      #orderList is [daylist,pcklist,2dArray with shape (len(pcklist),len(daylist))

    untranList = orderList[2]               # dimensio of out put is len(daylist), so dimension of each input is len(packlist)
    tranlist = transfromIndex(untranList)   # so we need to change the shape(orderlist[2]) = len(pcklst),len(daylist) to len(daylist), len(pcklist)
    orderList[2] = tranlist

    fittingDict = batchTraining(orderList,onlineList)

    print fittingDict
    print fittingDict.keys()
    print len(fittingDict.keys())

    print " start predict future"
    queryCurOMS.execute(querystatementOMSFuture)
    omsFutureOrderGenerator = fetchsome(queryCurOMS,3000)
    for row in omsFutureOrderGenerator:
        accumulator(row,omsFutureDict)

    trimFutureDict(processDate,omsFutureDict)

    print ".......>>>>  Dates after trimed <<<<<............."
    print omsFutureDict.keys()

    futureOrderList = getTestingList(omsFutureDict)

    untranList = futureOrderList[2]               # dimensio of out put is len(daylist), so dimension of each input is len(packlist)
    tranlist = transfromIndex(untranList)   # so we need to change the shape(orderlist[2]) = len(pcklst),len(daylist) to len(daylist), len(pcklist)
    futureOrderList[2] = tranlist

    futureOrderList = reformTrainingSet(orderList,futureOrderList)      #the predict set need  to be reformed so that packages in predict set can be aligned with pcks in training set.
    forecastDict = batchForecasting(fittingDict,futureOrderList)

    querydbOMS.close()
    insertdb.close()

    print forecastDict
    print futureOrderList[0]
    print len(futureOrderList[0])
    print len(forecastDict['JP'])

    querydbOMS = getDBconnection(querydbinfoOMS,querydbnameOMS)
    insertdb = getDBconnection(insertdbinfo,insertdbname)
    queryCurOMS = querydbOMS.cursor()
    insertCur = insertdb.cursor()

    insertCur.execute("delete from t_maxpredict_std")
    insertdb.commit()

    fillDailyMaxMatrix(dailyMax,futureOrderList,forecastDict)

    now = datetime.datetime.now()
    today = datetime.datetime.now().date()
    for dateindex,furturedate in enumerate(futureOrderList[0]):
        for country,forecast in forecastDict.iteritems():
            avg = 0
            std = 0
            if len(countryAdjust.keys()) != 0:
                interval = (furturedate-today).days
                rDict = countryAdjust.get(country,dict())
                if interval in rDict.keys():
                    rT = rDict.get(interval)
                    avg = rT[0]
                    std = rT[1]
            insertCur.execute('''insert into t_maxpredict_std(recordtime,date,country,max,std) values(%s,%s,%s,%s,%s)''',
                              (now,furturedate,country,int(forecast[dateindex]-avg),std))


    querydbGTBU.close()
    querydbOMS.close()
    insertdb.commit()
    insertdb.close()

if __name__=="__main__":
    global adjustDict

    initialAdjustDict(adjustDict)
    queryandinsert()
    schedule.every(2).hours.do(queryandinsert)
    while True:
        schedule.run_pending()
        time.sleep(1)
        gtbuDict.clear()
        omsDict.clear()
        presisDict.clear()
        testingDict.clear()
        onlineDict.clear()
        omsFutureDict.clear()
