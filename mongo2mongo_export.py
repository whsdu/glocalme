#!/usr/bionpython
#coding:utf-8
import os
import MySQLdb
import pymongo
import datetime
from pymongo import MongoClient
from datetime import timedelta
from xlsxwriter.workbook import Workbook
import schedule
import time
import json
import csv
import operator
import xlrd
from kits import getdbinfo

def getOss():
    try:
        ossurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55065/bss'
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['bss']
        ossCol = ossDB['Customer']
    except Exception as e:
        time.sleep(30)
        print e
        return getOss()

    print "get oss mongo connection"
    return [ossCon,ossCol]

def getMymongo():
    try:
        ossurl = "mongodb://52.76.216.36:55053/login_history"
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['login_history']
        ossCol = ossDB['Customer']
    except Exception as e:
        time.sleep(30)
        print e
        return getMymongo()

    print "get mymongo connection"
    return [ossCon,ossCol]

def getTask1():
    ossCon,ossCol = getOss()
    myCon,mysCol = getMymongo()

    ossCur = ossCol.find()

    for doc in ossCur:
        mysCol.insert_one(doc)

    ossCon.close()
    myCon.close()


if __name__ == "__main__":
    getTask1()
    #
    # schedule.every().hour.do(getTask1)
    # schedule.every().day.at("00:02").do(createDailyFloder)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
