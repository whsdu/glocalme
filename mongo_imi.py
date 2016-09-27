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
import thread

def getOss():
    try:
        ossurl = 'mongodb://mongoquery:FnVWVDvnWeuinx7@52.74.132.61:55075/oss_system'
        ossCon = MongoClient(ossurl)
        ossDB = ossCon['oss_system']
        ossCol = ossDB['t_monitor_term_online']
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
        ossCol = ossDB['t_monitor_term_online']
    except Exception as e:
        time.sleep(30)
        print e
        return getMymongo()

    print "get mymongo connection"
    return [ossCon,ossCol]

def ossOnline():
    ossCon,ossCol = getOss()
    ossCur = ossCol.find()

    ossList = list()
    ossImei = list()
    for doc in ossCur:
        ossList.append(doc)
        ossImei.append(doc['sessionid'])

    ossCon.close()
    print "get oss List and oss imei list"
    return ossList, ossImei

def myOnline():
    myCon,mysCol = getMymongo()
    myCur = mysCol.find()

    myImei = list()
    for doc in myCur:
        myImei.append(doc['sessionid'])

    myCon.close()
    print " get myonline imei list"
    return myImei

def getDelete():
    ossList, ossImei = ossOnline()
    myImei = myOnline()

    deleteImei = list()
    for imei  in myImei:
        if imei not in ossImei: deleteImei.append(imei)

    print "get deltelist information and oss list"
    return deleteImei, ossList

def updateMymongo(osslist):
    myCon,mysCol = getMymongo()

    r = 1
    for doc in osslist:
        imei = doc['imei']
        sessionid = doc['sessionid']
        mysCol.update({'imei':imei,'sessionid':sessionid},{'$set':doc},upsert = True)

        print "insert doc: " + str(r)
        r += 1
    myCon.close()

    print "finish this insert update!"

def deleteMymongo(deletelist):
    myCon,mysCol = getMymongo()

    r = 1
    for sessionid in deletelist:
        mysCol.delete_many({'sessionid':sessionid})
        print "delete record: " + str(r)
        r += 1
    myCon.close()

    print "finish the delete"

def getTask1():
    deleteImei, ossList = getDelete()

    print len(deleteImei)
    print len(ossList)

    # thread.start_new_thread(updateMymongo,(ossList,))
    # thread.start_new_thread(deleteMymongo,(deleteImei,))

    deleteMymongo(deleteImei)
    updateMymongo(ossList)

    print "finish this round!"

if __name__ == "__main__":
    while(True):
        getTask1()
        time.sleep(1)
