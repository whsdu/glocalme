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

def getTask1():

    clientGtbu = MongoClient('52.76.228.13',57111)

    dbGtbu = clientGtbu['ucl_oss_performancelog']
    colg1Gtbu = dbGtbu['t_term_vsim_estsucc']
#
#     g1CursorGtbu = colg1Gtbu.aggregate(
# [
#     {"$match":{
#         "$and":
#             [
#             {"mcc":{"$in":["208","262","222","240","235","234"]}},
#             {"succTime":{"$lte":1462060800}},
#             {"band":{"$in":[43,44,47,48,80,81,84,87,120,122,124,126,127,136,139,158,159,160]}}
#             ]
#         }
#     },
#     {"$group":{
#         "_id":{"mcc":"$mcc","band":"$band"},
#         "total":{"$sum":1}
#             }
#     },
#     {"$sort":{"_id.mcc":1,"_id.band":1}}
# ]
# )
#
    g1CursorGtbu = colg1Gtbu.aggregate(
[
    {"$match":{
        "$and":
            [
            {"succTime":{"$lte":1462060800}},
            {"band":{"$in":[43,44,47,48,80,81,84,87,120,122,124,126,127,136,139,158,159,160]}}
            ]
        }
    },
    {"$group":{
        "_id":"$band",
        "total":{"$sum":1}
            }
    },
    {"$sort":{"_id.band":1}}
]
)
    totalcounter = 0
    reslist = list()
    for doc in g1CursorGtbu:
        tmplist = list()
        print doc

        # mcc = doc['_id']['mcc']
        # band = doc['_id']['band']
        # total = doc['total']
        #
        # tmplist.append(mcc)
        # tmplist.append(band)
        # tmplist.append(total)

        band = doc['_id']
        total = doc['total']
        tmplist.append(band)
        tmplist.append(total)
        reslist.append(tmplist)

    print reslist

    filename = "testGTBu.xlsx"

    workbook = Workbook("./"+filename)
    sheet = workbook.add_worksheet()

    # sheet.write(0,0,"mcc")
    # sheet.write(0,1,"band")
    # sheet.write(0,2,"total")

    sheet.write(0,0,"band")
    sheet.write(0,1,"total")

    r =1
    for index,val in enumerate(reslist):
        # sheet.write(r,0,val[0])
        # sheet.write(r,1,val[1])
        # sheet.write(r,2,val[2])

        sheet.write(r,0,val[0])
        sheet.write(r,1,val[1])

        r += 1


    workbook.close()



if __name__ == "__main__":
    getTask1()
    #
    # schedule.every().hour.do(getTask1)
    # schedule.every().day.at("00:02").do(createDailyFloder)
    # while 1:
    #     schedule.run_pending()
    #     time.sleep(1)
