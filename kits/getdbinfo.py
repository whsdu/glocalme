#!/usr/bin/python
import json
import sys
def getdbinfo(datasource):
    
    hostinfor = {}
    readfrom = datasource


    flag = 0

    if readfrom == 'GTBU':
        flag += 1

    if readfrom == 'NON_GTBU':
        flag += 1

    if readfrom == 'REMOTE':
        flag += 1
    if readfrom == "MONGO_GTBU":
        flag = 2
    if readfrom == "MONGO_NONGTBU":
        flag = 2

    print readfrom
    print flag
    if flag == 0:
        print """ Usage: use key words GTBU,NON_GTBU,REMOTE to chose the
        database!!!"""
        exit(0)

    with open('./dbhost.json') as data_file:
        data = json.load(data_file)
    
        print readfrom
        
        if flag == 2:
            hostinfor["host"] = data[readfrom]["host"]
            hostinfor["port"] = data[readfrom]["port"]

            print hostinfor["host"]
            print hostinfor["port"]
        else:
            hostinfor["usr"] = data[readfrom]["usr"]
            hostinfor["pwd"] = data[readfrom]["pwd"]
            hostinfor["host"] = data[readfrom]["host"]
            hostinfor["port"] = data[readfrom]["port"]

    return hostinfor


if __name__=="__main__":
    gtbu = sys.argv[1]
    print gtbu

    hostinfor = getdbinfo(gtbu)
    
    for key,value in hostinfor.iteritems():
        print key +" : " + str(value)
