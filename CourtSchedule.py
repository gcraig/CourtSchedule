#!/usr/bin/env python

import os
import sys
import re
import sqlite3
import glob
import time
import urllib

# A simple script to import court schedule data from Will County, Illinois.
# Fetches data from web, then saves it in an ETL table
# (c)2011. George Craig

import_path = "import/"
db_path = "db/schedule.db"

datafile_url = "http://www.willcountycircuitcourt.com/schedule/PUBLIC/%s.txt"
datafile_spec = "*.txt"
datafile_fields = (0,19), (20,29), (30,37), (38,45), (46,61)
datafile_format = None, formatdate, None, formattime, None

x = ((20, 29),
    ((30, 39), formatedate))

def parsedate(ln, dateptn):
    match = dateptn.search(ln[20:29])
    date = "%s/%s/%s" % match.group(1), match.group(2), match.group(3)
    return date

def fetchdata():
    print "fetching data..."
    for filename in xrange(ord('A'), ord('Z')+1):
        url = datafile_url % chr(filename)
        idx = url.rfind('/')
        datafile = url[idx+1:]
        print url, "->", datafile
        if not os.path.exists(import_path + datafile):
            urllib.urlretrieve(url, datafile, fetchstatus)

def importdata():
    print "importing data..."

    titleptn = re.compile(" NAME\s*DATE.*")
    dateptn = re.compile(r'(\d{1,2})\s*(\d{1,2})\s*(\d{2})')

    conn = sqlite3.connect(db_path)
    for datafile in glob.glob(os.path.join(import_path, datafile_spec)):
        print "reading and inserting: " + datafile
        f = open(datafile, 'r')
        for ln in f:
            if titleptn.match() or not ln.strip():
                continue
            else:
                name = ln[0 :19].strip()
                date = parsedate(ln, position, dateptn)
                room = ln[30:37].strip()
                hrs = ln[38:45].strip()
                ctime = hrs[0:-2] + ":" + hrs[-2:]
                ccase = ln[46:61].strip()
                parms = (cname, cdate, croom, ctime, ccase)
                cur = conn.cursor()
                cur.execute("insert into schedule (name, datetime, room, case) values (?,?,?,?)", parms)
        f.close()

def dumpdata():
    print "dumping data..."

def fetchstatus(x,y,z):
    print "% 3.1f%% of %d bytes\r" % (min(100, float(x * y) / z * 100), z),
    sys.stdout.flush()

if __name__ == "__main__":
    print "CourtScheduleImport"
    total = 0
    start = time.clock()
    fetchdata()
    importdata()
    end = time.clock()
    print end-start