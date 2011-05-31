#!/usr/bin/env python

import os
import sys
import re
import sqlite3
import glob
import time
import urllib

# A simple ETL script to import court schedule data from Will County, Illinois.
# (c)2011. George Craig <georgeacraig@gmail.com>

import_path = "import/"
db_path = "db/schedule.db"

datafile_url = "http://www.willcountycircuitcourt.com/schedule/PUBLIC/%s.txt"
datafile_insert = "insert into schedule (schedule_name, schedule_date, schedule_room, schedule_time, schedule_case) values (?,?,?,?,?)"
datafile_spec = "*.txt"
datafile_sql = "select count(schedule_name) from schedule"

def fetchdata():
    print "fetching data..."
    for filename in xrange(ord('A'), ord('Z')+1):
        url = datafile_url % chr(filename)
        idx = url.rfind('/')
        datafile = import_path + url[idx+1:]
        print url, "->", datafile
        if not os.path.exists(datafile):
            urllib.urlretrieve(url, datafile, fetchstatus)

def importdata():
    print "importing data..."
    titleptn = re.compile(" NAME\s*DATE.*")
    totctr = 0

    try:
        conn = sqlite3.connect(db_path)
        for datafile in glob.glob(os.path.join(import_path, datafile_spec)):
            print "reading %s" % datafile
            f = open(datafile, 'r')
            ctr = 0
            for ln in f:
                if not ln.strip() or titleptn.match(ln):
                    continue
                else:
                    ctr += 1
                    _name = ln[0:19].strip()
                    _date = "".join((ln[20:22].strip(), "/", ln[23:25].strip(), "/20", ln[27:29].strip()))
                    _room = ln[30:37].strip()
                    _time = ln[38:45].strip()
                    _time = _time[0:-2] + ":" + _time[-2:]
                    _case = ln[46:61].strip()
                    parms = (_name, _date, _room, _time, _case)
                    cur = conn.cursor()
                    cur.execute(datafile_insert, parms)
            f.close()
            totctr += ctr
            print "read and insert: %d recs" % ctr
            sys.stdout.flush()

        cur = conn.cursor()
        cur.execute(datafile_sql)
        for row in cur:
            print  "total records in db: %d" % row[0]
        print "total records read from file: %d" % totctr
    finally:
        conn.commit()
        conn.close()

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
    elapsed = end - start
    print "%.2f secs" % elapsed