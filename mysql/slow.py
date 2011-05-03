import re
import sqlite3
import codecs
import argparse
import sys

lex = {
    "header1" : "[\w/].*started with:$",
    "header2" : "Tcp port:.*$",
    "header3" : "Time                 Id Command    Argument$",
    "date" : r"(?P<date>\d{6})",
    "time" : r"(?P<time>\d{1,2}:\d{2}:\d{2})",
    "float" : r"(\d*\.\d*)",
    "int" : r"(\d+)",
    "str" : r"\w*",
}
lex.update({
    "qtime" : r"(?P<qtime>%(float)s)" % lex,
    "ltime" : r"(?P<ltime>%(float)s)" % lex,
    "rsent" : r"(?P<rsent>%(int)s)" % lex,
    "rexam" : r"(?P<rexam>%(int)s)" % lex,
    "host" : r"(?P<host>%(str)s)" % lex,
    "user" : r"(?P<user>%(str)s)" % lex,
    "schema" : r"(?P<schema>%(str)s)" % lex,
    "db" : r"(?P<db>%(str)s)" % lex,
    "timestamp" : r"(?P<timestamp>%(int)s)" % lex,
    "sql" : r"(?P<sql>.*)",
    "param" : r"'.*?(?<!\\)'",
})

def params(sql):
    "handles params in sql"
    return re.findall(lex["param"],sql)

def sql(sql):
    "strip params"
    return re.sub(lex["param"],"'?'",sql).strip()

### RULES ###

def row_sql(state):
    "Handles sql"
    regexp = re.compile("%(sql)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["params"] +=  params(match.group("sql"))
        state["sql"] += " \n" if state["sql"] else ""
        state["sql"] += sql(match.group("sql"))
        return True
    return False

def row_query(state):
    "Handles # Query_time: Lock_time: Rows_sent: Rows_examined:"
    regexp = re.compile("# Query_time: %(qtime)s  Lock_time: %(ltime)s Rows_sent: %(rsent)s  Rows_examined: %(rexam)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["qtime"] = match.group("qtime")
        state["ltime"] = match.group("ltime")
        state["rsent"] = match.group("rsent")
        state["rexam"] = match.group("rexam")
        return True
    return False

def row_db(state):
    "Handles use"
    regexp = re.compile("use %(db)s;" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["db"] = match.group("db")
        return True
    return False

def row_timestamp(state):
    "Handles SET timestamp"
    regexp = re.compile("SET timestamp=%(timestamp)s;" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["timestamp"] = match.group("timestamp")
        return True
    return False

def row_user(state):
    "Handles User@Host"
    regexp = re.compile("# User@Host: %(user)s\\[%(str)s\\] @ %(host)s \\[%(str)s\\]" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["host"] = match.group("host")
        state["user"] = match.group("user")
        return True
    return False

def row_time(state):
    "Handles Time: date time"
    regexp = re.compile("# Time: %(date)s %(time)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["date"] = match.group("date")
        state["time"] = match.group("time")
        return True
    return False

def row_header(state):
    "Skips header"
    regexp1 = re.compile("%(header1)s" % lex)
    regexp2 = re.compile("%(header2)s" % lex)
    regexp3 = re.compile("%(header3)s" % lex)
    match = re.match(regexp1, state["buf"]) or \
        re.match(regexp2, state["buf"]) or \
        re.match(regexp3, state["buf"])
    if match:
        return True
    return False
### DB ###

def connect(ofile):
    "Connent and init database"
    conn = sqlite3.connect(ofile)
    cur = conn.cursor()
    row = cur.execute("select 1 from sqlite_master where name='slow'")
    if row.fetchone():
        cur.execute("delete from slow")
        cur.execute("delete from slowparams")
    else:
        cur.execute("create table slow "+
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, "+
            "host Text, user TEXT, db TEXT, timestamp INTEGER, qtime REAL, ltime REAL, "+
            "rsent INTEGER, rexam INTEGER, sql TEXT)")
        cur.execute("create table slowparams "+
            "(id_log INTEGER, id INTEGER, param TEXT)")
    conn.commit()
    return conn


def push_row(state,conn):
    "Push row to database"
    if not state["sql"]:
        return

    cur = conn.cursor()
    cur.execute(
        "insert into slow(date,time,host,user,db,timestamp,qtime,ltime,rsent,rexam,sql) " +
        "values(:date,:time,:host,:user,:db,:timestamp,:qtime,:ltime,:rsent,:rexam,:sql)",
        state)
    sql_id = cur.lastrowid
    for id, param in enumerate(state["params"]):
        cur.execute("insert into slowparams(id_log,id,param) " +
        "values(:id_log,:id,:param)",
        {"id_log":sql_id,"id":id,"param":param})

### ENTRY POINT ###
def parse(ifile,ofile,cp):
    rowcount = 0
    state = {
        "date":None,
        "time":None,
        "host":None,
        "user":None,
        "qtime":None,
        "ltime":None,
        "rsent":None,
        "rexam":None,
        "db":None,
        "timestamp":None,
        "sql":"",
        "buf":"",
        "params":[],
    }

    with codecs.open(ifile,"r",cp,buffering=1024*1024) as f:
        conn = connect(ofile)
        for line in f:
            state_old = state.copy()
            state["buf"] = line
            if row_header(state):
                if state["sql"]:
                    push_row(state_old, conn)
                    state["sql"] = ""
            elif row_time(state):
                if state["sql"]:
                    push_row(state_old, conn)
                    state["sql"] = ""
            elif row_user(state):
                if state["sql"]:
                    push_row(state_old, conn)
                    state["sql"] = ""
            elif row_db(state):
                pass
            elif row_timestamp(state):
                pass
            elif row_query(state):
                pass
            elif row_sql(state):
                pass

            rowcount  += 1
            if rowcount % 100000 == 0:
                conn.commit()
        push_row(state_old,conn)
        conn.commit()
        conn.close()

    print "Total %d rows processed" % rowcount

