import re
import sqlite3
import codecs
import argparse
import sys

### TOKENS ###

re_timestamp = re.compile(r"(?P<date>\d{6})\s*(?P<time>\d{1,2}:\d{2}:\d{2})\t")
def timestamp(state):
    "Handles timsestamp hh:mm:ss"
    match = re.match(re_timestamp, state["buf"])
    if match:
        state["date"] = match.group("date")
        state["time"] = match.group("time")
        state["buf"] = state["buf"][match.end():]
        return True
    return False

re_num = re.compile(r"(\t{2})? *(?P<num>\d+)")
def num(state):
    "Handles 5 space left padded number"
    match = re.match(re_num, state["buf"])
    if match:
        state["num"] = match.group("num")
        state["buf"] = state["buf"][match.end():]
        return True
    return False

re_command = re.compile(r" (?P<command>Connect|Init DB|Query|Quit)\t")
def command(state):
    "Handle database command"
    match = re.match(re_command, state["buf"])
    if match:
        state["command"] = match.group("command")
        state["buf"] = state["buf"][match.end():]
        state["sql"] = ""
        state["params"] = []
    return None

re_sql = re.compile(r"(?P<sql>.*)$")
re_param = re.compile(r"'(?:[^'\\]|\\')*'")
def sql(state):
    "Handles sql with params in it"
    state["params"] +=  re.findall(re_param,state["buf"])
    state["buf"] = re.sub(re_param,"'?'",state["buf"])

    if state["sql"]:
        state["sql"]+=" \n"
    state["sql"]+=state["buf"].strip()
    return True

### LOG ROWS ###

def row_sql(state):
    "Handles \\t\\t\\t sql\\n"
    if sql(state):
        return True
    return False

def row_short(state):
    "Handles \\t\\t num command sql\\n"
    if num(state):
        command(state)
        return row_sql(state)
    return False

def row_full(state):
    "Handles date time num command sql\\n"
    if timestamp(state):
        return row_short(state)
    return False

### DB MANIPULATION ###

def connect(ofile):
    "Connent and init database"
    conn = sqlite3.connect(ofile)
    cur = conn.cursor()
    row = cur.execute("select 1 from sqlite_master where name='log'")
    if row.fetchone():
        cur.execute("delete from log")
        cur.execute("delete from params")
    else:
        cur.execute("create table log "+
            "(date TEXT, time TEXT, id INTEGER, command TEXT, sql TEXT)")
        cur.execute("create table params "+
            "(id_log INTEGER, id INTEGER, param TEXT)")
    conn.commit()
    return conn

def push_row(state,conn):
    "Push row to database"
    if not state["num"]:
        return

    cur = conn.cursor()
    cur.execute(
        "insert into log(date,time,id,command,sql) " +
        "values(:date,:time,:num,:command,:sql)",
        state)
    for id, param in enumerate(state["params"]):
        cur.execute("insert into params(id_log,id,param) " +
        "values(:id_log,:id,:param)",
        {"id_log":state["num"],"id":id,"param":param})

### ENTRY POINT ###

def argparser():
    parser = argparse.ArgumentParser(description='Parse mysql log file')
    parser.add_argument("-i",metavar="log_file", dest="ifile",
        help="Log file", required=True)
    parser.add_argument("-o",metavar="db_file",dest="ofile",
        default="mysql.db",help="SQLite file")
    parser.add_argument("-cp",metavar="code_page",dest="cp",
        default="cp1251",help="codepage")
    return parser

def parse(ifile,ofile,cp):
    conn = connect(ofile)
    rowcount = 0
    state = {
        "date":None,
        "time":None,
        "num":None,
        "command":None,
        "sql":"",
        "buf":"",
        "params":[],
    }

    with codecs.open(ifile,"r",cp,buffering=1024*1024) as f:
        #skip header
        for i in range(3):
            f.readline()
        for line in f:
            state_old = state.copy()
            state["buf"] = line
            if row_full(state):
                push_row(state_old, conn)
            elif row_short(state):
                push_row(state_old, conn)
            else:
                row_sql(state)
            rowcount  += 1
            if rowcount % 100000 == 0:
                conn.commit()
        push_row(state_old,conn)

    conn.commit()
    conn.close()
    print "Total %d rows processed" % rowcount

if __name__ == "__main__":
    args = argparser().parse_args()
    parse(args.ifile,args.ofile,args.cp)