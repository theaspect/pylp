import re
import sqlite3
import codecs
import argparse
import sys

lex = {
    "header" : "[\w/].*",
    "date" : r"(?P<date>\d{6})",
    "time" : r"(?P<time>\d{1,2}:\d{2}:\d{2})",
    "num" : r" *(?P<num>\d+)",
    "command" : r"(?P<command>Connect|Init DB|Query|Quit)",
    "sql" : r"(?P<sql>.*)",
    "param" : r"'.*?(?<!\\)'",
}

def params(sql):
    "handles params in sql"
    return re.findall(lex["param"],sql)

def sql(sql):
    "strip params"
    return re.sub(lex["param"],"'?'",sql).strip()

### RULES ###

def row_sql(state):
    "Handles \\t\\t\\t sql\\n"
    regexp = re.compile("\t\t\t%(sql)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["params"] +=  params(match.group("sql"))
        state["sql"] += " \n" if state["sql"] else ""
        state["sql"] += sql(match.group("sql"))
        return True
    return False

def row_short(state):
    "Handles \\t\\t num command sql\\n"
    regexp = re.compile("\t\t%(num)s %(command)s\t%(sql)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["num"] = match.group("num")
        state["command"] = match.group("command")
        state["params"] =  params(match.group("sql"))
        state["sql"] = sql(match.group("sql"))
        return True
    return False

def row_full(state):
    "Handles date time num command sql\\n"
    regexp = re.compile("%(date)s  %(time)s\t%(num)s %(command)s\t%(sql)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        state["date"] = match.group("date")
        state["time"] = match.group("time")
        state["num"] = match.group("num")
        state["command"] = match.group("command")
        state["params"] =  params(match.group("sql"))
        state["sql"] = sql(match.group("sql"))
        return True
    return False

def row_header(state):
    "Skips header"
    regexp = re.compile("%(header)s" % lex)
    match = re.match(regexp, state["buf"])
    if match:
        return True
    return False
### DB ###

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
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, time TEXT, num INTEGER, command TEXT, sql TEXT)")
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
        "insert into log(date,time,num,command,sql) " +
        "values(:date,:time,:num,:command,:sql)",
        state)
    sql_id = cur.lastrowid
    for id, param in enumerate(state["params"]):
        cur.execute("insert into params(id_log,id,param) " +
        "values(:id_log,:id,:param)",
        {"id_log":sql_id,"id":id,"param":param})

### ENTRY POINT ###
def parse(ifile,ofile,cp):
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
        conn = connect(ofile)
        for line in f:
            state_old = state.copy()
            state["buf"] = line
            if row_full(state):
                push_row(state_old, conn)
            elif row_short(state):
                push_row(state_old, conn)
            elif row_sql(state):
                pass
            elif row_header(state):
                push_row(state_old, conn)

            rowcount  += 1
            if rowcount % 100000 == 0:
                conn.commit()
        push_row(state_old,conn)
        conn.commit()
        conn.close()

    print "Total %d rows processed" % rowcount
