import argparse
import sys
from mysql import query,slow

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse log file')
    parser.add_argument("-t", dest="ftype", required = True,
        help="log format", choices = ["mysql.log","mysql.slow"])
    parser.add_argument("-i",metavar="log_file", dest="ifile",
        help="log file", required = True)
    parser.add_argument("-o",metavar="db_file",dest="ofile",
        default="mysql.db",help="SQLite file")
    parser.add_argument("-cp",metavar="code_page",dest="cp",
        default="cp1251",help="codepage")

    args = parser.parse_args()

    if args.ftype == "mysql.log":
        query.parse(args.ifile,args.ofile,args.cp)
    elif args.ftype == "mysql.slow":
        slow.parse(args.ifile,args.ofile,args.cp)