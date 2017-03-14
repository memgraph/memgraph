from flask import Flask, jsonify, request
import os, json
from argparse import ArgumentParser

app = Flask(__name__)
"""
Script runs web application used for getting test results. 
File with results is "../tck_engine/results". 
Application lists files with results or returnes result
file as json.
Default host is 127.0.0.1, and default port is 5000. 
Host and port can be passed as arguments of a script.
"""

path = "../tck_engine/results"

def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--host", default="127.0.0.1", help="Application host ip, default is 127.0.0.1.")
    argp.add_argument("--port", default="5000", help="Application host ip, default is 5000.")
    return argp.parse_args()


@app.route("/results")
def results():
    """
    Function accessed with route /result. Function lists
    names of last tail result files added to results and 
    separeted by whitespace. Tail is a parameter given in 
    route. If tail is not given, function lists all files
    from the last added file.

    @return:
        string of file names separated by whitespace
    """
    tail = request.args.get("tail")
    return list_to_str(os.listdir(path), tail)

@app.route("/results/<dbname>")
def results_for_db(dbname):
    """
    Function accessed with route /result/<dbname>. Function 
    lists names of last tail result files of database <dbname> 
    added to results and separeted by whitespace. Tail is a 
    parameter given in route. If tail is not given, function 
    lists all files of database <dbname> from the last added 
    file.

    @param dbname:
        string, database name
    @return:
        string of file names separated by whitespace
    """
    tail = request.args.get("tail")
    return list_to_str(([f for f in os.listdir(path) if f.startswith(dbname)]), tail)

@app.route("/results/<dbname>/<timestamp>")
def result(dbname, timestamp):
    """
    Function accessed with route /results/<dbname>/<timestamp>
    Returns json of result file with name <dbname>_<timestamp>.json. 
    <timestamp> is in format yyyy_mm_dd__HH_MM.

    @param dbname:
        string, database name
    @param timestamp:
        string, timestamp from description
    @return:
        json of a file.
    """
    fname = dbname + "_" + timestamp + ".json"
    with open(path + "/" + fname) as f:
        json_data = json.load(f)
        return jsonify(json_data)

def list_to_str(l, tail):
    """
    Function returns first tail results of list l in decreasing 
    order as string separated by whitespace. If tail is None, 
    function returns string of whole list.

    @param l:
        list to return as string
    @param tail:
        number of results
    @return:
        list as string
    """
    l.sort()
    l.reverse()
    if tail is None:
        tail = len(l)
    tail = max(tail, len(l))
    return ' '.join(l[0:int(tail)])

def main():
    args = parse_args()
    app.run(
        host = args.host,
        port = int(args.port)
    )

if __name__ == "__main__":
    main()
