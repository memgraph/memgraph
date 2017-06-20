from flask import Flask, jsonify, request
import os
import json
from argparse import ArgumentParser

app = Flask(__name__)
"""
Script runs web application used for getting test results.
Default file with results is "../tck_engine/results".
Application lists files with results or returnes result
file as json.
Default host is 0.0.0.0, and default port is 5000.
Host, port and result file can be passed as arguments of a script.
"""


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--host", default="0.0.0.0",
                      help="Application host ip, default is 0.0.0.0.")
    argp.add_argument("--port", default="5000",
                      help="Application host ip, default is 5000.")
    argp.add_argument("--results", default="tck_engine/results",
                      help="Path where the results are stored.")
    return argp.parse_args()


@app.route("/results")
def results():
    """
    Function accessed with route /result. Function lists
    last tail result files added. Tail is a parameter given
    in the route. If the tail is not given, function lists
    last ten added files. If parameter last is true, only last
    test result is returned.

    @return:
        json list of test results
    """
    l = [f for f in os.listdir(app.config["RESULTS_PATH"])
         if f != ".gitignore"]
    return get_ret_list(l)


@app.route("/results/<dbname>")
def results_for_db(dbname):
    """
    Function accessed with route /result/<dbname>. Function
    lists last tail result files added of database <dbname.
    Tail is a parameter given in the route. If tail is not
    given, function lists last ten added files of database
    <dbname>. If param last is true, only last test result
    is returned.

    @param dbname:
        string, database name
    @return:
        json list of test results
    """
    print(os.listdir(app.config["RESULTS_PATH"]))
    l = [f for f in os.listdir(app.config["RESULTS_PATH"])
         if f != ".gitignore" and f.split('-')[1] == dbname]
    return get_ret_list(l)


@app.route("/results/<dbname>/<test_suite>")
def result(dbname, test_suite):
    """
    Function accessed with route /results/<dbname>/<test_suite>
    Function lists last tail result files added of database <dbname>
    tested on <test_suite>. Tail is a parameter given in the
    route. If tail is not given, function lists last ten results.
    If param last is true, only last test result is returned.

    @param dbname:
        string, database name
    @param test_suite:
        string, test suite of result file
    @return:
        json list of test results
    """
    fname = dbname + "-" + test_suite + ".json"
    l = [f for f in os.listdir(app.config["RESULTS_PATH"])
         if f.endswith(fname)]
    return get_ret_list(l)


def get_ret_list(l):
    """
    Function returns json list of test results of files given in
    list l.

    @param l:
        list of file names
    @return:
        json list of test results
    """
    l.sort()
    ret_list = []
    for f in l:
        ret_list.append(get_content(f))
    return list_to_json(
        ret_list,
        request.args.get("last"),
        request.args.get("tail")
    )


def get_content(fname):
    """
    Function returns data of the json file fname located in
    results directory in json format.

    @param fname:
        string, name of the file
    @return:
        json of a file
    """
    with open(app.config["RESULTS_PATH"] + "/" + fname) as f:
        json_data = json.load(f)
        return json_data


def list_to_json(l, last, tail):
    """
    Function converts list to json format. If last is true,
    only the first item in list is returned list, else last
    tail results are returned in json list. If tail is not
    given, last ten results are returned in json list.

    @param l:
        list to convert to json format
    @param last:
        string from description
    @param tail:
        string from description
    """
    l.reverse()
    if len(l) == 0:
        return jsonify(results=[])

    if last == "true":
        return jsonify(results=[l[0]])

    if tail is None:
        tail = 10
    else:
        tail = int(tail)
        if tail > len(l):
            tail = len(l)
    return jsonify(results=l[0:tail])


def main():
    args = parse_args()
    app.config.update(dict(
        RESULTS_PATH=os.path.abspath(args.results)
    ))
    app.run(
        host=args.host,
        port=int(args.port)
    )


if __name__ == "__main__":
    main()
