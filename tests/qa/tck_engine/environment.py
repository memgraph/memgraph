import logging
import datetime
import time
import json
import sys
import os
from steps.test_parameters import TestParameters
from neo4j.v1 import GraphDatabase, basic_auth
from steps.graph_properties import GraphProperties
from test_results import TestResults

test_results = TestResults()

"""
Executes before every step. Checks if step is execution
step and sets context variable to true if it is.
"""
def before_step(context, step):
    context.execution_step = False
    if step.name == "executing query":
        context.execution_step = True


"""
Executes before every scenario. Initializes test parameters,
graph properties, exception and test execution time.
"""
def before_scenario(context, scenario):
    context.test_parameters = TestParameters()
    context.graph_properties = GraphProperties()
    context.exception = None
    context.execution_time = None


"""
Executes after every scenario. Pauses execution if flags are set.
Adds execution time to latency dict if it is not None.
"""
def after_scenario(context, scenario):
    test_results.add_test(scenario.status)
    if context.config.single_scenario or \
            (context.config.single_fail and scenario.status == "failed"):
        print("Press enter to continue")
        sys.stdin.readline()

    if context.execution_time is not None:
        context.js['data'][scenario.name] = {
            "execution_time": context.execution_time, "status": scenario.status
        }


"""
Executes after every feature. If flag is set, pauses before
executing next scenario.
"""
def after_feature(context, feature):
    if context.config.single_feature:
        print("Press enter to continue")
        sys.stdin.readline()


"""
Executes before running tests. Initializes driver and latency
dict and creates needed directories.
"""
def before_all(context):
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%Y_%m_%d__%H_%M_%S")
    latency_file = "latency/" + context.config.database + "/" + \
        get_test_suite(context) + "/" + timestamp + ".json"
    if not os.path.exists(os.path.dirname(latency_file)):
        os.makedirs(os.path.dirname(latency_file))
    context.driver = create_db_driver(context)
    context.latency_file = latency_file
    context.js = dict()
    context.js["metadata"] = dict()
    context.js["metadata"]["execution_time_unit"] = "seconds"
    context.js["data"] = dict()
    set_logging(context)


"""
Executes when testing is finished. Creates JSON files of test latency
and test results.
"""
def after_all(context):
    context.driver.close()
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%Y_%m_%d__%H_%M")

    test_suite = get_test_suite(context)
    file_name = context.config.output_folder + timestamp + \
        "-" + context.config.database + "-" + context.config.test_name + ".json"

    js = {
        "total": test_results.num_total(), "passed": test_results.num_passed(),
         "test_suite": test_suite, "timestamp": timestamp, "db": context.config.database}
    with open(file_name, 'w') as f:
        json.dump(js, f)

    with open(context.latency_file, "a") as f:
            json.dump(context.js, f)


"""
Returns test suite from a test root folder.
If test root is a feature file, name of file is returned without
.feature extension.
"""
def get_test_suite(context):
    root = context.config.root

    if root.endswith("/"):
        root = root[0:len(root) - 1]
    if root.endswith("features"):
        root = root[0: len(root) - len("features") - 1]

    test_suite = root.split('/')[-1]

    return test_suite


"""
Initializes log and sets logging level to debug.
"""
def set_logging(context):
    logging.basicConfig(level="DEBUG")
    log = logging.getLogger(__name__)
    context.log = log


"""
Creates database driver and returns it.
"""
def create_db_driver(context):
    uri = context.config.database_uri
    auth_token = basic_auth(
        context.config.database_username, context.config.database_password)
    if context.config.database == "neo4j" or context.config.database == "memgraph":
        driver = GraphDatabase.driver(uri, auth=auth_token, encrypted=0)
    else:
        raise "Unsupported database type"
    return driver
