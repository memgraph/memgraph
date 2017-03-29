import logging
import datetime
import time
import json
from steps.test_parameters import TestParameters
from neo4j.v1 import GraphDatabase, basic_auth
from steps.graph_properties import GraphProperties
from test_results import TestResults

test_results = TestResults()


def before_scenario(context, step):
    context.test_parameters = TestParameters()
    context.graph_properties = GraphProperties()
    context.exception = None
    context.driver = create_db_driver(context)
    context.session = context.driver.session()


def after_scenario(context, scenario):
    test_results.add_test(scenario.status)
    context.session.close()



def before_all(context):
    set_logging(context)


def after_all(context):
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime("%Y_%m_%d__%H_%M")

    root = context.config.root

    if root.endswith("/"):
        root = root[0:len(root) - 1]
    if root.endswith("features"):
        root = root[0: len(root) - len("features") - 1]

    test_suite = root.split('/')[-1]
    file_name = context.config.output_folder + timestamp + \
        "-" + context.config.database + "-" + test_suite + ".json"

    js = {
        "total": test_results.num_total(), "passed": test_results.num_passed(),
         "test_suite": test_suite, "timestamp": timestamp, "db": context.config.database}
    with open(file_name, 'w') as f:
        json.dump(js, f)


def set_logging(context):
    logging.basicConfig(level="DEBUG")
    log = logging.getLogger(__name__)
    context.log = log


def create_db_driver(context):
    uri = context.config.database_uri
    auth_token = basic_auth(
        context.config.database_username, context.config.database_password)
    if context.config.database == "neo4j" or context.config.database == "memgraph":
        driver = GraphDatabase.driver(uri, auth=auth_token, encrypted=0)
    else:
        raise "Unsupported database type"
    return driver
