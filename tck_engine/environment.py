import logging, datetime, time, json
from steps.test_parameters import TestParameters
from neo4j.v1 import GraphDatabase, basic_auth
from steps.graph_properties import GraphProperties
from test_results import TestResults

test_results = TestResults()

def before_scenario(context, step):
    context.test_parameters = TestParameters()
    context.graph_properties = GraphProperties()
    context.exception = None

def after_scenario(context, scenario):
    test_results.add_test(scenario.status)

def before_all(context):
    set_logging(context)
    context.driver = create_db_driver(context)
    
def after_all(context):
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime("%Y_%m_%d__%H_%M")
    file_name = context.config.output_folder + context.config.database + "_" + timestamp + ".json"
    js = {"total": test_results.num_total(), "passed": test_results.num_passed(), "test_suite": context.config.root}
    with open(file_name, 'w') as f:
        json.dump(js, f)

def set_logging(context):
    logging.basicConfig(level="DEBUG")
    log = logging.getLogger(__name__)
    context.log = log

def create_db_driver(context):
    uri = context.config.database_uri
    auth_token = basic_auth(context.config.database_username, context.config.database_password)
    if context.config.database == "neo4j":
        driver = GraphDatabase.driver(uri, auth=auth_token)
    else:
        #Memgraph
        pass
    return driver
