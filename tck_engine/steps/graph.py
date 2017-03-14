import database
from graph_properties import GraphProperties
from behave import *


@given('an empty graph')
def empty_graph_step(context):
    database.query("MATCH (n) DETACH DELETE n", context)
    context.graph_properties.set_beginning_parameters()

@given('any graph')
def any_graph_step(context):
    database.query("MATCH (n) DETACH DELETE n", context)
    context.graph_properties.set_beginning_parameters()
