import database
import os
from behave import *


@given('an empty graph')
def empty_graph_step(context):
    database.query("MATCH (n) DETACH DELETE n", context)
    context.graph_properties.set_beginning_parameters()


@given('any graph')
def any_graph_step(context):
    database.query("MATCH (n) DETACH DELETE n", context)
    context.graph_properties.set_beginning_parameters()


@given('graph "{name}"')
def graph_name(context, name):
    create_graph(name, context)


def create_graph(name, context):
    """
    Function deletes everything from database and creates a new
    graph. Graph file name is an argument of function. Function
    executes queries written in a .cypher file separated by ';'
    and sets graph properties to beginning values.
    """
    database.query("MATCH (n) DETACH DELETE n", context)
    path = find_graph_path(name, context.config.graphs_root)

    q_marks = ["'", '"', '`']

    with open(path, 'r') as f:
        content = f.read().replace('\n', ' ')
        q = ''
        in_string = False
        i = 0
        while i < len(content):
            ch = content[i]
            if ch == '\\' and i != len(content) - 1 and content[i + 1] in q_marks:
                q += ch + content[i + 1]
                i += 2
            else:
                q += ch
                if in_string and q_mark == ch:
                    in_string = False
                elif ch in q_marks:
                    in_string = True
                if ch == ';' and not in_string:
                    database.query(q, context)
                    q = ''
                i += 1
        if q.strip() != '':
            database.query(q, context)
    context.graph_properties.set_beginning_parameters()


def find_graph_path(name, path):
    """
    Function returns path to .cypher file with given name in
    given folder or subfolders. Argument path is path to a given
    folder.
    """
    for root, dirs, files in os.walk(path):
        if name + '.cypher' in files:
            return root + '/' + name + '.cypher'
