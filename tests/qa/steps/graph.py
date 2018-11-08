# -*- coding: utf-8 -*-

import database
import os
from behave import given


def clear_graph(context):
    database.query("MATCH (n) DETACH DELETE n", context)
    if context.exception is not None:
        context.exception = None
        database.query("MATCH (n) DETACH DELETE n", context)


@given('an empty graph')
def empty_graph_step(context):
    clear_graph(context)


@given('any graph')
def any_graph_step(context):
    clear_graph(context)


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
    clear_graph(context)
    path = os.path.join(context.config.test_directory, "graphs",
                        name + ".cypher")

    q_marks = ["'", '"', '`']

    with open(path, 'r') as f:
        content = f.read().replace('\n', ' ')
        single_query = ''
        quote = None
        i = 0
        while i < len(content):
            ch = content[i]
            if ch == '\\' and i != len(content) - 1 and \
                    content[i + 1] in q_marks:
                single_query += ch + content[i + 1]
                i += 2
            else:
                single_query += ch
                if quote == ch:
                    quote = None
                elif ch in q_marks and quote is None:
                    quote = ch
                if ch == ';' and quote is None:
                    database.query(single_query, context)
                    single_query = ''
                i += 1
        if single_query.strip() != '':
            database.query(single_query, context)
