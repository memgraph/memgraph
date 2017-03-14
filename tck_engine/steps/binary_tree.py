from behave import *
import os
import database

@given(u'the binary-tree-1 graph')
def step_impl(context):
   create_given_graph('binary-tree-1', context) 

@given(u'the binary-tree-2 graph')
def step_impl(context):
    create_given_graph('binary-tree-2', context)

def create_given_graph(name, context):
    """
    Function deletes everything from database and creates a new
    graph. Graph file name is an argument of function. Function 
    executes query written in a .cypher file and sets graph 
    properties to beginning values.
    """
    database.query("MATCH (n) DETACH DELETE n", context)  
    path = find_graph_path(name, context.config.graphs_root)
    with open(path, 'r') as f:
        #TODO not good solution for all queries
        q = f.read().replace('\n', ' ')
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
