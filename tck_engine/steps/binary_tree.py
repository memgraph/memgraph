from behave import *
import graph

@given(u'the binary-tree-1 graph')
def step_impl(context):
   graph.create_graph('binary-tree-1', context) 

@given(u'the binary-tree-2 graph')
def step_impl(context):
    graph.create_graph('binary-tree-2', context)
