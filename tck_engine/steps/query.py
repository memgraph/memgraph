import json

import database, parser
from behave import *
from neo4j.v1.types import Node, Path, Relationship

@given('parameters are')
def parameters_step(context):
    context.test_parameters.set_parameters_from_table(context.table)

@then('parameters are') 
def parameters_step(context):
    context.test_parameters.set_parameters_from_table(context.table)

@step('having executed')
def having_executed_step(context):
    context.results = database.query(context.text, context, context.test_parameters.get_parameters())
    context.graph_properties.set_beginning_parameters()

@when('executing query')
def executing_query_step(context):
    context.results = database.query(context.text, context, context.test_parameters.get_parameters())

@when('executing control query')
def executing_query_step(context):
    context.results = database.query(context.text, context, context.test_parameters.get_parameters())

def parse_props(prop_json):
    """
    Function used to parse properties from map of properties to string.

    @param prop_json:
        Dictionary, map of properties of graph element.
    @return:
        Map of properties in string format.
    """
    if not prop_json:
        return ""
    properties = "{"
    for prop in prop_json:
        print (prop + " " + str(prop_json[prop]))
        if prop_json[prop] is None:
            properties += prop + ": null, "
        elif isinstance(prop_json[prop], str):
            properties += prop + ": " + "'" + prop_json[prop] + "', "
        elif isinstance(prop_json[prop], bool):
            if prop_json[prop] == True:
                properties += prop + ": true, "
            else:
                properties += prop + ": false, "
        else:
            properties += prop + ": " + str(prop_json[prop]) + ", "
    properties = properties[:-2]
    properties += "}"
    return properties


def to_string(element):
    """
    Function used to parse result from database to string. Format of
    the string is same as format of a result given in cucumber test.

    @param element:
        Can be None, string, bool, number, list, dict or any of
        neo4j.v1.types Node, Path, Relationship. Element which
        will be parsed.
    @return:
        String of parsed element.
    """
    if element is None:
        #parsing None
        return "null"

    if isinstance(element, Node):
        #parsing Node
        sol = "("
        if element.labels:
            sol += ':' + ': '.join(element.labels)
      
        if element.properties:
            if element.labels:
                sol += ' '
            sol +=  parse_props(element.properties)

        sol += ")"
        return sol

    elif isinstance(element, Relationship):
        #parsing Relationship
        sol = "[:"
        if element.type:
            sol += element.type
        if element.properties:
            sol += ' '
        sol += parse_props(element.properties)
        sol += "]"
        return sol

    elif isinstance(element, Path):
        #parsing Path
        # TODO add longer paths
        edges = []
        nodes = []

        for rel in element.relationships:
            edges.append([rel.start, to_string(rel)])

        for node in element.nodes:
            nodes.append([node.id, to_string(node)])

        sol = "<"
        for i in range(0, len(edges)):
            if edges[i][0] == nodes[i][0]:
                sol += nodes[i][1] + "-" + edges[i][1] + "->"
            else:
                sol += nodes[i][1] + "<-" + edges[i][1] + "-"
            
        sol += nodes[len(edges)][1]
        sol += ">"
        
        return sol

    elif isinstance(element, str):
        #parsing string
        return "'" + element + "'"
    
    elif isinstance(element, list):
        #parsing list
        sol = '['
        el_str = []
        for el in element:
            el_str.append(to_string(el))
        sol += ', '.join(el_str)
        sol += ']'

        return sol

    elif isinstance(element, bool):
        #parsing bool
        if element:
            return "true"
        return "false"

    elif isinstance(element, dict):
        #parsing map
        if len(element) == 0:
            return '{}'
        sol = '{'
        for key, val in element.items():
            sol += key + ':' + to_string(val) + ','
        sol = sol[:-1] + '}'
        return sol
    
    elif isinstance(element, float):
        #parsing float, scientific
        if 'e' in str(element):
            if str(element)[-3] == '-':
                zeroes = int(str(element)[-2:])-1
                num_str = ''
                if str(element)[0] == '-':
                    num_str += '-'
                num_str += '.' + zeroes * '0' + str(element)[:-4].replace("-", "").replace(".", "")
                return num_str


    return str(element)


def get_result_rows(context, ignore_order):
    """
    Function returns results from database queries stored in context
    as list.

    @param context:
        behave.runner.Context, behave context.
    @param ignore_order:
        bool, ignore order in result and expected list.
    @return 
        Result rows.
    """
    result_rows = []
    for result in context.results:
        keys = result.keys()
        values = result.values()
        for i in range(0, len(keys)):
            result_rows.append(keys[i] + ":" + parser.parse(to_string(values[i]).replace("\n", "\\n").replace(" ", ""), ignore_order))
    return result_rows


def get_expected_rows(context, ignore_order):
    """
    Fuction returns expected results as list from context table.

    @param context:
        behave.runner.Context, behave context.
    @param ignore_order:
        bool, ignore order in result and expected list.
    @return
        Expected rows
    """
    expected_rows = []
    for row in context.table:
        for col in context.table.headings:
            expected_rows.append(col + ":" + parser.parse(row[col].replace(" ", ""), ignore_order))
    return expected_rows

def validate(context, ignore_order):
    """
    Function used to check if results from database are same
    as expected results in any order.

    @param context:
        behave.runner.Context, behave context.
    @param ignore_order:
        bool, ignore order in result and expected list.
    """
    result_rows = get_result_rows(context, ignore_order)
    expected_rows = get_expected_rows(context, ignore_order)

    context.log.info("Expected: %s", str(expected_rows))
    context.log.info("Results:  %s", str(result_rows))
    assert(len(expected_rows) ==  len(result_rows))

    for i in range(0, len(expected_rows)):
        if expected_rows[i] in result_rows:
            result_rows.remove(expected_rows[i])
        else:
            assert(False)


def validate_in_order(context, ignore_order):
    """
    Function used to check if results from database are same
    as exected results. First result from database should be
    first in expected results list etc.

    @param context:
        behave.runner.Context, behave context.
    @param ignore_order:
        bool, ignore order in result and expected list.
    """
    result_rows = get_result_rows(context, ignore_order)
    expected_rows = get_expected_rows(context, ignore_order)

    context.log.info("Expected: %s", str(expected_rows))
    context.log.info("Results:  %s", str(result_rows))
    assert(len(expected_rows) ==  len(result_rows))

    for i in range(0, len(expected_rows)):
        if expected_rows[i] != result_rows[i]:
            assert(False)

@then('the result should be')
def expected_result_step(context):
    validate(context, False)
    check_exception(context)

@then('the result should be, in order')
def expected_result_step(context):
    validate_in_order(context, False)
    check_exception(context)

@then('the result should be (ignoring element order for lists)')
def expected_result_step(context):
    validate(context, True)
    check_exception(context)

def check_exception(context):
    if context.exception is not None:
        context.log.info("Exception when eqecuting query!")
        assert(False)

@then('the result should be empty')
def empty_result_step(context):
    assert(len(context.results) == 0)
    check_exception(context)

def side_effects_number(prop, table):
    """
    Function returns an expected list of side effects for property prop
    from a table given in a cucumber test.

    @param prop:
        String, roperty from description, can be nodes, relationships,
        labels or properties.
    @param table:
        behave.model.Table, context table with side effects.
    @return 
        Description.
    """
    ret = []
    for row in table:
        sign = -1
        if row[0][0] == '+':
            sign = 1
        if row[0][1:] == prop:
            ret.append(int(row[1])*sign)
    sign = -1
    row = table.headings
    if row[0][0] == '+':
        sign = 1
    if row[0][1:] == prop:
        ret.append(int(row[1])*sign)
    return ret

@then('the side effects should be')
def side_effects_step(context):
    if context.config.no_side_effects:
        return
    table = context.table
    #get side effects from db queries
    nodes_dif = side_effects_number("nodes", table)
    relationships_dif = side_effects_number("relationships", table)
    labels_dif = side_effects_number("labels", table)
    properties_dif = side_effects_number("properties", table)
    #compare side effects
    assert(context.graph_properties.compare(nodes_dif, relationships_dif, labels_dif, properties_dif) == True)

@then('no side effects')
def side_effects_step(context):
    if context.config.no_side_effects:
        return
    #check if side effects are non existing
    assert(context.graph_properties.compare([], [], [], []) == True)
