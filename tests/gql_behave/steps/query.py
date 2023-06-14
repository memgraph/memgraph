# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# -*- coding: utf-8 -*-

import database
import parser
from behave import given, then, step, when
from neo4j.graph import Node, Path, Relationship


@given('parameters are')
def parameters_step(context):
    context.test_parameters.set_parameters_from_table(context.table)


@then('parameters are')
def parameters_step(context):
    context.test_parameters.set_parameters_from_table(context.table)


@step('having executed')
def having_executed_step(context):
    context.results = database.query(
        context.text, context, context.test_parameters.get_parameters())


@when('executing query')
def executing_query_step(context):
    context.results = database.query(
        context.text, context, context.test_parameters.get_parameters())


@when('executing control query')
def executing_query_step(context):
    context.results = database.query(
        context.text, context, context.test_parameters.get_parameters())


def parse_props(props_key_value):
    """
    Function used to parse properties from map of properties to string.

    @param prop_json:
        Dictionary, map of properties of graph element.
    @return:
        Map of properties in string format.
    """
    if not props_key_value:
        return ""
    properties = "{"
    for key, value in props_key_value:
        if value is None:
            properties += key + ": null, "
        elif isinstance(value, str):
            properties += key + ": " + "'" + value + "', "
        elif isinstance(value, bool):
            if value:
                properties += key + ": true, "
            else:
                properties += key + ": false, "
        else:
            properties += key + ": " + str(value) + ", "
    properties = properties[:-2]
    properties += "}"
    return properties


def to_string(element):
    """
    Function used to parse result from database to string. Format of
    the string is same as format of a result given in cucumber test.

    @param element:
        Can be None, string, bool, number, list, dict or any of neo4j.graph
        Node, Path, Relationship. Element which will be parsed.
    @return:
        String of parsed element.
    """
    if element is None:
        # parsing None
        return "null"

    if isinstance(element, Node):
        # parsing Node
        sol = "("
        if element.labels:
            sol += ':' + ': '.join(element.labels)

        if element.keys():
            if element.labels:
                sol += ' '
            sol += parse_props(element.items())

        sol += ")"
        return sol

    elif isinstance(element, Relationship):
        # parsing Relationship
        sol = "[:"
        if element.type:
            sol += element.type
        if element.keys():
            sol += ' '
        sol += parse_props(element.items())
        sol += "]"
        return sol

    elif isinstance(element, Path):
        # parsing Path
        # TODO add longer paths
        edges = []
        nodes = []

        for rel in element.relationships:
            edges.append([rel.start_node.id, to_string(rel)])

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
        # parsing string
        return "'" + element + "'"

    elif isinstance(element, list):
        # parsing list
        sol = '['
        el_str = []
        for el in element:
            el_str.append(to_string(el))
        sol += ', '.join(el_str)
        sol += ']'

        return sol

    elif isinstance(element, bool):
        # parsing bool
        if element:
            return "true"
        return "false"

    elif isinstance(element, dict):
        # parsing map
        if len(element) == 0:
            return '{}'
        sol = '{'
        for key, val in element.items():
            sol += key + ':' + to_string(val) + ','
        sol = sol[:-1] + '}'
        return sol

    elif isinstance(element, float):
        # parsing float, scientific
        if 'e' in str(element):
            if str(element)[-3] == '-':
                zeroes = int(str(element)[-2:]) - 1
                num_str = ''
                if str(element)[0] == '-':
                    num_str += '-'
                num_str += '.' + zeroes * '0' + \
                    str(element)[:-4].replace("-", "").replace(".", "")
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
            result_rows.append(keys[i] + ":" + parser.parse(
                to_string(values[i]).replace("\n", "\\n").replace(" ", ""),
                ignore_order))
    return result_rows


def get_expected_rows(context, ignore_order):
    """
    Function returns expected results as list from context table.

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
            expected_rows.append(
                col + ":" + parser.parse(row[col].replace(" ", ""),
                                         ignore_order))
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
    assert(len(expected_rows) == len(result_rows))

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
    assert(len(expected_rows) == len(result_rows))

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
        context.log.info("Exception when executing query!")
        assert(False)


@then('the result should be empty')
def empty_result_step(context):
    assert(len(context.results) == 0)
    check_exception(context)


@then('the side effects should be')
def side_effects_step(context):
    return


@then('no side effects')
def side_effects_step(context):
    return
