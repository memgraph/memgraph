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

import yaml


class TestParameters:

    """
    Class used to store parameters from a cucumber test.
    """

    def __init__(self):
        """
        Constructor initializes parameters to empty dict.
        """
        self.parameters = dict()

    def set_parameters_from_table(self, table):
        """
        Method gets table, parses parameters and sets them
        in self.parameters.

        @param self:
            Class instance.
        @param table:
            behave.model.Table, table of unparsed parameters
            from behave.runner.Context.
        """
        par = dict()
        for row in table:
            par[row[0]] = self.parse_parameters(row[1])
            if isinstance(par[row[0]], str) and par[row[0]].startswith("'") and par[row[0]].endswith("'"):
                par[row[0]] = par[row[0]][1 : len(par[row[0]]) - 1]
        par[table.headings[0]] = self.parse_parameters(table.headings[1])
        if (
            isinstance(par[table.headings[0]], str)
            and par[table.headings[0]].startswith("'")
            and par[table.headings[0]].endswith("'")
        ):
            par[table.headings[0]] = par[table.headings[0]][1 : len(par[table.headings[0]]) - 1]

        self.parameters = par

    def get_parameters(self):
        """
        Method returns parameters.

        @param self:
            Instance of a class.
        return:
            Dictionary of parameters.
        """
        return self.parameters

    def parse_parameters(self, val):
        """
        Method used for parsing parameters given in a cucumber test table
        to a readable format for a database.
        Integers are parsed to int values, floats to float values, bools
        to bool values, null to None and structures are recursively
        parsed and returned.

        @param val:
            String that needs to be parsed.
        @return:
            Format readable for a database.
        """

        return yaml.load(val, Loader=yaml.FullLoader)
