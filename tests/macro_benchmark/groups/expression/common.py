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

def generate(expressions, repetitions):
    idx = 0
    def get_alias():
        nonlocal idx
        idx += 1
        return "a" + str(idx)

    query = []
    for i in range(repetitions):
        for expression in expressions:
            query.append(expression + " as " + get_alias())

    return "RETURN " + ", ".join(query)
