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

import common

expressions = [
    "1 + 3",
    "2 - 1",
    "2 * 5",
    "5 / 2",
    "5 % 5",
    "-5" + "1.4 + 3.3",
    "6.2 - 5.4",
    "6.5 * 1.2",
    "6.6 / 1.2",
    "8.7 % 3.2",
    "-6.6",
    '"Flo" + "Lasta"',
    "true AND false",
    "true OR false",
    "true XOR false",
    "NOT true",
    "1 < 2",
    "2 = 3",
    "6.66 < 10.2",
    "3.14 = 3.2",
    '"Ana" < "Ivana"',
    '"Ana" = "Mmmmm"',
    "Null < Null",
    "Null = Null",
]

print(common.generate(expressions, 30))
