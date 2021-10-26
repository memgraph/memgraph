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

print("""CREATE (:L1:L2:L3:L4:L5:L6:L7 {p1: true, p2: 42, p3: "Here is some text that is not extremely short", p4:"Short text", p5: 234.434, p6: 11.11, p7: false});""" * 1000)
