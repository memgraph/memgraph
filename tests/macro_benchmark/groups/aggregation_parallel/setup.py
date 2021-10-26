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

BATCH_SIZE = 100
VERTEX_COUNT = 1000000

for i in range(VERTEX_COUNT):
    print("CREATE (n%d {x: %d})" % (i, i))
    # batch CREATEs because we can't execute all at once
    if (i != 0 and i % BATCH_SIZE == 0) or \
            (i + 1 == VERTEX_COUNT):
        print(";")
