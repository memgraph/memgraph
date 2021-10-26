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

from setup import VERTEX_COUNT

SKIP = VERTEX_COUNT // 4
LIMIT = VERTEX_COUNT // 4

print("MATCH (n) RETURN n ORDER BY n.id SKIP %d LIMIT %d" % (SKIP, LIMIT))
