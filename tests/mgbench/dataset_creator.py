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

import random

import helpers

# Explaination of datasets:
#   - empty_only_index: contains index; contains no data
#   - small: contains index; contains data (small dataset)
#
# Datamodel is as follow:
#
#                               ┌──────────────┐
#                               │ Permission   │
#         ┌────────────────┐    │  Schema:uuid │   ┌────────────┐
#         │:IS_FOR_IDENTITY├────┤  Index:name  ├───┤:IS_FOR_USER│
#         └┬───────────────┘    └──────────────┘   └────────────┤
#          │                                                    │
#   ┌──────▼──────────────┐                                  ┌──▼───────────┐
#   │  Identity           │                                  │ User         │
#   │   Schema:uuid       │                                  │  Schema:uuid │
#   │   Index:platformId  │                                  │  Index:email │
#   │   Index:name        │                                  └──────────────┘
#   └─────────────────────┘
#
#
#   - User: attributes: ["uuid", "name", "platformId"]
#   - Permission: attributes: ["uuid", "name"]
#   - Identity: attributes: ["uuid", "email"]
#
# Indexes:
#   - User: [User(uuid), User(platformId), User(name)]
#   - Permission: [Permission(uuid), Permission(name)]
#   - Identity: [Identity(uuid), Identity(email)]
#
# Edges:
#   - (:Permission)-[:IS_FOR_USER]->(:User)
#   - (:Permission)-[:IS_FOR_IDENTITYR]->(:Identity)
#
# Distributed specific: uuid is the schema

filename = "dataset.cypher"
f = open(filename, "x")

f.write("MATCH (n) DETACH DELETE n;\n")

# Create the indexes
f.write("CREATE INDEX ON :User;\n")
f.write("CREATE INDEX ON :Permission;\n")
f.write("CREATE INDEX ON :Identity;\n")
f.write("CREATE INDEX ON :User(platformId);\n")
f.write("CREATE INDEX ON :User(name);\n")
f.write("CREATE INDEX ON :Permission(name);\n")
f.write("CREATE INDEX ON :Identity(email);\n")

# Create extra index: in distributed, this will be the schema
f.write("CREATE INDEX ON :User(uuid);\n")
f.write("CREATE INDEX ON :Permission(uuid);\n")
f.write("CREATE INDEX ON :Identity(uuid);\n")

platform_ids = [f"somePlatformId_{id}" for id in range(10)]

# This is the number of clusters to change if you want a bigger dataset
number_of_clusters = 3000000

for index in range(1, number_of_clusters + 1):
    platform_id = platform_ids[random.randint(0, len(platform_ids) - 1)]
    user_uuid = index
    platform_uuid = number_of_clusters + index
    identity_uuid = 2 * number_of_clusters + index

    # Create the nodes
    f.write(f'CREATE (:User {{uuid: {user_uuid}, platformId: "{platform_id}", name: "name_user_{user_uuid}"}});\n')
    f.write(f'CREATE (:Permission {{uuid: {platform_uuid}, name: "name_permission_{platform_uuid}"}});\n')
    f.write(f'CREATE (:Permission {{uuid: {identity_uuid}, name: "mail_{identity_uuid}@something.com"}});\n')

    # Create the edges
    f.write(
        f"MATCH (permission:Permission {{uuid: {platform_uuid}}}), (user:User {{uuid: {user_uuid}}}) CREATE (permission)-[e: IS_FOR_USER]->(user);\n"
    )
    f.write(
        f"MATCH (permission:Permission {{uuid: {platform_uuid}}}), (identity:Identity {{uuid: {identity_uuid}}}) CREATE (permission)-[e: IS_FOR_IDENTITY]->(identity);\n"
    )

f.close()
