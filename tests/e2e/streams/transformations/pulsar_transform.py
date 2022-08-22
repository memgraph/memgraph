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

import mgp


@mgp.transformation
def simple(context: mgp.TransCtx, messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        assert message.source_type() == mgp.SOURCE_TYPE_PULSAR
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(
            mgp.Record(
                query=f"""
                CREATE (n:MESSAGE {{
                    payload: '{payload_as_str}',
                    topic: '{message.topic_name()}'
                }})""",
                parameters=None,
            )
        )

    return result_queries


@mgp.transformation
def with_parameters(context: mgp.TransCtx, messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        assert message.source_type() == mgp.SOURCE_TYPE_PULSAR
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(
            mgp.Record(
                query="""
                CREATE (n:MESSAGE {
                    payload: $payload,
                    topic: $topic
                })""",
                parameters={"payload": payload_as_str, "topic": message.topic_name()},
            )
        )

    return result_queries


@mgp.transformation
def query(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        assert message.source_type() == mgp.SOURCE_TYPE_PULSAR
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(mgp.Record(query=payload_as_str, parameters=None))

    return result_queries
