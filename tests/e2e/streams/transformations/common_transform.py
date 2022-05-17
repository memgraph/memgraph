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
def check_stream_no_filtering(
    context: mgp.TransCtx, messages: mgp.Messages
) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(
            mgp.Record(query=f"Message: {payload_as_str}", parameters={"value": f"Parameter: {payload_as_str}"})
        )

    return result_queries


@mgp.transformation
def check_stream_with_filtering(
    context: mgp.TransCtx, messages: mgp.Messages
) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")

        if "a" in payload_as_str:
            continue

        result_queries.append(
            mgp.Record(query=f"Message: {payload_as_str}", parameters={"value": f"Parameter: {payload_as_str}"})
        )

        if "b" in payload_as_str:
            result_queries.append(
                mgp.Record(
                    query=f"Message: extra_{payload_as_str}", parameters={"value": f"Parameter: extra_{payload_as_str}"}
                )
            )

    return result_queries
