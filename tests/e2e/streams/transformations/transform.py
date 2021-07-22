import mgp


@mgp.transformation
def simple(context: mgp.TransCtx,
           messages: mgp.Messages
           ) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(mgp.Record(
            query=f"CREATE (n:MESSAGE {{timestamp: '{message.timestamp()}', payload: '{payload_as_str}', topic: '{message.topic_name()}'}})",
            parameters=None))

    return result_queries


@mgp.transformation
def with_parameters(context: mgp.TransCtx,
                    messages: mgp.Messages
                    ) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(mgp.Record(
            query="CREATE (n:MESSAGE {timestamp: $timestamp, payload: $payload, topic: $topic})",
            parameters={"timestamp": message.timestamp(),
                        "payload": payload_as_str,
                        "topic": message.topic_name()}))

    return result_queries


@mgp.transformation
def query(messages: mgp.Messages
          ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(mgp.Record(
            query=payload_as_str, parameters=None))

    return result_queries
