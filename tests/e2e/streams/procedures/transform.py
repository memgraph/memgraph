import mgp


@mgp.transformation
def transformation(context: mgp.TransCtx,
                   messages: mgp.Messages
                   ) -> mgp.Record(query=str, parameters=mgp.Map):

    result_queries = []

    for i in range(0, messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        result_queries.append(mgp.Record(
            query=f"CREATE (n:MESSAGE {{timestamp: '{message.timestamp()}', payload: '{payload_as_str}', topic: '{message.topic_name()}'}})",
            parameters={"param1": "value1", "param2": 2}))

    return result_queries
