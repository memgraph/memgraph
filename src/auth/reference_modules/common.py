#!/usr/bin/python3
"""
Shared helpers for auth modules that communicate with Memgraph.
"""

MEMGRAPH_CALL_ID_KEY = "memgraph_call_id"


def pop_call_id(params):
    """
    Remove memgraph_call_id from params so the authenticate() function
    does not receive it. Return the value for echoing in the response.
    """
    return params.pop(MEMGRAPH_CALL_ID_KEY, None)


def add_call_id_to_response(ret, call_id):
    """
    Add memgraph_call_id to the response so Memgraph can correlate
    each response with the corresponding request.
    """
    if call_id is not None:
        ret[MEMGRAPH_CALL_ID_KEY] = call_id
    return ret
