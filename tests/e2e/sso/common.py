import os

# Helpers for auth modules (oidc/saml) that are copied into this folder for e2e tests.
MEMGRAPH_CALL_ID_KEY = "memgraph_call_id"


def pop_call_id(params):
    """Remove memgraph_call_id from params; return the value for echoing in the response."""
    return params.pop(MEMGRAPH_CALL_ID_KEY, None)


def add_call_id_to_response(ret, call_id):
    """Add memgraph_call_id to the response for request/response correlation."""
    if call_id is not None:
        ret[MEMGRAPH_CALL_ID_KEY] = call_id
    return ret


def get_data_path(file: str, test: str):
    """
    Data is stored in sso folder.
    """
    return f"sso/{file}/{test}"


def get_logs_path(file: str, test: str):
    """
    Logs are stored in sso folder.
    """
    return f"sso/{file}/{test}"


def compose_path(filename: str):
    return os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", filename))


def load_test_data(filename: str):
    with open(file=compose_path(filename=filename), mode="r") as test_data_file:
        return test_data_file.read()
