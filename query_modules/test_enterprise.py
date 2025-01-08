import mgp

@mgp.read_proc
def is_enterprise_valid(context: mgp.ProcCtx) -> mgp.Record(valid=bool):
    """
    A procedure that checks if the enterprise license is valid.
    """
    return mgp.Record(valid=mgp.is_enterprise_valid())
