import mgp


@mgp.write_proc
def void_proc(ctx: mgp.ProcCtx):
    pass


@mgp.write_proc
def signature_returns_but_impl_does_not(ctx: mgp.ProcCtx) -> mgp.Record(result=str):
    pass


@mgp.write_proc
def signature_void_but_impl_returns(ctx: mgp.ProcCtx):
    return mgp.Record(result="hello")


@mgp.write_proc
def explicit_empty_record(ctx: mgp.ProcCtx) -> mgp.Record():
    return mgp.Record()
