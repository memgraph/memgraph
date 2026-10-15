import mgp


@mgp.read_proc
def compute(ctx: mgp.ProcCtx, a: mgp.Number, b: mgp.Number) -> mgp.Record(result=mgp.Number):
    return mgp.Record(result=a + b)
