import mgp


@mgp.function
def combine(ctx: mgp.FuncCtx, a: mgp.Number, b: mgp.Number):
    return a + b
