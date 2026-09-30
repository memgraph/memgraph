import mgp


def batch_void(ctx: mgp.ProcCtx):
    pass


def init(ctx: mgp.ProcCtx):
    pass


def cleanup():
    pass


mgp.add_batch_read_proc(batch_void, init, cleanup)
