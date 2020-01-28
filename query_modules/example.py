# Build Memgraph with Python 3.5+ support and run Memgraph so that it loads
# this file as a Query Module. The procedure implemented in this module is just
# a rewrite of the `example.c` procedure.
import mgp

import copy

# mgp.read_proc will use inspect.signature to get the arguments and types which
# we map to our C API. Alternative registration examples are written below
# this definition. Also, we can support other builtin types that make sense, as
# well as the `typing` module, but `typing.Any` maps to `mgp.Nullable(mgp.Any)`
# instead of `mgp.Any` which prevents `None`. Additionally, all `mgp.read_proc`
# decorator registration relies on the assumption that they will have a
# globally visible `mgp_module` on which procedures are registered. For this to
# work, importing Python modules as Query Modules will need to go through a
# single thread or should use a thread local variable. Currently, we load
# everything on startup in a single thread, so there are no issues.
@mgp.read_proc
def procedure(context: mgp.ProcCtx,
              required_arg: mgp.Nullable[mgp.Any],
              optional_arg: mgp.Nullable[mgp.Any] = None
              ) -> mgp.Record(result=str, args=list):
    '''
    This example procedure returns 2 fields: `args` and `result`.
      * `args` is a copy of arguments passed to the procedure.
      * `result` is the result of this procedure, a "Hello World!" string.
    Any errors can be reported by raising an Exception.

    The procedure can be invoked in openCypher using the following calls:
      CALL example.procedure(1, 2) YIELD args, result;
      CALL example.procedure(1) YIELD args, result;
    Naturally, you may pass in different arguments or yield less fields.
    '''
    # Copying to make it equivalent to the C example.
    args_copy = [copy.deepcopy(required_arg), copy.deepcopy(optional_arg)]
    # Multiple rows can be produced by returning an iterable of mgp.Record
    return mgp.Record(args=args_copy, result='Hello World!')
