# Python 3 Query Modules

## Introduction

Memgraph exposes a C API for writing the so called Query Modules. These
modules contain definitions of procedures which can be invoked through the
query language using the `CALL ... YIELD ...` syntax. This mechanism allows
database users to extend Memgraph with their own algorithms and
functionalities.

Using a low level language like C can be quite cumbersome for writing modules,
so it seems natural to add support for a higher level language on top of the
existing C API.

There are languages written exactly for this purpose of extending C with high
level constructs, for example Lua and Guile. Instead of those, we have chosen
Python 3 to be the first high level language we will support. The primary reason
being that it's very popular, so more people should be able to write modules.
Another benefit of Python which comes out of its popularity is the large
ecosystem of libraries, especially graph algorithm related ones like NetworkX.
Python does have significant performance and implementation downsides compared
to Lua and Guile, but these are described in more detail later in this
document.

## Python 3 API Overview

The Python 3 API should be as user friendly as possible as well as look
Pythonic. This implies that some functions from the C API will not map to the
exact same functions. The most obvious case for a Pythonic approach is
registering procedures of a query module. Let's take a look at the C example
and its transformation to Python.

```c
static void procedure(const struct mgp_list *args,
                      const struct mgp_graph *graph, struct mgp_result *result,
                      struct mgp_memory *memory);

int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  struct mgp_proc *proc =
      mgp_module_add_read_procedure(module, "procedure", procedure);
  if (!proc) return 1;
  if (!mgp_proc_add_arg(proc, "required_arg",
                        mgp_type_nullable(mgp_type_any())))
    return 1;
  struct mgp_value *null_value = mgp_value_make_null(memory);
  if (!mgp_proc_add_opt_arg(proc, "optional_arg",
                            mgp_type_nullable(mgp_type_any()), null_value)) {
    mgp_value_destroy(null_value);
    return 1;
  }
  mgp_value_destroy(null_value);
  if (!mgp_proc_add_result(proc, "result", mgp_type_string())) return 1;
  if (!mgp_proc_add_result(proc, "args",
                           mgp_type_list(mgp_type_nullable(mgp_type_any()))))
    return 1;
  return 0;
}
```

In Python things should be a lot simpler.

```Python
# mgp.read_proc obtains the procedure name via __name__ attribute of a function.
@mgp.read_proc(# Arguments passed to multiple mgp_proc_add_arg calls
               (('required_arg', mgp.Nullable(mgp.Any)), ('optional_arg', mgp.Nullable(mgp.Any), None)),
               # Result fields passed to multiple mgp_proc_add_result calls
               (('result', str), ('args', mgp.List(mgp.Nullable(mgp.Any)))))
def procedure(args, graph, result, memory):
    pass
```

Here we have replaced `mgp_module_*` and `mgp_proc_*` C API with a much
simpler decorator function in Python -- `mgp.read_proc`. The types of
arguments and result fields can both be our types as well as Python builtin
types which can map to supported `mgp_value` types. The expected builtin types
we ought to support are: `bool`, `str`, `int`, `float` and `map`. While the
rest of the types are provided via our Python API. Optionally, we can add
convenience support for `object` type which would map to
`mgp.Nullable(mgp.Any)` and `list` which would map to
`mgp.List(mgp.Nullable(mgp.Any))`. Also, it makes sense to take a look if we
can leverage Python's `typing` module here.

Another Pythonic change is to remove `mgp_value` C API from Python altogether.
This means that the arguments a Python procedure receives are not `mgp_value`
instances but rather `PyObject` instances. In other words, our implementation
would immediately marshal `mgp_value` to corresponding type in Python.
Obviously we would need to provide our own Python types for non-builtin
things like `mgp.Vertex` (equivalent to `mgp_vertex`) and other.

Continuing from our example above, let's say the procedure was invoked through
Cypher using the following query.

    MATCH (n) CALL py_module.procedure(42, n) YIELD *;

The Python procedure could then do the following and complete without throwing
neither the AssertionError nor the ValueError.

```Python
def procedure(args, graph, result, memory):
    assert isinstance(args, list)
    # Unpacking throws ValueError if args does not contain exactly 2 values.
    required_arg, optional_arg = args
    assert isintance(required_arg, int)
    assert isinstance(optional_arg, mgp.Vertex)
```

The rest of the C API should naturally map to either top level functions or
class methods as appropriate.

## Loading Python Query Modules

Our current mechanism for loading the modules is to look for `.so` files in
the directory specified by `--query-modules` flag. This is done when Memgraph
is started. We can extend this mechanism to look for `.py` files in addition
to `.so` files in the same directory and import them in the embedded Python
interpreter. The only issue is embedding the interpreter in Memgraph.  There
are multiple choices:

  1. Building Memgraph and statically linking to Python.
  2. Building Memgraph and dynamically linking to Python, and distributing
     Python with Memgraph's installation.
  3. Building Memgraph and dynamically linking to Python, but without
     distributing the Python library.
  4. Building Memgraph and optionally loading Python library by trying to
     `dlopen` it.

The first two options are only viable if the Python license allows, and this
will need further investigation.

The third option adds Python as an installation dependency for Memgraph, and
without it Memgraph will not run. This is problematic for users which cannot
or do not want to install Python 3.

The fourth option avoids all of the issues present in the first 3 options, but
comes at a higher implementation cost. We would need to try to `dlopen` the
Python library and setup function pointers. If we succeed we would import
`.py` files from the `--query-modules` directory. On the other hand, if the
user does not have Python, `dlopen` would fail and Memgraph would run without
Python support.

After live discussion, we've decided to go with option 3. This way we don't
have to worry about mismatching Python versions we support and what the users
expect. Also, we should target Python 3.5 as that should be common between
Debian and CentOS for which we ship installation packages.

## Performance and Implementation Problems

As previously mentioned, embedding Python introduces usability issues compared
to other embeddable languages.

The first, major issue is Global Interpreter Lock (GIL). Initializing Python
will start a single global interpreter and running multiple threads will
require acquiring GIL. In practice, this means that when multiple users run a
procedure written in Python in parallel the execution will not actually be
parallel. Python's interpreter will jump between executing one user's
procedure and the other's. This can be quite an issue for long running
procedures when multiple users are querying Memgraph. The solution for this
issue is Python's API for sub-interpreters. Unfortunately, the support for
them is rather poor and the API contains a lot of critical bugs when we tried
to use them. For the time being, we will have to accept GIL and its downsides.
Perhaps in the future we will gain more knowledge on how we could reduce the
acquire rate of GIL or the sub-interpreter API will get improved.

Another major issue is memory allocation. Python's C API does not have support
for setting up a temporary allocator during execution of a single function.
It only has support for setting up a global heap allocator. This obviously
impacts our control of memory during a query procedure invocation. Besides
potential performance penalty, a procedure could allocate much more memory
than we would actually allow for execution of a single query. This means that
options controlling the memory limit during query execution are useless. On
the bright side, Python does use block style allocators and reference
counting, so the performance penalty and global memory usage should not be
that terrible.

The final issue that isn't as major as the ones above is the global state of
the interpreter. In practice this means that any registered procedure and
imported module has access to any other procedure and module. This may pollute
the namespace for other users, but it should not be much of a problem because
Python always has things under a module scope. The other, slightly bigger
downside is that a malicious user could use this knowledge to modify other
modules and procedures. This seems like a major issue, but if we take the
bigger picture into consideration, we already have a security issue in general
by invoking `dlopen` on `.so` and potentially running arbitrary code. This was
the trade off we chose to allow users to extend Memgraph. It's up to the users
to write sane extensions and protect their servers from access.
