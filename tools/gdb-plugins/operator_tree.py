import io

import gdb


def _logical_operator_type():
    '''Returns the LogicalOperator gdb.Type'''
    # This is a function, because the type may appear during gdb runtime.
    # Therefore, we cannot assign it on import.
    return gdb.lookup_type('query::plan::LogicalOperator')


def _shared_ptr_pointee(shared_ptr):
    '''Returns the address of the pointed to object inside shared_ptr.'''
    # This function may not be needed when gdb adds dereferencing shared_ptr
    # via Python API.
    with io.StringIO() as string_io:
        print(shared_ptr, file=string_io)
        addr = string_io.getvalue().split()[-1]
        return int(addr, base=16)


def _get_operator_input(operator):
    '''Returns the input operator of given operator, if it has any.'''
    if 'input_' not in [f.name for f in operator.type.fields()]:
        return None
    input_addr = _shared_ptr_pointee(operator['input_'])
    if input_addr == 0:
        return None
    pointer_type = _logical_operator_type().pointer()
    input_op = gdb.Value(input_addr).cast(pointer_type).dereference()
    return input_op.cast(input_op.dynamic_type)


class PrintOperatorTree(gdb.Command):
    '''Print the tree of logical operators from the expression.'''
    def __init__(self):
        super(PrintOperatorTree, self).__init__("print-operator-tree",
                                                gdb.COMMAND_USER)

    def invoke(self, argument, from_tty):
        try:
            operator = gdb.parse_and_eval(argument)
        except gdb.error as e:
            raise gdb.GdbError(*e.args)
        # Currently, gdb doesn't provide API to check if the dynamic_type is
        # subtype of a base type. So, this check will fail, for example if we
        # get 'query::plan::ScanAll'. The user can avoid this by up-casting.
        if operator.type != _logical_operator_type():
            raise gdb.GdbError("Expected a '%s', but got '%s'" %
                               (_logical_operator_type(), operator.type))
        next_op = operator.cast(operator.dynamic_type)
        tree = []
        while next_op is not None:
            tree.append('* %s <%s>' % (next_op.type.name, next_op.address))
            next_op = _get_operator_input(next_op)
        print('\n'.join(tree))


PrintOperatorTree()
