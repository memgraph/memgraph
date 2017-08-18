import io
import re

import gdb


def _logical_operator_type():
    '''Returns the LogicalOperator gdb.Type'''
    # This is a function, because the type may appear during gdb runtime.
    # Therefore, we cannot assign it on import.
    return gdb.lookup_type('query::plan::LogicalOperator')


# Pattern for matching std::unique_ptr<T, Deleter> and std::shared_ptr<T>
_SMART_PTR_TYPE_PATTERN = \
        re.compile('^std::(unique|shared)_ptr<(?P<pointee_type>[\w:]*)')


def _is_smart_ptr(maybe_smart_ptr, type_name=None):
    if maybe_smart_ptr.type.name is None:
        return False
    match = _SMART_PTR_TYPE_PATTERN.match(maybe_smart_ptr.type.name)
    if match is None or type_name is None:
        return bool(match)
    return type_name == match.group('pointee_type')


def _smart_ptr_pointee(smart_ptr):
    '''Returns the address of the pointed to object in shared_ptr/unique_ptr.'''
    # This function may not be needed when gdb adds dereferencing
    # shared_ptr/unique_ptr via Python API.
    with io.StringIO() as string_io:
        print(smart_ptr, file=string_io)
        addr = string_io.getvalue().split()[-1]
        return int(addr, base=16)


def _get_operator_input(operator):
    '''Returns the input operator of given operator, if it has any.'''
    types_to_process = [operator.type]
    all_fields = []
    while types_to_process:
        for field in types_to_process.pop().fields():
            if field.is_base_class:
                types_to_process.append(field.type)
            else:
                all_fields.append(field)
    if "input_" not in [f.name for f in all_fields]:
        return None
    input_addr = _smart_ptr_pointee(operator['input_'])
    pointer_type = _logical_operator_type().pointer()
    input_op = gdb.Value(input_addr).cast(pointer_type).dereference()
    return input_op.cast(input_op.dynamic_type)


class PrintOperatorTree(gdb.Command):
    '''Print the tree of logical operators from the expression.'''
    def __init__(self):
        super(PrintOperatorTree, self).__init__("print-operator-tree",
                                                gdb.COMMAND_USER,
                                                gdb.COMPLETE_EXPRESSION)

    def invoke(self, argument, from_tty):
        try:
            operator = gdb.parse_and_eval(argument)
        except gdb.error as e:
            raise gdb.GdbError(*e.args)
        logical_operator_type = _logical_operator_type()
        if _is_smart_ptr(operator, 'query::plan::LogicalOperator'):
            pointee = gdb.Value(_smart_ptr_pointee(operator))
            if pointee == 0:
                raise gdb.GdbError("Expected a '%s', but got nullptr" %
                                   logical_operator_type)
            operator = \
                pointee.cast(logical_operator_type.pointer()).dereference()
        elif operator.type == logical_operator_type.pointer():
            operator = operator.dereference()
        # Currently, gdb doesn't provide API to check if the dynamic_type is
        # subtype of a base type. So, this check will fail, for example if we
        # get 'query::plan::ScanAll'. The user can avoid this by up-casting.
        if operator.type != logical_operator_type:
            raise gdb.GdbError("Expected a '%s', but got '%s'" %
                               (logical_operator_type, operator.type))
        next_op = operator.cast(operator.dynamic_type)
        tree = []
        while next_op is not None:
            tree.append('* %s <%s>' % (next_op.type.name, next_op.address))
            next_op = _get_operator_input(next_op)
        print('\n'.join(tree))


PrintOperatorTree()
