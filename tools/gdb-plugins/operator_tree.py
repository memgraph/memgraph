import re

import gdb


def _logical_operator_type():
    '''Returns the LogicalOperator gdb.Type'''
    # This is a function, because the type may appear during gdb runtime.
    # Therefore, we cannot assign it on import.
    return gdb.lookup_type('query::plan::LogicalOperator')


def _iter_fields_and_base_classes(value):
    '''Iterate all fields of value.type'''
    types_to_process = [value.type]
    while types_to_process:
        for field in types_to_process.pop().fields():
            if field.is_base_class:
                types_to_process.append(field.type)
            yield field


def _fields(value):
    '''Return a list of value.type fields.'''
    return [f for f in _iter_fields_and_base_classes(value)
            if not f.is_base_class]


def _has_field(value, field_name):
    '''Return True if value.type has a field named field_name.'''
    return field_name in [f.name for f in _fields(value)]


def _base_classes(value):
    '''Return a list of base classes for value.type.'''
    return [f for f in _iter_fields_and_base_classes(value)
            if f.is_base_class]


def _is_instance(value, type_):
    '''Return True if value is an instance of type.'''
    return value.type.unqualified() == type_ or \
            type_ in [base.type for base in _base_classes(value)]


# Pattern for matching std::unique_ptr<T, Deleter> and std::shared_ptr<T>
_SMART_PTR_TYPE_PATTERN = \
        re.compile('^std::(unique|shared)_ptr<(?P<pointee_type>[\w:]*)')


def _is_smart_ptr(maybe_smart_ptr, type_name=None):
    type_ = maybe_smart_ptr.type.unqualified()
    if type_.name is None:
        return False
    match = _SMART_PTR_TYPE_PATTERN.match(type_.name)
    if match is None or type_name is None:
        return bool(match)
    return type_name == match.group('pointee_type')


def _smart_ptr_pointee(smart_ptr):
    '''Returns the pointer to object in shared_ptr/unique_ptr.'''
    # This function may not be needed when gdb adds dereferencing
    # shared_ptr/unique_ptr via Python API.
    if _has_field(smart_ptr, '_M_ptr'):
        # shared_ptr
        return smart_ptr['_M_ptr']
    if _has_field(smart_ptr, '_M_t'):
        # unique_ptr
        smart_ptr = smart_ptr['_M_t']
        if _has_field(smart_ptr, '_M_t'):
            # Check for one more level of _M_t
            smart_ptr = smart_ptr['_M_t']
        if _has_field(smart_ptr, '_M_head_impl'):
            return smart_ptr['_M_head_impl']


def _get_operator_input(operator):
    '''Returns the input operator of given operator, if it has any.'''
    if not _has_field(operator, 'input_'):
        return None
    input_op = _smart_ptr_pointee(operator['input_']).dereference()
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
        if operator.type.code in (gdb.TYPE_CODE_PTR, gdb.TYPE_CODE_REF):
            operator = operator.referenced_value()
        if _is_smart_ptr(operator, 'query::plan::LogicalOperator'):
            operator = _smart_ptr_pointee(operator).dereference()
        if not _is_instance(operator, logical_operator_type):
            raise gdb.GdbError("Expected a '%s', but got '%s'" %
                               (logical_operator_type, operator.type))
        next_op = operator.cast(operator.dynamic_type)
        tree = []
        while next_op is not None:
            tree.append('* %s <%s>' % (next_op.type.name, next_op.address))
            next_op = _get_operator_input(next_op)
        print('\n'.join(tree))


PrintOperatorTree()
