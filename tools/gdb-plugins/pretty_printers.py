import gdb
import gdb.printing


def build_memgraph_pretty_printers():
    '''Instantiate and return all memgraph pretty printer classes.'''
    pp = gdb.printing.RegexpCollectionPrettyPrinter('memgraph')
    pp.add_printer('query::TypedValue', '^query::TypedValue$', TypedValuePrinter)
    return pp


class TypedValuePrinter(gdb.printing.PrettyPrinter):
    '''Pretty printer for query::TypedValue'''
    def __init__(self, val):
        super(TypedValuePrinter, self).__init__('TypedValue')
        self.val = val

    def to_string(self):
        def _to_str(val):
            return '{%s %s}' % (value_type, self.val[val])
        value_type = str(self.val['type_'])
        if value_type == 'query::TypedValue::Type::Null':
            return '{%s}' % value_type
        elif value_type == 'query::TypedValue::Type::Bool':
            return _to_str('bool_v')
        elif value_type == 'query::TypedValue::Type::Int':
            return _to_str('int_v')
        elif value_type == 'query::TypedValue::Type::Double':
            return _to_str('double_v')
        elif value_type == 'query::TypedValue::Type::String':
            return _to_str('string_v')
        elif value_type == 'query::TypedValue::Type::List':
            return _to_str('list_v')
        elif value_type == 'query::TypedValue::Type::Map':
            return _to_str('map_v')
        elif value_type == 'query::TypedValue::Type::Vertex':
            return _to_str('vertex_v')
        elif value_type == 'query::TypedValue::Type::Edge':
            return _to_str('edge_v')
        elif value_type == 'query::TypedValue::Type::Path':
            return _to_str('path_v')
        return '{%s}' % value_type

gdb.printing.register_pretty_printer(None, build_memgraph_pretty_printers(),
                                     replace=True)
