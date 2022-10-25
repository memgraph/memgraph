import gdb
import gdb.printing


def build_memgraph_pretty_printers():
    """Instantiate and return all memgraph pretty printer classes."""
    pp = gdb.printing.RegexpCollectionPrettyPrinter("memgraph")
    pp.add_printer("memgraph::query::TypedValue", "^memgraph::query::TypedValue$", TypedValuePrinter)
    pp.add_printer("memgraph::query::v2::TypedValue", "^memgraph::query::v2::TypedValue$", TypedValuePrinter2)
    pp.add_printer("memgraph::storage::v3::TypedValue", "^memgraph::storage::v3::TypedValue$", TypedValuePrinter3)
    pp.add_printer(
        "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>",
        "^memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>$",
        TypedValuePrinter4,
    )
    return pp


class TypedValuePrinter(gdb.printing.PrettyPrinter):
    """Pretty printer for memgraph::query::TypedValue"""

    def __init__(self, val):
        super(TypedValuePrinter, self).__init__("TypedValue")
        self.val = val

    def to_string(self):
        def _to_str(val):
            return "{%s %s}" % (value_type, self.val[val])

        value_type = str(self.val["type_"])
        if value_type == "memgraph::query::TypedValue::Type::Null":
            return "{%s}" % value_type
        elif value_type == "memgraph::query::TypedValue::Type::Bool":
            return _to_str("bool_v")
        elif value_type == "memgraph::query::TypedValue::Type::Int":
            return _to_str("int_v")
        elif value_type == "memgraph::query::TypedValue::Type::Double":
            return _to_str("double_v")
        elif value_type == "memgraph::query::TypedValue::Type::String":
            return _to_str("string_v")
        elif value_type == "memgraph::query::TypedValue::Type::List":
            return _to_str("list_v")
        elif value_type == "memgraph::query::TypedValue::Type::Map":
            return _to_str("map_v")
        elif value_type == "memgraph::query::TypedValue::Type::Vertex":
            return _to_str("vertex_v")
        elif value_type == "memgraph::query::TypedValue::Type::Edge":
            return _to_str("edge_v")
        elif value_type == "memgraph::query::TypedValue::Type::Path":
            return _to_str("path_v")
        return "{%s}" % value_type


class TypedValuePrinter2(gdb.printing.PrettyPrinter):
    """Pretty printer for memgraph::query::TypedValue"""

    def __init__(self, val):
        super(TypedValuePrinter2, self).__init__("TypedValue2")
        self.val = val

    def to_string(self):
        def _to_str(val):
            return "{%s %s}" % (value_type, self.val[val])

        value_type = str(self.val["type_"])
        if value_type == "memgraph::query::v2::TypedValue::Type::Null":
            return "{%s}" % value_type
        elif value_type == "memgraph::query::v2::TypedValue::Type::Bool":
            return _to_str("bool_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Int":
            return _to_str("int_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Double":
            return _to_str("double_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::String":
            return _to_str("string_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::List":
            return _to_str("list_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Map":
            return _to_str("map_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Vertex":
            return _to_str("vertex_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Edge":
            return _to_str("edge_v")
        elif value_type == "memgraph::query::v2::TypedValue::Type::Path":
            return _to_str("path_v")
        return "{%s}" % value_type


class TypedValuePrinter3(gdb.printing.PrettyPrinter):
    """Pretty printer for memgraph::query::TypedValue"""

    def __init__(self, val):
        super(TypedValuePrinter3, self).__init__("TypedValue3")
        self.val = val

    def to_string(self):
        def _to_str(val):
            return "{%s %s}" % (value_type, self.val[val])

        value_type = str(self.val["type_"])
        if value_type == "memgraph::storage::v3::TypedValue::Type::Null":
            return "{%s}" % value_type
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Bool":
            return _to_str("bool_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Int":
            return _to_str("int_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Double":
            return _to_str("double_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::String":
            return _to_str("string_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::List":
            return _to_str("list_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Map":
            return _to_str("map_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Vertex":
            return _to_str("vertex_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Edge":
            return _to_str("edge_v")
        elif value_type == "memgraph::storage::v3::TypedValue::Type::Path":
            return _to_str("path_v")
        return "{%s}" % value_type


class TypedValuePrinter4(gdb.printing.PrettyPrinter):
    """Pretty printer for memgraph::query::TypedValue"""

    def __init__(self, val):
        super(TypedValuePrinter4, self).__init__("TypedValue4")
        self.val = val

    def to_string(self):
        def _to_str(val):
            return "{%s %s}" % (value_type, self.val[val])

        value_type = str(self.val["type_"])
        if (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Null"
        ):
            return "{%s}" % value_type
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Bool"
        ):
            return _to_str("bool_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Int"
        ):
            return _to_str("int_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Double"
        ):
            return _to_str("double_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::String"
        ):
            return _to_str("string_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::List"
        ):
            return _to_str("list_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Map"
        ):
            return _to_str("map_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Vertex"
        ):
            return _to_str("vertex_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Edge"
        ):
            return _to_str("edge_v")
        elif (
            value_type
            == "memgraph::expr::TypedValueT<memgraph::storage::v3::VertexAccessor, memgraph::storage::v3::EdgeAccessor, memgraph::storage::v3::Path>::Type::Path"
        ):
            return _to_str("path_v")
        return "{%s}" % value_type


gdb.printing.register_pretty_printer(None, build_memgraph_pretty_printers(), replace=True)
