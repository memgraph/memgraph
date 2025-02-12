import gdb
import gdb.printing


def build_memgraph_pretty_printers():
    """Instantiate and return all memgraph pretty printer classes."""
    pp = gdb.printing.RegexpCollectionPrettyPrinter("memgraph")
    pp.add_printer("memgraph::query::TypedValue", "^memgraph::query::TypedValue$", TypedValuePrinter)
    pp.add_printer("memgraph::utils::SkipListNode", "^memgraph::utils::SkipListNode<.*>$", SkipListNodePrinter)
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


class SkipListNodePrinter(gdb.printing.PrettyPrinter):
    def __init__(self, val):
        self.val = val
        self.typename = str(val.type)

    def to_string(self):
        height = int(self.val["height"])
        return f"{self.typename}(height={height})"

    def children(self):
        fields = ["obj", "lock", "marked", "fully_linked", "height"]
        for field in fields:
            yield field, self.val[field]

        height = int(self.val["height"])
        nexts = self.val["nexts"]
        for i in range(height):
            atomic_ptr = nexts[i]
            raw_ptr = atomic_ptr["_M_b"]["_M_p"]
            yield f"nexts[{i}]", raw_ptr

    def display_hint(self):
        return "nexts"


gdb.printing.register_pretty_printer(None, build_memgraph_pretty_printers(), replace=True)
