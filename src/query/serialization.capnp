@0xf47e119e21912f20;

using Ast = import "/query/frontend/ast/ast_serialization.capnp";
using Cxx = import "/capnp/c++.capnp";
using Storage = import "/storage/serialization.capnp";
using Utils = import "/utils/serialization.capnp";

$Cxx.namespace("query::capnp");

enum GraphView {
  old @0;
  new @1;
}

struct TypedValueVectorCompare {
  ordering @0 :List(Ast.Ordering);
}

struct EvaluationContext {
  timestamp @0 : Int64;
  params @1 : Utils.Map(Utils.BoxInt64, Storage.PropertyValue);
}

struct TypedValue {
  union {
    nullType @0 :Void;
    bool @1 :Bool;
    integer @2 :Int64;
    double @3 :Float64;
    string @4 :Text;
    list @5 :List(TypedValue);
    map @6 :List(Entry);
    vertex @7 :Storage.VertexAccessor;
    edge @8 :Storage.EdgeAccessor;
    path @9 :Path;
  }

  struct Entry {
    key @0 :Text;
    value @1 :TypedValue;
  }

  struct Path {
    vertices @0 :List(Storage.VertexAccessor);
    edges @1 :List(Storage.EdgeAccessor);
  }
}
