@0xcbc2c66202fdf643;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("query::capnp");

using Ast = import "/query/frontend/ast/ast.capnp";

enum GraphView {
  old @0;
  new @1;
}

struct TypedValueVectorCompare {
  ordering @0 :List(Ast.Ordering);
}
