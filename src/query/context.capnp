@0xf2d47a8877eb7f4f;

using Cxx = import "/capnp/c++.capnp";
using Dis = import "/distributed/serialization.capnp";
using Utils = import "/utils/serialization.capnp";

$Cxx.namespace("query::capnp");

struct EvaluationContext {
  timestamp @0 : Int64;
  params @1 : Utils.Map(Utils.BoxInt64, Dis.TypedValue);
}
