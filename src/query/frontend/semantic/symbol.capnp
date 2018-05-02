@0x93c1dcee84e93b76;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("query::capnp");

struct Symbol {
  enum Type {
    any @0;
    vertex @1;
    edge @2;
    path @3;
    number @4;
    edgeList @5;
  }

  name @0 :Text;
  position @1 :Int32;
  type @2 :Type;
  userDeclared @3 :Bool;
  tokenPosition @4 :Int32;
}
