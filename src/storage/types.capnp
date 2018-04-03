@0x8678c6a8817808d9;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("storage::capnp");

struct Common {
  storage @0 :UInt16;
  union {
    label @1 :Label;
    edgeType @2 :EdgeType;
    property @3 :Property;
  }
}

struct Label {}
struct EdgeType {}
struct Property {}
