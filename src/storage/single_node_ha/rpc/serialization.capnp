@0x8424471a44ccd2df;

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

struct PropertyValue {
  union {
    nullType @0 :Void;
    bool @1 :Bool;
    integer @2 :Int64;
    double @3 :Float64;
    string @4 :Text;
    list @5 :List(PropertyValue);
    map @6 :List(MapEntry);
  }

  struct MapEntry {
    key @0 :Text;
    value @1 :PropertyValue;
  }
}
