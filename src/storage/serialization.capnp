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

struct Address {
  storage @0 :UInt64;
}

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

struct PropertyValueStore {
  properties @0 :List(Entry);
  
  struct Entry {
    id @0 :Common;
    value @1 :PropertyValue;
  }
}

struct Edge {
  from @0 :Address;
  to @1 :Address;
  typeId @2 :UInt16;
  properties @3 :PropertyValueStore;
}

struct Vertex {
  outEdges @0 :List(EdgeEntry);
  inEdges @1 :List(EdgeEntry);
  labelIds @2 :List(UInt16);
  properties @3 :PropertyValueStore;

  struct EdgeEntry {
    vertexAddress @0 :Address;
    edgeAddress @1 :Address;
    edgeTypeId @2 :UInt16;
  }
}

enum SendVersions {
  both @0;
  onlyOld @1;
  onlyNew @2;
}

struct VertexAccessor {
  cypherId @0 :Int64;
  address @1 :UInt64;
  old @2 :Vertex;
  new @3 :Vertex;
}

struct EdgeAccessor {
  cypherId @0 :Int64;
  address @1 :UInt64;
  old @2 :Edge;
  new @3 :Edge;
}

