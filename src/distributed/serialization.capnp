@0xccb448f0b998d9c8;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("distributed::capnp");

struct Address {
  gid @0 :UInt64;
  workerId @1 :Int16;
}

struct PropertyValue {
  id @0 :UInt16;
  value @1 :TypedValue;
}

struct Edge {
  from @0 :Address;
  to @1 :Address;
  typeId @2 :UInt16;
  properties @3 :List(PropertyValue);
}

struct Vertex {
  outEdges @0 :List(EdgeEntry);
  inEdges @1 :List(EdgeEntry);
  labelIds @2 :List(UInt16);
  properties @3 :List(PropertyValue);

  struct EdgeEntry {
    vertexAddress @0 :Address;
    edgeAddress @1 :Address;
    edgeTypeId @2 :UInt16;
  }
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
    vertex @7 :VertexAccessor;
    edge @8 :EdgeAccessor;
    path @9 :Path;
  }

  struct Entry {
    key @0 :Text;
    value @1 :TypedValue;
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

  struct Path {
    vertices @0 :List(VertexAccessor);
    edges @1 :List(EdgeAccessor);
  }
}
