@0xe7647d63b36c2c65;

using Cxx = import "/capnp/c++.capnp";

$Cxx.namespace("utils::capnp");

# Primitive type wrappers

struct BoxInt16 {
  value @0 :Int16;
}

struct BoxInt32 {
  value @0 :Int32;
}

struct BoxInt64 {
  value @0 :Int64;
}

struct BoxUInt16 {
  value @0 :UInt16;
}

struct BoxUInt32 {
  value @0 :UInt32;
}

struct BoxUInt64 {
  value @0 :UInt64;
}

struct BoxFloat32 {
  value @0 :Float32;
}

struct BoxFloat64 {
  value @0 :Float64;
}

struct BoxBool {
  value @0 :Bool;
}

# C++ STL types

struct Optional(T) {
  union {
    nullopt @0 :Void;
    value @1 :T;
  }
}

struct UniquePtr(T) {
  union {
    nullptr @0 :Void;
    value @1 :T;
  }
}

struct SharedPtr(T) {
  union {
    nullptr @0 :Void;
    entry @1 :Entry;
  }

  struct Entry {
    id @0 :UInt64;
    value @1 :T;
  }
}

struct Map(K, V) {
  entries @0 :List(Entry);

  struct Entry {
    key @0 :K;
    value @1 :V;
  }
}

struct Pair(First, Second) {
  first @0 :First;
  second @1 :Second;
}

# Our types

struct Bound(T) {
  type @0 :Type;
  value @1 :T;

  enum Type {
    inclusive @0;
    exclusive @1;
  }
}
