@0xe7647d63b36c2c65;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("utils::capnp");


# Primitive types wrappers.
struct BoxInt32 {
  value @0 :Int32;
}

struct BoxInt64 {
  value @0 :Int64;
}

struct BoxUInt32 {
  value @0 :UInt32;
}

struct BoxUInt64 {
  value @0 :UInt32;
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


# CPP Types.

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

# Our types

struct TypedValue {
  union {
    nullType @0 :Void;
    bool @1 :Bool;
    integer @2 :Int64;
    double @3 :Float64;
    string @4 :Text;
    list @5 :List(TypedValue);
    map @6 :List(Entry);
    # TODO vertex accessor
    # TODO edge accessor
    # TODO path
  }

  struct Entry {
    key @0 :Text;
    value @1 :TypedValue;
  }
}

struct Bound(T) {
  type @0 :Type;
  value @1 :T;

  enum Type {
    inclusive @0;
    exclusive @1;
  }
}
