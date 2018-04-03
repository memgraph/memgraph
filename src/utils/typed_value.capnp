@0xd229a9c0f7e55750;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("query::capnp");


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
