@0xd3832c9a1a3d8ec7;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("communication::rpc::capnp");

struct Message {
  typeId @0 :UInt64;
  data @1 :AnyPointer;
}
