@0x93c2449a1e02365a;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("io::network::capnp");

struct Endpoint {
  address @0 :Text;
  port @1 :UInt16;
  family @2 :UInt8;
}
