@0xcdbe169866471033;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("tx::capnp");

struct Snapshot {
  transactionIds @0 :List(UInt64);
}

struct CommitLogInfo {
  flags @0 :UInt8;
}
