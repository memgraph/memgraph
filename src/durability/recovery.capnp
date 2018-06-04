@0xb3d70bc0576218f3;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("durability::capnp");

struct RecoveryInfo {
  snapshotTxId @0 :UInt64;
  maxWalTxId @1 :UInt64;
}
