@0xb3d70bc0576218f3;

using Cxx = import "/capnp/c++.capnp";
using Utils = import "/utils/serialization.capnp";

$Cxx.namespace("durability::capnp");

struct RecoveryInfo {
  durabilityVersion @0 :UInt64;
  snapshotTxId @1 :UInt64;
  walRecovered @2 :List(UInt64);
}

struct RecoveryData {
  snapshooterTxId @0 :UInt64;
  walTxToRecover @1 :List(UInt64);
  snapshooterTxSnapshot @2 :List(UInt64);
  indexes @3 :List(Utils.Pair(Text, Text));
}
