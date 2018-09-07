# -*- buffer-read-only: t; -*-
# vim: readonly
# DO NOT EDIT! Generated using LCP from 'dynamic_worker_rpc_messages.lcp'

@0x8c53f6c9a0c71b05;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("distributed::capnp");

using Utils = import "/utils/serialization.capnp";

struct DynamicWorkerRes {
  recoverIndices @0 :List(Utils.Pair(Text, Text));
}

struct DynamicWorkerReq {
}

