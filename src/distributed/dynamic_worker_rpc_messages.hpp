// -*- buffer-read-only: t; -*-
// vim: readonly
// DO NOT EDIT! Generated using LCP from 'dynamic_worker_rpc_messages.lcp'

#pragma once

#include <string>
#include <vector>

#include "communication/rpc/messages.hpp"
#include "distributed/dynamic_worker_rpc_messages.capnp.h"

namespace distributed {

struct DynamicWorkerReq {
  using Capnp = capnp::DynamicWorkerReq;
  static const communication::rpc::MessageType TypeInfo;
  DynamicWorkerReq() {}

  void Save(capnp::DynamicWorkerReq::Builder *builder) const;

  static std::unique_ptr<DynamicWorkerReq> Construct(
      const capnp::DynamicWorkerReq::Reader &reader);

  void Load(const capnp::DynamicWorkerReq::Reader &reader);
};

struct DynamicWorkerRes {
  using Capnp = capnp::DynamicWorkerRes;
  static const communication::rpc::MessageType TypeInfo;
  DynamicWorkerRes() {}
  explicit DynamicWorkerRes(
      std::vector<std::pair<std::string, std::string>> recover_indices)
      : recover_indices(recover_indices) {}

  std::vector<std::pair<std::string, std::string>> recover_indices;

  void Save(capnp::DynamicWorkerRes::Builder *builder) const;

  static std::unique_ptr<DynamicWorkerRes> Construct(
      const capnp::DynamicWorkerRes::Reader &reader);

  void Load(const capnp::DynamicWorkerRes::Reader &reader);
};

using DynamicWorkerRpc =
    communication::rpc::RequestResponse<DynamicWorkerReq, DynamicWorkerRes>;
}
