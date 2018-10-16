#pragma once

#include "distributed/coordination.hpp"

const std::string kLocal = "127.0.0.1";

// TODO (mferencevic): These test classes should be replaced with the real
// coordination once `ClusterDiscoveryXYZ` is merged with `CoordinationXYZ`.

class TestMasterCoordination : public distributed::Coordination {
 public:
  TestMasterCoordination()
      : distributed::Coordination({kLocal, 0}, 0, {kLocal, 0}) {}

  void Stop() {
    server_.Shutdown();
    server_.AwaitShutdown();
  }
};

class TestWorkerCoordination : public distributed::Coordination {
 public:
  TestWorkerCoordination(const io::network::Endpoint &master_endpoint,
                         int worker_id)
      : distributed::Coordination({kLocal, 0}, worker_id, master_endpoint) {}

  void Stop() {
    server_.Shutdown();
    server_.AwaitShutdown();
  }
};
