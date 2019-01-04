#include "gtest/gtest.h"

#include <unordered_map>
#include <vector>

#include "durability/single_node_ha/state_delta.hpp"
#include "raft/raft_interface.hpp"
#include "transactions/single_node_ha/engine.hpp"
#include "transactions/transaction.hpp"

using namespace tx;

class RaftMock final : public raft::RaftInterface {
 public:
  void Emplace(const database::StateDelta &delta) override {
    log_[delta.transaction_id].emplace_back(std::move(delta));
  }

  bool SafeToCommit(const tx::TransactionId &tx_id) override { return true; }

  bool IsLeader() override { return true; }

  std::vector<database::StateDelta> GetLogForTx(
      const tx::TransactionId &tx_id) {
    return log_[tx_id];
  }

 private:
  std::unordered_map<tx::TransactionId, std::vector<database::StateDelta>> log_;
};

TEST(Engine, Reset) {
  RaftMock raft;
  Engine engine{&raft};

  auto t0 = engine.Begin();
  EXPECT_EQ(t0->id_, 1);
  engine.Commit(*t0);

  engine.Reset();

  auto t1 = engine.Begin();
  EXPECT_EQ(t1->id_, 1);
  engine.Commit(*t1);
}

TEST(Engine, TxStateDelta) {
  RaftMock raft;
  Engine engine{&raft};

  auto t0 = engine.Begin();
  tx::TransactionId tx_id = t0->id_;
  engine.Commit(*t0);

  auto t0_log = raft.GetLogForTx(tx_id);
  EXPECT_EQ(t0_log.size(), 2);

  using Type = enum database::StateDelta::Type;
  EXPECT_EQ(t0_log[0].type, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(t0_log[0].transaction_id, tx_id);
  EXPECT_EQ(t0_log[1].type, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(t0_log[1].transaction_id, tx_id);
}
