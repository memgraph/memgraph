#include <iostream>
#include <libnuraft/nuraft.hxx>

struct ex_logger : ::nuraft::logger {
  void set_level(int) override {}

  void debug(const std::string &) override {}

  void info(const std::string &) override {}

  void warn(const std::string &) override {}

  void err(const std::string &log_line) override { std::cout << log_line << std::endl; }

  void put_details(int, const char *, const char *, size_t, const std::string &) override {}
};

class echo_state_machine : public nuraft::state_machine {
  nuraft::ulong last_commit_idx_;

 public:
  echo_state_machine() : last_commit_idx_(0) {}

  nuraft::ptr<nuraft::buffer> commit(const nuraft::ulong log_idx, nuraft::buffer &) override {
    last_commit_idx_ = log_idx;
    return nullptr;
  }

  nuraft::ptr<nuraft::buffer> pre_commit(const nuraft::ulong, nuraft::buffer &) override { return nullptr; }

  void rollback(const nuraft::ulong, nuraft::buffer &) override {}

  void save_snapshot_data(nuraft::snapshot &, const nuraft::ulong, nuraft::buffer &) override {}

  bool apply_snapshot(nuraft::snapshot &) override { return true; }

  int read_snapshot_data(nuraft::snapshot &, const nuraft::ulong, nuraft::buffer &) override { return 0; }

  nuraft::ptr<nuraft::snapshot> last_snapshot() override { return nullptr; }

  void create_snapshot(nuraft::snapshot &, nuraft::async_result<bool>::handler_type &) override {}

  nuraft::ulong last_commit_index() override { return last_commit_idx_; }
};

int main(int argc, char **argv) {
  // State machine.
  nuraft::ptr<nuraft::state_machine> smachine(nuraft::cs_new<echo_state_machine>());

  // Parameters.
  nuraft::raft_params params;

  // ASIO service.
  nuraft::ptr<nuraft::logger> l = nuraft::cs_new<ex_logger>();
  nuraft::ptr<nuraft::asio_service> asio_svc_ = nuraft::cs_new<nuraft::asio_service>();
  nuraft::ptr<nuraft::rpc_listener> listener(asio_svc_->create_rpc_listener((ushort)(9001), l));
  return 0;
}
