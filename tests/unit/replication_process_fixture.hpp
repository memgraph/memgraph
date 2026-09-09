// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

// Process-isolated replica fixture. The replica handlers' two-phase-commit cache is static and process-global, so
// several replicas in one process would share one prepared-accessor slot; every test with more than one real
// replica server launches each replica as a fresh executable (this test binary in its replica role) through
// posix_spawn, with MinMemgraph constructed only there. Fault selection and observations travel over pipes; the
// controller (the test process) owns and reaps every role process.
//
// Protocol (one line per message):
//   controller -> replica: "refuse_prepare", "refuse_abort_decision", "quit"; each answered with "ok".
//   replica -> controller: "ready" once the replication server listens; "prepared <ts>" after a prepare response was
//   sent; "abort_applied <ts>" after an abort decision was applied.

#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <deque>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "dbms/inmemory/replication_handlers.hpp"
#include "io/network/endpoint.hpp"
#include "replication/config.hpp"
#include "storage/v2/commit_probe.hpp"
#include "storage/v2/config.hpp"
#include "tests/unit/replication_min_memgraph.hpp"

namespace memgraph::tests {

constexpr char kReplicaRoleFlag[] = "--mg-replica-role";

// The replica role: runs until "quit" on stdin. Arguments: <port> <data-directory>.
// Writes the whole buffer, retrying on EINTR; false when the peer is gone.
inline bool WriteAll(int fd, std::string const &data) {
  size_t written = 0;
  while (written < data.size()) {
    auto const n = write(fd, data.data() + written, data.size() - written);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return false;
    written += static_cast<size_t>(n);
  }
  return true;
}

inline int RunReplicaRole(int argc, char **argv) {
  if (argc < 4) return 2;
  // The IPC channel is the inherited stdout; point fd 1 at stderr so nothing the logger prints can be parsed as an
  // event.
  int const ipc_fd = dup(STDOUT_FILENO);
  dup2(STDERR_FILENO, STDOUT_FILENO);
  signal(SIGPIPE, SIG_IGN);
  auto const port = static_cast<uint16_t>(std::stoi(argv[2]));
  std::filesystem::path const data_dir{argv[3]};
  std::filesystem::remove_all(data_dir);
  std::filesystem::create_directories(data_dir);
  auto const emit = [ipc_fd](std::string line) {
    line.push_back('\n');
    static_cast<void>(WriteAll(ipc_fd, line));
  };

  storage::Config config{
      .durability =
          {
              .root_data_directory = data_dir,
              .snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
          },
      .salient.items = {.properties_on_edges = true},
      .register_metrics = false,
  };
  storage::UpdatePaths(config, data_dir);
  MinMemgraph replica(config);
  std::atomic<bool> refuse_prepare{false};
  std::atomic<bool> refuse_abort_decision{false};
  storage::ReplicationTestHooks hooks;
  hooks.refuse_next_prepare = [&](uint64_t) { return refuse_prepare.exchange(false); };
  hooks.refuse_next_abort_decision = [&](uint64_t) { return refuse_abort_decision.exchange(false); };
  hooks.on_prepared = [&](uint64_t ts) { emit("prepared " + std::to_string(ts)); };
  hooks.on_abort_applied = [&](uint64_t ts) { emit("abort_applied " + std::to_string(ts)); };
  static_cast<storage::InMemoryStorage *>(replica.db.storage())->SetReplicationTestHooks(&hooks);
  replica.repl_handler.TrySetReplicationRoleReplica(
      replication::ReplicationServerConfig{.repl_server = io::network::Endpoint("127.0.0.1", port)});
  emit("ready");

  std::string buffer;
  char chunk[256];
  while (true) {
    auto const n = read(STDIN_FILENO, chunk, sizeof(chunk));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;  // the controller went away
    buffer.append(chunk, static_cast<size_t>(n));
    size_t newline = 0;
    while ((newline = buffer.find('\n')) != std::string::npos) {
      auto const command = buffer.substr(0, newline);
      buffer.erase(0, newline + 1);
      if (command == "refuse_prepare") {
        refuse_prepare = true;
        emit("ok");
      } else if (command == "refuse_abort_decision") {
        refuse_abort_decision = true;
        emit("ok");
      } else if (command == "quit") {
        emit("ok");
        static_cast<storage::InMemoryStorage *>(replica.db.storage())->SetReplicationTestHooks(nullptr);
        // A prepared transaction the main never decided on (it was killed by a death test) lives in the static 2PC
        // cache and would be destroyed after the storage; abort it while the storage is still alive. The abort
        // hands the transaction's deltas to GC, so it runs under the database's arena scope like any storage work.
        const memory::DbArenaScope arena_scope{&replica.db.Arena()};
        dbms::InMemoryReplicationHandlers::AbortTwoPCForTenant(replica.db.storage()->uuid());
        return 0;
      } else {
        emit("unknown");
      }
    }
  }
  static_cast<storage::InMemoryStorage *>(replica.db.storage())->SetReplicationTestHooks(nullptr);
  return 0;
}

// A replica running in its own process, owned and reaped by the test.
class ReplicaProcess {
 public:
  ReplicaProcess(uint16_t port, std::filesystem::path data_dir) : port_{port}, data_dir_{std::move(data_dir)} {
    signal(SIGPIPE, SIG_IGN);  // a dead replica must not take the controller down on the next Send
    int to_child[2];
    int from_child[2];
    // Close-on-exec so a later spawn does not inherit an earlier replica's pipe ends (which would defeat the
    // "controller went away" EOF); the dup2 file actions clear the flag on the child's own ends.
    if (pipe2(to_child, O_CLOEXEC) != 0) throw std::runtime_error("pipe failed");
    if (pipe2(from_child, O_CLOEXEC) != 0) {
      close(to_child[0]);
      close(to_child[1]);
      throw std::runtime_error("pipe failed");
    }
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions, to_child[0], STDIN_FILENO);
    posix_spawn_file_actions_adddup2(&actions, from_child[1], STDOUT_FILENO);
    auto const port_arg = std::to_string(port_);
    auto const dir_arg = data_dir_.string();
    std::vector<char *> argv{const_cast<char *>("/proc/self/exe"),
                             const_cast<char *>(kReplicaRoleFlag),
                             const_cast<char *>(port_arg.c_str()),
                             const_cast<char *>(dir_arg.c_str()),
                             nullptr};
    auto const rc = posix_spawn(&pid_, "/proc/self/exe", &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    close(to_child[0]);
    close(from_child[1]);
    if (rc != 0) {
      close(to_child[1]);
      close(from_child[0]);
      pid_ = -1;
      throw std::runtime_error("posix_spawn failed");
    }
    stdin_fd_ = to_child[1];
    stdout_fd_ = from_child[0];
    reader_ = std::thread{[this] { ReadLoop(); }};
  }

  ReplicaProcess(ReplicaProcess const &) = delete;
  ReplicaProcess &operator=(ReplicaProcess const &) = delete;

  // Asks the replica to quit, reaps it (SIGKILL after 10 s) and records how it ended; a replica that crashed
  // rather than exited is reported as a test failure.
  ~ReplicaProcess() {
    if (pid_ > 0) {
      static_cast<void>(Send("quit"));
      int status = 0;
      pid_t reaped = 0;
      auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while ((reaped = waitpid(pid_, &status, WNOHANG)) == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      if (reaped == 0) {
        kill(pid_, SIGKILL);
        reaped = waitpid(pid_, &status, 0);
        ADD_FAILURE() << "replica process on port " << port_ << " did not quit and was killed";
      } else if (reaped == pid_ && WIFSIGNALED(status)) {
        ADD_FAILURE() << "replica process on port " << port_ << " died with signal " << WTERMSIG(status);
      } else if (reaped == pid_ && WIFEXITED(status) && WEXITSTATUS(status) != 0) {
        ADD_FAILURE() << "replica process on port " << port_ << " exited with status " << WEXITSTATUS(status);
      }
    }
    if (stdin_fd_ >= 0) close(stdin_fd_);
    if (reader_.joinable()) reader_.join();
    if (stdout_fd_ >= 0) close(stdout_fd_);
    std::filesystem::remove_all(data_dir_);
  }

  auto port() const -> uint16_t { return port_; }

  auto endpoint() const -> io::network::Endpoint { return io::network::Endpoint("127.0.0.1", port_); }

  // Blocks until the replica reports that its server listens.
  bool WaitReady(std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
    return WaitEvent("ready", timeout).has_value();
  }

  // Sends one command and waits for its acknowledgement.
  bool Send(std::string const &command, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    if (!WriteAll(stdin_fd_, command + "\n")) return false;
    return WaitEvent("ok", timeout).has_value();
  }

  // Waits for the next event line starting with `prefix` and returns it (consumed); other events stay queued.
  std::optional<std::string> WaitEvent(std::string const &prefix, std::chrono::milliseconds timeout) {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
      {
        auto guard = std::lock_guard{mutex_};
        for (auto it = events_.begin(); it != events_.end(); ++it) {
          if (it->starts_with(prefix)) {
            auto event = *it;
            events_.erase(it);
            return event;
          }
        }
      }
      if (std::chrono::steady_clock::now() > deadline) return std::nullopt;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }

  // Number of queued events starting with `prefix` (not consumed).
  size_t CountEvents(std::string const &prefix) {
    auto guard = std::lock_guard{mutex_};
    size_t count = 0;
    for (auto const &event : events_) {
      if (event.starts_with(prefix)) ++count;
    }
    return count;
  }

 private:
  void ReadLoop() {
    std::string buffer;
    char chunk[256];
    while (true) {
      auto const n = read(stdout_fd_, chunk, sizeof(chunk));
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return;
      buffer.append(chunk, static_cast<size_t>(n));
      size_t newline = 0;
      while ((newline = buffer.find('\n')) != std::string::npos) {
        auto guard = std::lock_guard{mutex_};
        events_.push_back(buffer.substr(0, newline));
        buffer.erase(0, newline + 1);
      }
    }
  }

  uint16_t port_;
  std::filesystem::path data_dir_;
  pid_t pid_{-1};
  int stdin_fd_{-1};
  int stdout_fd_{-1};
  std::thread reader_;
  std::mutex mutex_;
  std::deque<std::string> events_;
};

}  // namespace memgraph::tests
