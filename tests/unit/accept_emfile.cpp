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

// Accept loops must neither spin nor die when the process runs out of file descriptors (EMFILE), and must accept again
// once descriptors are freed. Each scenario runs in a forked child (own RLIMIT_NOFILE, own CPU accounting).

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <boost/beast/http.hpp>

#include "communication/bolt/v1/state.hpp"
#include "communication/http/server.hpp"
#include "communication/server.hpp"
#include "communication/v2/server.hpp"
#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"
#include "utils/priority_thread_pool.hpp"

using namespace std::chrono_literals;

namespace {

constexpr rlim_t kFdLimit = 64;
constexpr auto kHold = 1500ms;
constexpr double kMaxCpuSecondsWhileHeld = 0.5;
constexpr auto kRecoveryDeadline = 10s;
constexpr int kSkipExitCode = 77;

double CpuSeconds() {
  rusage ru{};
  getrusage(RUSAGE_SELF, &ru);
  auto const tv = [](timeval t) { return static_cast<double>(t.tv_sec) + static_cast<double>(t.tv_usec) / 1e6; };
  return tv(ru.ru_utime) + tv(ru.ru_stime);
}

std::vector<int> ExhaustFds() {
  std::vector<int> fds;
  while (true) {
    int const fd = open("/dev/null", O_RDONLY);
    if (fd < 0) break;
    fds.push_back(fd);
  }
  return fds;
}

void SetRecvTimeout(int fd, int seconds) {
  timeval tv{.tv_sec = seconds, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

bool RawExchange(uint16_t port, std::string_view request, std::string_view expected_prefix) {
  int const fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  SetRecvTimeout(fd, 2);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bool ok = false;
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0 &&
      write(fd, request.data(), request.size()) == static_cast<ssize_t>(request.size())) {
    char buf[256];
    auto const n = read(fd, buf, sizeof(buf));
    ok = n >= static_cast<ssize_t>(expected_prefix.size()) && std::string_view(buf, n).starts_with(expected_prefix);
  }
  close(fd);
  return ok;
}

bool Retry(const std::function<bool()> &probe) {
  auto const deadline = std::chrono::steady_clock::now() + kRecoveryDeadline;
  while (std::chrono::steady_clock::now() < deadline) {
    if (probe()) return true;
    std::this_thread::sleep_for(100ms);
  }
  return false;
}

int ExhaustionScenario(uint16_t port, const std::function<bool()> &accepted_probe) {
  // Created before descriptors run out; connect() needs no new fd.
  std::vector<int> clients;
  for (int i = 0; i < 3; ++i) clients.push_back(socket(AF_INET, SOCK_STREAM, 0));

  auto fillers = ExhaustFds();
  if (fillers.empty()) {
    std::fprintf(stderr, "scenario: baseline fd usage already at the limit\n");
    return kSkipExitCode;
  }
  errno = 0;
  if (int const fd = open("/dev/null", O_RDONLY); fd < 0 && errno == ENFILE) {
    std::fprintf(stderr, "scenario: hit the system-wide fd limit (ENFILE) instead of EMFILE\n");
    return kSkipExitCode;
  } else if (fd >= 0 || errno != EMFILE) {
    std::fprintf(stderr, "scenario: expected open() to fail with EMFILE, got errno %d\n", errno);
    return 5;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  for (int const c : clients) {
    if (connect(c, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
      std::fprintf(stderr, "scenario: connect failed\n");
      return 2;
    }
  }

  auto const cpu_before = CpuSeconds();
  std::this_thread::sleep_for(kHold);
  auto const cpu_used = CpuSeconds() - cpu_before;

  for (int const fd : fillers) close(fd);
  for (int const c : clients) close(c);

  if (cpu_used > kMaxCpuSecondsWhileHeld) {
    std::fprintf(stderr, "scenario: accept loop burned %.2fs CPU while at the fd limit\n", cpu_used);
    return 3;
  }
  if (!Retry(accepted_probe)) {
    std::fprintf(stderr, "scenario: server did not accept after descriptors were freed\n");
    return 4;
  }
  return 0;
}

// `body` runs in the forked child and returns its exit code.
void RunInChild(const std::function<int()> &body) {
  pid_t const pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    rlimit const lim{kFdLimit, kFdLimit};
    setrlimit(RLIMIT_NOFILE, &lim);
    _exit(body());
  }
  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status)) << "child terminated abnormally";
  if (WEXITSTATUS(status) == kSkipExitCode) GTEST_SKIP() << "could not reach EMFILE (see child stderr)";
  EXPECT_EQ(WEXITSTATUS(status), 0) << "scenario failed (see child stderr)";
}

struct Empty {};

struct OkHandler {
  explicit OkHandler(Empty * /*unused*/) {}

  template <class Body, class Allocator>
  void HandleRequest(boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>> &&req,
                     std::function<void(boost::beast::http::response<boost::beast::http::string_body>)> &&send) {
    boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::ok, req.version()};
    res.keep_alive(false);
    res.prepare_payload();
    send(std::move(res));
  }
};

struct OpenAuth : memgraph::communication::websocket::AuthenticationInterface {
  bool Authenticate(const std::string & /*u*/, const std::string & /*p*/) const override { return true; }

  bool HasWebsocketPermission() const override { return true; }

  bool AccessControlled() const override { return false; }
};

class EchoSession {
 public:
  EchoSession(Empty *, const memgraph::io::network::Endpoint &, memgraph::communication::InputStream *in,
              memgraph::communication::OutputStream *out)
      : in_(in), out_(out) {}

  void Execute() {
    out_->Write(in_->data(), in_->size());
    in_->Shift(in_->size());
  }

 private:
  memgraph::communication::InputStream *in_;
  memgraph::communication::OutputStream *out_;
};

std::atomic<int> g_v2_sessions_created{0};

struct V2Context {
  using TaskID = memgraph::utils::PriorityThreadPool::TaskID;

  template <class F, class P>
  TaskID AddTask(F &&, P, bool = true) {
    return 0;
  }

  void ParkAdmission(auto &&, TaskID, std::chrono::steady_clock::time_point, memgraph::utils::WaitResource, uint64_t) {}

  uint64_t WakeEpoch(memgraph::utils::WaitResource) const noexcept { return 0; }

  bool IsDrainingAdmissions() const noexcept { return false; }

  uint32_t RescheduleCap() const noexcept { return 1; }
};

struct V2Session {
  template <class In, class Out>
  V2Session(V2Context &, In *, Out *) {
    g_v2_sessions_created.fetch_add(1);
  }

  bool Execute() { return false; }

  void HandleError() {}

  int ApproximateQueryPriority() const { return 0; }

  bool HasPendingCommit() const { return false; }

  bool HasPendingBegin() const { return false; }

  bool HasBufferedData() const { return false; }

  memgraph::communication::bolt::PendingOutcome FinishPendingCommit() {
    return memgraph::communication::bolt::PendingOutcome::Done;
  }

  memgraph::communication::bolt::PendingOutcome FinishPendingBegin() {
    return memgraph::communication::bolt::PendingOutcome::Done;
  }

  std::chrono::steady_clock::time_point PendingBeginDeadline() const { return {}; }
};

// v2::Server reports the requested endpoint, not the bound one, so pick a port up front. The returned socket holds the
// port until the server has bound it: with SO_REUSEADDR on both, Linux lets the server bind and listen next to a
// non-listening holder, while unrelated binds get EADDRINUSE.
std::pair<int, uint16_t> ReserveFreePort() {
  int const fd = socket(AF_INET, SOCK_STREAM, 0);
  int const one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t len = sizeof(addr);
  if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0 ||
      getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    close(fd);
    return {-1, 0};
  }
  return {fd, ntohs(addr.sin_port)};
}

}  // namespace

TEST(AcceptEmfile, HttpListenerRearmsAfterTransientAcceptError) {
  RunInChild([] {
    Empty data;
    memgraph::communication::ServerContext context;
    memgraph::communication::http::Server<OkHandler, Empty> server{{"127.0.0.1", 0}, &data, &context};
    server.Start();
    auto const port = server.GetEndpoint()->port();
    auto const rc = ExhaustionScenario(port, [port] {
      return RawExchange(port, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", "HTTP/1.1 200");
    });
    server.Shutdown();
    server.AwaitShutdown();
    return rc;
  });
}

TEST(AcceptEmfile, WebsocketListenerRearmsAfterTransientAcceptError) {
  RunInChild([] {
    OpenAuth auth;
    memgraph::communication::ServerContext context;
    memgraph::communication::websocket::Server server{{"127.0.0.1", 0}, &context, auth};
    server.Start();
    auto const port = server.GetEndpoint()->port();
    auto const rc = ExhaustionScenario(port, [port] {
      return RawExchange(port,
                         "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                         "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
                         "HTTP/1.1 101");
    });
    server.Shutdown();
    server.AwaitShutdown();
    return rc;
  });
}

TEST(AcceptEmfile, RpcV1AcceptThreadDoesNotSpinAndRecovers) {
  RunInChild([] {
    Empty data;
    memgraph::communication::ServerContext context;
    memgraph::communication::Server<EchoSession, Empty> server{{"127.0.0.1", 0}, &data, &context, 60, "Test", 1};
    if (!server.Start()) return 1;
    auto const port = server.endpoint().GetPort();
    auto const rc = ExhaustionScenario(port, [port] { return RawExchange(port, "tttt", "tttt"); });
    server.Shutdown();
    server.AwaitShutdown();
    return rc;
  });
}

TEST(AcceptEmfile, RpcV2AcceptDoesNotSpinAndRecovers) {
  RunInChild([] {
    V2Context data;
    memgraph::communication::ServerContext context;
    auto const [reservation, port] = ReserveFreePort();
    if (reservation < 0) return 1;
    memgraph::communication::v2::ServerEndpoint endpoint{boost::asio::ip::make_address("127.0.0.1"), port};
    memgraph::communication::v2::Server<V2Session, V2Context> server{endpoint, &data, &context, "Test", 1};
    close(reservation);
    if (!server.Start()) return 1;
    auto const rc = ExhaustionScenario(port, [] { return g_v2_sessions_created.load() > 0; });
    server.Shutdown();
    server.AwaitShutdown();
    return rc;
  });
}
