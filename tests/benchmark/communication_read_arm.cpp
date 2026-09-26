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

// Measures what it costs to arm a session's next socket read from the thread that just executed a
// request, which under the priority scheduler is never the thread the session's strand runs on.
// Arming it through the strand has to hand the work to an io thread, so the read is armed later and
// an io thread has to be woken for every request.
//
// Reports percentiles, not just a mean: the arming happens after the reply has been sent, so the
// delay only reaches a client that comes back for its next request before the arming has run, which
// makes this a question about the tail rather than about the average.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <sys/socket.h>

#include <benchmark/benchmark.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>

namespace {

using tcp = boost::asio::ip::tcp;
using Clock = std::chrono::steady_clock;

// How the thread that finished a request arms the next read.
enum class ArmMode : std::uint8_t {
  // What the session did before the read was funnelled onto the strand: arm it here and now.
  kInline,
  // What it does now: hand the arming to the strand, which is an io thread.
  kStrand,
  // Arm it here, but take a lock so the choice between arming and shutting down cannot interleave
  // with a foreign terminate request, which is what funnelling it onto the strand achieved.
  kInlineGuarded,
};

// The Bolt server's io threads, which every session's strand runs on. Idle ones are parked in
// epoll_wait, so handing them work has to wake one.
class IoPool {
 public:
  explicit IoPool(int threads) : io_{threads}, guard_{boost::asio::make_work_guard(io_)} {
    for (int i = 0; i < threads; ++i) {
      threads_.emplace_back([this] { io_.run(); });
    }
  }

  IoPool(const IoPool &) = delete;
  IoPool &operator=(const IoPool &) = delete;

  ~IoPool() {
    guard_.reset();
    io_.stop();
    for (auto &thread : threads_) {
      thread.join();
    }
  }

  boost::asio::io_context &Context() noexcept { return io_; }

 private:
  boost::asio::io_context io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard_;
  std::vector<std::thread> threads_;
};

// Stands in for the priority thread pool a request is executed on. The hop onto it happens in every
// mode, so it cancels out of the comparison.
class WorkerPool {
 public:
  explicit WorkerPool(int threads) {
    for (int i = 0; i < threads; ++i) {
      threads_.emplace_back([this] { Run(); });
    }
  }

  WorkerPool(const WorkerPool &) = delete;
  WorkerPool &operator=(const WorkerPool &) = delete;

  ~WorkerPool() {
    {
      auto guard = std::lock_guard{mutex_};
      stop_ = true;
    }
    cv_.notify_all();
    for (auto &thread : threads_) {
      thread.join();
    }
  }

  void Post(std::function<void()> task) {
    {
      auto guard = std::lock_guard{mutex_};
      tasks_.push_back(std::move(task));
    }
    cv_.notify_one();
  }

 private:
  void Run() {
    while (true) {
      auto task = std::function<void()>{};
      {
        auto lock = std::unique_lock{mutex_};
        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
        if (stop_) return;
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  bool stop_{false};
  std::vector<std::thread> threads_;
};

constexpr std::size_t kRequestSize = 64;
constexpr std::size_t kReplySize = 64;

// A session reduced to the part this measures: read a request on the strand, execute it on a worker
// thread, write the reply from there with a synchronous send as the real Write does, then arm the
// next read the way `mode` says.
class MiniSession : public std::enable_shared_from_this<MiniSession> {
 public:
  MiniSession(tcp::socket socket, WorkerPool &worker, ArmMode mode)
      : socket_{std::move(socket)},
        strand_{boost::asio::make_strand(socket_.get_executor())},
        worker_{worker},
        mode_{mode} {}

  void Start() {
    boost::asio::dispatch(strand_, [self = shared_from_this()] { self->ArmRead(); });
  }

 private:
  void ArmRead() {
    socket_.async_read_some(
        boost::asio::buffer(buffer_),
        boost::asio::bind_executor(
            strand_, [self = shared_from_this()](const auto &ec, std::size_t bytes) { self->OnRead(ec, bytes); }));
  }

  void OnRead(const boost::system::error_code &ec, std::size_t /*bytes*/) {
    if (ec) return;
    worker_.Post([self = shared_from_this()] { self->ExecuteAndArm(); });
  }

  // Runs on a worker thread, which is foreign to strand_.
  void ExecuteAndArm() {
    auto ec = boost::system::error_code{};
    socket_.send(boost::asio::buffer(reply_), MSG_NOSIGNAL, ec);
    if (ec) return;

    switch (mode_) {
      case ArmMode::kInline:
        ArmRead();
        break;
      case ArmMode::kStrand:
        boost::asio::dispatch(strand_, [self = shared_from_this()] { self->ArmRead(); });
        break;
      case ArmMode::kInlineGuarded: {
        auto guard = std::lock_guard{arm_mutex_};
        if (terminate_requested_.load(std::memory_order_acquire)) return;
        ArmRead();
        break;
      }
    }
  }

  tcp::socket socket_;
  boost::asio::strand<tcp::socket::executor_type> strand_;
  WorkerPool &worker_;
  ArmMode mode_;
  std::array<std::uint8_t, kRequestSize> buffer_{};
  std::array<std::uint8_t, kReplySize> reply_{};
  std::mutex arm_mutex_;
  std::atomic<bool> terminate_requested_{false};
};

// One client's connection, driven from its own thread so each behaves like one of mgbench's
// concurrent worker processes: send a request, wait for the reply, repeat.
class Client {
 public:
  Client(boost::asio::io_context &io, std::uint16_t port) : socket_{io} {
    socket_.connect(tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), port});
    socket_.set_option(tcp::no_delay{true});
  }

  // Returns each round trip's duration in microseconds.
  std::vector<double> Run(int round_trips) {
    auto durations = std::vector<double>{};
    durations.reserve(round_trips);
    for (int i = 0; i < round_trips; ++i) {
      const auto start = Clock::now();
      boost::asio::write(socket_, boost::asio::buffer(request_));
      auto received = std::size_t{0};
      while (received < kReplySize) {
        received += socket_.read_some(boost::asio::buffer(reply_.data() + received, kReplySize - received));
      }
      durations.push_back(std::chrono::duration<double, std::micro>(Clock::now() - start).count());
    }
    return durations;
  }

 private:
  tcp::socket socket_;
  std::array<std::uint8_t, kRequestSize> request_{};
  std::array<std::uint8_t, kReplySize> reply_{};
};

// Server and the given number of concurrent clients, over loopback.
class Fixture {
 public:
  // The priority pool is a fixed size in the server, so it does not grow with the client count.
  Fixture(int io_threads, int clients, ArmMode mode) : pool_{io_threads}, worker_{io_threads} {
    auto acceptor = tcp::acceptor{pool_.Context(), tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), 0}};
    const auto port = acceptor.local_endpoint().port();

    for (int i = 0; i < clients; ++i) {
      auto accepted = std::promise<tcp::socket>{};
      auto accepted_future = accepted.get_future();
      acceptor.async_accept([&accepted](const boost::system::error_code &ec, tcp::socket socket) {
        if (!ec) accepted.set_value(std::move(socket));
      });

      clients_.push_back(std::make_unique<Client>(client_io_, port));
      auto session = std::make_shared<MiniSession>(accepted_future.get(), worker_, mode);
      session->Start();
      sessions_.push_back(std::move(session));
    }
  }

  // Every client runs `round_trips` requests at once; returns all durations pooled, as mgbench
  // pools its workers' durations before taking percentiles.
  std::vector<double> Run(int round_trips) {
    auto per_client = std::vector<std::vector<double>>(clients_.size());
    auto threads = std::vector<std::thread>{};
    threads.reserve(clients_.size());
    for (std::size_t i = 0; i < clients_.size(); ++i) {
      threads.emplace_back([this, i, round_trips, &per_client] { per_client[i] = clients_[i]->Run(round_trips); });
    }
    for (auto &thread : threads) {
      thread.join();
    }

    auto pooled = std::vector<double>{};
    for (const auto &durations : per_client) {
      pooled.insert(pooled.end(), durations.begin(), durations.end());
    }
    return pooled;
  }

 private:
  IoPool pool_;
  WorkerPool worker_;
  boost::asio::io_context client_io_;
  std::vector<std::unique_ptr<Client>> clients_;
  std::vector<std::shared_ptr<MiniSession>> sessions_;
};

double Percentile(std::vector<double> &sorted, double fraction) {
  const auto index = static_cast<std::size_t>(static_cast<double>(sorted.size()) * fraction);
  return sorted[std::min(index, sorted.size() - 1)];
}

// mgbench runs six concurrent client workers and takes percentiles over all of them; the sweep
// either side of six says whether what is measured there is particular to that concurrency.
constexpr int kIoThreads = 22;
constexpr int kRoundTripsPerClient = 2000;

void ConcurrentRoundTrip(benchmark::State &state, ArmMode mode) {
  const auto clients = static_cast<int>(state.range(0));
  auto fixture = Fixture{kIoThreads, clients, mode};
  fixture.Run(200);

  for (auto _ : state) {
    const auto started = Clock::now();
    auto durations = fixture.Run(kRoundTripsPerClient);
    const auto elapsed = std::chrono::duration<double>(Clock::now() - started).count();

    state.PauseTiming();
    std::sort(durations.begin(), durations.end());
    state.counters["p50_us"] = Percentile(durations, 0.50);
    state.counters["p90_us"] = Percentile(durations, 0.90);
    state.counters["p99_us"] = Percentile(durations, 0.99);
    auto total = 0.0;
    for (const auto duration : durations) {
      total += duration;
    }
    state.counters["mean_us"] = total / static_cast<double>(durations.size());
    state.counters["kqps"] = static_cast<double>(durations.size()) / elapsed / 1000.0;
    state.ResumeTiming();
  }
}

// The arming delay on its own, with no socket in the way: how long after a foreign thread asks for
// the read to be armed does the arming actually run.
void ArmDelay(benchmark::State &state, ArmMode mode) {
  auto pool = IoPool{static_cast<int>(state.range(0))};
  auto strand = boost::asio::make_strand(pool.Context());

  auto mutex = std::mutex{};
  auto cv = std::condition_variable{};
  auto armed = Clock::time_point{};
  auto done = false;

  for (auto _ : state) {
    done = false;
    const auto asked = Clock::now();
    auto arm = [&] {
      const auto now = Clock::now();
      {
        auto guard = std::lock_guard{mutex};
        armed = now;
        done = true;
      }
      cv.notify_one();
    };

    if (mode == ArmMode::kStrand) {
      boost::asio::dispatch(strand, arm);
      auto lock = std::unique_lock{mutex};
      cv.wait(lock, [&done] { return done; });
    } else {
      arm();
    }

    state.SetIterationTime(std::chrono::duration<double>(armed - asked).count());
  }
}

}  // namespace

#define CLIENT_SWEEP RangeMultiplier(2)->Range(1, 64)->UseRealTime()

BENCHMARK_CAPTURE(ConcurrentRoundTrip, inline_arm, ArmMode::kInline)->CLIENT_SWEEP;
BENCHMARK_CAPTURE(ConcurrentRoundTrip, strand_arm, ArmMode::kStrand)->CLIENT_SWEEP;
BENCHMARK_CAPTURE(ConcurrentRoundTrip, inline_guarded_arm, ArmMode::kInlineGuarded)->CLIENT_SWEEP;

BENCHMARK_CAPTURE(ArmDelay, inline_arm, ArmMode::kInline)->Arg(22)->UseManualTime();
BENCHMARK_CAPTURE(ArmDelay, strand_arm, ArmMode::kStrand)->Arg(22)->UseManualTime();

BENCHMARK_MAIN();
