#pragma once
/// @file

#include <future>

namespace utils {

/// Wraps an `std::future` object to ensure that upon destruction the
/// `std::future` is waited on.
template <typename TResult>
class Future {
 public:
  Future() {}
  explicit Future(std::future<TResult> future) : future_(std::move(future)) {}

  Future(const Future &) = delete;
  Future(Future &&) = default;
  Future &operator=(const Future &) = delete;
  Future &operator=(Future &&) = default;

  ~Future() {
    if (future_.valid()) future_.wait();
  }

  /// Returns true if the future has the result available. NOTE: The behaviour
  /// is undefined if future isn't valid, i.e.  `future.valid() == false`.
  bool IsReady() const {
    auto status = future_.wait_for(std::chrono::seconds(0));
    return status == std::future_status::ready;
  }

  auto get() { return future_.get(); }
  auto wait() { return future_.wait(); }
  auto valid() { return future_.valid(); }

 private:
  std::future<TResult> future_;
};

/// Creates a `Future` from the given `std::future`.
template <typename TResult>
Future<TResult> make_future(std::future<TResult> future) {
  return Future<TResult>(std::move(future));
}

}  // namespace utils
