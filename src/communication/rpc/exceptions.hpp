#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"

namespace communication::rpc {

/// Exception that is thrown whenever a RPC call fails.
/// This exception inherits `std::exception` directly because
/// `utils::BasicException` is used for transient errors that should be reported
/// to the user and `utils::StacktraceException` is used for fatal errors.
/// This exception always requires explicit handling.
class RpcFailedException final : public utils::BasicException {
 public:
  RpcFailedException(const io::network::Endpoint &endpoint)
      : utils::BasicException::BasicException(
            "Couldn't communicate with the cluster! Please contact your "
            "database administrator."),
        endpoint_(endpoint) {}

  /// Returns the endpoint associated with the error.
  const io::network::Endpoint &endpoint() const { return endpoint_; }

 private:
  io::network::Endpoint endpoint_;
};
}  // namespace communication::rpc
