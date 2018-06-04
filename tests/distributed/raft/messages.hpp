#include "communication/rpc/messages.hpp"

using namespace communication::rpc;

struct AppendEntryReq {
  AppendEntryReq() {}
  explicit AppendEntryReq(int val) : val(val) {}
  int val;
};

struct AppendEntryRes {
  AppendEntryRes() {}
  AppendEntryRes(int status, std::string interface, uint16_t port)
      : status(status), interface(interface), port(port) {}
  int status;
  std::string interface;
  uint16_t port;

};

using AppendEntry = RequestResponse<AppendEntryReq, AppendEntryRes>;
