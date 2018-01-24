#include <thread>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/archive/text_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/rpc/client.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "gtest/gtest.h"

using namespace communication::rpc;
using namespace std::literals::chrono_literals;

struct SumReq : public Message {
  SumReq(int x, int y) : x(x), y(y) {}
  int x;
  int y;

 private:
  friend class boost::serialization::access;
  SumReq() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &x;
    ar &y;
  }
};
BOOST_CLASS_EXPORT(SumReq);

struct SumRes : public Message {
  SumRes(int sum) : sum(sum) {}
  int sum;

 private:
  friend class boost::serialization::access;
  SumRes() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &sum;
  }
};
BOOST_CLASS_EXPORT(SumRes);
using Sum = RequestResponse<SumReq, SumRes>;

TEST(Rpc, Call) {
  System server_system({"127.0.0.1", 0});
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server_system.endpoint(), "main");
  auto sum = client.Call<Sum>(10, 20);
  EXPECT_EQ(sum->sum, 30);
}

/* TODO (mferencevic): enable when async calls are implemented!
TEST(Rpc, Timeout) {
  System server_system({"127.0.0.1", 0});
  Server server(server_system, "main");
  server.Register<Sum>([](const SumReq &request) {
    std::this_thread::sleep_for(300ms);
    return std::make_unique<SumRes>(request.x + request.y);
  });
  std::this_thread::sleep_for(100ms);

  Client client(server_system.endpoint(), "main");
  auto sum = client.Call<Sum>(100ms, 10, 20);
  EXPECT_FALSE(sum);
}
*/
