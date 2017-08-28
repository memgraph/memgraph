#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "utils/timer.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using ClientT = communication::bolt::Client<SocketT>;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "7687", "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // TODO: handle endpoint exception
  EndpointT endpoint(FLAGS_address, FLAGS_port);
  SocketT socket;

  if (!socket.Connect(endpoint)) return 1;

  ClientT client(std::move(socket), FLAGS_username, FLAGS_password);

  std::cout << "Memgraph bolt client is connected and running." << std::endl;

  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") {
      break;
    }
    try {
      utils::Timer t;
      auto ret = client.Execute(s, {});
      auto elapsed = t.Elapsed().count();
      std::cout << "Wall time:\n    " << elapsed << std::endl;

      std::cout << "Fields:" << std::endl;
      for (auto &field : ret.fields) {
        std::cout << "    " << field << std::endl;
      }

      std::cout << "Records:" << std::endl;
      for (int i = 0; i < static_cast<int>(ret.records.size()); ++i) {
        std::cout << "    " << i << std::endl;
        for (auto &value : ret.records[i]) {
          std::cout << "        " << value << std::endl;
        }
      }

      std::cout << "Metadata:" << std::endl;
      for (auto &data : ret.metadata) {
        std::cout << "    " << data.first << " : " << data.second << std::endl;
      }
    } catch (const communication::bolt::ClientQueryException &e) {
      std::cout << "Client received exception: " << e.what() << std::endl;
    }
  }

  client.Close();

  return 0;
}
