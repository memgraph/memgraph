#include <gflags/gflags.h>
#include <glog/logging.h>

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetUsageMessage("Memgraph E2E Replication Benchmark Test");
  google::InitGoogleLogging(argv[0]);
  return 0;
}
