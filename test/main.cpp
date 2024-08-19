#include "Forest.h"
#include "mc.h"

std::unique_ptr<rdmacm::multicast::multicastCM> mcm;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  int ret;
  mcm = std::make_unique<rdmacm::multicast::multicastCM>();
  ret = mcm->test_node();

  printf("test complete\n");
  printf("return status %d\n", ret);
  return ret;
}