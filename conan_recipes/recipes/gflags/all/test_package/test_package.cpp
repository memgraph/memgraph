#include <gflags/gflags.h>
#include <iostream>

DEFINE_bool(big_menu, true, "Include 'advanced' options in the menu listing");
DEFINE_string(languages, "english,french,german", "comma-separated list of languages to offer in the 'lang' menu");

int main(int argc, char **argv) {
  GFLAGS_NAMESPACE::ShowUsageWithFlags(argv[0]);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  return 0;
}
