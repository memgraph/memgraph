#include <cstdlib>
#include "rocksdb/c.h"
#include "rocksdb/db.h"

int main() {
  rocksdb_options_t *options = rocksdb_options_create();
  rocksdb_free(options);

  return EXIT_SUCCESS;
}
