#pragma once

#include <errno.h>
#include <linux/futex.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>

namespace sys {
// Code from stackoverflow:
// http://stackoverflow.com/questions/676787/how-to-do-fsync-on-an-ofstream
// Extracts FILE* from streams in std.
inline int GetFileDescriptor(std::filebuf &filebuf) {
  class my_filebuf : public std::filebuf {
   public:
    int handle() { return _M_file.fd(); }
  };

  return static_cast<my_filebuf &>(filebuf).handle();
}

inline int futex(void *addr1, int op, int val1, const struct timespec *timeout,
                 void *addr2, int val3) {
  return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
};

// Ensures that everything written to file will be writen on disk when the
// function call returns. !=0 if error occured
template <class STREAM>
inline size_t flush_file_to_disk(STREAM &file) {
  file.flush();
  if (fsync(GetFileDescriptor(*file.rdbuf())) == 0) {
    return 0;
  }

  return errno;
};

// True if succesffull
inline bool ensure_directory_exists(std::string const &path) {
  struct stat st = {0};

  if (stat(path.c_str(), &st) == -1) {
    return mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
  }
  return true;
}
};
