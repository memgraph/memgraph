#pragma once

#include <cassert>
#include <errno.h>
#include <fstream>
#include <linux/futex.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

namespace sys
{
// Code from stackoverflow
// http://stackoverflow.com/questions/109449/getting-a-file-from-a-stdfstream
// Extracts FILE* from streams in std.
template <class STREAM>
struct STDIOAdapter
{
    static FILE *yield(STREAM *stream)
    {
        assert(stream != NULL);

        static cookie_io_functions_t Cookies = {.read = NULL,
                                                .write = cookieWrite,
                                                .seek = NULL,
                                                .close = cookieClose};

        return fopencookie(stream, "w", Cookies);
    }

    ssize_t static cookieWrite(void *cookie, const char *buf, size_t size)
    {
        if (cookie == NULL) return -1;

        STREAM *writer = static_cast<STREAM *>(cookie);

        writer->write(buf, size);

        return size;
    }

    int static cookieClose(void *cookie) { return EOF; }
}; // STDIOAdapter

inline int futex(void *addr1, int op, int val1, const struct timespec *timeout,
                 void *addr2, int val3)
{
    return syscall(SYS_futex, addr1, op, val1, timeout, addr2, val3);
};

// Ensures that everything written to file will be writen on disk when the
// function call returns. !=0 if error occured
template <class STREAM>
inline size_t flush_file_to_disk(STREAM &file)
{
    file.flush();
    FILE *f = STDIOAdapter<STREAM>::yield(&file);
    if (fsync(fileno(f)) == 0) {
        return 0;
    }

    return errno;
};

// True if succesffull
inline bool ensure_directory_exists(std::string const &path)
{
    struct stat st = {0};

    if (stat(path.c_str(), &st) == -1) {
        return mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == 0;
    }
    return true;
}
};
