#pragma once

// ** Books **
// http://instructor.sdu.edu.kz/~konst/sysprog2015fall/readings/linux%20system%20programming/The%20Linux%20Programming%20Interface-Michael%20Kerrisk.pdf

// ** Documentation **
// http://man7.org/linux/man-pages/man2/read.2.html
// http://man7.org/linux/man-pages/man2/select.2.html
// http://man7.org/linux/man-pages/man2/fcntl.2.html

// ** Community **
// http://stackoverflow.com/questions/5616092/non-blocking-call-for-reading-descriptor
// http://stackoverflow.com/questions/2917881/how-to-implement-a-timeout-in-read-function-call

#include <unistd.h>
#include <fcntl.h>

#include "utils/exceptions/basic_exception.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"
#include "utils/likely.hpp"

namespace os_linux
{
    class LinuxException : public BasicException
    {
        using BasicException::BasicException;
    };

    /**
     * Sets non blocking flag to a file descriptor.
     */
    void set_non_blocking(int fd)
    {
        auto flags = fcntl(fd, F_GETFL, 0);

        if(UNLIKELY(flags == -1))
            throw LinuxException("Cannot read flags from file descriptor.");

        flags |= O_NONBLOCK;

        auto status = fcntl(fd, F_SETFL, flags);

        if(UNLIKELY(status == -1))
            throw LinuxException("Can't set NON_BLOCK flag to file descriptor");
    }

    /**
     * Reads a file descriptor with timeout.
     */
    void tread()
    {
        throw NotYetImplemented();
    }
}
