#pragma once

#include "utils/auto_scope.hpp"

#include <iostream>
#include <execinfo.h>

// TODO: log to local file or remote database
void stacktrace(std::ostream& stream) noexcept
{
    void* array[50];
    int size = backtrace(array, 50);

    stream << __FUNCTION__ << " backtrace returned "
           << size << " frames." << std::endl;

    char** messages = backtrace_symbols(array, size);
    Auto(free(messages));

    for (int i = 0; i < size && messages != NULL; ++i)
        stream << "[bt]: (" << i << ") " << messages[i] << std::endl;

    stream << std::endl;
}

// TODO: log to local file or remote database
void terminate_handler(std::ostream& stream) noexcept
{
    if (auto exc = std::current_exception())
    {
        try
        {
            std::rethrow_exception(exc);
        }
        catch(std::exception& ex)
        {
            stream << ex.what() << std::endl << std::endl;
            stacktrace(stream);
        }
    }

    std::abort();
}

void terminate_handler() noexcept
{
    terminate_handler(std::cout);
}
