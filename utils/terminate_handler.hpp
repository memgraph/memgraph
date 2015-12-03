#pragma once

#include <iostream>
#include <execinfo.h>

// TODO: log to local file or remote database
void stacktrace() noexcept
{
    void *array[50];
    int size = backtrace(array, 50);    
    std::cout << __FUNCTION__ << " backtrace returned " << size << " frames\n\n";
    char **messages = backtrace_symbols(array, size);
    for (int i = 0; i < size && messages != NULL; ++i) {
        std::cout << "[bt]: (" << i << ") " << messages[i] << std::endl;
    }
    std::cout << std::endl;
    free(messages);
}

// TODO: log to local file or remote database
void terminate_handler() noexcept
{
    if (auto exc = std::current_exception()) { 
        try {
            std::rethrow_exception(exc);
        } catch (std::exception &ex) {
            std::cout << ex.what() << std::endl << std::endl;
            stacktrace();
        }
    }
    std::_Exit(EXIT_FAILURE);
}
