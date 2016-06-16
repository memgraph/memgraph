#pragma once

#include <sstream>

#include "exceptions/basic_exception.hpp"

// #define THROW_EXCEPTION_ON_ERROR
// #define RUNTIME_ASSERT_ON

// handle assertion error
void assert_error_handler(const char *file_name, unsigned line_number,
                          const char *message)
{
// this is a good place to put your debug breakpoint
// and add some other destination for error message
#ifdef THROW_EXCEPTION_ON_ERROR
    throw BasicException(message);
#else
    std::cerr << message << " in file " << file_name << " #" << line_number
              << std::endl;
    exit(1);
#endif
}

// parmanant exception will always be executed
#define permanent_assert(condition, message)                                   \
    if (!(condition)) {                                                        \
        std::ostringstream s;                                                  \
        s << message;                                                          \
        assert_error_handler(__FILE__, __LINE__, s.str().c_str());             \
    }

// runtime exception
#ifdef RUNTIME_ASSERT_ON
#define runtime_assert(condition, message) permanent_assert(condition, message)
#else
#define runtime_assert(condition, message)
#endif
