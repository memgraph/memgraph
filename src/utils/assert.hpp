/**
 * Permanent Assert -> always active
 * Runtime Assert -> active only if RUNTIME_ASSERT_ON is present
 */
#pragma once

#include <iostream>
#include <sstream>

#include "utils/stacktrace/stacktrace.hpp"

/**
 * if STACKTRACE_ASSERT_ON is defined the full stacktrace will be printed on
 * stderr otherwise just the basic information will be printed out (message,
 * file, line)
 */
#ifdef STACKTRACE_ASSERT_ON
#define __handle_assert_message(message)           \
  Stacktrace stacktrace;                           \
  std::cerr << "ASSERT: " << message << std::endl; \
  std::cerr << stacktrace.dump();
#else
#define __handle_assert_message(message)                                \
  std::cerr << "ASSERT: " << message << " In file " << __FILE__ << " #" \
            << __LINE__ << std::endl;
#endif

/**
 * parmanant assertion will always be active
 * when condition is satisfied program has to exit
 *
 * a good use-case for this type of assert is during unit testing because
 * assert has to stay active all the time
 */
#define permanent_assert(condition, message) \
  if (!(condition)) {                        \
    std::ostringstream s;                    \
    s << message;                            \
    __handle_assert_message(s.str());        \
    std::exit(EXIT_FAILURE);                 \
  }

#define permanent_fail(message)                                     \
  std::ostringstream s;                                             \
  s << message;                                                     \
  __handle_assert_message(s.str());                                 \
  std::exit(EXIT_FAILURE);                                          \
/**                                                                 \
 * runtime assertion is more like standart C assert but with custom \
 * define which controls when the assertion will be active          \
 *                                                                  \
 * could be used wherever the standard C assert is used but         \
 * the user should not forget about RUNTIME_ASSERT_ON define        \
 */
#ifdef RUNTIME_ASSERT_ON
#define runtime_assert(condition, message) permanent_assert(condition, message)
#else
#define runtime_assert(condition, message)
#endif
