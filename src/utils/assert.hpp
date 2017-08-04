/**
 * @file
 * This file defines @c permanent_assert and @c debug_assert.
 *
 * If @c STACKTRACE_ASSERT_ON is defined, when an assertion fails, the full
 * stack trace will be printed to @c stderr, otherwise just the basic
 * information will be printed out (message, file, line)
 */
#pragma once

#include <cstdlib>
#include <iostream>
#include <sstream>

#include "utils/stacktrace.hpp"

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
 * Always check that the condition is satisfied, otherwise abort the program.
 *
 * Unlike @c debug_assert, @c permanent_assert is always active. A good use-case
 * for this type of assert is during unit testing, because assert has to be
 * active regardless of the build type.
 *
 * @param condition Expression which has to evaluate to @c true.
 * @param message Message that is to be displayed before aborting, if the
 *                evaluated @c condition is @c false.
 *
 * @sa permanent_fail
 * @sa debug_assert
 * @sa debug_fail
 */
#define permanent_assert(condition, message) \
  if (!(condition)) {                        \
    std::ostringstream s;                    \
    s << message;                            \
    __handle_assert_message(s.str());        \
    std::abort();                            \
  }

/**
 * Always abort the program with given message.
 *
 * Unlike @c debug_fail, @c permanent_fail is always active. This should be used
 * like @c permanent_assert, but when the condition cannot be a simple
 * expression.
 *
 * @param message Message to display before aborting.
 *
 * @sa permanent_assert
 * @sa debug_assert
 * @sa debug_fail
 */
#define permanent_fail(message)       \
  {                                   \
    std::ostringstream s;             \
    s << message;                     \
    __handle_assert_message(s.str()); \
    std::abort();                     \
  }

/**
 * @def debug_assert(condition, message)
 * Check that the condition is satisfied, otherwise abort the program.
 *
 * This is like @c permanent_assert, but the @c NDEBUG define controls
 * whether this assertion is active. With this define, @c debug_assert will do
 * nothing. Therefore, this is more like the standard C @c assert facility and
 * it should be used as such. For example, validating pre and post conditions of
 * a function.
 *
 * @sa debug_fail
 * @sa permanent_assert
 * @sa permanent_fail
 */

/**
 * @def debug_fail(message)
 * Abort the program with given message.
 *
 * This is like @c permanent_fail, but the @c NDEBUG define controls
 * whether this assertion is active. With this define, @c debug_fail will do
 * nothing. This should be used like @c debug_assert, but when the condition
 * cannot be a simple expression.
 *
 * @sa debug_assert
 * @sa permanent_assert
 * @sa permanent_fail
 */

#ifndef NDEBUG
#define debug_assert(condition, message) permanent_assert(condition, message)
#define debug_fail(message) permanent_fail(message)
#else
#define debug_assert(condition, message) \
  {}
#define debug_fail(message) \
  {}
#endif
