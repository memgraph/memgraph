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
#define __handle_assert_message(message) \
  std::cerr << "ASSERT: " << message << std::endl;
#endif
