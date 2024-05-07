// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
///
/// This file defines convenience macros which wrap defining a command line flag
/// with validation function. The defined macros can only be used in tandem with
/// gflags.
///
/// For example, to define an integer flag which needs to be between 1 and 10.
/// The usual gflags approach is the following code.
///
/// @code
/// DEFINE_int32(my_flag, 2, "My flag, which needs to be in [1,10]");
///
/// bool ValidateMyFlag(const char *flagname, std::int32_t value) {
///   if (value >= 1 && value <= 10) return true;
///   std::cout << "Invalid value for --" << flagname << std::endl;
///   return false;
/// }
///
/// DEFINE_validator(my_flag, &ValidateMyFlag);
/// @endcode
///
/// With the macros defined in this file, the above can be simplified to the
/// following.
///
/// @code
/// DEFINE_VALIDATED_int32(my_flag, 2, "My flag, which needs to be in [1, 10]",
/// {
///   if (value >= 1 && value <= 10) return true;
///   std::cout << "Invalid value for --" << flagname << std::endl;
///   return false;
/// });
/// @endcode
///
/// Or even more simplified if you can use one of the general validators defined
/// in this file.
///
/// @code
/// DEFINE_VALIDATED_int32(my_flag, 2, "My flag, which needs to be in [1, 10]",
///                        FLAG_IN_RANGE(1, 10));
/// @endcode
///
/// Note that the `value` is implicitly bound to the new value of the flag. Name
/// of the flag as a string is implicitly bound to the `flagname` variable.

#include <cstdint>
#include <iostream>
#include <string>

#include "gflags/gflags.h"

/// Macro which defines a flag of given type and registers a validator function.
/// The function is generated from the `validation_body` and `cpp_type` is used
/// as the type of the implicitly bound `value`.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_FLAG(flag_type, flag_name, default_value, description, cpp_type, validation_body) \
  DEFINE_##flag_type(flag_name, default_value, description);                                               \
  namespace {                                                                                              \
  bool validate_##flag_name(const char *flagname, cpp_type value) validation_body                          \
  }                                                                                                        \
  DEFINE_validator(flag_name, &validate_##flag_name)

#define DEFINE_VALIDATED_HIDDEN_FLAG(flag_type, flag_name, default_value, description, cpp_type, validation_body) \
  DEFINE_HIDDEN_##flag_type(flag_name, default_value, description);                                               \
  namespace {                                                                                                     \
  bool validate_##flag_name(const char *flagname, cpp_type value) validation_body                                 \
  }                                                                                                               \
  DEFINE_validator(flag_name, &validate_##flag_name)

/// Define a boolean command line flag with validation.
///
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_bool(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(bool, flag_name, default_value, description, bool, validation_body)
#define DEFINE_VALIDATED_HIDDEN_bool(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(bool, flag_name, default_value, description, bool, validation_body)

/// Define an integer command line flag with validation.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_int32(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(int32, flag_name, default_value, description, std::int32_t, validation_body)
#define DEFINE_VALIDATED_HIDDEN_int32(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(int32, flag_name, default_value, description, std::int32_t, validation_body)

/// Define an integer command line flag with validation.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_int64(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(int64, flag_name, default_value, description, std::int64_t, validation_body)
#define DEFINE_VALIDATED_HIDDEN_int64(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(int64, flag_name, default_value, description, std::int64_t, validation_body)

/// Define an unsigned integer command line flag with validation.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_double
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_uint64(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(uint64, flag_name, default_value, description, std::uint64_t, validation_body)
#define DEFINE_VALIDATED_HIDDEN_uint64(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(uint64, flag_name, default_value, description, std::uint64_t, validation_body)

/// Define a double floating point command line flag with validation.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_string
#define DEFINE_VALIDATED_double(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(double, flag_name, default_value, description, double, validation_body)
#define DEFINE_VALIDATED_HIDDEN_double(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(double, flag_name, default_value, description, double, validation_body)

/// Define a character string command line flag with validation.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
#define DEFINE_VALIDATED_string(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_FLAG(string, flag_name, default_value, description, const std::string &, validation_body)
#define DEFINE_VALIDATED_HIDDEN_string(flag_name, default_value, description, validation_body) \
  DEFINE_VALIDATED_HIDDEN_FLAG(string, flag_name, default_value, description, const std::string &, validation_body)

/// General flag validator for numeric flag values inside a range (inclusive).
///
/// This should only be used with DEFINE_VALIDATED_* macros.
///
/// @sa DEFINE_VALIDATED_bool
/// @sa DEFINE_VALIDATED_int32
/// @sa DEFINE_VALIDATED_int64
/// @sa DEFINE_VALIDATED_uint64
/// @sa DEFINE_VALIDATED_double
#define FLAG_IN_RANGE(lower_bound, upper_bound)                                                                \
  {                                                                                                            \
    if (value >= lower_bound && value <= upper_bound) return true;                                             \
    std::cout << "Expected --" << flagname << " to be in range [" << lower_bound << ", " << upper_bound << "]" \
              << std::endl;                                                                                    \
    return false;                                                                                              \
  }
