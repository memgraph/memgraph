#pragma once

#include <strings.h>
#include <cstdlib>
#include <cstdlib>
#include <iostream>
#include <string>
#include "storage/model/properties/all.hpp"

bool string2bool(const char *v) {
  return strcasecmp(v, "true") == 0 || atoi(v) != 0;
}

bool to_bool(const char *str) { return string2bool(str); }

float to_float(const char *str) { return stof(str); }

double to_double(const char *str) { return atof(str); }

int32_t to_int32(const char *str) { return atoi(str); }

int64_t to_int64(const char *str) { return atoll(str); }

std::string to_string(const char *str) { return std::string(str); }
