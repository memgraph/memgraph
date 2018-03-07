#pragma once

#include <glog/logging.h>

#include "utils/stacktrace.hpp"

class LogDestructor {
 protected:
  ~LogDestructor() {
    Stacktrace st;
    DLOG(INFO) << st.dump();
  }
};
