// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// LD_PRELOAD shim that simulates a full disk for snapshot files: while the file named by
// MG_TEST_NO_SPACE_TRIGGER exists, write, sendfile and fsync on any file under a `snapshots/`
// directory fail with ENOSPC. Everything else is passed through untouched.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <unistd.h>

static int is_out_of_space(int fd) {
  const char *trigger = getenv("MG_TEST_NO_SPACE_TRIGGER");
  if (trigger == NULL || access(trigger, F_OK) != 0) return 0;

  char link[64];
  char path[PATH_MAX];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t const length = readlink(link, path, sizeof(path) - 1);
  if (length < 0) return 0;
  path[length] = '\0';
  return strstr(path, "/snapshots/") != NULL;
}

ssize_t write(int fd, const void *buf, size_t count) {
  static ssize_t (*real_write)(int, const void *, size_t) = NULL;
  if (real_write == NULL) real_write = (ssize_t (*)(int, const void *, size_t))dlsym(RTLD_NEXT, "write");
  if (is_out_of_space(fd)) {
    errno = ENOSPC;
    return -1;
  }
  return real_write(fd, buf, count);
}

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
  static ssize_t (*real_sendfile)(int, int, off_t *, size_t) = NULL;
  if (real_sendfile == NULL) real_sendfile = (ssize_t (*)(int, int, off_t *, size_t))dlsym(RTLD_NEXT, "sendfile");
  if (is_out_of_space(out_fd)) {
    errno = ENOSPC;
    return -1;
  }
  return real_sendfile(out_fd, in_fd, offset, count);
}

int fsync(int fd) {
  static int (*real_fsync)(int) = NULL;
  if (real_fsync == NULL) real_fsync = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
  if (is_out_of_space(fd)) {
    errno = ENOSPC;
    return -1;
  }
  return real_fsync(fd);
}
