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

// LD_PRELOAD shim: while the file named by MG_TEST_EMFILE_TRIGGER exists, open()/openat() with O_CREAT on a path
// containing `/wal/` fails with EMFILE. The trigger file may hold N: the first N such creations still succeed.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Creations still allowed through while armed; -1 = disarmed (no trigger).
static int allowed = -1;

static int should_fail(const char *path, int flags) {
  if (!(flags & O_CREAT) || path == NULL || strstr(path, "/wal/") == NULL) return 0;
  const char *trigger = getenv("MG_TEST_EMFILE_TRIGGER");
  if (trigger == NULL || access(trigger, F_OK) != 0) {
    __atomic_store_n(&allowed, -1, __ATOMIC_RELAXED);
    return 0;
  }
  int expected = -1;
  if (__atomic_load_n(&allowed, __ATOMIC_RELAXED) == -1) {
    int initial = 0;
    FILE *file = fopen(trigger, "r");
    if (file != NULL) {
      if (fscanf(file, "%d", &initial) != 1) initial = 0;
      fclose(file);
    }
    __atomic_compare_exchange_n(&allowed, &expected, initial, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
  int remaining = __atomic_load_n(&allowed, __ATOMIC_RELAXED);
  while (remaining > 0) {
    if (__atomic_compare_exchange_n(&allowed, &remaining, remaining - 1, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
      return 0;
    }
  }
  return 1;
}

#define DEFINE_OPEN(NAME)                                                       \
  int NAME(const char *path, int flags, ...) {                                  \
    static int (*real)(const char *, int, ...) = NULL;                          \
    if (!real) real = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, #NAME); \
    mode_t mode = 0;                                                            \
    if (flags & O_CREAT) {                                                      \
      va_list ap;                                                               \
      va_start(ap, flags);                                                      \
      mode = va_arg(ap, mode_t);                                                \
      va_end(ap);                                                               \
    }                                                                           \
    if (should_fail(path, flags)) {                                             \
      errno = EMFILE;                                                           \
      return -1;                                                                \
    }                                                                           \
    return real(path, flags, mode);                                             \
  }

#define DEFINE_OPENAT(NAME)                                                          \
  int NAME(int dirfd, const char *path, int flags, ...) {                            \
    static int (*real)(int, const char *, int, ...) = NULL;                          \
    if (!real) real = (int (*)(int, const char *, int, ...))dlsym(RTLD_NEXT, #NAME); \
    mode_t mode = 0;                                                                 \
    if (flags & O_CREAT) {                                                           \
      va_list ap;                                                                    \
      va_start(ap, flags);                                                           \
      mode = va_arg(ap, mode_t);                                                     \
      va_end(ap);                                                                    \
    }                                                                                \
    if (should_fail(path, flags)) {                                                  \
      errno = EMFILE;                                                                \
      return -1;                                                                     \
    }                                                                                \
    return real(dirfd, path, flags, mode);                                           \
  }

DEFINE_OPEN(open)
DEFINE_OPEN(open64)
DEFINE_OPENAT(openat)
DEFINE_OPENAT(openat64)
