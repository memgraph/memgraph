// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

namespace memgraph::io {
// Signifies that a retriable operation was unable to
// complete after a configured number of retries.
struct RetriesExhausted {
  friend std::ostream &operator<<(std::ostream &in, const RetriesExhausted & /* retries_exhausted */) {
    in << "RetriesExhausted {}";
    return in;
  }
};

// Signifies that a request was unable to receive a response
// within some configured timeout duration. It is important
// to remember that in distributed systems, a timeout does
// not signify that a request was not received or processed.
// It may be the case that the request was fully processed
// but that the response was not received.
struct TimedOut {
  friend std::ostream &operator<<(std::ostream &in, const TimedOut & /* timed_out */) {
    in << "TimedOut {}";
    return in;
  }
};

// This error signifies that a shard has been contacted that does
// not match our expected shard version, and that we should retry
// the operation so that we can ensure that we are talking to the
// correct shard.
struct ShardVersionMismatch {
  friend std::ostream &operator<<(std::ostream &in, const ShardVersionMismatch & /* shard_version_mismatch */) {
    in << "ShardVersionMismatch {}";
    return in;
  }
};
};  // namespace memgraph::io
