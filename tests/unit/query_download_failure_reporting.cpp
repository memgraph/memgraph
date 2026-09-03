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

#include <string>

#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "requests/requests.hpp"
#include "utils/exceptions.hpp"

using memgraph::query::QueryException;
using memgraph::query::QueryRuntimeException;
using memgraph::query::ThrowDownloadFailed;
using memgraph::query::TransientDownloadException;
using memgraph::requests::ClassifyHttpStatus;
using memgraph::requests::DownloadError;
using memgraph::requests::DownloadFailure;

namespace {
auto Retryable(DownloadFailure kind) -> bool { return DownloadError{.kind = kind}.Retryable(); }
}  // namespace

TEST(DownloadFailureReporting, RepeatingTheRequestCouldHelpWhenNothingRefusedIt) {
  EXPECT_TRUE(Retryable(DownloadFailure::Network)) << "the server may be reachable on another attempt";
  EXPECT_TRUE(Retryable(DownloadFailure::HttpServerError)) << "the server accepted the request and may serve it later";
  EXPECT_TRUE(Retryable(DownloadFailure::HttpTryAgain)) << "the server turned this attempt away, not the request";
  EXPECT_TRUE(Retryable(DownloadFailure::Stalled)) << "a transfer that stopped arriving may arrive next time";
}

TEST(DownloadFailureReporting, RepeatingTheRequestCannotHelpWhenItWasRefusedOrCouldNotBeStored) {
  EXPECT_FALSE(Retryable(DownloadFailure::HttpClientError)) << "the server will refuse the same request again";
  EXPECT_FALSE(Retryable(DownloadFailure::LocalWrite)) << "the destination, not the request, is what failed";
}

TEST(DownloadFailureReporting, AStatusAskingForAnotherAttemptIsWorthRetrying) {
  EXPECT_TRUE(Retryable(ClassifyHttpStatus(429))) << "a rate limit lifts, and the request that hit it then succeeds";
  EXPECT_TRUE(Retryable(ClassifyHttpStatus(408))) << "the server gave up waiting for the request, not on its content";
  EXPECT_TRUE(Retryable(ClassifyHttpStatus(500))) << "the server failed something it had accepted";
  EXPECT_TRUE(Retryable(ClassifyHttpStatus(503)))
      << "a server that is unavailable now may serve the same request later";
}

TEST(DownloadFailureReporting, AStatusRefusingTheRequestItselfIsNotWorthRetrying) {
  EXPECT_FALSE(Retryable(ClassifyHttpStatus(400))) << "the request is malformed however many times it is sent";
  EXPECT_FALSE(Retryable(ClassifyHttpStatus(403))) << "the credentials do not grant access on a second attempt either";
  EXPECT_FALSE(Retryable(ClassifyHttpStatus(404)))
      << "the object is missing, and repeating the request will not find it";
}

TEST(DownloadFailureReporting, ARetryableFailureIsReportedAsWorthRetrying) {
  EXPECT_THROW(ThrowDownloadFailed(true, "server is having a moment"), TransientDownloadException);
}

TEST(DownloadFailureReporting, AFailureThatCannotSucceedIsReportedAsTheCallersFault) {
  EXPECT_THROW(ThrowDownloadFailed(false, "no such object"), QueryRuntimeException);
}

// A session reports QueryException to the client as a permanent error and every other
// BasicException as one worth retrying, catching QueryException first. A retryable download that
// inherited from QueryException would therefore reach the client as permanent, and the driver would
// give up on a failure that another attempt could get past.
TEST(DownloadFailureReporting, WhatIsWorthRetryingIsNotReportedAsPermanent) {
  static_assert(!std::is_base_of_v<QueryException, TransientDownloadException>,
                "a retryable download must not be caught as a permanent query error");
  static_assert(std::is_base_of_v<memgraph::utils::BasicException, TransientDownloadException>,
                "a retryable download must still be reported to the client at all");

  try {
    ThrowDownloadFailed(true, "server is having a moment");
    FAIL() << "the call must not return";
  } catch (QueryException const &) {
    FAIL() << "a retryable download would reach the client as a permanent error";
  } catch (memgraph::utils::BasicException const &e) {
    EXPECT_STREQ(e.what(), "server is having a moment");
  }
}

TEST(DownloadFailureReporting, WhatCannotSucceedIsReportedAsPermanent) {
  try {
    ThrowDownloadFailed(false, "no such object");
    FAIL() << "the call must not return";
  } catch (QueryException const &e) {
    EXPECT_STREQ(e.what(), "no such object");
  }
}
