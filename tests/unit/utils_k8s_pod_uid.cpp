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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "utils/system_info.hpp"

using memgraph::utils::ExtractPodUidFromServiceAccountToken;
using memgraph::utils::GetKubernetesPodUid;

namespace {
// The tokens below are unsigned, made-up JWTs shaped like the ones Kubernetes mounts into pods.
constexpr auto kHeader = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImFiYyJ9";
constexpr auto kPodUid = "7e0d4c3a-5f1b-4e2a-9c8d-1b2a3c4d5e6f";

// A bound service account token, as projected into pods by default.
constexpr auto kBoundToken =
    "eyJhbGciOiJSUzI1NiIsImtpZCI6ImFiYyJ9."
    "eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNzk0MDAwMDAwLCJpYXQiOjE3NjI0"
    "NjQwMDAsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3Bh"
    "Y2UiOiJtZW1ncmFwaCIsInBvZCI6eyJuYW1lIjoibWVtZ3JhcGgtZGF0YS0wLTAiLCJ1aWQiOiI3ZTBkNGMzYS01ZjFiLTRlMmEtOWM4ZC0xYjJh"
    "M2M0ZDVlNmYifSwic2VydmljZWFjY291bnQiOnsibmFtZSI6ImRlZmF1bHQiLCJ1aWQiOiIwYjZmM2ExZS04YzJkLTRmNWUtYTFiMi1jM2Q0ZTVm"
    "NjA3MTgifX0sIm5iZiI6MTc2MjQ2NDAwMCwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50Om1lbWdyYXBoOmRlZmF1bHQifQ."
    "c2lnbmF0dXJl";

std::string Token(const std::string &payload) { return std::string{kHeader} + "." + payload + ".c2lnbmF0dXJl"; }
}  // namespace

TEST(KubernetesPodUid, ExtractsUidFromBoundToken) {
  EXPECT_EQ(ExtractPodUidFromServiceAccountToken(kBoundToken), kPodUid);
}

TEST(KubernetesPodUid, IgnoresSurroundingWhitespace) {
  EXPECT_EQ(ExtractPodUidFromServiceAccountToken(std::string{kBoundToken} + "\n"), kPodUid);
}

TEST(KubernetesPodUid, HandlesEveryPaddingLength) {
  // {"kubernetes.io":{"pod":{"uid":"<kPodUid>"}},"x":"<0 to 3 y's>"}: the encoded payloads need 2, 3, 0 and 2 padding
  // characters respectively, and none of them are padded in the token.
  for (const auto *payload : {
           "eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOiI3ZTBkNGMzYS01ZjFiLTRlMmEtOWM4ZC0xYjJhM2M0ZDVlNmYifX0sIngiOiIifQ",
           "eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOiI3ZTBkNGMzYS01ZjFiLTRlMmEtOWM4ZC0xYjJhM2M0ZDVlNmYifX0sIngiOiJ5In"
           "0",
           "eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOiI3ZTBkNGMzYS01ZjFiLTRlMmEtOWM4ZC0xYjJhM2M0ZDVlNmYifX0sIngiOiJ5eSJ"
           "9",
           "eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOiI3ZTBkNGMzYS01ZjFiLTRlMmEtOWM4ZC0xYjJhM2M0ZDVlNmYifX0sIngiOiJ5eXk"
           "ifQ",
       }) {
    EXPECT_EQ(ExtractPodUidFromServiceAccountToken(Token(payload)), kPodUid) << payload;
  }
}

TEST(KubernetesPodUid, NoUidInLegacyToken) {
  // Legacy secret-based tokens aren't bound to a pod, so they don't carry its UID.
  const auto legacy = Token(
      "eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJtZW1ncmFw"
      "aCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDptZW1ncmFwaDpkZWZhdWx0In0");
  EXPECT_EQ(ExtractPodUidFromServiceAccountToken(legacy), std::nullopt);
}

TEST(KubernetesPodUid, RejectsUidThatIsNotAUuid) {
  // {"kubernetes.io":{"pod":{"uid":"not-a-uuid"}}}
  EXPECT_EQ(
      ExtractPodUidFromServiceAccountToken(Token("eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOiJub3QtYS11dWlkIn19fQ")),
      std::nullopt);
  // {"kubernetes.io":{"pod":{"uid":42}}}
  EXPECT_EQ(ExtractPodUidFromServiceAccountToken(Token("eyJrdWJlcm5ldGVzLmlvIjp7InBvZCI6eyJ1aWQiOjQyfX19")),
            std::nullopt);
}

TEST(KubernetesPodUid, RejectsMalformedTokens) {
  for (const auto *token : {"",
                            "\n",
                            "no-dots-at-all",
                            "only.one-dot",
                            "header..signature",
                            "a.!!!notbase64!!!.c",
                            // valid base64 that isn't JSON
                            "a.aGVsbG8gd29ybGQ.c",
                            // valid JSON that isn't an object: [1,2,3]
                            "a.WzEsMiwzXQ.c"}) {
    EXPECT_EQ(ExtractPodUidFromServiceAccountToken(token), std::nullopt) << "token: '" << token << "'";
  }
}

TEST(KubernetesPodUid, ReadsTokenFromFile) {
  const auto dir = std::filesystem::temp_directory_path() / "MG_test_unit_utils_k8s_pod_uid";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto token_path = dir / "token";
  {
    std::ofstream out(token_path);
    out << kBoundToken;
  }
  EXPECT_EQ(GetKubernetesPodUid(token_path), kPodUid);
  EXPECT_EQ(GetKubernetesPodUid(dir / "missing"), std::nullopt);
  std::filesystem::remove_all(dir);
}
