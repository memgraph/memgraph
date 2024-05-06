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

#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "utils/temporal.hpp"

// clang-format off
const int64_t int_decoded[] = {
    0, -1, -8, -16, 1, 63, 127, -128, -20, -17, -32768, -12345, -129, 128,
    12345, 32767, -2147483648L, -12345678L, -32769L, 32768L, 12345678L,
    2147483647L, -9223372036854775807L, -12345678912345L, -2147483649L,
    2147483648L, 12345678912345L, 9223372036854775807};

const uint8_t int_encoded[][10] = {
    "\x00", "\xFF", "\xF8", "\xF0", "\x01", "\x3F", "\x7F", "\xC8\x80",
    "\xC8\xEC", "\xC8\xEF", "\xC9\x80\x00", "\xC9\xCF\xC7", "\xC9\xFF\x7F",
    "\xC9\x00\x80", "\xC9\x30\x39", "\xC9\x7F\xFF", "\xCA\x80\x00\x00\x00",
    "\xCA\xFF\x43\x9E\xB2", "\xCA\xFF\xFF\x7F\xFF", "\xCA\x00\x00\x80\x00",
    "\xCA\x00\xBC\x61\x4E", "\xCA\x7F\xFF\xFF\xFF",
    "\xCB\x80\x00\x00\x00\x00\x00\x00\x01",
    "\xCB\xFF\xFF\xF4\xC5\x8C\x31\xA4\xA7",
    "\xCB\xFF\xFF\xFF\xFF\x7F\xFF\xFF\xFF",
    "\xCB\x00\x00\x00\x00\x80\x00\x00\x00",
    "\xCB\x00\x00\x0B\x3A\x73\xCE\x5B\x59",
    "\xCB\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"};
// clang-format on

const uint32_t int_encoded_len[] = {1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9, 9};

const double double_decoded[] = {5.834, 108.199, 43677.9882, 254524.5851};
const uint8_t double_encoded[][10] = {"\xC1\x40\x17\x56\x04\x18\x93\x74\xBC", "\xC1\x40\x5B\x0C\xBC\x6A\x7E\xF9\xDB",
                                      "\xC1\x40\xE5\x53\xBF\x9F\x55\x9B\x3D", "\xC1\x41\x0F\x11\xE4\xAE\x48\xE8\xA7"};

const uint8_t vertexedge_encoded[] =
    "\xB1\x71\x93\xB3\x4E\x00\x92\x86\x6C\x61\x62\x65\x6C\x31\x86\x6C\x61\x62"
    "\x65\x6C\x32\xA2\x85\x70\x72\x6F\x70\x31\x0C\x85\x70\x72\x6F\x70\x32\xC9"
    "\x00\xC8\xB3\x4E\x00\x90\xA0\xB5\x52\x00\x00\x00\x88\x65\x64\x67\x65\x74"
    "\x79\x70\x65\xA2\x85\x70\x72\x6F\x70\x33\x2A\x85\x70\x72\x6F\x70\x34\xC9"
    "\x04\xD2";

const uint64_t sizes[] = {0, 1, 5, 15, 16, 120, 255, 256, 12345, 65535, 65536};
const uint64_t sizes_num = 11;

inline constexpr const int STRING = 0, LIST = 1, MAP = 2;
const uint8_t type_tiny_magic[] = {0x80, 0x90, 0xA0};
const uint8_t type_8_magic[] = {0xD0, 0xD4, 0xD8};
const uint8_t type_16_magic[] = {0xD1, 0xD5, 0xD9};
const uint8_t type_32_magic[] = {0xD2, 0xD6, 0xDA};

namespace zdt_testdata {
using Sig = memgraph::communication::bolt::Signature;

namespace {
// Test cases from the examples at https://neo4j.com/docs/bolt/current/bolt/structure-semantics/
// * DateTime
// * DateTimeZoneId
// * Legacy DateTime
// * Legacy DateTimeZoneId

struct ExpectedZonedDateTimeValues {
  const memgraph::communication::bolt::Signature type;
  const std::vector<uint8_t> seconds;
  const std::vector<uint8_t> nanoseconds;
  const std::vector<uint8_t> tz;
};

// Encoded field values from the examples
const std::vector<uint8_t> v4500s = {0xC9, 0x11, 0x94};
const std::vector<uint8_t> v8100s = {0xC9, 0x1F, 0xA4};
const std::vector<uint8_t> v3600s = {0xC9, 0x0E, 0x10};
const std::vector<uint8_t> v100300000ns = {0xCA, 0x05, 0xFA, 0x74, 0xE0};
const std::vector<uint8_t> veurope_vienna = {0x8D, 'E', 'u', 'r', 'o', 'p', 'e', '/', 'V', 'i', 'e', 'n', 'n', 'a'};
}  // namespace

const auto zdt = memgraph::utils::ZonedDateTime(
    memgraph::utils::ParseZonedDateTimeParameters("1970-01-01T02:15:00.100300[Europe/Vienna]"));
const auto zdt_offset =
    memgraph::utils::ZonedDateTime(memgraph::utils::ParseZonedDateTimeParameters("1970-01-01T02:15:00.100300+01:00"));

const auto expected_zdt = ExpectedZonedDateTimeValues{
    .type = Sig::DateTimeZoneId, .seconds = v4500s, .nanoseconds = v100300000ns, .tz = veurope_vienna};
const auto expected_zdt_offset =
    ExpectedZonedDateTimeValues{.type = Sig::DateTime, .seconds = v4500s, .nanoseconds = v100300000ns, .tz = v3600s};
const auto expected_legacy_zdt = ExpectedZonedDateTimeValues{
    .type = Sig::LegacyDateTimeZoneId, .seconds = v8100s, .nanoseconds = v100300000ns, .tz = veurope_vienna};
const auto expected_legacy_zdt_offset = ExpectedZonedDateTimeValues{
    .type = Sig::LegacyDateTime, .seconds = v8100s, .nanoseconds = v100300000ns, .tz = v3600s};
}  // namespace zdt_testdata
