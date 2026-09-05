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

#pragma once

#include <memory>
#include <string>

namespace memgraph::utils {

/// Process-wide FIPS 140-3 approved-mode state.
struct FipsStatus {
  bool enabled{false};

  /// Identity of the validated module, as reported by the provider itself.
  /// Empty when `enabled` is false, and also when the toggle was set without
  /// consulting a provider (which tests do).
  std::string provider_name;
  std::string provider_version;

  /// The TLS floor approved mode pins, for `SHOW FIPS INFO`. Empty when
  /// `enabled` is false, because Memgraph then sets no floor of its own and the
  /// effective minimum is whatever the OpenSSL configuration says.
  std::string tls_min_version;
};

/// Publish the approved-mode state. Called from startup wiring once the state
/// is known; `communication::EnableFipsMode()` supplies the module identity.
void SetFipsStatus(FipsStatus status);

/// Returns by shared_ptr so the snapshot stays alive while the caller reads it.
[[nodiscard]] auto GetFipsStatus() -> std::shared_ptr<FipsStatus const>;

/// Shorthand for `GetFipsStatus()->enabled`, which is the only field most
/// callers want.
[[nodiscard]] bool FipsEnabled();

}  // namespace memgraph::utils
