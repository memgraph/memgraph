#pragma once

namespace storage {

/// Indicator for obtaining the state before or after a transaction & command.
enum class View {
  OLD,
  NEW,
};

}  // namespace storage
