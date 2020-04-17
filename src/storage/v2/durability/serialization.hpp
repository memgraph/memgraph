#pragma once

#include <cstdint>
#include <filesystem>
#include <string_view>

#include "storage/v2/config.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/file.hpp"

namespace storage::durability {

/// Encoder that is used to generate a snapshot/WAL.
class Encoder final {
 public:
  void Initialize(const std::filesystem::path &path,
                  const std::string_view &magic, uint64_t version);

  // Main write function, the only one that is allowed to write to the `file_`
  // directly.
  void Write(const uint8_t *data, uint64_t size);

  void WriteMarker(Marker marker);
  void WriteBool(bool value);
  void WriteUint(uint64_t value);
  void WriteDouble(double value);
  void WriteString(const std::string_view &value);
  void WritePropertyValue(const PropertyValue &value);

  uint64_t GetPosition();
  void SetPosition(uint64_t position);

  void Sync();

  void Finalize();

 private:
  utils::OutputFile file_;
};

/// Decoder that is used to read a generated snapshot/WAL.
class Decoder final {
 public:
  std::optional<uint64_t> Initialize(const std::filesystem::path &path,
                                     const std::string &magic);

  // Main read functions, the only one that are allowed to read from the `file_`
  // directly.
  bool Read(uint8_t *data, size_t size);
  bool Peek(uint8_t *data, size_t size);

  std::optional<Marker> PeekMarker();

  std::optional<Marker> ReadMarker();
  std::optional<bool> ReadBool();
  std::optional<uint64_t> ReadUint();
  std::optional<double> ReadDouble();
  std::optional<std::string> ReadString();
  std::optional<PropertyValue> ReadPropertyValue();

  bool SkipString();
  bool SkipPropertyValue();

  std::optional<uint64_t> GetSize();
  std::optional<uint64_t> GetPosition();
  bool SetPosition(uint64_t position);

 private:
  utils::InputFile file_;
};

}  // namespace storage::durability
