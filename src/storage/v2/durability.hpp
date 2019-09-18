#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "storage/v2/property_value.hpp"
#include "utils/file.hpp"

namespace storage::durability {

static_assert(std::is_same_v<uint8_t, unsigned char>);

/// Markers that are used to indicate crucial parts of the snapshot/WAL.
/// IMPORTANT: Don't forget to update the list of all markers `kMarkersAll` when
/// you add a new Marker.
enum class Marker : uint8_t {
  TYPE_NULL = 0x10,
  TYPE_BOOL = 0x11,
  TYPE_INT = 0x12,
  TYPE_DOUBLE = 0x13,
  TYPE_STRING = 0x14,
  TYPE_LIST = 0x15,
  TYPE_MAP = 0x16,
  TYPE_PROPERTY_VALUE = 0x17,

  SECTION_VERTEX = 0x20,
  SECTION_EDGE = 0x21,
  SECTION_MAPPER = 0x22,
  SECTION_METADATA = 0x23,
  SECTION_INDICES = 0x24,
  SECTION_CONSTRAINTS = 0x25,
  SECTION_OFFSETS = 0x42,

  VALUE_FALSE = 0x00,
  VALUE_TRUE = 0xff,
};

/// List of all available markers.
/// IMPORTANT: Don't forget to update this list when you add a new Marker.
static const Marker kMarkersAll[] = {
    Marker::TYPE_NULL,       Marker::TYPE_BOOL,
    Marker::TYPE_INT,        Marker::TYPE_DOUBLE,
    Marker::TYPE_STRING,     Marker::TYPE_LIST,
    Marker::TYPE_MAP,        Marker::TYPE_PROPERTY_VALUE,
    Marker::SECTION_VERTEX,  Marker::SECTION_EDGE,
    Marker::SECTION_MAPPER,  Marker::SECTION_METADATA,
    Marker::SECTION_INDICES, Marker::SECTION_CONSTRAINTS,
    Marker::SECTION_OFFSETS, Marker::VALUE_FALSE,
    Marker::VALUE_TRUE,
};

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

  uint64_t GetSize();
  uint64_t GetPosition();
  void SetPosition(uint64_t position);

 private:
  utils::InputFile file_;
};

}  // namespace storage::durability
