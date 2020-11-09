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

/// Encoder interface class. Used to implement streams to different targets
/// (e.g. file and network).
class BaseEncoder {
 protected:
  ~BaseEncoder() {}

 public:
  virtual void WriteMarker(Marker marker) = 0;
  virtual void WriteBool(bool value) = 0;
  virtual void WriteUint(uint64_t value) = 0;
  virtual void WriteDouble(double value) = 0;
  virtual void WriteString(const std::string_view &value) = 0;
  virtual void WritePropertyValue(const PropertyValue &value) = 0;
};

/// Encoder that is used to generate a snapshot/WAL.
class Encoder final : public BaseEncoder {
 public:
  void Initialize(const std::filesystem::path &path,
                  const std::string_view &magic, uint64_t version);

  // Main write function, the only one that is allowed to write to the `file_`
  // directly.
  void Write(const uint8_t *data, uint64_t size);

  void WriteMarker(Marker marker) override;
  void WriteBool(bool value) override;
  void WriteUint(uint64_t value) override;
  void WriteDouble(double value) override;
  void WriteString(const std::string_view &value) override;
  void WritePropertyValue(const PropertyValue &value) override;

  uint64_t GetPosition();
  void SetPosition(uint64_t position);

  void Sync();

  void Finalize();

  void DisableFlushing();
  void EnableFlushing();
  void TryFlushing();
  std::pair<const uint8_t *, size_t> CurrentFileBuffer() const;

  size_t GetSize() const;

 private:
  utils::OutputFile file_;
};

/// Decoder interface class. Used to implement streams from different sources
/// (e.g. file and network).
class BaseDecoder {
 protected:
  ~BaseDecoder() {}

 public:
  virtual std::optional<Marker> ReadMarker() = 0;
  virtual std::optional<bool> ReadBool() = 0;
  virtual std::optional<uint64_t> ReadUint() = 0;
  virtual std::optional<double> ReadDouble() = 0;
  virtual std::optional<std::string> ReadString() = 0;
  virtual std::optional<PropertyValue> ReadPropertyValue() = 0;

  virtual bool SkipString() = 0;
  virtual bool SkipPropertyValue() = 0;
};

/// Decoder that is used to read a generated snapshot/WAL.
class Decoder final : public BaseDecoder {
 public:
  std::optional<uint64_t> Initialize(const std::filesystem::path &path,
                                     const std::string &magic);

  // Main read functions, the only one that are allowed to read from the `file_`
  // directly.
  bool Read(uint8_t *data, size_t size);
  bool Peek(uint8_t *data, size_t size);

  std::optional<Marker> PeekMarker();

  std::optional<Marker> ReadMarker() override;
  std::optional<bool> ReadBool() override;
  std::optional<uint64_t> ReadUint() override;
  std::optional<double> ReadDouble() override;
  std::optional<std::string> ReadString() override;
  std::optional<PropertyValue> ReadPropertyValue() override;

  bool SkipString() override;
  bool SkipPropertyValue() override;

  std::optional<uint64_t> GetSize();
  std::optional<uint64_t> GetPosition();
  bool SetPosition(uint64_t position);

 private:
  utils::InputFile file_;
};

}  // namespace storage::durability
