#pragma once

#include <string>

#include "communication/bolt/v1/packing/codes.hpp"
#include "query/exception/decoder_exception.hpp"
#include "utils/bswap.hpp"
#include "utils/types/byte.hpp"

namespace bolt {

// BoltDecoder for streams. Meant for use in SnapshotDecoder.
// This should be recoded to recieve the current caller so that decoder can
// based on a current type call it.
template <class STREAM>
class StreamedBoltDecoder {
  static constexpr int64_t plus_2_to_the_31 = 2147483648L;
  static constexpr int64_t plus_2_to_the_15 = 32768L;
  static constexpr int64_t plus_2_to_the_7 = 128L;
  static constexpr int64_t minus_2_to_the_4 = -16L;
  static constexpr int64_t minus_2_to_the_7 = -128L;
  static constexpr int64_t minus_2_to_the_15 = -32768L;
  static constexpr int64_t minus_2_to_the_31 = -2147483648L;

 public:
  StreamedBoltDecoder(STREAM &stream) : stream(stream) {}

  // Returns mark of a data.
  size_t mark() { return peek_byte(); }

  // Calls handle with current primitive data. Throws DecoderException if it
  // isn't a primitive.
  template <class H, class T>
  T accept_primitive(H &handle) {
    switch (byte()) {
      case pack::False: {
        return handle.handle(false);
      }
      case pack::True: {
        return handle.handle(true);
      }
      case pack::Float64: {
        return handle.handle(read_double());
      }
      default: { return handle.handle(integer()); }
    };
  }

  // Reads map header. Throws DecoderException if it isn't map header.
  size_t map_header() {
    auto marker = byte();

    size_t size;

    if ((marker & 0xF0) == pack::TinyMap) {
      size = marker & 0x0F;

    } else if (marker == pack::Map8) {
      size = byte();

    } else if (marker == pack::Map16) {
      size = read<uint16_t>();

    } else if (marker == pack::Map32) {
      size = read<uint32_t>();

    } else {
      // Error
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read map header but found ", marker);
    }

    return size;
  }

  bool is_list() {
    auto marker = peek_byte();

    if ((marker & 0xF0) == pack::TinyList) {
      return true;

    } else if (marker == pack::List8) {
      return true;

    } else if (marker == pack::List16) {
      return true;

    } else if (marker == pack::List32) {
      return true;
    } else {
      return false;
    }
  }

  // Reads list header. Throws DecoderException if it isn't list header.
  size_t list_header() {
    auto marker = byte();

    if ((marker & 0xF0) == pack::TinyList) {
      return marker & 0x0F;

    } else if (marker == pack::List8) {
      return byte();

    } else if (marker == pack::List16) {
      return read<uint16_t>();

    } else if (marker == pack::List32) {
      return read<uint32_t>();

    } else {
      // Error
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read list header but found ", marker);
    }
  }

  bool is_bool() {
    auto marker = peek_byte();

    if (marker == pack::True) {
      return true;
    } else if (marker == pack::False) {
      return true;
    } else {
      return false;
    }
  }

  // Reads bool.Throws DecoderException if it isn't bool.
  bool read_bool() {
    auto marker = byte();

    if (marker == pack::True) {
      return true;
    } else if (marker == pack::False) {
      return false;
    } else {
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read bool header but found ", marker);
    }
  }

  bool is_integer() {
    auto marker = peek_byte();

    if (marker >= minus_2_to_the_4 && marker < plus_2_to_the_7) {
      return true;

    } else if (marker == pack::Int8) {
      return true;

    } else if (marker == pack::Int16) {
      return true;

    } else if (marker == pack::Int32) {
      return true;

    } else if (marker == pack::Int64) {
      return true;

    } else {
      return false;
    }
  }

  // Reads integer.Throws DecoderException if it isn't integer.
  int64_t integer() {
    auto marker = byte();

    if (marker >= minus_2_to_the_4 && marker < plus_2_to_the_7) {
      return marker;

    } else if (marker == pack::Int8) {
      return byte();

    } else if (marker == pack::Int16) {
      return read<int16_t>();

    } else if (marker == pack::Int32) {
      return read<int32_t>();

    } else if (marker == pack::Int64) {
      return read<int64_t>();

    } else {
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read integer but found ", marker);
    }
  }

  bool is_double() {
    auto marker = peek_byte();

    return marker == pack::Float64;
  }

  // Reads double.Throws DecoderException if it isn't double.
  double read_double() {
    auto marker = byte();
    if (marker == pack::Float64) {
      auto tmp = read<int64_t>();
      return *reinterpret_cast<const double *>(&tmp);
    } else {
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read double but found ", marker);
    }
  }

  bool is_string() {
    auto marker = peek_byte();

    // if the first 4 bits equal to 1000 (0x8), this is a tiny string
    if ((marker & 0xF0) == pack::TinyString) {
      return true;
    }
    // if the marker is 0xD0, size is an 8-bit unsigned integer
    else if (marker == pack::String8) {
      return true;
    }
    // if the marker is 0xD1, size is a 16-bit big-endian unsigned integer
    else if (marker == pack::String16) {
      return true;
    }
    // if the marker is 0xD2, size is a 32-bit big-endian unsigned integer
    else if (marker == pack::String32) {
      return true;

    } else {
      return false;
    }
  }

  // Reads string into res. Throws DecoderException if it isn't string.
  void string(std::string &res) {
    if (!string_try(res)) {
      throw DecoderException(
          "StreamedBoltDecoder: Tryed to read string but found ",
          std::to_string(peek_byte()));
    }
  }
  // Try-s to read string. Retunrns true on success. If it didn't succed
  // stream remains unchanged
  bool string_try(std::string &res) {
    auto marker = peek_byte();

    uint32_t size;

    // if the first 4 bits equal to 1000 (0x8), this is a tiny string
    if ((marker & 0xF0) == pack::TinyString) {
      byte();
      // size is stored in the lower 4 bits of the marker byte
      size = marker & 0x0F;
    }
    // if the marker is 0xD0, size is an 8-bit unsigned integer
    else if (marker == pack::String8) {
      byte();
      size = byte();
    }
    // if the marker is 0xD1, size is a 16-bit big-endian unsigned integer
    else if (marker == pack::String16) {
      byte();
      size = read<uint16_t>();
    }
    // if the marker is 0xD2, size is a 32-bit big-endian unsigned integer
    else if (marker == pack::String32) {
      byte();
      size = read<uint32_t>();
    } else {
      // Error
      return false;
    }

    if (size > 0) {
      res.resize(size);
      stream.read(&res.front(), size);
    } else {
      res.clear();
    }

    return true;
  }

 private:
  // Reads T from stream. It doens't care for alligment so this is valid only
  // for primitives.
  template <class T>
  T read() {
    buffer.resize(sizeof(T));

    // Load value
    stream.read(&buffer.front(), sizeof(T));

    // reinterpret bytes as the target value
    auto value = reinterpret_cast<const T *>(&buffer.front());

    // swap values to little endian
    return bswap(*value);
  }

  ::byte byte() { return stream.get(); }
  ::byte peek_byte() { return stream.peek(); }

  STREAM &stream;
  std::string buffer;
};
};
