#pragma once

namespace communication::bolt {

enum class PackType {
  /** denotes absence of a value */
  Null,

  /** denotes a type with two possible values (t/f) */
  Boolean,

  /** 64-bit signed integral number */
  Integer,

  /** 64-bit floating point number */
  Float,

  /** binary data */
  Bytes,

  /** unicode string */
  String,

  /** collection of values */
  List,

  /** collection of zero or more key/value pairs */
  Map,

  /** zero or more packstream values */
  Struct,

  /** denotes stream value end */
  EndOfStream,

  /** reserved for future use */
  Reserved
};
}
