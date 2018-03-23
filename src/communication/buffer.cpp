#include "glog/logging.h"

#include "communication/buffer.hpp"

namespace communication {

Buffer::Buffer()
    : data_(kBufferInitialSize, 0), read_end_(*this), write_end_(*this) {}

Buffer::ReadEnd::ReadEnd(Buffer &buffer) : buffer_(buffer) {}

uint8_t *Buffer::ReadEnd::data() { return buffer_.data(); }

size_t Buffer::ReadEnd::size() const { return buffer_.size(); }

void Buffer::ReadEnd::Shift(size_t len) { buffer_.Shift(len); }

void Buffer::ReadEnd::Resize(size_t len) { buffer_.Resize(len); }

void Buffer::ReadEnd::Clear() { buffer_.Clear(); }

Buffer::WriteEnd::WriteEnd(Buffer &buffer) : buffer_(buffer) {}

io::network::StreamBuffer Buffer::WriteEnd::Allocate() {
  return buffer_.Allocate();
}

void Buffer::WriteEnd::Written(size_t len) { buffer_.Written(len); }

void Buffer::WriteEnd::Resize(size_t len) { buffer_.Resize(len); }

void Buffer::WriteEnd::Clear() { buffer_.Clear(); }

Buffer::ReadEnd &Buffer::read_end() { return read_end_; }

Buffer::WriteEnd &Buffer::write_end() { return write_end_; }

uint8_t *Buffer::data() { return data_.data(); }

size_t Buffer::size() const { return have_; }

void Buffer::Shift(size_t len) {
  DCHECK(len <= have_) << "Tried to shift more data than the buffer has!";
  if (len == have_) {
    have_ = 0;
  } else {
    memmove(data_.data(), data_.data() + len, have_ - len);
    have_ -= len;
  }
}

io::network::StreamBuffer Buffer::Allocate() {
  DCHECK(data_.size() > have_) << "The buffer thinks that there is more data "
                                  "in the buffer than there is underlying "
                                  "storage space!";
  return {data_.data() + have_, data_.size() - have_};
}

void Buffer::Written(size_t len) {
  have_ += len;
  DCHECK(have_ <= data_.size()) << "Written more than storage has space!";
}

void Buffer::Resize(size_t len) {
  if (len < data_.size()) return;
  data_.resize(len, 0);
}

void Buffer::Clear() { have_ = 0; }

}  // namespace communication
