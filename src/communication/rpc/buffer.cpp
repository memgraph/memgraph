#include "glog/logging.h"

#include "communication/rpc/buffer.hpp"

namespace communication::rpc {

Buffer::Buffer() : data_(kBufferInitialSize, 0) {}

io::network::StreamBuffer Buffer::Allocate() {
  return {data_.data() + have_, data_.size() - have_};
}

void Buffer::Written(size_t len) {
  have_ += len;
  DCHECK(have_ <= data_.size()) << "Written more than storage has space!";
}

void Buffer::Shift(size_t len) {
  DCHECK(len <= have_) << "Tried to shift more data than the buffer has!";
  if (len == have_) {
    have_ = 0;
  } else {
    data_.erase(data_.begin(), data_.begin() + len);
    have_ -= len;
  }
}

void Buffer::Resize(size_t len) {
  if (len < data_.size()) return;
  data_.resize(len, 0);
}

void Buffer::Clear() { have_ = 0; }

uint8_t *Buffer::data() { return data_.data(); }

size_t Buffer::size() { return have_; }

}  // namespace communication::rpc
