/// @file

#pragma once

namespace storage {
/// CachedDataLock wrapps RecordAccessors HoldCachedData and ReleaseCachedData
/// methods and provides a RAII style mechanism for releasing the lock at the
/// end of the scope.
template <typename TRecordAccessor>
class CachedDataLock {
  explicit CachedDataLock(const TRecordAccessor &accessor)
      : accessor_(&accessor) {
    accessor_->HoldCachedData();
  }

  template <typename URecordAccessor>
  friend CachedDataLock<URecordAccessor> GetDataLock(const URecordAccessor &);

 public:
  ~CachedDataLock() { Release(); }

  void Release() {
    if (accessor_) {
      accessor_->ReleaseCachedData();
      accessor_ = nullptr;
    }
  }

 private:
  const TRecordAccessor *accessor_;
};

/// Constructs CachedDataLock and then lockes given accessor.
template <typename TRecordAccessor>
CachedDataLock<TRecordAccessor> GetDataLock(const TRecordAccessor &accessor) {
  return CachedDataLock<TRecordAccessor>(accessor);
}
} // namespace storage
