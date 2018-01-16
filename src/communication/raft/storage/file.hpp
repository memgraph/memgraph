/**
 * @file
 *
 * Raft log is stored inside a folder. Each log entry is stored in a file named
 * by its index. There is a special file named "metadata" which stores Raft
 * metadata and also the last log index, which is used on startup to identify
 * which log entry files are valid.
 */
#pragma once

#include <fcntl.h>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/iostreams/device/file_descriptor.hpp"
#include "boost/iostreams/stream.hpp"

#include "communication/raft/raft.hpp"
#include "communication/raft/storage/memory.hpp"
#include "utils/file.hpp"

namespace communication::raft {

struct SimpleFileStorageMetadata {
  TermId term;
  std::experimental::optional<MemberId> voted_for;
  LogIndex last_log_index;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &term &voted_for &last_log_index;
  }
};

template <class State>
class SimpleFileStorage : public RaftStorageInterface<State> {
 public:
  explicit SimpleFileStorage(const fs::path &parent_dir) : memory_storage_() {
    try {
      dir_ = utils::OpenDir(parent_dir);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Error opening log directory: {}", e.what());
    }

    auto md = utils::TryOpenFile(dir_, "metadata", O_RDONLY);
    if (!md) {
      LOG(WARNING) << fmt::format("No metadata file found in directory '{}'",
                                  parent_dir);
      return;
    }

    boost::iostreams::file_descriptor_source src(
        md->Handle(),
        boost::iostreams::file_descriptor_flags::never_close_handle);
    boost::iostreams::stream<boost::iostreams::file_descriptor_source> is(src);
    boost::archive::binary_iarchive iar(is);

    SimpleFileStorageMetadata metadata;

    try {
      iar >> metadata;
    } catch (boost::archive::archive_exception &e) {
      LOG(FATAL) << "Failed to deserialize Raft metadata: " << e.what();
    }

    LOG(INFO) << fmt::format(
        "Read term = {} and voted_for = {} from storage", metadata.term,
        metadata.voted_for ? *metadata.voted_for : "(none)");

    memory_storage_.term_ = metadata.term;
    memory_storage_.voted_for_ = metadata.voted_for;
    memory_storage_.log_.reserve(metadata.last_log_index);

    for (LogIndex idx = 1; idx <= metadata.last_log_index; ++idx) {
      utils::File entry_file;

      try {
        entry_file = utils::OpenFile(dir_, fmt::format("{}", idx), O_RDONLY);
      } catch (std::system_error &e) {
        LOG(FATAL) << fmt::format("Failed to open entry file {}: {}", idx,
                                  e.what());
      }

      boost::iostreams::file_descriptor_source src(
          entry_file.Handle(),
          boost::iostreams::file_descriptor_flags::never_close_handle);
      boost::iostreams::stream<boost::iostreams::file_descriptor_source> is(
          src);
      boost::archive::binary_iarchive iar(is);
      LogEntry<State> entry;

      try {
        iar >> entry;
        memory_storage_.log_.emplace_back(std::move(entry));
      } catch (boost::archive::archive_exception &e) {
        LOG(FATAL) << fmt::format("Failed to deserialize log entry {}: {}", idx,
                                  e.what());
      }
    }

    LOG(INFO) << fmt::format("Read {} log entries", metadata.last_log_index);
  }

  void WriteTermAndVotedFor(
      TermId term,
      const std::experimental::optional<MemberId> &voted_for) override {
    memory_storage_.WriteTermAndVotedFor(term, voted_for);
    WriteMetadata();

    // Metadata file might be newly created so we have to fsync the directory.
    try {
      utils::Fsync(dir_);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to fsync Raft log directory: {}",
                                e.what());
    }
  }

  std::pair<TermId, std::experimental::optional<MemberId>> GetTermAndVotedFor()
      override {
    return memory_storage_.GetTermAndVotedFor();
  }

  void AppendLogEntry(const LogEntry<State> &entry) override {
    memory_storage_.AppendLogEntry(entry);

    utils::File entry_file;

    try {
      entry_file = utils::OpenFile(
          dir_, fmt::format("{}", memory_storage_.GetLastLogIndex()),
          O_WRONLY | O_CREAT | O_TRUNC, 0644);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to open log entry file: {}", e.what());
    }

    boost::iostreams::file_descriptor_sink sink(
        entry_file.Handle(),
        boost::iostreams::file_descriptor_flags::never_close_handle);
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink> os(sink);
    boost::archive::binary_oarchive oar(os);

    try {
      oar << entry;
      os.flush();
    } catch (boost::archive::archive_exception &e) {
      LOG(FATAL) << fmt::format("Failed to serialize log entry: {}", e.what());
    }

    try {
      utils::Fsync(entry_file);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to write log entry file to disk: {}",
                                e.what());
    }

    // We update the metadata only after the log entry file is written to
    // disk. This ensures that no file in range [1, last_log_index] is
    // corrupted.
    WriteMetadata();

    try {
      utils::Fsync(dir_);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to fsync Raft log directory: {}",
                                e.what());
    }
  }

  TermId GetLogTerm(const LogIndex index) override {
    return memory_storage_.GetLogTerm(index);
  }

  LogEntry<State> GetLogEntry(const LogIndex index) override {
    return memory_storage_.GetLogEntry(index);
  }

  std::vector<LogEntry<State>> GetLogSuffix(const LogIndex index) override {
    return memory_storage_.GetLogSuffix(index);
  }

  LogIndex GetLastLogIndex() override {
    return memory_storage_.GetLastLogIndex();
  }

  void TruncateLogSuffix(const LogIndex index) override {
    return memory_storage_.TruncateLogSuffix(index);
  }

 private:
  InMemoryStorage<State> memory_storage_;
  utils::File dir_;

  void WriteMetadata() {
    // We first write data to a temporary file, ensure data is safely written
    // to disk, and then rename the file. Since rename is an atomic operation,
    // "metadata" file won't get corrupted in case of program crash.
    utils::File md_tmp;
    try {
      md_tmp =
          OpenFile(dir_, "metadata.new", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to open temporary metadata file: {}",
                                e.what());
    }

    boost::iostreams::file_descriptor_sink sink(
        md_tmp.Handle(),
        boost::iostreams::file_descriptor_flags::never_close_handle);
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink> os(sink);
    boost::archive::binary_oarchive oar(os);

    try {
      oar << SimpleFileStorageMetadata{
          memory_storage_.GetTermAndVotedFor().first,
          memory_storage_.GetTermAndVotedFor().second,
          memory_storage_.GetLastLogIndex()};
    } catch (boost::archive::archive_exception &e) {
      LOG(FATAL) << "Error serializing Raft metadata";
    }
    os.flush();

    try {
      utils::Fsync(md_tmp);
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format(
          "Failed to write temporary metadata file to disk: {}", e.what());
    }

    try {
      utils::Rename(dir_, "metadata.new", dir_, "metadata");
    } catch (std::system_error &e) {
      LOG(FATAL) << fmt::format("Failed to move temporary metadata file: {}",
                                e.what());
    }
  }
};

}  // namespace communication::raft
