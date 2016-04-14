// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Utilities for dealing with protocol buffers.
// These are mostly just functions similar to what are found in the protobuf
// library itself, but using kudu::faststring instances instead of STL strings.
#ifndef KUDU_UTIL_PB_UTIL_H
#define KUDU_UTIL_PB_UTIL_H

#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/faststring.h"

namespace google {
namespace protobuf {
class FileDescriptor;
class FileDescriptorSet;
class MessageLite;
class Message;
}
}

namespace kudu {

class Env;
class RandomAccessFile;
class SequentialFile;
class Slice;
class Status;
class RWFile;

namespace pb_util {

using google::protobuf::MessageLite;

enum SyncMode {
  SYNC,
  NO_SYNC
};

enum CreateMode {
  OVERWRITE,
  NO_OVERWRITE
};

// See MessageLite::AppendToString
bool AppendToString(const MessageLite &msg, faststring *output);

// See MessageLite::AppendPartialToString
bool AppendPartialToString(const MessageLite &msg, faststring *output);

// See MessageLite::SerializeToString.
bool SerializeToString(const MessageLite &msg, faststring *output);

// See MessageLite::ParseFromZeroCopyStream
// TODO: change this to return Status - differentiate IO error from bad PB
bool ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile);

// Similar to MessageLite::ParseFromArray, with the difference that it returns
// Status::Corruption() if the message could not be parsed.
Status ParseFromArray(MessageLite* msg, const uint8_t* data, uint32_t length);

// Load a protobuf from the given path.
Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg);

// Serialize a protobuf to the given path.
//
// If SyncMode SYNC is provided, ensures the changes are made durable.
Status WritePBToPath(Env* env, const std::string& path, const MessageLite& msg, SyncMode sync);

// Truncate any 'bytes' or 'string' fields of this message to max_len.
// The text "<truncated>" is appended to any such truncated fields.
void TruncateFields(google::protobuf::Message* message, int max_len);

// A protobuf "container" has the following format (all integers in
// little-endian byte order).
//
// File header format
// ------------------
//
// Each protobuf container file contains a file header identifying the file.
// This includes:
//
// magic number: 8 byte string identifying the file format.
//
//     Included so that we have a minimal guarantee that this file is of the
//     type we expect and that we are not just reading garbage.
//
// container_version: 4 byte unsigned integer indicating the "version" of the
//                    container format. May be set to 1 or 2. The differences
//                    between versions are described below.
//
//     Included so that this file format may be extended at some later date
//     while maintaining backwards compatibility.
//
// The remaining container fields are considered part of a "record". There may
// be 1 or more records in a valid protobuf container file.
//
// Versions 1 and 2 of the file format
// -----------------------------------
//
// Version 2 differs from version 1 in that each record in version 2 contains a
// length checksum, while version 1 contains only a data checksum.
//
// Record format
// -------------
//
// data length: 4 byte unsigned integer indicating the size of the encoded data.
//
//    Included because PB messages aren't self-delimiting, and thus
//    writing a stream of messages to the same file requires
//    delimiting each with its size.
//
//    See https://developers.google.com/protocol-buffers/docs/techniques?hl=zh-cn#streaming
//    for more details.
//
// length checksum (version 2+ only): 4-byte unsigned integer containing the
//                                    CRC32C checksum of "data length".
//
// data: "size" bytes of protobuf data encoded according to the schema.
//
//     Our payload.
//
// data checksum: 4 byte unsigned integer containing the CRC32C checksum of "data".
//
//     Included to ensure validity of the data on-disk.
//
// Supplemental header
// -------------------
//
// A valid container must have at least one protobuf message, the first of
// which is known as the "supplemental header". The supplemental header
// contains additional container-level information, including the protobuf
// schema used for the records following it. See pb_util.proto for details. As
// a containerized PB message, the supplemental header is protected by a CRC32C
// checksum like any other message.
//
// Error detection and tolerance
// -----------------------------
//
// It is worth describing the kinds of errors that can be detected by the
// protobuf container and the kinds that cannot.
//
// The checksums in the container are independent, not rolling. As such,
// they won't detect the disappearance or reordering of entire protobuf
// messages, which can happen if a range of the file is collapsed (see
// man fallocate(2)) or if the file is otherwise manually manipulated.
// Moreover, the checksums do not protect against corruption in the data
// size fields, though that is mitigated by validating each data size
// against the remaining number of bytes in the container.
//
// Additionally, the container does not include footers or periodic
// checkpoints. As such, it will not detect if entire protobuf messages
// are truncated.
//
// That said, all corruption or truncation of the magic number or the
// container version will be detected, as will most corruption/truncation
// of the data size, data, and checksum (subject to CRC32 limitations).
//
// These tradeoffs in error detection are reasonable given the failure
// environment that Kudu operates within. We tolerate failures such as
// "kill -9" of the Kudu process, machine power loss, or fsync/fdatasync
// failure, but not failures like runaway processes mangling data files
// in arbitrary ways or attackers crafting malicious data files.
//
// The one kind of failure that clients must handle is truncation of entire
// protobuf messages (see above). The protobuf container will not detect
// these failures, so clients must tolerate them in some way.
//
// For further reading on what files might look like following a normal
// filesystem failure, see:
//
// https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf

// Protobuf container file opened for writing.
//
// Can be built around an existing file or a completely new file.
//
// Not thread-safe.
class WritablePBContainerFile {
 public:

  // Initializes the class instance; writer must be open.
  explicit WritablePBContainerFile(gscoped_ptr<RWFile> writer);

  // Closes the container if not already closed.
  ~WritablePBContainerFile();

  // Writes the header information to the container.
  //
  // 'msg' need not be populated; its type is used to "lock" the container
  // to a particular protobuf message type in Append().
  Status Init(const google::protobuf::Message& msg);

  // Reopen a previously initialized protobuf container file with the specified version.
  // Reopen() determines the initial end-of-file write offset by reading the
  // length of the file at the time it is called. Because
  // WritablePBContainerFile caches the offset instead of constantly calling
  // stat() on the file, if the length of the file is changed externally then
  // Reopen() must be called again for the writer to see the change.
  // For example, if a file is truncated, and you wish to continue writing from
  // that point forward, you must call Reopen() again for the writer to reset
  // its write offset to the new end of file location.
  Status Reopen();

  // Writes a protobuf message to the container, beginning with its size
  // and ending with its CRC32 checksum.
  Status Append(const google::protobuf::Message& msg);

  // Asynchronously flushes all dirty container data to the filesystem.
  Status Flush();

  // Synchronizes all dirty container data to the filesystem.
  //
  // Note: the parent directory is _not_ synchronized. Because the
  // container file was provided during construction, we don't know whether
  // it was created or reopened, and parent directory synchronization is
  // only needed in the former case.
  Status Sync();

  // Closes the container.
  Status Close();

 private:
  friend class TestPBUtil;
  FRIEND_TEST(TestPBUtil, TestPopulateDescriptorSet);

  // Set the file format version. Only used for testing.
  // Must be called before Init().
  Status SetVersionForTests(int version);

  // Write the protobuf schemas belonging to 'desc' and all of its
  // dependencies to 'output'.
  //
  // Schemas are written in dependency order (i.e. if A depends on B which
  // depends on C, the order is C, B, A).
  static void PopulateDescriptorSet(const google::protobuf::FileDescriptor* desc,
                                    google::protobuf::FileDescriptorSet* output);

  // Serialize the contents of 'msg' into 'buf' along with additional metadata
  // to aid in deserialization.
  Status AppendMsgToBuffer(const google::protobuf::Message& msg, faststring* buf);

  // Append bytes to the file.
  Status AppendBytes(const Slice& data);

  bool initialized_;
  bool closed_;

  // Current write offset into the file.
  uint64_t offset_;
  int version_;

  gscoped_ptr<RWFile> writer_;
};

// Protobuf container file opened for reading.
//
// Can be built around a file with existing contents or an empty file (in
// which case it's safe to interleave with WritablePBContainerFile).
class ReadablePBContainerFile {
 public:

  // Initializes the class instance; reader must be open.
  explicit ReadablePBContainerFile(gscoped_ptr<RandomAccessFile> reader);

  // Closes the file if not already closed.
  ~ReadablePBContainerFile();

  // Reads the header information from the container and validates it.
  Status Open();

  // Reads a protobuf message from the container, validating its size and
  // data using a CRC32 checksum.
  // Return values:
  // * If there are no more records in the file, returns Status::EndOfFile.
  // * If there is a partial record, but it is not long enough to be a full
  //   record or the written length of the record is less than the remaining
  //   bytes in the file, returns Status::Incomplete. If Status::Incomplete
  //   is returned, calling offset() will return the point in the file where
  //   the invalid partial record begins. In order to append additional records
  //   to the file, the file must first be truncated at that offset.
  //   Note: Version 1 of this file format will never return
  //   Status::Incomplete() from this method.
  // * If a corrupt record is encountered, returns Status::Corruption.
  Status ReadNextPB(google::protobuf::Message* msg);

  // Dumps any unread protobuf messages in the container to 'os'. Each
  // message's DebugString() method is invoked to produce its textual form.
  //
  // If 'oneline' is true, prints each message on a single line.
  Status Dump(std::ostream* os, bool oneline);

  // Closes the container.
  Status Close();

  // Expected PB type and schema for each message to be read.
  //
  // Only valid after a successful call to Open().
  const std::string& pb_type() const { return pb_type_; }
  const google::protobuf::FileDescriptorSet* protos() const {
    return protos_.get();
  }

  // Return the protobuf container file format version.
  // Open() must be called first.
  int version() const;

  // Return current read offset. File must be open.
  uint64_t offset() const;

 private:
  bool initialized_;
  bool closed_;
  int version_;
  uint64_t offset_;

  // The fully-qualified PB type name of the messages in the container.
  std::string pb_type_;

  // Wrapped in a gscoped_ptr so that clients need not include PB headers.
  gscoped_ptr<google::protobuf::FileDescriptorSet> protos_;

  gscoped_ptr<RandomAccessFile> reader_;
};

// Convenience functions for protobuf containers holding just one record.

// Load a "containerized" protobuf from the given path.
// If the file does not exist, returns Status::NotFound(). Otherwise, may
// return other Status error codes such as Status::IOError.
Status ReadPBContainerFromPath(Env* env, const std::string& path,
                               google::protobuf::Message* msg);

// Serialize a "containerized" protobuf to the given path.
//
// If create == NO_OVERWRITE and 'path' already exists, the function will fail.
// If sync == SYNC, the newly created file will be fsynced before returning.
Status WritePBContainerToPath(Env* env, const std::string& path,
                              const google::protobuf::Message& msg,
                              CreateMode create,
                              SyncMode sync);

} // namespace pb_util
} // namespace kudu
#endif
