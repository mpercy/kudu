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
// Some portions copyright (C) 2008, Google, inc.
//
// Utilities for working with protobufs.
// Some of this code is cribbed from the protobuf source,
// but modified to work with kudu's 'faststring' instead of STL strings.

#include "kudu/util/pb_util.h"

#include <deque>
#include <initializer_list>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/coding.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/pb_util.pb.h"
#include "kudu/util/status.h"

using google::protobuf::Descriptor;
using google::protobuf::DescriptorPool;
using google::protobuf::DynamicMessageFactory;
using google::protobuf::FieldDescriptor;
using google::protobuf::FileDescriptor;
using google::protobuf::FileDescriptorProto;
using google::protobuf::FileDescriptorSet;
using google::protobuf::io::ArrayInputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::Reflection;
using google::protobuf::SimpleDescriptorDatabase;
using kudu::crc::Crc;
using kudu::pb_util::internal::SequentialFileFileInputStream;
using kudu::pb_util::internal::WritableFileOutputStream;
using std::deque;
using std::endl;
using std::initializer_list;
using std::shared_ptr;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;
using strings::Utf8SafeCEscape;

static const char* const kTmpTemplateSuffix = ".tmp.XXXXXX";

// Protobuf container constants.
static const uint32_t kPBContainerInvalidVersion = 0;
static const uint32_t kPBContainerDefaultVersion = 2;
static const char kPBContainerMagic[] = "kuducntr";
static const int kPBContainerMagicLen = 8;
static const int kPBContainerV1HeaderLen =
    // magic number + version
    kPBContainerMagicLen + sizeof(uint32_t);
static const int kPBContainerChecksumLen = sizeof(uint32_t);

COMPILE_ASSERT((arraysize(kPBContainerMagic) - 1) == kPBContainerMagicLen,
               kPBContainerMagic_does_not_match_expected_length);

namespace kudu {
namespace pb_util {

namespace {

// When serializing, we first compute the byte size, then serialize the message.
// If serialization produces a different number of bytes than expected, we
// call this function, which crashes.  The problem could be due to a bug in the
// protobuf implementation but is more likely caused by concurrent modification
// of the message.  This function attempts to distinguish between the two and
// provide a useful error message.
void ByteSizeConsistencyError(int byte_size_before_serialization,
                              int byte_size_after_serialization,
                              int bytes_produced_by_serialization) {
  CHECK_EQ(byte_size_before_serialization, byte_size_after_serialization)
      << "Protocol message was modified concurrently during serialization.";
  CHECK_EQ(bytes_produced_by_serialization, byte_size_before_serialization)
      << "Byte size calculation and serialization were inconsistent.  This "
         "may indicate a bug in protocol buffers or it may be caused by "
         "concurrent modification of the message.";
  LOG(FATAL) << "This shouldn't be called if all the sizes are equal.";
}

string InitializationErrorMessage(const char* action,
                                  const MessageLite& message) {
  // Note:  We want to avoid depending on strutil in the lite library, otherwise
  //   we'd use:
  //
  // return strings::Substitute(
  //   "Can't $0 message of type \"$1\" because it is missing required "
  //   "fields: $2",
  //   action, message.GetTypeName(),
  //   message.InitializationErrorString());

  string result;
  result += "Can't ";
  result += action;
  result += " message of type \"";
  result += message.GetTypeName();
  result += "\" because it is missing required fields: ";
  result += message.InitializationErrorString();
  return result;
}

// Returns true iff the specified protobuf container file version is supported
// by this implementation.
bool IsSupportedContainerVersion(uint32_t version) {
  if (version == 1 || version == 2) {
    return true;
  }
  return false;
}

// Reads exactly 'length' bytes from the container file into 'scratch',
// validating that there is sufficient data in the file to read this length
// before attempting to do so, and validating that it has read that length
// after performing the read.
//
// If the file size is less than the requested size of the read, returns
// Status::Incomplete.
// If there is an unexpected short read, returns Status::Corruption.
//
// A Slice of the bytes read into 'scratch' is returned in 'result'.
template<typename ReadableFileType>
Status ValidateAndReadData(ReadableFileType* reader, uint64_t file_size,
                           uint64_t* offset, uint64_t length,
                           Slice* result, gscoped_ptr<uint8_t[]>* scratch) {
  // Validate the read length using the file size.
  if (*offset + length > file_size) {
    return Status::Incomplete("File size not large enough to be valid",
                              Substitute("Proto container file $0: "
                                  "Tried to read $1 bytes at offset "
                                  "$2 but file size is only $3 bytes",
                                  reader->filename(), length,
                                  *offset, file_size));
  }

  // Perform the read.
  Slice s;
  gscoped_ptr<uint8_t[]> local_scratch(new uint8_t[length]);
  RETURN_NOT_OK(reader->Read(*offset, length, &s, local_scratch.get()));

  // Sanity check the result.
  // TODO: Should this be a CHECK? Our IO abstractions should prevent this.
  if (PREDICT_FALSE(s.size() < length)) {
    return Status::Corruption("Unexpected short read", Substitute(
        "Proto container file $0: tried to read $1 bytes; got $2 bytes",
        reader->filename(), length, s.size()));
  }

  *offset += length;
  *result = s;
  scratch->swap(local_scratch);
  return Status::OK();
}

// Reads a 32-bit checksum from the file at the specified offset.
// On success, 'offset' is updated to the byte location following the read.
template<typename ReadableFileType>
Status ReadChecksum(ReadableFileType* reader, uint64_t file_size, uint64_t* offset,
                    uint32_t* checksum) {
  Slice encoded_checksum;
  gscoped_ptr<uint8_t[]> scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, offset, kPBContainerChecksumLen,
                                            &encoded_checksum, &scratch),
                        Substitute("Could not read checksum from proto container file $0 "
                                   "at offset $1", reader->filename(), *offset));
  *checksum = DecodeFixed32(encoded_checksum.data());
  return Status::OK();
}

// Reads a checksum from the specified offset and compares it to the bytes
// given in 'slices' by calculating a rolling CRC32 checksum of the bytes in
// the 'slices'.
// If they match, returns OK and 'offset' is updated to the byte location
// after the specified offset.
// If the checksums do not match, returns Status::Corruption and 'offset' is
// not modified.
template<typename ReadableFileType>
Status ReadAndCompareChecksum(ReadableFileType* reader, uint64_t file_size, uint64_t* offset,
                              const initializer_list<Slice>& slices) {
  uint32_t written_checksum = 0;
  uint64_t tmp_offset = *offset;
  RETURN_NOT_OK(ReadChecksum(reader, file_size, &tmp_offset, &written_checksum));
  uint64_t actual_checksum = 0;
  Crc* crc32c = crc::GetCrc32cInstance();
  for (Slice s : slices) {
    crc32c->Compute(s.data(), s.size(), &actual_checksum);
  }
  if (PREDICT_FALSE(actual_checksum != written_checksum)) {
    return Status::Corruption(Substitute("Incorrect checksum in file $0 at offset $1. "
                                         "Expected: $2. Actual: $3",
                                         reader->filename(), *offset,
                                         written_checksum, actual_checksum));
  }
  *offset = tmp_offset;
  return Status::OK();
}

// Read and parse a message of the specified format at the given offset in the
// format documented in pb_util.h. 'offset' is an in-out parameter and will be
// updated with the new offset on success. On failure, 'offset' is not modified.
template<typename ReadableFileType>
Status ReadPBStartingAt(ReadableFileType* reader, int version, uint64_t* offset, Message* msg) {
  uint64_t tmp_offset = *offset;
  VLOG(1) << "Reading PB with version " << version << " starting at offset " << *offset;

  uint64_t file_size;
  RETURN_NOT_OK(reader->Size(&file_size));
  if (tmp_offset == file_size) {
    return Status::EndOfFile("Reached end of file");
  }

  // Read the size from the file. EOF here is acceptable: it means we're
  // out of PB entries.
  Slice length;
  gscoped_ptr<uint8_t[]> length_scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, sizeof(uint32_t),
                                            &length, &length_scratch),
                        Substitute("Could not read data length from proto container file $0",
                                   reader->filename()));
  uint32_t data_length = DecodeFixed32(length.data());

  // Versions >= 2 have an individual checksum for the data length.
  if (version >= 2) {
    RETURN_NOT_OK_PREPEND(ReadAndCompareChecksum(reader, file_size, &tmp_offset, { length }),
                          "Data length checksum does not match");
  }

  // Read body into buffer for checksum & parsing.
  Slice body;
  gscoped_ptr<uint8_t[]> body_scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, data_length,
                                            &body, &body_scratch),
                        Substitute("Could not read PB message data from proto container file $0 "
                                   "at offset $1",
                                   reader->filename(), tmp_offset));

  // Version 1 has a single checksum for length, body.
  // Version 2+ has individual checksums for length and body, respectively.
  if (version == 1) {
    RETURN_NOT_OK_PREPEND(ReadAndCompareChecksum(reader, file_size, &tmp_offset, { length, body }),
                          "Length and data checksum does not match");
  } else {
    RETURN_NOT_OK_PREPEND(ReadAndCompareChecksum(reader, file_size, &tmp_offset, { body }),
                          "Data checksum does not match");
  }

  // The checksum is correct. Time to decode the body.
  //
  // We could compare pb_type_ against msg.GetTypeName(), but:
  // 1. pb_type_ is not available when reading the supplemental header,
  // 2. ParseFromArray() should fail if the data cannot be parsed into the
  //    provided message type.

  // To permit parsing of very large PB messages, we must use parse through a
  // CodedInputStream and bump the byte limit. The SetTotalBytesLimit() docs
  // say that 512MB is the shortest theoretical message length that may produce
  // integer overflow warnings, so that's what we'll use.
  ArrayInputStream ais(body.data(), body.size());
  CodedInputStream cis(&ais);
  cis.SetTotalBytesLimit(512 * 1024 * 1024, -1);
  if (PREDICT_FALSE(!msg->ParseFromCodedStream(&cis))) {
    return Status::IOError("Unable to parse PB from path", reader->filename());
  }

  *offset = tmp_offset;
  return Status::OK();
}

// Wrapper around ReadPBStartingAt() to enforce that we don't return
// Status::Incomplete() for V1 format files.
template<typename ReadableFileType>
Status ReadFullPB(ReadableFileType* reader, int version, uint64_t* offset, Message* msg) {
  Status s = ReadPBStartingAt(reader, version, offset, msg);
  if (PREDICT_FALSE(s.IsIncomplete() && version == 1)) {
    return Status::Corruption("Unrecoverable incomplete record", s.ToString());
  }
  return s;
}

// Read and parse the protobuf container file-level header documented above.
template<typename ReadableFileType>
Status ParsePBFileHeader(ReadableFileType* reader, uint64_t* offset, int* version) {
  uint64_t file_size;
  RETURN_NOT_OK(reader->Size(&file_size));

  uint64_t tmp_offset = *offset;
  Slice header;
  gscoped_ptr<uint8_t[]> scratch;
  RETURN_NOT_OK_PREPEND(ValidateAndReadData(reader, file_size, &tmp_offset, kPBContainerV1HeaderLen,
                                            &header, &scratch),
                        Substitute("Could not read header for proto container file $0",
                                   reader->filename()));

  // Validate magic number.
  if (PREDICT_FALSE(!strings::memeq(kPBContainerMagic, header.data(), kPBContainerMagicLen))) {
    string file_magic(reinterpret_cast<const char*>(header.data()), kPBContainerMagicLen);
    return Status::Corruption("Invalid magic number",
                              Substitute("Expected: $0, found: $1",
                                         Utf8SafeCEscape(kPBContainerMagic),
                                         Utf8SafeCEscape(file_magic)));
  }

  // Validate container file version.
  uint32_t tmp_version = DecodeFixed32(header.data() + kPBContainerMagicLen);
  if (PREDICT_FALSE(!IsSupportedContainerVersion(tmp_version))) {
    return Status::NotSupported(
        Substitute("Protobuf container has unsupported version: $0. Default version: $1",
                   tmp_version, kPBContainerDefaultVersion));
  }

  // Versions >= 2 have a checksum after the magic number and encoded version
  // to ensure the integrity of these fields.
  if (tmp_version >= 2) {
    RETURN_NOT_OK_PREPEND(ReadAndCompareChecksum(reader, file_size, &tmp_offset, { header }),
                          "File header checksum does not match");
  }

  *offset = tmp_offset;
  *version = tmp_version;
  return Status::OK();
}

// Read and parse the supplemental header from the container file.
template<typename ReadableFileType>
Status ReadSupplementalHeader(ReadableFileType* reader, int version, uint64_t* offset,
                              ContainerSupHeaderPB* sup_header) {
  RETURN_NOT_OK_PREPEND(ReadFullPB(reader, version, offset, sup_header),
      Substitute("Could not read supplemental header from proto container file $0 "
                 "with version $1 at offset $2",
                 reader->filename(), version, *offset));
  return Status::OK();
}

} // anonymous namespace

bool AppendToString(const MessageLite &msg, faststring *output) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  return AppendPartialToString(msg, output);
}

bool AppendPartialToString(const MessageLite &msg, faststring* output) {
  int old_size = output->size();
  int byte_size = msg.ByteSize();

  output->resize(old_size + byte_size);

  uint8* start = &((*output)[old_size]);
  uint8* end = msg.SerializeWithCachedSizesToArray(start);
  if (end - start != byte_size) {
    ByteSizeConsistencyError(byte_size, msg.ByteSize(), end - start);
  }
  return true;
}

bool SerializeToString(const MessageLite &msg, faststring *output) {
  output->clear();
  return AppendToString(msg, output);
}

bool ParseFromSequentialFile(MessageLite *msg, SequentialFile *rfile) {
  SequentialFileFileInputStream istream(rfile);
  return msg->ParseFromZeroCopyStream(&istream);
}

Status ParseFromArray(MessageLite* msg, const uint8_t* data, uint32_t length) {
  if (!msg->ParseFromArray(data, length)) {
    return Status::Corruption("Error parsing msg", InitializationErrorMessage("parse", *msg));
  }
  return Status::OK();
}

Status WritePBToPath(Env* env, const std::string& path,
                     const MessageLite& msg,
                     SyncMode sync) {
  const string tmp_template = path + kTmpTemplateSuffix;
  string tmp_path;

  gscoped_ptr<WritableFile> file;
  RETURN_NOT_OK(env->NewTempWritableFile(WritableFileOptions(), tmp_template, &tmp_path, &file));
  env_util::ScopedFileDeleter tmp_deleter(env, tmp_path);

  WritableFileOutputStream ostream(file.get());
  bool res = msg.SerializeToZeroCopyStream(&ostream);
  if (!res || !ostream.Flush()) {
    return Status::IOError("Unable to serialize PB to file");
  }

  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(file->Sync(), "Failed to Sync() " + tmp_path);
  }
  RETURN_NOT_OK_PREPEND(file->Close(), "Failed to Close() " + tmp_path);
  RETURN_NOT_OK_PREPEND(env->RenameFile(tmp_path, path), "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(env->SyncDir(DirName(path)), "Failed to SyncDir() parent of " + path);
  }
  return Status::OK();
}

Status ReadPBFromPath(Env* env, const std::string& path, MessageLite* msg) {
  shared_ptr<SequentialFile> rfile;
  RETURN_NOT_OK(env_util::OpenFileForSequential(env, path, &rfile));
  if (!ParseFromSequentialFile(msg, rfile.get())) {
    return Status::IOError("Unable to parse PB from path", path);
  }
  return Status::OK();
}

static void TruncateString(string* s, int max_len) {
  if (s->size() > max_len) {
    s->resize(max_len);
    s->append("<truncated>");
  }
}

void TruncateFields(Message* message, int max_len) {
  const Reflection* reflection = message->GetReflection();
  vector<const FieldDescriptor*> fields;
  reflection->ListFields(*message, &fields);
  for (const FieldDescriptor* field : fields) {
    if (field->is_repeated()) {
      for (int i = 0; i < reflection->FieldSize(*message, field); i++) {
        switch (field->cpp_type()) {
          case FieldDescriptor::CPPTYPE_STRING: {
            const string& s_const = reflection->GetRepeatedStringReference(*message, field, i,
                                                                           nullptr);
            TruncateString(const_cast<string*>(&s_const), max_len);
            break;
          }
          case FieldDescriptor::CPPTYPE_MESSAGE: {
            TruncateFields(reflection->MutableRepeatedMessage(message, field, i), max_len);
            break;
          }
          default:
            break;
        }
      }
    } else {
      switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_STRING: {
          const string& s_const = reflection->GetStringReference(*message, field, nullptr);
          TruncateString(const_cast<string*>(&s_const), max_len);
          break;
        }
        case FieldDescriptor::CPPTYPE_MESSAGE: {
          TruncateFields(reflection->MutableMessage(message, field), max_len);
          break;
        }
        default:
          break;
      }
    }
  }
}

WritablePBContainerFile::WritablePBContainerFile(gscoped_ptr<RWFile> writer)
  : initialized_(false),
    closed_(false),
    offset_(0),
    version_(kPBContainerDefaultVersion),
    writer_(std::move(writer)) {
}

WritablePBContainerFile::~WritablePBContainerFile() {
  WARN_NOT_OK(Close(), "Could not Close() when destroying file");
}

Status WritablePBContainerFile::SetVersionForTests(int version) {
  DCHECK(!initialized_);
  if (!IsSupportedContainerVersion(version)) {
    return Status::NotSupported(Substitute("Version $0 is not supported", version));
  }
  version_ = version;
  return Status::OK();
}

Status WritablePBContainerFile::Init(const Message& msg) {
  DCHECK(!closed_);
  DCHECK(!initialized_);

  const uint64_t kHeaderLen = (version_ == 1) ? kPBContainerV1HeaderLen
                                              : kPBContainerV1HeaderLen + kPBContainerChecksumLen;

  faststring buf;
  buf.resize(kHeaderLen);

  // Serialize the magic.
  strings::memcpy_inlined(buf.data(), kPBContainerMagic, kPBContainerMagicLen);
  uint64_t offset = kPBContainerMagicLen;

  // Serialize the version.
  InlineEncodeFixed32(buf.data() + offset, version_);
  offset += sizeof(uint32_t);
  DCHECK_EQ(kPBContainerV1HeaderLen, offset)
    << "Serialized unexpected number of total bytes";

  // Versions >= 2: Checksum the magic and version.
  if (version_ >= 2) {
    uint32_t header_checksum = crc::Crc32c(buf.data(), offset);
    InlineEncodeFixed32(buf.data() + offset, header_checksum);
    offset += sizeof(uint32_t);
  }

  // Serialize the supplemental header.
  ContainerSupHeaderPB sup_header;
  PopulateDescriptorSet(msg.GetDescriptor()->file(),
                        sup_header.mutable_protos());
  sup_header.set_pb_type(msg.GetTypeName());
  RETURN_NOT_OK_PREPEND(AppendMsgToBuffer(sup_header, &buf),
                        "Failed to prepare supplemental header for writing");

  // Write the serialized buffer to the file.
  RETURN_NOT_OK_PREPEND(AppendBytes(buf),
                        "Failed to append header to file");
  initialized_ = true;
  return Status::OK();
}

Status WritablePBContainerFile::Reopen() {
  DCHECK(!closed_);
  offset_ = 0;
  RETURN_NOT_OK(ParsePBFileHeader(writer_.get(), &offset_, &version_));
  ContainerSupHeaderPB sup_header;
  RETURN_NOT_OK(ReadSupplementalHeader(writer_.get(), version_, &offset_, &sup_header));
  RETURN_NOT_OK(writer_->Size(&offset_)); // Reset the write offset to the end of the file.
  initialized_ = true;
  return Status::OK();
}

Status WritablePBContainerFile::AppendBytes(const Slice& data) {
  RETURN_NOT_OK(writer_->Write(offset_, data));
  offset_ += data.size();
  return Status::OK();
}

Status WritablePBContainerFile::Append(const Message& msg) {
  DCHECK(initialized_);
  DCHECK(!closed_);

  faststring buf;
  RETURN_NOT_OK_PREPEND(AppendMsgToBuffer(msg, &buf),
                        "Failed to prepare buffer for writing");
  RETURN_NOT_OK_PREPEND(AppendBytes(buf), "Failed to append data to file");

  return Status::OK();
}

Status WritablePBContainerFile::Flush() {
  DCHECK(initialized_);
  DCHECK(!closed_);

  // TODO: Flush just the dirty bytes.
  RETURN_NOT_OK_PREPEND(writer_->Flush(RWFile::FLUSH_SYNC, 0, 0), "Failed to Flush() file");

  return Status::OK();
}

Status WritablePBContainerFile::Sync() {
  DCHECK(initialized_);
  DCHECK(!closed_);

  RETURN_NOT_OK_PREPEND(writer_->Sync(), "Failed to Sync() file");

  return Status::OK();
}

Status WritablePBContainerFile::Close() {
  if (!closed_) {
    closed_ = true;
    Status s = writer_->Close();
    writer_.reset();
    RETURN_NOT_OK_PREPEND(s, "Failed to Close() file");
  }
  return Status::OK();
}

Status WritablePBContainerFile::AppendMsgToBuffer(const Message& msg, faststring* buf) {
  DCHECK(msg.IsInitialized()) << InitializationErrorMessage("serialize", msg);
  uint64_t data_len = msg.ByteSize();
  uint64_t record_buflen =  sizeof(uint32_t) + data_len + kPBContainerChecksumLen;
  if (version_ >= 2) {
    record_buflen += kPBContainerChecksumLen; // Additional checksum just for the length.
  }

  // Grow the buffer to hold the new data.
  uint64_t orig_size = buf->size();
  buf->resize(orig_size + record_buflen);
  uint8_t* dst = buf->data() + orig_size;

  // Serialize the data length.
  InlineEncodeFixed32(dst, static_cast<uint32_t>(data_len));
  size_t offset = sizeof(data_len);

  // For version >= 2: Serialize the checksum of the data length.
  if (version_ >= 2) {
    uint32_t len_checksum = crc::Crc32c(&data_len, sizeof(data_len));
    InlineEncodeFixed32(dst + offset, len_checksum);
    offset += sizeof(len_checksum);
  }

  // Serialize the data.
  if (PREDICT_FALSE(!msg.SerializeWithCachedSizesToArray(dst + offset))) {
    return Status::IOError("Failed to serialize PB to array");
  }
  size_t serialized_len = offset + data_len;

  // Calculate and serialize the data checksum.
  // For version 1, this is the checksum of the len + data.
  // For version >= 2, this is only the checksum of the data.
  uint32_t checksum;
  if (version_ == 1) {
    checksum = crc::Crc32c(dst, serialized_len);
  } else {
    checksum = crc::Crc32c(dst + offset, data_len);
  }
  InlineEncodeFixed32(dst + serialized_len, checksum);
  serialized_len += kPBContainerChecksumLen;

  DCHECK_EQ(record_buflen, serialized_len) << "Serialized unexpected number of total bytes";
  return Status::OK();
}

void WritablePBContainerFile::PopulateDescriptorSet(
    const FileDescriptor* desc, FileDescriptorSet* output) {
  // Because we don't compile protobuf with TSAN enabled, copying the
  // static PB descriptors in this function ends up triggering a lot of
  // race reports. We suppress the reports, but TSAN still has to walk
  // the stack, etc, and this function becomes very slow. So, we ignore
  // TSAN here.
  debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;

  FileDescriptorSet all_descs;

  // Tracks all schemas that have been added to 'unemitted' at one point
  // or another. Is a superset of 'unemitted' and only ever grows.
  unordered_set<const FileDescriptor*> processed;

  // Tracks all remaining unemitted schemas.
  deque<const FileDescriptor*> unemitted;

  InsertOrDie(&processed, desc);
  unemitted.push_front(desc);
  while (!unemitted.empty()) {
    const FileDescriptor* proto = unemitted.front();

    // The current schema is emitted iff we've processed (i.e. emitted) all
    // of its dependencies.
    bool emit = true;
    for (int i = 0; i < proto->dependency_count(); i++) {
      const FileDescriptor* dep = proto->dependency(i);
      if (InsertIfNotPresent(&processed, dep)) {
        unemitted.push_front(dep);
        emit = false;
      }
    }
    if (emit) {
      unemitted.pop_front();
      proto->CopyTo(all_descs.mutable_file()->Add());
    }
  }
  all_descs.Swap(output);
}

ReadablePBContainerFile::ReadablePBContainerFile(gscoped_ptr<RandomAccessFile> reader)
  : initialized_(false),
    closed_(false),
    version_(kPBContainerInvalidVersion),
    offset_(0),
    reader_(std::move(reader)) {
}

ReadablePBContainerFile::~ReadablePBContainerFile() {
  Close();
}

Status ReadablePBContainerFile::Open() {
  DCHECK(!initialized_);
  DCHECK(!closed_);
  RETURN_NOT_OK(ParsePBFileHeader(reader_.get(), &offset_, &version_));
  ContainerSupHeaderPB sup_header;
  RETURN_NOT_OK(ReadSupplementalHeader(reader_.get(), version_, &offset_, &sup_header));
  protos_.reset(sup_header.release_protos());
  pb_type_ = sup_header.pb_type();
  initialized_ = true;
  return Status::OK();;
}

Status ReadablePBContainerFile::ReadNextPB(Message* msg) {
  DCHECK(initialized_);
  DCHECK(!closed_);
  return ReadFullPB(reader_.get(), version_, &offset_, msg);
}

Status ReadablePBContainerFile::Dump(ostream* os, bool oneline) {
  DCHECK(initialized_);
  DCHECK(!closed_);

  // Use the embedded protobuf information from the container file to
  // create the appropriate kind of protobuf Message.
  //
  // Loading the schemas into a DescriptorDatabase (and not directly into
  // a DescriptorPool) defers resolution until FindMessageTypeByName()
  // below, allowing for schemas to be loaded in any order.
  SimpleDescriptorDatabase db;
  for (int i = 0; i < protos()->file_size(); i++) {
    if (!db.Add(protos()->file(i))) {
      return Status::Corruption("Descriptor not loaded", Substitute(
          "Could not load descriptor for PB type $0 referenced in container file",
          pb_type()));
    }
  }
  DescriptorPool pool(&db);
  const Descriptor* desc = pool.FindMessageTypeByName(pb_type());
  if (!desc) {
    return Status::NotFound("Descriptor not found", Substitute(
        "Could not find descriptor for PB type $0 referenced in container file",
        pb_type()));
  }
  DynamicMessageFactory factory;
  const Message* prototype = factory.GetPrototype(desc);
  if (!prototype) {
    return Status::NotSupported("Descriptor not supported", Substitute(
        "Descriptor $0 referenced in container file not supported",
        pb_type()));
  }
  gscoped_ptr<Message> msg(prototype->New());

  // Dump each message in the container file.
  int count = 0;
  Status s;
  for (s = ReadNextPB(msg.get());
      s.ok();
      s = ReadNextPB(msg.get())) {
    if (oneline) {
      *os << count++ << "\t" << msg->ShortDebugString() << endl;
    } else {
      *os << "Message " << count << endl;
      *os << "-------" << endl;
      *os << msg->DebugString() << endl;
      count++;
    }
  }
  return s.IsEndOfFile() ? s.OK() : s;
}

Status ReadablePBContainerFile::Close() {
  closed_ = true;
  reader_.reset();
  return Status::OK();
}

int ReadablePBContainerFile::version() const {
  CHECK(initialized_);
  return version_;
}

uint64_t ReadablePBContainerFile::offset() const {
  DCHECK(initialized_);
  DCHECK(!closed_);
  return offset_;
}

Status ReadPBContainerFromPath(Env* env, const std::string& path, Message* msg) {
  gscoped_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &file));

  ReadablePBContainerFile pb_file(std::move(file));
  RETURN_NOT_OK(pb_file.Open());
  RETURN_NOT_OK(pb_file.ReadNextPB(msg));
  return pb_file.Close();
}

Status WritePBContainerToPath(Env* env, const std::string& path,
                              const Message& msg,
                              CreateMode create,
                              SyncMode sync) {
  TRACE_EVENT2("io", "WritePBContainerToPath",
               "path", path,
               "msg_type", msg.GetTypeName());

  if (create == NO_OVERWRITE && env->FileExists(path)) {
    return Status::AlreadyPresent(Substitute("File $0 already exists", path));
  }

  const string tmp_template = path + kTmpTemplateSuffix;
  string tmp_path;

  gscoped_ptr<RWFile> file;
  RETURN_NOT_OK(env->NewTempRWFile(RWFileOptions(), tmp_template, &tmp_path, &file));
  env_util::ScopedFileDeleter tmp_deleter(env, tmp_path);

  WritablePBContainerFile pb_file(std::move(file));
  RETURN_NOT_OK(pb_file.Init(msg));
  RETURN_NOT_OK(pb_file.Append(msg));
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK(pb_file.Sync());
  }
  RETURN_NOT_OK(pb_file.Close());
  RETURN_NOT_OK_PREPEND(env->RenameFile(tmp_path, path),
                        "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  if (sync == pb_util::SYNC) {
    RETURN_NOT_OK_PREPEND(env->SyncDir(DirName(path)),
                          "Failed to SyncDir() parent of " + path);
  }
  return Status::OK();
}

} // namespace pb_util
} // namespace kudu
