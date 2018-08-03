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
#include "kudu/tablet/cfile_set.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/vector.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_bool(consult_bloom_filters, true, "Whether to consult bloom filters on row presence checks");
TAG_FLAG(consult_bloom_filters, hidden);

DECLARE_bool(rowset_metadata_store_keys);

DEFINE_bool(enable_skip_scan, true, "Whether to enable index skip scan");
DEFINE_int32(skip_scan_short_circuit_loops, 100,
    "Max number of skip attempts the skip scan optimization should make before "
    "returning control to the main loop");

namespace kudu {

class MemTracker;

namespace tablet {

using cfile::BloomFileReader;
using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::ColumnIterator;
using cfile::ReaderOptions;
using cfile::DefaultColumnValueIterator;
using fs::ReadableBlock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////

static Status OpenReader(FsManager* fs,
                         shared_ptr<MemTracker> parent_mem_tracker,
                         const BlockId& block_id,
                         unique_ptr<CFileReader>* new_reader) {
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs->OpenBlock(block_id, &block));

  ReaderOptions opts;
  opts.parent_mem_tracker = std::move(parent_mem_tracker);
  return CFileReader::OpenNoInit(std::move(block),
                                 std::move(opts),
                                 new_reader);
}

////////////////////////////////////////////////////////////
// CFile Base
////////////////////////////////////////////////////////////

CFileSet::CFileSet(shared_ptr<RowSetMetadata> rowset_metadata,
                   shared_ptr<MemTracker> parent_mem_tracker)
    : rowset_metadata_(std::move(rowset_metadata)),
      parent_mem_tracker_(std::move(parent_mem_tracker)) {
}

CFileSet::~CFileSet() {
}

Status CFileSet::Open(shared_ptr<RowSetMetadata> rowset_metadata,
                      shared_ptr<MemTracker> parent_mem_tracker,
                      shared_ptr<CFileSet>* cfile_set) {
  shared_ptr<CFileSet> cfs(new CFileSet(std::move(rowset_metadata),
                                        std::move(parent_mem_tracker)));
  RETURN_NOT_OK(cfs->DoOpen());

  cfile_set->swap(cfs);
  return Status::OK();
}

Status CFileSet::DoOpen() {
  RETURN_NOT_OK(OpenBloomReader());

  // Lazily open the column data cfiles. Each one will be fully opened
  // later, when the first iterator seeks for the first time.
  RowSetMetadata::ColumnIdToBlockIdMap block_map = rowset_metadata_->GetColumnBlocksById();
  for (const RowSetMetadata::ColumnIdToBlockIdMap::value_type& e : block_map) {
    ColumnId col_id = e.first;
    DCHECK(!ContainsKey(readers_by_col_id_, col_id)) << "already open";

    unique_ptr<CFileReader> reader;
    RETURN_NOT_OK(OpenReader(rowset_metadata_->fs_manager(),
                             parent_mem_tracker_,
                             rowset_metadata_->column_data_block_for_col_id(col_id),
                             &reader));
    readers_by_col_id_[col_id] = std::move(reader);
    VLOG(1) << "Successfully opened cfile for column id " << col_id
            << " in " << rowset_metadata_->ToString();
  }
  readers_by_col_id_.shrink_to_fit();

  if (rowset_metadata_->has_adhoc_index_block()) {
    RETURN_NOT_OK(OpenReader(rowset_metadata_->fs_manager(),
                             parent_mem_tracker_,
                             rowset_metadata_->adhoc_index_block(),
                             &ad_hoc_idx_reader_));
  }

  // If the user specified to store the min/max keys in the rowset metadata,
  // fetch them. Otherwise, load the min and max keys from the key reader.
  if (FLAGS_rowset_metadata_store_keys && rowset_metadata_->has_encoded_keys()) {
    min_encoded_key_ = rowset_metadata_->min_encoded_key();
    max_encoded_key_ = rowset_metadata_->max_encoded_key();
  } else {
    RETURN_NOT_OK(LoadMinMaxKeys());
  }
  // Verify the loaded keys are valid.
  if (Slice(min_encoded_key_).compare(max_encoded_key_) > 0) {
    return Status::Corruption(Substitute("Min key $0 > max key $1",
                                         KUDU_REDACT(Slice(min_encoded_key_).ToDebugString()),
                                         KUDU_REDACT(Slice(max_encoded_key_).ToDebugString())),
                              ToString());
  }

  return Status::OK();
}

Status CFileSet::OpenBloomReader() {
  FsManager* fs = rowset_metadata_->fs_manager();
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs->OpenBlock(rowset_metadata_->bloom_block(), &block));

  ReaderOptions opts;
  opts.parent_mem_tracker = parent_mem_tracker_;
  Status s = BloomFileReader::OpenNoInit(std::move(block),
                                         std::move(opts),
                                         &bloom_reader_);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open bloom file in " << rowset_metadata_->ToString() << ": "
                 << s.ToString();
    // Continue without bloom.
  }

  return Status::OK();
}

Status CFileSet::LoadMinMaxKeys() {
  CFileReader* key_reader = key_index_reader();
  RETURN_NOT_OK(key_index_reader()->Init());
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMinKeyMetaEntryName, &min_encoded_key_)) {
    return Status::Corruption("No min key found", ToString());
  }
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMaxKeyMetaEntryName, &max_encoded_key_)) {
    return Status::Corruption("No max key found", ToString());
  }
  return Status::OK();
}

CFileReader* CFileSet::key_index_reader() const {
  if (ad_hoc_idx_reader_) {
    return ad_hoc_idx_reader_.get();
  }
  // If there is no special index cfile, then we have a non-compound key
  // and we can just use the key column.
  // This is always the first column listed in the tablet schema.
  int key_col_id = tablet_schema().column_id(0);
  return FindOrDie(readers_by_col_id_, key_col_id).get();
}

Status CFileSet::NewColumnIterator(ColumnId col_id, CFileReader::CacheControl cache_blocks,
                                   CFileIterator **iter) const {
  return FindOrDie(readers_by_col_id_, col_id)->NewIterator(iter, cache_blocks);
}

CFileSet::Iterator *CFileSet::NewIterator(const Schema *projection) const {
  return new CFileSet::Iterator(shared_from_this(), projection);
}

Status CFileSet::CountRows(rowid_t *count) const {
  RETURN_NOT_OK(key_index_reader()->Init());
  return key_index_reader()->CountRows(count);
}

Status CFileSet::GetBounds(string* min_encoded_key,
                           string* max_encoded_key) const {
  *min_encoded_key = min_encoded_key_;
  *max_encoded_key = max_encoded_key_;
  return Status::OK();
}

uint64_t CFileSet::AdhocIndexOnDiskSize() const {
  if (ad_hoc_idx_reader_) {
    return ad_hoc_idx_reader_->file_size();
  }
  return 0;
}

uint64_t CFileSet::BloomFileOnDiskSize() const {
  return bloom_reader_->FileSize();
}

uint64_t CFileSet::OnDiskDataSize() const {
  uint64_t ret = 0;
  for (const auto& e : readers_by_col_id_) {
    ret += e.second->file_size();
  }
  return ret;
}

Status CFileSet::FindRow(const RowSetKeyProbe &probe,
                         boost::optional<rowid_t>* idx,
                         ProbeStats* stats) const {
  if (FLAGS_consult_bloom_filters) {
    // Fully open the BloomFileReader if it was lazily opened earlier.
    //
    // If it's already initialized, this is a no-op.
    RETURN_NOT_OK(bloom_reader_->Init());

    stats->blooms_consulted++;
    bool present;
    Status s = bloom_reader_->CheckKeyPresent(probe.bloom_probe(), &present);
    if (s.ok() && !present) {
      *idx = boost::none;
      return Status::OK();
    }
    if (!s.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 1) << Substitute("Unable to query bloom in $0: $1",
                                                  rowset_metadata_->bloom_block().ToString(), s.ToString());
      if (PREDICT_FALSE(s.IsDiskFailure())) {
        // If the bloom lookup failed because of a disk failure, return early
        // since I/O to the tablet should be stopped.
        return s;
      }
      // Continue with the slow path
    }
  }

  stats->keys_consulted++;
  CFileIterator *key_iter = nullptr;
  RETURN_NOT_OK(NewKeyIterator(&key_iter));

  unique_ptr<CFileIterator> key_iter_scoped(key_iter); // free on return

  bool exact;
  Status s = key_iter->SeekAtOrAfter(probe.encoded_key(), &exact);
  if (s.IsNotFound() || (s.ok() && !exact)) {
    *idx = boost::none;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  *idx = key_iter->GetCurrentOrdinal();
  return Status::OK();
}

Status CFileSet::CheckRowPresent(const RowSetKeyProbe &probe, bool *present,
                                 rowid_t *rowid, ProbeStats* stats) const {
  boost::optional<rowid_t> opt_rowid;
  RETURN_NOT_OK(FindRow(probe, &opt_rowid, stats));
  *present = opt_rowid != boost::none;
  if (*present) {
    *rowid = *opt_rowid;
  }
  return Status::OK();
}

Status CFileSet::NewKeyIterator(CFileIterator **key_iter) const {
  RETURN_NOT_OK(key_index_reader()->Init());
  return key_index_reader()->NewIterator(key_iter, CFileReader::CACHE_BLOCK);
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileSet::Iterator::~Iterator() {
}

Status CFileSet::Iterator::CreateColumnIterators(const ScanSpec* spec) {
  DCHECK_EQ(0, col_iters_.size());
  vector<unique_ptr<ColumnIterator>> ret_iters;
  ret_iters.reserve(projection_->num_columns());

  CFileReader::CacheControl cache_blocks = CFileReader::CACHE_BLOCK;
  if (spec && !spec->cache_blocks()) {
    cache_blocks = CFileReader::DONT_CACHE_BLOCK;
  }

  for (int proj_col_idx = 0;
       proj_col_idx < projection_->num_columns();
       proj_col_idx++) {
    ColumnId col_id = projection_->column_id(proj_col_idx);

    if (!base_data_->has_data_for_column_id(col_id)) {
      // If we have no data for a column, most likely it was added via an ALTER
      // operation after this CFileSet was flushed. In that case, we're guaranteed
      // that it is either NULLable, or has a "read-default". Otherwise, consider it a corruption.
      const ColumnSchema& col_schema = projection_->column(proj_col_idx);
      if (PREDICT_FALSE(!col_schema.is_nullable() && !col_schema.has_read_default())) {
        return Status::Corruption(Substitute("column $0 has no data in rowset $1",
                                             col_schema.ToString(), base_data_->ToString()));
      }
      ret_iters.emplace_back(new DefaultColumnValueIterator(col_schema.type_info(),
                                                            col_schema.read_default_value()));
      continue;
    }
    CFileIterator *iter;
    RETURN_NOT_OK_PREPEND(base_data_->NewColumnIterator(col_id, cache_blocks, &iter),
                          Substitute("could not create iterator for column $0",
                                     projection_->column(proj_col_idx).ToString()));
    ret_iters.emplace_back(iter);
  }

  col_iters_.swap(ret_iters);
  return Status::OK();
}

Status CFileSet::Iterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  RETURN_NOT_OK(base_data_->CountRows(&row_count_));

  // Setup Key Iterator
  CFileIterator *tmp;
  RETURN_NOT_OK(base_data_->NewKeyIterator(&tmp));
  key_iter_.reset(tmp);

  // Setup column iterators.
  RETURN_NOT_OK(CreateColumnIterators(spec));

  // If there is a range predicate on the key column, push that down into an
  // ordinal range.
  RETURN_NOT_OK(PushdownRangeScanPredicate(spec));

  initted_ = true;

  // Don't actually seek -- we'll seek when we first actually read the
  // data.
  cur_idx_ = lower_bound_idx_;
  Unprepare(); // Reset state.
  return Status::OK();
}

void CFileSet::Iterator::TryEnableSkipScan(ScanSpec& spec) {
  int num_key_cols =  base_data_->tablet_schema().num_key_columns();

  if (!FLAGS_enable_skip_scan || num_key_cols <= 1) {
    use_skip_scan_ = false;
    return;
  }

  // Return if primary key push down has already occurred.
  if (!(lower_bound_idx_ == 0 && upper_bound_idx_ == row_count_)) {
    use_skip_scan_ = false;
    return;
  }

  bool nonfirst_key_pred_exists = false;

  // Initialize predicate column id to an upperbound value.
  skip_scan_predicate_column_id_ = num_key_cols;

  const std::unordered_map<std::string, ColumnPredicate> &predicates = spec.predicates();
  for (auto it=predicates.begin(); it!=predicates.end(); ++it) {
    // Get the column id from the predicate
    string col_name = it->second.column().name();
    StringPiece sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
    int col_id = base_data_->tablet_schema().find_column(sp);

    if (col_id == 0) { // Predicate exists on the first PK column.
      use_skip_scan_ = false;
      return;
    }

    if (base_data_->tablet_schema().is_key_column(col_id) &&
        ((it->second).predicate_type() == PredicateType::Equality)) {
      if (col_id < skip_scan_predicate_column_id_) { // Track the minimum column id.
        nonfirst_key_pred_exists = true;

        // Store the predicate column id.
        skip_scan_predicate_column_id_ = col_id;

        // Store the predicate value.
        skip_scan_predicate_value_ = (it->second).raw_lower();
      }
    }
  }

  use_skip_scan_ = nonfirst_key_pred_exists;
}

Status CFileSet::Iterator::PushdownRangeScanPredicate(ScanSpec *spec) {
  CHECK_GT(row_count_, 0);

  lower_bound_idx_ = 0;
  upper_bound_idx_ = row_count_;

  if (spec == nullptr) {
    // No predicate.
    return Status::OK();
  }

  Schema key_schema_for_vlog;
  if (VLOG_IS_ON(1)) {
    key_schema_for_vlog = base_data_->tablet_schema().CreateKeyProjection();
  }

  if (spec->lower_bound_key() &&
      spec->lower_bound_key()->encoded_key().compare(base_data_->min_encoded_key_) > 0) {
    bool exact;
    Status s = key_iter_->SeekAtOrAfter(*spec->lower_bound_key(), &exact);
    if (s.IsNotFound()) {
      // The lower bound is after the end of the key range.
      // Thus, no rows will pass the predicate, so we set the lower bound
      // to the end of the file.
      lower_bound_idx_ = row_count_;
      return Status::OK();
    }
    RETURN_NOT_OK(s);

    lower_bound_idx_ = std::max(lower_bound_idx_, key_iter_->GetCurrentOrdinal());
    VLOG(1) << "Pushed lower bound value "
            << spec->lower_bound_key()->Stringify(key_schema_for_vlog)
            << " as row_idx >= " << lower_bound_idx_;
  }
  if (spec->exclusive_upper_bound_key() &&
      spec->exclusive_upper_bound_key()->encoded_key().compare(
          base_data_->max_encoded_key_) <= 0) {
    bool exact;
    Status s = key_iter_->SeekAtOrAfter(*spec->exclusive_upper_bound_key(), &exact);
    if (PREDICT_FALSE(s.IsNotFound())) {
      LOG(DFATAL) << "CFileSet indicated upper bound was within range, but "
                  << "key iterator could not seek. "
                  << "CFileSet upper_bound = "
                  << KUDU_REDACT(Slice(base_data_->max_encoded_key_).ToDebugString())
                  << ", enc_key = "
                  << KUDU_REDACT(spec->exclusive_upper_bound_key()->encoded_key().ToDebugString());
    } else {
      RETURN_NOT_OK(s);

      rowid_t cur = key_iter_->GetCurrentOrdinal();
      upper_bound_idx_ = std::min(upper_bound_idx_, cur);

      VLOG(1) << "Pushed upper bound value "
              << spec->exclusive_upper_bound_key()->Stringify(key_schema_for_vlog)
              << " as row_idx < " << upper_bound_idx_;
    }
  }
  TryEnableSkipScan(*spec);
  return Status::OK();
}

void CFileSet::Iterator::Unprepare() {
  prepared_count_ = 0;
  cols_prepared_.assign(col_iters_.size(), false);
}

Status CFileSet::Iterator::DecodeCurrentKey(Arena* arena, gscoped_ptr<EncodedKey>* enc_key) {
  RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
      base_data_->tablet_schema(), arena,
      key_iter_->GetCurrentValue(), enc_key), "Invalid scan key");
  return Status::OK();
}

Status CFileSet::Iterator::SeekToNextPrefixKey(size_t num_prefix_cols, bool cache_seeked_value) {
  gscoped_ptr<EncodedKey> enc_key;
  Arena arena(1024);
  RETURN_NOT_OK(DecodeCurrentKey(&arena, &enc_key));

  // Increment the "prefix_key" or the first "num_prefix_cols" columns of the
  // current key.
  CHECK_OK(EncodedKey::IncrementEncodedKey(
      base_data_->tablet_schema(),
      &enc_key, &arena, num_prefix_cols));

  return key_iter_->SeekAtOrAfter(*enc_key,
                                  /* exact_match= */ nullptr,
                                  /* set_current_value= */ cache_seeked_value);
}

Status CFileSet::Iterator::SeekToNextPredicateMatchCurPrefix(size_t num_prefix_cols) {
  gscoped_ptr<EncodedKey> enc_key;
  Arena arena(1024);
  RETURN_NOT_OK(DecodeCurrentKey(&arena, &enc_key));

  // Check to see if the current key matches the predicate value. If so, then
  // there is no need to seek forward.
  if (CheckPredicateMatch(enc_key)) {
    return Status::OK();
  }

  // If we got this far, the current key doesn't match the predicate, so search
  // for the next key that matches the current prefix and predicate.
  KuduPartialRow partial_row(&(base_data_->tablet_schema()));
  gscoped_ptr<EncodedKey> key_with_pred_value =
      GetKeyWithPredicateVal(&partial_row, enc_key);
  return key_iter_->SeekAtOrAfter(*key_with_pred_value,
                                  /* exact_match= */ nullptr,
                                  /* set_current_value= */ true);
}

gscoped_ptr<EncodedKey> CFileSet::Iterator::GetKeyWithPredicateVal(KuduPartialRow *p_row,
                                                                   const gscoped_ptr<EncodedKey>& cur_enc_key) {
  int col_id = 0;

  // Build a new partial row with the current "prefix_key" value and the
  // predicate value.
  for (auto const &value: cur_enc_key->raw_keys()) {
    if (col_id < skip_scan_predicate_column_id_) {
      const uint8_t *data = reinterpret_cast<const uint8_t *>(value);
      p_row->Set(col_id, data);
    } else {
      // Set the predicate value.
      const uint8_t *suffix_col_value =
          reinterpret_cast<const uint8_t *>(skip_scan_predicate_value_);
      p_row->Set(skip_scan_predicate_column_id_, suffix_col_value);
      break;
    }
    col_id++;
  }

  // Fill the values after the predicate column id with their
  // minimum possible values.
  ContiguousRow cont_row(&(base_data_->tablet_schema()), p_row->row_data_);
  for (size_t i = skip_scan_predicate_column_id_+1;
       i < base_data_->tablet_schema().num_key_columns(); i++) {
    const ColumnSchema &col = cont_row.schema()->column(i);
    col.type_info()->CopyMinValue(cont_row.mutable_cell_ptr(i));
  }

  // Return the new encoded key.
  ConstContiguousRow const_row(cont_row);
  gscoped_ptr<EncodedKey> new_enc_key(EncodedKey::FromContiguousRow(const_row));

  return new_enc_key.Pass();
}

bool CFileSet::Iterator::CheckPredicateMatch(const gscoped_ptr<EncodedKey>& enc_key) const {
  return base_data_->tablet_schema().column(skip_scan_predicate_column_id_).Compare(
      skip_scan_predicate_value_,
      enc_key->raw_keys()[skip_scan_predicate_column_id_]) == 0;
}

bool CFileSet::Iterator::KeyColumnsMatch(const gscoped_ptr<EncodedKey>& key1,
                                         const gscoped_ptr<EncodedKey>& key2,
                                         int start_col_id, int end_col_id) const {
  const auto& schema = base_data_->tablet_schema();
  for (int col_id = start_col_id; col_id <= end_col_id; col_id++) {
    if (schema.column(col_id).Compare(key1->raw_keys()[col_id], key2->raw_keys()[col_id]) != 0) {
      return false;
    }
  }
  return true;
}

void CFileSet::Iterator::SkipToNextScan(size_t *remaining) {
  // Keep scanning if we're still in the range that needs scanning from our
  // previous seek.
  if (cur_idx_ < skip_scan_upper_bound_idx_) {
    *remaining = std::max<int64_t>(skip_scan_upper_bound_idx_ - cur_idx_, 1);
    return;
  }

  // This is a three seek approach for index skip scan implementation:
  // 1. Place the validx_iter_ at the entry containing the next
  //    unique "prefix_key" value.
  // 2. Place the iterator at or after the entry formed by the "prefix_key"
  //    found in 1. and the predicate value.
  // 3. If the seek in 2. results in exact match with the predicate value,
  //    store the row id that represents the last relevant entry (upper bound) wrt the
  //    current "prefix_key"(found in 1.)

  skip_scan_upper_bound_idx_ = upper_bound_idx_;
  size_t skip_scan_lower_bound_idx = cur_idx_;

  // Whether we found our lower bound key.
  bool lower_bound_key_found = false;

  // Whether the lower bound prefix key rolled over between the first and
  // second seek to determine the lower bound key.
  bool prefix_key_rolled = false;

  // Continue seeking the next matching row if we didn't find
  // a predicate match.
  for (int loop_num = 0;
       !lower_bound_key_found && loop_num < FLAGS_skip_scan_short_circuit_loops;
       loop_num++) {
    DCHECK_LT(cur_idx_, skip_scan_upper_bound_idx_);

    //////////////////////////////////////////////////////////
    // First, find the first row that matches the predicate.
    //////////////////////////////////////////////////////////
    Status s;
    // We only want to seek to the first entry if this is the first time we
    // are entering this loop on the first call to this method.
    if (cur_idx_ == 0 && loop_num == 0) {
      // Get the first entry of the validx_iter_.
      s = key_iter_->SeekToFirst();

    // Only seek to the next prefix if our previous call to
    // SeekToNextPredicateMatchCurPrefix() didn't "roll" past the previous prefix.
    } else if (!prefix_key_rolled) {
      // Search for the next unique prefix key, as the predicate-matching
      // entries for the current prefix have already been scanned.
      s = SeekToNextPrefixKey(skip_scan_predicate_column_id_, /* cache_seeked_value=*/ true);
    }
    prefix_key_rolled = false;

    // We fell off the end of the cfile. No more rows will match.
    if (s.IsNotFound()) {
      lower_bound_key_found = false;
      break;
    }
    CHECK_OK(s);

    Arena arena(1024); // TODO(mpercy): Is this a big enough arena for a large key???
    gscoped_ptr<EncodedKey> next_prefix_key;

    CHECK_OK(DecodeCurrentKey(&arena, &next_prefix_key));

    // Attempt to seek to the next predicate match.
    s = SeekToNextPredicateMatchCurPrefix(skip_scan_predicate_column_id_);
    if (s.IsNotFound()) {
      lower_bound_key_found = false;
      break;
    }
    CHECK_OK(s);

    Arena arena2(1024); // TODO(mpercy): Is this a big enough arena for a large key???
    gscoped_ptr<EncodedKey> lower_bound_key;

    // Check if we successfully seeked to a predicate key match.
    CHECK_OK(DecodeCurrentKey(&arena, &lower_bound_key));

    // Keep track of the lower bound on a matching key.
    skip_scan_lower_bound_idx = key_iter_->GetCurrentOrdinal();

    // Does this lower bound key match?
    lower_bound_key_found = CheckPredicateMatch(lower_bound_key);

    // We weren't able to find a predicate match for our lower bound key, so loop and search again.
    if (!lower_bound_key_found)  {
      // If the prefix key rolled between our initial lower bound next prefix
      // seek and our seek to the predicate match with that prefix, it's
      // possible that the latest prefix will have a predicate match, so on our
      // next iteration of the loop, we should not seek to the next prefix.
      if (!KeyColumnsMatch(next_prefix_key, lower_bound_key,
                           /* start_col_id= */ 0,
                           /* end_col_id= */ skip_scan_predicate_column_id_ - 1)) {
        prefix_key_rolled = true;
      }
      continue;
    }

    /////////////////////////////////////////////////////////////
    // Next, find the following row that matches the predicate.
    /////////////////////////////////////////////////////////////

    int num_prefix_cols_including_predicate = skip_scan_predicate_column_id_ + 1;
    // Do not cache the seeked value for the upper bound because the next time
    // we seek for the lower bound we want to be able to get a match on the
    // previous upper bound -- it's possible that it has a predicate match.
    s = SeekToNextPrefixKey(num_prefix_cols_including_predicate, /* cache_seeked_value=*/ false);
    if (s.IsNotFound()) {
      // We hit the end of the file. Simply scan to the end.
      skip_scan_upper_bound_idx_ = upper_bound_idx_;
      break;
    }
    CHECK_OK(s);

    skip_scan_upper_bound_idx_ = key_iter_->GetCurrentOrdinal();

    // Check to see whether we have effectively seeked backwards. If so, we
    // need to keep looking until our upper bound is past the last row that we
    // previously scanned.
    if (skip_scan_upper_bound_idx_ <= cur_idx_) {
      skip_scan_upper_bound_idx_ = upper_bound_idx_; // Reset upper bound to max.
      lower_bound_key_found = false;
    }
  }

  // Now update cur_idx_, which controls the next row we will scan.
  // Seek to the next lower bound match.
  // Never seek backward.
  cur_idx_ = std::max<int64_t>(cur_idx_, skip_scan_lower_bound_idx);
  if (!lower_bound_key_found) {
    // TODO: We scan a single row (guaranteed not to match) for now, because
    // having entered PrepareBatch() implies that we have rows to scan. Can
    // we prepare 0 rows instead of doing this?
    *remaining = 1;
    // Reset our upper bound since we use it to short-circuit.
    skip_scan_upper_bound_idx_ = -1;
  } else {
    // Always read at least one row.
    *remaining = std::max<int64_t>(skip_scan_upper_bound_idx_ - cur_idx_, 1);
  }
}

Status CFileSet::Iterator::PrepareBatch(size_t *nrows) {
  DCHECK_EQ(prepared_count_, 0) << "Already prepared";

  size_t remaining = upper_bound_idx_ - cur_idx_;

  if (use_skip_scan_) {
    SkipToNextScan(&remaining);
  }

  if (*nrows > remaining) {
    *nrows = remaining;
  }

  prepared_count_ = *nrows;

  // Lazily prepare the first column when it is materialized.
  return Status::OK();
}

Status CFileSet::Iterator::PrepareColumn(ColumnMaterializationContext *ctx) {
  if (cols_prepared_[ctx->col_idx()]) {
    // Already prepared in this batch.
    return Status::OK();
  }

  ColumnIterator* col_iter = col_iters_[ctx->col_idx()].get();
  size_t n = prepared_count_;

  if (!col_iter->seeked() || col_iter->GetCurrentOrdinal() != cur_idx_) {
    // Either this column has not yet been accessed, or it was accessed
    // but then skipped in a prior block (e.g because predicates on other
    // columns completely eliminated the block).
    //
    // Either way, we need to seek it to the correct offset.
    RETURN_NOT_OK(col_iter->SeekToOrdinal(cur_idx_));
  }

  Status s = col_iter->PrepareBatch(&n);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to prepare column " << ctx->col_idx() << ": " << s.ToString();
    return s;
  }

  if (n != prepared_count_) {
    return Status::Corruption(
        StringPrintf("Column %zd (%s) didn't yield enough rows at offset %zd: expected "
                     "%zd but only got %zd", ctx->col_idx(),
                     projection_->column(ctx->col_idx()).ToString().c_str(),
                     cur_idx_, prepared_count_, n));
  }

  cols_prepared_[ctx->col_idx()] = true;

  return Status::OK();
}

Status CFileSet::Iterator::InitializeSelectionVector(SelectionVector *sel_vec) {
  sel_vec->SetAllTrue();
  return Status::OK();
}

Status CFileSet::Iterator::MaterializeColumn(ColumnMaterializationContext *ctx) {
  CHECK_EQ(prepared_count_, ctx->block()->nrows());
  DCHECK_LT(ctx->col_idx(), col_iters_.size());

  RETURN_NOT_OK(PrepareColumn(ctx));
  ColumnIterator* iter = col_iters_[ctx->col_idx()].get();

  RETURN_NOT_OK(iter->Scan(ctx));

  return Status::OK();
}

Status CFileSet::Iterator::FinishBatch() {
  CHECK_GT(prepared_count_, 0);

  for (size_t i = 0; i < col_iters_.size(); i++) {
    if (cols_prepared_[i]) {
      Status s = col_iters_[i]->FinishBatch();
      if (!s.ok()) {
        LOG(WARNING) << "Unable to FinishBatch() on column " << i;
        return s;
      }
    }
  }

  cur_idx_ += prepared_count_;
  Unprepare();

  return Status::OK();
}


void CFileSet::Iterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  stats->clear();
  stats->reserve(col_iters_.size());
  for (const auto& iter : col_iters_) {
    ANNOTATE_IGNORE_READS_BEGIN();
    stats->push_back(iter->io_statistics());
    ANNOTATE_IGNORE_READS_END();
  }
}

} // namespace tablet
} // namespace kudu
