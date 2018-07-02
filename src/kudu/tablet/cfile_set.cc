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

bool CFileSet::Iterator::CanUseSkipScan(ScanSpec *spec) {
  int num_key_cols =  base_data_->tablet_schema().num_key_columns();

  if (!FLAGS_enable_skip_scan || num_key_cols <= 1) {
    return false;
  }

  // Return if primary key push down has already occurred
  if (!(lower_bound_idx_ == 0 && upper_bound_idx_ == row_count_)) {
    return false;
  }

  bool nonfirst_key_pred_exists = false;
  suffix_key_column_id_ = num_key_cols; // Initialize "suffix_key" column id to an upperbound value

  std::unordered_map<std::string, ColumnPredicate> predicates = spec->predicates();
  for (auto it=predicates.begin(); it!=predicates.end(); ++it) {

    // Get the column id from the predicate
    string col_name = (it->second).column().name();
    StringPiece sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
    int col_id = base_data_->tablet_schema().find_column(sp);

    if (col_id == 0) { // Predicate exists on the first PK column
      return false;
    }

    if (base_data_->tablet_schema().is_key_column(col_id) &&
        ((it->second).predicate_type() == PredicateType::Equality)) {
        if (col_id < suffix_key_column_id_) { // Track the minimum column id
          nonfirst_key_pred_exists = true;

          // Store the "suffix_key" column id
          suffix_key_column_id_ = col_id;

          // Store the "suffix_key" equality predicate value
          suffix_key_pred_value_ = (it->second).raw_lower();
        }
    }
  }

  return nonfirst_key_pred_exists;
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
  skip_scan_enabled_ = CanUseSkipScan(spec);
  return Status::OK();
}

void CFileSet::Iterator::Unprepare() {
  prepared_count_ = 0;
  cols_prepared_.assign(col_iters_.size(), false);
}

bool CFileSet::Iterator::CheckKeyMatch(const std::vector<const void *> &raw_keys, size_t col_id) {
  // Encode the key currently pointed to by validx_iter_
  Arena arena2(1024);
  gscoped_ptr<EncodedKey> cur_enc_key;
  EncodedKey::DecodeEncodedString(
      base_data_->tablet_schema(), &arena2,
      Slice(key_iter_->GetCurrentValue()), &cur_enc_key);

  string column_value_1 =
      base_data_->tablet_schema().column(col_id).Stringify(raw_keys[col_id]);
  string column_value_2 =
      base_data_->tablet_schema().column(col_id).Stringify(cur_enc_key->raw_keys()[col_id]);
  return column_value_1 == column_value_2;
}

Status CFileSet::Iterator::PrepareBatch(size_t *n) {
  DCHECK_EQ(prepared_count_, 0) << "Already prepared";

  size_t remaining = upper_bound_idx_ - cur_idx_;

  // This is a three seek approach for index skip scan implementation:
  // 1. Place the validx_iter_ at the entry containing the next
  //    unique "prefix_key" value.
  // 2. Place the iterator at or after the entry formed by the "prefix_key"
  //    found in 1. and the "suffix_key" predicate value.
  // 3. If the seek in 2. results in exact match with the "suffix_key" predicate value,
  //    store the row id that represents the last relevant entry (upper bound) wrt the
  //    current "prefix_key"(found in 1.)

  if (skip_scan_enabled_ && (static_cast<int>(cur_idx_) > suffix_key_upper_bound_idx_)) {
    size_t num_key_cols = base_data_->tablet_schema().num_key_columns();
    bool suffix_key_match, continue_seeking_next_prefix = false;
    int suffix_key_lower_bound_idx = 0;
    Status next_entry_match, suffix_key_upper_bound_match;

    do {
      if (cur_idx_ == 0 && !continue_seeking_next_prefix) {
        // Get the first entry of the validx_iter_
        next_entry_match = key_iter_->SeekToFirst();
      } else {
        // Search for the next unique prefix key, as the entry wrt to the current
        // "prefix_key" has already been scanned.
        next_entry_match = SeekToNextPrefixKey(suffix_key_column_id_);
      }

      if (next_entry_match.IsNotFound()) {
        VLOG(1) << "next prefix not found";
        break;
      }

      // Build a new partial row with the current "prefix_key" value and the
      // "suffix_key" predicate value
      KuduPartialRow p_row(&(base_data_->tablet_schema()));
      BuildNewPartialRow(&p_row);

      // Fill the values for the columns succeeding the "suffix_key" column with their
      // minimum possible values.
      ContiguousRow cont_row(&(base_data_->tablet_schema()), p_row.row_data_);
      for (size_t i = suffix_key_column_id_+1; i < num_key_cols; i++) {
        const ColumnSchema &col = cont_row.schema()->column(i);
        col.type_info()->CopyMinValue(cont_row.mutable_cell_ptr(i));
      }

      // Get the new encoded key
      ConstContiguousRow const_row(cont_row);
      gscoped_ptr<EncodedKey> new_enc_key(EncodedKey::FromContiguousRow(const_row));

      // Seek at or after the new encoded key
      bool exact_match;
      next_entry_match = key_iter_->SeekAtOrAfter(*new_enc_key, &exact_match);

      if (next_entry_match.IsNotFound()) {
        VLOG(1) << "entry for the next prefix not found";
        break;
      }

      // Check if "suffix_key" column value is the same for the new encoded key
      // and currently seeked key.
      suffix_key_match = CheckKeyMatch(new_enc_key->raw_keys(), suffix_key_column_id_);

      // Store the row id of the first entry that matches the "suffix_key" predicate
      // against the "prefix_key"
      suffix_key_lower_bound_idx = key_iter_->GetCurrentOrdinal();

      if (suffix_key_match) {
        suffix_key_upper_bound_match = SeekToNextPrefixKey(suffix_key_column_id_+1);

        if ( suffix_key_upper_bound_match.IsNotFound() ) {
          break;
        }
        suffix_key_upper_bound_idx_ = key_iter_->GetCurrentOrdinal();
      }

    continue_seeking_next_prefix  = true;
    } while (!suffix_key_match || (suffix_key_upper_bound_idx_ < cur_idx_));
    // Repeat seeking for the next unique prefix if:
    // 1. The "suffix_key" predicate value was not found for the "prefix_key" or
    // 2. If all "suffix_key" predicate value entries for the "prefix_key" were
    //    already scanned in the previous batch

    if (next_entry_match.IsNotFound()) {
      // No more entries exists to scan
      cur_idx_ = upper_bound_idx_ - 1;
      remaining = 1;
    } else {
      // Update cur_idx_ to a higher value
      if (suffix_key_lower_bound_idx > cur_idx_) {
        cur_idx_ = suffix_key_lower_bound_idx;
      }
      if (suffix_key_upper_bound_match.IsNotFound()) {
        remaining = upper_bound_idx_ - cur_idx_;
      } else {
        remaining =
            (suffix_key_upper_bound_idx_ > cur_idx_) ? suffix_key_upper_bound_idx_ - cur_idx_ : 1;
      }
    }
  }


  if (*n > remaining) {
    *n = remaining;
  }

  prepared_count_ = *n;

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

Status CFileSet::Iterator::SeekToNextPrefixKey(size_t num_cols) { // Increment id no of columns
  gscoped_ptr<EncodedKey> enc_key;
  Arena arena(1024);

  // Convert current key slice to encoded key
  RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
      base_data_->tablet_schema(), &arena,
      Slice(key_iter_->GetCurrentValue()), &enc_key), "Invalid scan prefix key");

  // Increment the "prefix_key"
  CHECK_OK(EncodedKey::IncrementEncodedKey(
      base_data_->tablet_schema(),
      &enc_key, &arena, num_cols));

  bool exact_match;
  // Seek at or after the incremented "prefix_key" to get to the next unique "prefix_key" value.
  Status s = key_iter_->SeekAtOrAfter(*enc_key, &exact_match);
  return s;
}

void CFileSet::Iterator::BuildNewPartialRow(KuduPartialRow *p_row) {
  gscoped_ptr<EncodedKey> enc_key;
  Arena arena(1024);
  EncodedKey::DecodeEncodedString(
      base_data_->tablet_schema(), &arena,
      Slice(key_iter_->GetCurrentValue()), &enc_key);

  const std::vector<const void *> raw_keys = enc_key->raw_keys();
  int col_id = 0;

  for (auto const &value: raw_keys) {
    if (col_id < suffix_key_column_id_) {
      const uint8_t *data = reinterpret_cast<const uint8_t *>(value);
      p_row->Set(col_id, data);
    } else {
      // Set the predicate value for the "suffix_key" column
      const uint8_t *suffix_col_value = reinterpret_cast<const uint8_t *>(suffix_key_pred_value_);
      p_row->Set(suffix_key_column_id_, suffix_col_value);
      break;
    }
    col_id++;
  }
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
