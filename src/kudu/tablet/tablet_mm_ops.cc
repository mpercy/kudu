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

#include "kudu/tablet/tablet_mm_ops.h"

#include <mutex>

#include "kudu/util/locks.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metrics.h"

DEFINE_int32(mm_init_undo_deltas_millis_per_cycle, 100,
             "The maximum number of milliseconds we will spend opened undo "
             "delta files per maintenance manager cycle");

using std::string;
using strings::Substitute;

namespace kudu {
namespace tablet {

TabletOpBase::TabletOpBase(string name, IOUsage io_usage, Tablet* tablet)
    : MaintenanceOp(std::move(name), io_usage),
      tablet_(tablet) {
}

string TabletOpBase::LogPrefix() const {
  return tablet_->LogPrefix();
}

////////////////////////////////////////////////////////////
// CompactRowSetsOp
////////////////////////////////////////////////////////////

CompactRowSetsOp::CompactRowSetsOp(Tablet* tablet)
  : TabletOpBase(Substitute("CompactRowSetsOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_rs_compacted_(0) {
}

void CompactRowSetsOp::UpdateStats(MaintenanceOpStats* stats) {
  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the on-disk row layout invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    uint64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    uint64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
    }
  }

  tablet_->UpdateCompactionStats(&prev_stats_);
  *stats = prev_stats_;
}

bool CompactRowSetsOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another section of the tablet can
  // be compacted concurrently.
  //
  // TODO: we should acquire the rowset compaction locks here. Otherwise, until
  // Compact() acquires them, the maintenance manager may compute the same
  // stats for this op and run it again, even though Perform() will end up
  // performing a much less fruitful compaction. See KUDU-790 for more details.
  prev_stats_.Clear();
  return true;
}

void CompactRowSetsOp::Perform() {
  WARN_NOT_OK(tablet_->Compact(Tablet::COMPACT_NO_FLAGS),
              Substitute("$0Compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> CompactRowSetsOp::DurationHistogram() const {
  return tablet_->metrics()->compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > CompactRowSetsOp::RunningGauge() const {
  return tablet_->metrics()->compact_rs_running;
}

////////////////////////////////////////////////////////////
// MinorDeltaCompactionOp
////////////////////////////////////////////////////////////

MinorDeltaCompactionOp::MinorDeltaCompactionOp(Tablet* tablet)
  : TabletOpBase(Substitute("MinorDeltaCompactionOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_dms_flushed_(0),
    last_num_rs_compacted_(0),
    last_num_rs_minor_delta_compacted_(0) {
}

void MinorDeltaCompactionOp::UpdateStats(MaintenanceOpStats* stats) {
  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the number of REDO files invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    uint64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    uint64_t new_num_dms_flushed = metrics->flush_dms_duration->TotalCount();
    uint64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    uint64_t new_num_rs_minor_delta_compacted =
        metrics->delta_minor_compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_dms_flushed == last_num_dms_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_ &&
        new_num_rs_minor_delta_compacted == last_num_rs_minor_delta_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_dms_flushed_ = new_num_dms_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
      last_num_rs_minor_delta_compacted_ = new_num_rs_minor_delta_compacted;
    }
  }

  double perf_improv = tablet_->GetPerfImprovementForBestDeltaCompact(
      RowSet::MINOR_DELTA_COMPACTION, nullptr);
  prev_stats_.set_perf_improvement(perf_improv);
  prev_stats_.set_runnable(perf_improv > 0);
  *stats = prev_stats_;
}

bool MinorDeltaCompactionOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another rowset in the tablet can
  // be delta compacted concurrently.
  //
  // TODO: See CompactRowSetsOp::Prepare().
  prev_stats_.Clear();
  return true;
}

void MinorDeltaCompactionOp::Perform() {
  WARN_NOT_OK(tablet_->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION),
              Substitute("$0Minor delta compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> MinorDeltaCompactionOp::DurationHistogram() const {
  return tablet_->metrics()->delta_minor_compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > MinorDeltaCompactionOp::RunningGauge() const {
  return tablet_->metrics()->delta_minor_compact_rs_running;
}

////////////////////////////////////////////////////////////
// MajorDeltaCompactionOp
////////////////////////////////////////////////////////////

MajorDeltaCompactionOp::MajorDeltaCompactionOp(Tablet* tablet)
  : TabletOpBase(Substitute("MajorDeltaCompactionOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_dms_flushed_(0),
    last_num_rs_compacted_(0),
    last_num_rs_minor_delta_compacted_(0),
    last_num_rs_major_delta_compacted_(0) {
}

void MajorDeltaCompactionOp::UpdateStats(MaintenanceOpStats* stats) {
  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the size of the on-disk data invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    int64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    int64_t new_num_dms_flushed = metrics->flush_dms_duration->TotalCount();
    int64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    int64_t new_num_rs_minor_delta_compacted =
        metrics->delta_minor_compact_rs_duration->TotalCount();
    int64_t new_num_rs_major_delta_compacted =
        metrics->delta_major_compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_dms_flushed == last_num_dms_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_ &&
        new_num_rs_minor_delta_compacted == last_num_rs_minor_delta_compacted_ &&
        new_num_rs_major_delta_compacted == last_num_rs_major_delta_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_dms_flushed_ = new_num_dms_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
      last_num_rs_minor_delta_compacted_ = new_num_rs_minor_delta_compacted;
      last_num_rs_major_delta_compacted_ = new_num_rs_major_delta_compacted;
    }
  }

  double perf_improv = tablet_->GetPerfImprovementForBestDeltaCompact(
      RowSet::MAJOR_DELTA_COMPACTION, nullptr);
  prev_stats_.set_perf_improvement(perf_improv);
  prev_stats_.set_runnable(perf_improv > 0);
  *stats = prev_stats_;
}

bool MajorDeltaCompactionOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another rowset in the tablet can
  // be delta compacted concurrently.
  //
  // TODO: See CompactRowSetsOp::Prepare().
  prev_stats_.Clear();
  return true;
}

void MajorDeltaCompactionOp::Perform() {
  WARN_NOT_OK(tablet_->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION),
              Substitute("$0Major delta compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> MajorDeltaCompactionOp::DurationHistogram() const {
  return tablet_->metrics()->delta_major_compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > MajorDeltaCompactionOp::RunningGauge() const {
  return tablet_->metrics()->delta_major_compact_rs_running;
}

////////////////////////////////////////////////////////////
// UndoDeltaFileGCOp
////////////////////////////////////////////////////////////
UndoDeltaFileGCOp::UndoDeltaFileGCOp(Tablet* tablet)
  : TabletOpBase(Substitute("UndoDeltaFileGCOp($0)", tablet->tablet_id()),
                 MaintenanceOp::LOW_IO_USAGE, tablet) {
}

void UndoDeltaFileGCOp::UpdateStats(MaintenanceOpStats* stats) {
  MonoTime start = MonoTime::Now();
  int64_t bytes_in_ancient_undos = 0;
  WARN_NOT_OK(tablet_->InitAncientUndoDeltas(
      MonoDelta::FromMilliseconds(FLAGS_mm_init_undo_deltas_millis_per_cycle),
      &bytes_in_ancient_undos),
      Substitute("$0Unable to initialize old undo delta files on tablet $1",
                 LogPrefix(), tablet_->tablet_id()));
  stats->set_data_retained_bytes(bytes_in_ancient_undos);
  MonoDelta duration = MonoTime::Now() - start;
  tablet_->metrics()->undo_deltafile_gc_init_duration->Increment(duration.ToMilliseconds());
}

bool UndoDeltaFileGCOp::Prepare() {
  // Nothing for us to do.
  return true;
}

void UndoDeltaFileGCOp::Perform() {
  MonoTime start = MonoTime::Now();
  tablet_->metrics()->undo_deltafile_gc_delete_running->Increment();
  CHECK_OK_PREPEND(tablet_->DeleteAncientUndoDeltas(),
              Substitute("$0GC of undo delta files failed on tablet $1",
                         LogPrefix(), tablet_->tablet_id()));
  MonoDelta duration = MonoTime::Now() - start;
  tablet_->metrics()->undo_deltafile_gc_delete_duration->Increment(duration.ToMilliseconds());
  tablet_->metrics()->undo_deltafile_gc_delete_running->Decrement();
}

scoped_refptr<Histogram> UndoDeltaFileGCOp::DurationHistogram() const {
  return tablet_->metrics()->undo_deltafile_gc_delete_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > UndoDeltaFileGCOp::RunningGauge() const {
  return tablet_->metrics()->undo_deltafile_gc_delete_running;
}

} // namespace tablet
} // namespace kudu
