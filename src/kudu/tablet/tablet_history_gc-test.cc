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

#include <gflags/gflags.h>

#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet-test-base.h"

DECLARE_int32(tablet_history_max_age_sec);

using kudu::server::HybridClock;

namespace kudu {
namespace tablet {

class TabletHistoryGCTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 public:
  typedef TabletTestBase<IntKeyTestSetup<INT64>> Superclass;

  TabletHistoryGCTest()
      : Superclass(TabletHarness::Options::HYBRID_CLOCK) {
  }
};

TEST_F(TabletHistoryGCTest, TestHistoryGCOnMajorCompaction) {
  // Maximum number of seconds to retain tablet history.
  FLAGS_tablet_history_max_age_sec = 1;

  uint64_t max_rows = this->ClampRowCount(1000);
  uint64_t n_rows = max_rows / 3;
  max_rows = n_rows * 3;
  for (int rowset_id = 0; rowset_id < 3; rowset_id++) {
    this->InsertTestRows(rowset_id * n_rows, n_rows, 0);
    ASSERT_OK(this->tablet()->Flush());
  }

  // Add 2 seconds to the HybridClock.
  this->clock()->Update(HybridClock::AddPhysicalTimeToTimestamp(
          this->clock()->NowLatest(), MonoDelta::FromSeconds(2)));

  // Mutate all of the rows.
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  for (int row_idx = 0; row_idx < max_rows; row_idx++) {
    SCOPED_TRACE(Substitute("Row index: $0", row_idx));
    ASSERT_OK(this->UpdateTestRow(&writer, row_idx, row_idx));
  }

  // Compact all of the rowsets. This will cause GC of tablet history.
  for (int rowset_id = 0; rowset_id < 3; rowset_id++) {
    ASSERT_OK(this->tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }

  // TODO: Now try to read from the past. This should fail.

  // Delete all of the rows in the tablet.
  for (int row_idx = 0; row_idx < max_rows; row_idx++) {
    ASSERT_OK(this->DeleteTestRow(&writer, row_idx));
  }

  // Add 2 seconds to the HybridClock.
  this->clock()->Update(HybridClock::AddPhysicalTimeToTimestamp(
          this->clock()->NowLatest(), MonoDelta::FromSeconds(2)));

  // Compact all of the rowsets. This will cause GC of tablet history.
  for (int rowset_id = 0; rowset_id < 3; rowset_id++) {
    ASSERT_OK(this->tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }
  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // TODO: Now try to read from the past. This should fail.

  // Now check the on-disk tablet size. The tablet size should be 0.
  size_t size = this->tablet()->EstimateOnDiskSize();
  ASSERT_EQ(0, size);
}

} // namespace tablet
} // namespace kudu

