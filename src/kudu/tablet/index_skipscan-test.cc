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


#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::vector;


namespace kudu {

namespace tablet {

// Schemas vary in the number of composite primary keys
enum SchemaType {
  kOnePK, kTwoPK, kThreePK, kFourPK, kFivePK, kTwoPKrandom,  kThreePKrandom, kRandom
};


class IndexSkipScanTest : public KuduTabletTest,
                          public ::testing::WithParamInterface<SchemaType> {

public:

  IndexSkipScanTest()
      : KuduTabletTest(CreateSchema(static_cast<int>(GetParam()))) {

  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    FillTestTablet(static_cast<int>(GetParam()));
  }

  Schema CreateSchema(int schema_num) {
    SchemaBuilder builder;
    switch (schema_num) {

      case kOnePK: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        break;
      }

      case kTwoPK: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT16));
        break;
      }

      case kThreePK: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT16));
        CHECK_OK(builder.AddKeyColumn("P3", STRING));
        break;
      }

      case kFourPK: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT16));
        CHECK_OK(builder.AddKeyColumn("P3", STRING));
        CHECK_OK(builder.AddKeyColumn("P4", INT8));
        break;
      }

      case kFivePK: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT16));
        CHECK_OK(builder.AddKeyColumn("P3", STRING));
        CHECK_OK(builder.AddKeyColumn("P4", INT8));
        CHECK_OK(builder.AddKeyColumn("P5", INT8));
        break;
      }

      case kTwoPKrandom: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT16));
        break;
      }

      case kThreePKrandom: {
        CHECK_OK(builder.AddKeyColumn("P1", INT32));
        CHECK_OK(builder.AddKeyColumn("P2", INT8));
        CHECK_OK(builder.AddKeyColumn("P3", INT16));
        break;
      }

      default: {
        CHECK_OK(builder.AddKeyColumn("P1", INT16));
        CHECK_OK(builder.AddKeyColumn("P2", INT32));
        CHECK_OK(builder.AddColumn("K1", INT16));
        break;
      }
    }

    return builder.BuildWithoutIds();

  }


  void FillTestTablet(int schema_num) {

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);

    int32_t kNumDistinctP1 = 2;
    int16_t kNumDistinctP2 = 10;
    int kNumDistinctP3 = 5;
    int8_t kNumDistinctP4 = 1;
    int8_t kNumDistinctP5 = 20;

    switch (schema_num) {
      case kOnePK: {
        for (int32_t p1 = 1; p1 <= kNumDistinctP1; p1++) {
          CHECK_OK(row.SetInt32(0, p1));
          ASSERT_OK_FAST(writer.Insert(row));
        }

        break;
      }

      case kTwoPK: {
        for (int32_t p1 = 1; p1 <= kNumDistinctP1; p1++) {
          for (int32_t p2 = 1; p2 <= kNumDistinctP2; p2++) {
            CHECK_OK(row.SetInt32(0, p1));
            CHECK_OK(row.SetInt16(1, p2));
            ASSERT_OK_FAST(writer.Insert(row));
          }
        }
        break;
      }

      case kThreePK: {
        for (int32_t p1 = 1; p1 <= kNumDistinctP1; p1++) {
          for (int32_t p2 = 1; p2 <= kNumDistinctP2; p2++) {
            for (int p3 = 1; p3 <= kNumDistinctP3; p3++) {
              CHECK_OK(row.SetInt32(0, p1));
              CHECK_OK(row.SetInt16(1, p2));
              CHECK_OK(row.SetStringCopy(2, StringPrintf("%d_p3", p3)));
              ASSERT_OK_FAST(writer.Insert(row));
            }
          }
        }
        break;
      }

      case kFourPK: {
        for (int32_t p1 = 1; p1 <= kNumDistinctP1; p1++) {
          for (int32_t p2 = 1; p2 <= kNumDistinctP2; p2++) {
            for (int p3 = 1; p3 <= kNumDistinctP3; p3++) {
              for (int8_t p4 = 1; p4 <= kNumDistinctP4; p4++) {
                CHECK_OK(row.SetInt32(0, p1));
                CHECK_OK(row.SetInt16(1, p2));
                CHECK_OK(row.SetStringCopy(2, StringPrintf("%d_p3", p3)));
                CHECK_OK(row.SetInt8(3, p4));
                ASSERT_OK_FAST(writer.Insert(row));
              }
            }
          }
        }
        break;
      }

      case kFivePK: {
        for (int32_t p1 = 1; p1 <= kNumDistinctP1; p1 ++) {
          for (int16_t p2 = 1; p2 <= kNumDistinctP2; p2++) {
            for (int p3 = 1; p3 <= kNumDistinctP3; p3++) {
              for (int8_t p4 = 1; p4 <= kNumDistinctP4; p4++) {
                for (int8_t p5 = 1; p5 <= kNumDistinctP5; p5++) {

                  CHECK_OK(row.SetInt32(0, p1));
                  CHECK_OK(row.SetInt16(1, p2));
                  CHECK_OK(row.SetStringCopy(2, StringPrintf("%d_p3", p3)));
                  CHECK_OK(row.SetInt8(3, p4));
                  CHECK_OK(row.SetInt8(4, p5));
                  ASSERT_OK_FAST(writer.Insert(row));

                }
              }
            }
          }
        }
        break;
      }

      case kTwoPKrandom: {
        //Only 1 row inserted
        CHECK_OK(row.SetInt32(0, 5));
        CHECK_OK(row.SetInt16(1, 11));
        ASSERT_OK_FAST(writer.Insert(row));
        break;
      }

      case kThreePKrandom: {
        // Insert non-sequential rows
        /* 1 2 1
         * 1 3 4
         * 1 4 9
         * 1 5 16
         * 1 6 25
         * 1 7 36
         * 2 2 16
         * 4 2 25
         * 4 3 25 */

        for (int i = 1; i <= 6; i++) {
          CHECK_OK(row.SetInt32(0, 1));
          CHECK_OK(row.SetInt8(1, i+1));
          CHECK_OK(row.SetInt16(2, i*i));
          ASSERT_OK_FAST(writer.Insert(row));

        }

        for (int i = 1; i <= 1; i++) {
          CHECK_OK(row.SetInt32(0, 2));
          CHECK_OK(row.SetInt8(1, i+1));
          CHECK_OK(row.SetInt16(2, 16));
          ASSERT_OK_FAST(writer.Insert(row));

        }

        for (int i = 1; i <= 2; i++) {
          CHECK_OK(row.SetInt32(0, 4));
          CHECK_OK(row.SetInt8(1, i+1));
          CHECK_OK(row.SetInt16(2, 25));
          ASSERT_OK_FAST(writer.Insert(row));

        }
        break;
      }

      default: {
        int16_t distinct_p1 = 10;
        int32_t distinct_p2 = 100;

        for (int16_t p1 = 1; p1 <= distinct_p1; p1 ++) {
          for (int32_t p2 = 1; p2 <= distinct_p2; p2++) {
            CHECK_OK(row.SetInt16(0, p1));
            CHECK_OK(row.SetInt32(1, p2));
            CHECK_OK(row.SetInt16(2, (p1*distinct_p1)+p2));
            ASSERT_OK_FAST(writer.Insert(row));


          }
        }

        // Add two more rows with random values
        CHECK_OK(row.SetInt16(0, 24));
        CHECK_OK(row.SetInt32(1, 101));
        CHECK_OK(row.SetInt16(2, 1989));
        ASSERT_OK_FAST(writer.Insert(row));

        CHECK_OK(row.SetInt16(0, 13));
        CHECK_OK(row.SetInt32(1, 101));
        CHECK_OK(row.SetInt16(2, 1988));
        ASSERT_OK_FAST(writer.Insert(row));



        break;
      }
    }

    ASSERT_OK(tablet()->Flush());
  }

  void ScanTablet(ScanSpec *spec, vector<string> *results, const char *descr) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    ASSERT_OK(iter->Init(spec));
    ASSERT_TRUE(spec->predicates().empty()) << "Should have accepted all predicates";
    ASSERT_OK(IterateToStringList(iter.get(), results));
    /*for (const string &str : *results) {
      LOG(INFO) << str;
    }*/
  }


};

// The following set of tests evaluate the scan results with different schema types.
// This is mainly done to verify the correctness of index skip scan approach.
TEST_P(IndexSkipScanTest, IndexSkipScanCorrectnessTest) {
  int schema_num = static_cast<int>(GetParam());

  switch (schema_num) {
    case kOnePK: {
      // Test predicate on the PK column
      ScanSpec spec;
      int32_t value_p1 = 2;
      auto pred_p1 = ColumnPredicate::Equality(schema_.column(0), &value_p1);
      spec.AddPredicate(pred_p1);
      vector<string> results;
      ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on column P1"));
      EXPECT_EQ(1, results.size());
      break;

    }

    case kTwoPK: {
      {
        // Test predicate on the first PK column
        ScanSpec spec;
        int32_t value_p1 = 2;
        auto pred_p1 = ColumnPredicate::Equality(schema_.column(0), &value_p1);
        spec.AddPredicate(pred_p1);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P1"));
        EXPECT_EQ(10, results.size());
      }

      {
        // Test predicate on the second PK column
        ScanSpec spec;
        int16_t value_p2 = 9;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
        EXPECT_EQ(2, results.size());
      }

      {
        // Test predicate on the first and second PK column
        ScanSpec spec;
        int32_t value_p1 = 1;
        int16_t value_p2 = 1;
        auto pred_p1 = ColumnPredicate::Equality(schema_.column(0), &value_p1);
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p1);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P1 and P2"));
        EXPECT_EQ(1, results.size());
      }
      break;

    }

    case kThreePK: {
      {
        // Test predicate on the third PK column
        ScanSpec spec;
        Slice value_p3("2_p3");
        auto pred_p3 = ColumnPredicate::Equality(schema_.column(2), &value_p3);
        spec.AddPredicate(pred_p3);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(20, results.size());
      }

      break;

    }

    case kFourPK: {
      {
        // Test predicate on the fourth PK column on a non-existent value
        ScanSpec spec;
        int16_t value_p4 = 3;
        auto pred_p4 = ColumnPredicate::Equality(schema_.column(3), &value_p4);
        spec.AddPredicate(pred_p4);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P4"));
        EXPECT_EQ(0, results.size());
      }

      {
        // Test predicate on the fourth PK column
        ScanSpec spec;
        int16_t p4 = 1;
        auto pred_p1 = ColumnPredicate::Equality(schema_.column(3), &p4);
        spec.AddPredicate(pred_p1);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P4"));
        EXPECT_EQ(100, results.size());
      }

      break;

    }

    case kFivePK: {
      {
        // Test predicate on the fifth PK column
        ScanSpec spec;
        int16_t value_p5 = 20;
        auto pred_p5 = ColumnPredicate::Equality(schema_.column(4), &value_p5);
        spec.AddPredicate(pred_p5);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P5"));
        EXPECT_EQ(100, results.size());
      }

      {
        // Test predicate on the third and fifth PK column
        ScanSpec spec;
        Slice value_p3("5_p3");
        int16_t value_p5 = 20;
        auto pred_p3 = ColumnPredicate::Equality(schema_.column(2), &value_p3);
        auto pred_p5 = ColumnPredicate::Equality(schema_.column(4), &value_p5);
        spec.AddPredicate(pred_p3);
        spec.AddPredicate(pred_p5);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3 and P5"));
        EXPECT_EQ(20, results.size());
      }

      break;

    }

    case kTwoPKrandom: {
      // Test predicate when only one row exists
      ScanSpec spec;
      int16_t value_p2 = 11;
      auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
      spec.AddPredicate(pred_p2);
      vector<string> results;
      ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
      EXPECT_EQ(1, results.size());
      break;
    }

    case kThreePKrandom: {
      // Test predicates on non-sequential rows
      {
        // Test predicate on the third PK column with a value that occurs
        // wrt the values "1" and "2" of the first PK column
        ScanSpec spec;
        int16_t value_p3 = 16;
        auto pred_p3 = ColumnPredicate::Equality(schema_.column(2), &value_p3);
        spec.AddPredicate(pred_p3);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(2, results.size());
      }

      {
        // Test predicate on the third PK column with value that occurs
        // only wrt the values "1" and "3" of the first PK column
        ScanSpec spec;
        int16_t value_p3 = 25;
        auto pred_p3 = ColumnPredicate::Equality(schema_.column(2), &value_p3);
        spec.AddPredicate(pred_p3);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(3, results.size());
      }

      {
        // Test predicate on the second PK column with a non-existent value
        ScanSpec spec;
        int8_t value_p2 = 1;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
        EXPECT_EQ(0, results.size());
      }

      {
        // Test predicate on the second PK column with a value that occurs
        // atleast once wrt each distinct key of the first PK column
        ScanSpec spec;
        int8_t value_p2 = 2;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
        EXPECT_EQ(3, results.size());
      }

      {
        // Test predicate on the second PK column with a value that occurs
        // only once wrt the value "1" of the first PK column
        ScanSpec spec;
        int8_t value_p2 = 7;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(1, results.size());
      }

      {
        // Test predicate on the second PK column with a value that occurs in the last row
        ScanSpec spec;
        int8_t value_p2 = 3;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(2, results.size());
      }

      {
        // Test predicate on the second PK column with a value that occurs
        // only wrt the value "1" of the first PK column
        ScanSpec spec;
        int8_t value_p2 = 5;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P3"));
        EXPECT_EQ(1, results.size());
      }
      break;

    }

    default: {
      // Test for predicates on the second column
      {
        ScanSpec spec;
        int32_t value_p2 = 9;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
        EXPECT_EQ(10, results.size());
      }

      {
        ScanSpec spec;
        int32_t value_p2 = 101;
        auto pred_p2 = ColumnPredicate::Equality(schema_.column(1), &value_p2);
        spec.AddPredicate(pred_p2);
        vector<string> results;
        ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match on P2"));
        EXPECT_EQ(2, results.size());
      }

      break;

    }

  }

}


INSTANTIATE_TEST_CASE_P(IndexSkipScanCorrectnessTest, IndexSkipScanTest,
                        ::testing::Values(kOnePK,
                                          kTwoPK,
                                          kThreePK,
                                          kFourPK,
                                          kFivePK,
                                          kTwoPKrandom,
                                          kThreePKrandom,
                                          kRandom

));

} // namespace tablet
} // namespace kudu
