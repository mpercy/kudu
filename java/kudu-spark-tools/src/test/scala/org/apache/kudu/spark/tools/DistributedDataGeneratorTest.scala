package org.apache.kudu.spark.tools

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.spark.kudu.KuduTestSuite
import org.junit.Test
import org.junit.Assert.assertEquals

import scala.collection.JavaConverters._

class DistributedDataGeneratorTest extends KuduTestSuite {

  private val TABLE_NAME: String = "LoadRandomDataTest"

  @Test
  def testSparkImportExport() {

    val schema: Schema = {
      val columns = ImmutableList.of(
        new ColumnSchemaBuilder("key", Type.INT64).key(true).build(),
        new ColumnSchemaBuilder("column1_i", Type.INT64).build(),
        new ColumnSchemaBuilder("column2_i", Type.INT64).build(),
        new ColumnSchemaBuilder("column3_s", Type.STRING).build(),
        new ColumnSchemaBuilder("column4_s", Type.STRING).build()
      )
      new Schema(columns)
    }
    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    kuduClient.createTable(TABLE_NAME, schema, tableOptions)

    val NumRows = 100
    DistributedDataGenerator.testMain(
      Array(
        s"--num-rows=$NumRows",
        "--num-tasks=10",
        "--string-field-len=128",
        TABLE_NAME,
        harness.getMasterAddressesAsString
      ),
      ss
    )
    val rdd = kuduContext.kuduRDD(ss.sparkContext, TABLE_NAME, List("key"))
    assertEquals(NumRows, rdd.collect.length)
  }

}
