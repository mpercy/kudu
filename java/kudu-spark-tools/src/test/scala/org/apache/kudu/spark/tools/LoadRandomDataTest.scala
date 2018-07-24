package org.apache.kudu.spark.tools

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.spark.kudu.KuduTestSuite
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LoadRandomDataTest extends KuduTestSuite {

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

    LoadRandomData.testMain(
      Array(
        "--num-rows=100",
        "--num-tasks=10",
        "--string-field-len=128",
        s"--master-addrs=${miniCluster.getMasterAddresses}",
        s"--table-name=$TABLE_NAME"
      ),
      ss
    )
    val rdd = kuduContext.kuduRDD(ss.sparkContext, TABLE_NAME, List("key"))
    assert(rdd.collect.length == 100)
  }

}
