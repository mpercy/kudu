package org.apache.kudu.spark.tools

import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.PartialRow
import org.apache.kudu.mapreduce.tools.RandomDataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.yetus.audience.InterfaceAudience

import scala.collection.mutable.ArrayBuffer

case class TableOptions(
    numPartitions: Int,
    replicationFactor: Int,
    numColumns: Int,
    intColumnPercentage: Float)

object LoadRandomData {

  def writeRandomRows(
      masterAddresses: String,
      tableName: String,
      rowsToWrite: Long,
      stringFieldLen: Int) {
    val kuduClient = new KuduClientBuilder(masterAddresses).build()
    val session = kuduClient.newSession()
    val kuduTable = kuduClient.openTable(tableName)
    val schema = kuduTable.getSchema
    val numCols = schema.getColumnCount

    val generators = ArrayBuffer[RandomDataGenerator]()
    for (i <- 0 until numCols) {
      generators += new RandomDataGenerator(i, schema.getColumnByIndex(i).getType, stringFieldLen)
    }

    def generateRowData(row: PartialRow): Unit = {
      for (i <- 0 until numCols) {
        generators(i).generateColumnData(row)
      }
    }

    var rowsWritten: Long = 0
    while (rowsWritten < rowsToWrite) {
      val insert = kuduTable.newInsert()
      generateRowData(insert.getRow)
      session.apply(insert)
      rowsWritten += 1
      for (rowError <- session.getPendingErrors.getRowErrors) {
        if (rowError.getErrorStatus.isAlreadyPresent ||
          rowError.getErrorStatus.isServiceUnavailable) {
          rowsWritten -= 1
        } else {
          throw new RuntimeException("Kudu write error: " + rowError.getErrorStatus.toString)
        }
      }
    }
  }

  def run(config: LoadRandomDataOptions, ss: SparkSession): Unit = {
    val sc = ss.sparkContext
    val rowsPerPartition = config.numRows / config.numTasks
    sc.parallelize(0 to config.numTasks)
      .foreach(
        _ =>
          writeRandomRows(
            config.masterAddresses,
            config.tableName,
            rowsPerPartition,
            config.stringFieldLen))
  }

  /**
   * Entry point for testing. SparkContext is a singleton,
   * so tests must create and manage their own.
   */
  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(args: Array[String], ss: SparkSession): Unit = {
    LoadRandomDataOptions.parse(args) match {
      case None => System.exit(1) // Error was printed to stdout.
      case Some(config) => run(config, ss)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LoadRandomData")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    testMain(args, ss)
  }
}
