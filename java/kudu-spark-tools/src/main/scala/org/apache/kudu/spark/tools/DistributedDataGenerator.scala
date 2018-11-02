package org.apache.kudu.spark.tools

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.kudu.client.AsyncKuduClient
import org.apache.kudu.client.SessionConfiguration
import org.apache.kudu.mapreduce.tools.ColumnDataGenerator
import org.apache.kudu.mapreduce.tools.RandomDataGenerator
import org.apache.kudu.mapreduce.tools.SequentialDataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

import scala.collection.mutable.ArrayBuffer
import scopt.OptionParser

import scala.collection.mutable

case class TableOptions(
    numPartitions: Int,
    replicationFactor: Int,
    numColumns: Int,
    intColumnPercentage: Float)

case class GeneratorMetrics(rowsWritten: LongAccumulator, collisions: LongAccumulator)

object GeneratorMetrics {

  def create(sc: SparkContext): GeneratorMetrics = {
    GeneratorMetrics(sc.longAccumulator("rows_written"), sc.longAccumulator("row_collisions"))
  }
}

object DistributedDataGenerator {

  def generateRows(
      generatorType: String,
      masterAddresses: String,
      tableName: String,
      taskNum: Int,
      numTasks: Int,
      rowsToWrite: Long,
      stringFieldLen: Int,
      metrics: GeneratorMetrics) {
    val kuduClient = KuduClientCache.getAsyncClient(masterAddresses, None).syncClient
    val session = kuduClient.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
    val kuduTable = kuduClient.openTable(tableName)
    val schema = kuduTable.getSchema
    val numCols = schema.getColumnCount

    val generators = ArrayBuffer[ColumnDataGenerator]()
    for (i <- 0 until numCols) {
      if (generatorType == DistributedDataGeneratorOptions.RandomGenerator) {
        generators += new RandomDataGenerator(i, schema.getColumnByIndex(i).getType, stringFieldLen)
      } else if (generatorType == DistributedDataGeneratorOptions.SequentialGenerator) {
        val column = schema.getColumnByIndex(i)
        val sizeInBytes = column.getType.getSize(column.getTypeAttributes)
        val maxValForType: Long = 1L << math.max(sizeInBytes * 8 - 2, 62);
        val initialValue: Long = maxValForType / numTasks * taskNum
        generators += new SequentialDataGenerator(i, column.getType, initialValue, stringFieldLen)
      }
    }

    var rowsWritten: Long = 0
    while (rowsWritten < rowsToWrite) {
      val insert = kuduTable.newInsert()
      for (i <- 0 until numCols) {
        generators(i).generateColumnData(insert.getRow)
      }
      session.apply(insert)

      // Synchronously flush on potentially the last iteration of the
      // loop, so we can check whether we need to retry any collisions.
      if (rowsWritten + 1 == rowsToWrite) session.flush()

      for (error <- session.getPendingErrors.getRowErrors) {
        if (error.getErrorStatus.isAlreadyPresent || error.getErrorStatus.isServiceUnavailable) {
          rowsWritten -= 1
          metrics.collisions.add(1)
        } else {
          throw new RuntimeException("Kudu write error: " + error.getErrorStatus.toString)
        }
      }

      rowsWritten += 1
    }
    metrics.rowsWritten.add(rowsWritten)
  }

  def run(opts: DistributedDataGeneratorOptions, ss: SparkSession): Unit = {
    val sc = ss.sparkContext
    val rowsPerTask = opts.numRows / opts.numTasks
    val metrics = GeneratorMetrics.create(sc)
    sc.parallelize(0 until opts.numTasks, opts.numTasks)
      .foreach(
        taskNum =>
          generateRows(
            opts.generatorType,
            opts.masterAddresses,
            opts.tableName,
            taskNum,
            opts.numTasks,
            rowsPerTask,
            opts.stringFieldLen,
            metrics))
  }

  /**
   * Entry point for testing. SparkContext is a singleton,
   * so tests must create and manage their own.
   */
  @InterfaceAudience.LimitedPrivate(Array("Test"))
  def testMain(args: Array[String], ss: SparkSession): Unit = {
    DistributedDataGeneratorOptions.parse(args) match {
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

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class DistributedDataGeneratorOptions(
    tableName: String,
    masterAddresses: String,
    generatorType: String = DistributedDataGeneratorOptions.DefaultGeneratorType,
    numRows: Long = DistributedDataGeneratorOptions.DefaultNumRows,
    numTasks: Int = DistributedDataGeneratorOptions.DefaultNumTasks,
    stringFieldLen: Int = DistributedDataGeneratorOptions.DefaultStringFieldLen)

@InterfaceAudience.Private
@InterfaceStability.Unstable
object DistributedDataGeneratorOptions {
  val DefaultNumRows: Long = 10000
  val DefaultNumTasks: Int = 1
  val DefaultStringFieldLen: Int = 128
  val RandomGenerator: String = "random"
  val SequentialGenerator: String = "sequential"
  val DefaultGeneratorType = RandomGenerator

  private val parser: OptionParser[DistributedDataGeneratorOptions] =
    new OptionParser[DistributedDataGeneratorOptions]("LoadRandomData") {

      arg[String]("table-name")
        .action((v, o) => o.copy(tableName = v))
        .text("The table to load with random data")

      arg[String]("master-addresses")
        .action((v, o) => o.copy(masterAddresses = v))
        .text("Comma-separated addresses of Kudu masters")

      opt[String]("type")
        .action((v, o) => o.copy(generatorType = v))
        .text(
          s"The type of data generator. Must be one of 'random' or 'sequential'. Default: ${DefaultGeneratorType}")
        .optional()

      opt[Long]("num-rows")
        .action((v, o) => o.copy(numRows = v))
        .text(s"The total number of unique rows to generate. Default: ${DefaultNumRows}")
        .optional()

      opt[Int]("num-tasks")
        .action((v, o) => o.copy(numTasks = v))
        .text(s"The total number of Spark tasks to generate. Default: ${DefaultNumTasks}")
        .optional()

      opt[Int]("string-field-len")
        .action((v, o) => o.copy(stringFieldLen = v))
        .text(s"The length of generated string fields. Default: ${DefaultStringFieldLen}")
        .optional()
    }

  def parse(args: Seq[String]): Option[DistributedDataGeneratorOptions] = {
    parser.parse(args, DistributedDataGeneratorOptions("", ""))
  }
}

// Copy-pasted from the kudu-spark module; this is needed to avoid spawning too many reactor threads
// when multiple tasks are executed by the same executor.
private object KuduClientCache {
  private case class CacheKey(kuduMaster: String, socketReadTimeoutMs: Option[Long])

  /**
   * Set to
   * [[org.apache.spark.util.ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY]].
   * The client instances are closed through the JVM shutdown hook
   * mechanism in order to make sure that any unflushed writes are cleaned up
   * properly. Spark has no shutdown notifications.
   */
  private val ShutdownHookPriority = 100

  private val clientCache = new mutable.HashMap[CacheKey, AsyncKuduClient]()

  // Visible for testing.
  private[kudu] def clearCacheForTests() = clientCache.clear()

  def getAsyncClient(kuduMaster: String, socketReadTimeoutMs: Option[Long]): AsyncKuduClient = {
    val cacheKey = CacheKey(kuduMaster, socketReadTimeoutMs)
    clientCache.synchronized {
      if (!clientCache.contains(cacheKey)) {
        val builder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMaster)
        socketReadTimeoutMs match {
          case Some(timeout) => builder.defaultSocketReadTimeoutMs(timeout)
          case None =>
        }

        val asyncClient = builder.build()
        ShutdownHookManager
          .get()
          .addShutdownHook(new Runnable {
            override def run(): Unit = asyncClient.close()
          }, ShutdownHookPriority)
        clientCache.put(cacheKey, asyncClient)
      }
      return clientCache(cacheKey)
    }
  }
}