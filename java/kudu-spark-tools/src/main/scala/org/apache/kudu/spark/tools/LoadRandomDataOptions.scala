package org.apache.kudu.spark.tools

import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import scopt.OptionParser

@InterfaceAudience.Private
@InterfaceStability.Unstable
case class LoadRandomDataOptions(
    tableName: String,
    masterAddresses: String,
    numRows: Long = LoadRandomDataOptions.DefaultNumRows,
    numTasks: Int = LoadRandomDataOptions.DefaultNumTasks,
    stringFieldLen: Int = LoadRandomDataOptions.DefaultStringFieldLen)

object LoadRandomDataOptions {
  val DefaultNumRows: Long = 10000
  val DefaultNumTasks: Int = 1
  val DefaultStringFieldLen: Int = 128

  private val parser: OptionParser[LoadRandomDataOptions] =
    new OptionParser[LoadRandomDataOptions]("LoadRandomData") {

      arg[String]("table-name")
        .action((v, o) => o.copy(tableName = v))
        .text("The table to load with random data")

      arg[String]("master-addresses")
        .action((v, o) => o.copy(masterAddresses = v))
        .text("Comma-separated addresses of Kudu masters")

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

  def parse(args: Seq[String]): Option[LoadRandomDataOptions] = {
    parser.parse(args, LoadRandomDataOptions("", ""))
  }
}
