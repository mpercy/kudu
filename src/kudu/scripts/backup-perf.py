#!/usr/bin/python
#################################################################
# Backup and Restore performance test driver.
#################################################################

import argparse
import datetime
import json
import subprocess
import sys
import timeit

def create_table(opts):
  """ Create a Kudu table via impala-shell """
  print("--------------------------------------")
  print("Creating table %s" % (opts.table_name,))
  print("--------------------------------------")
  print_timestamp()
  create_table_ddl = "CREATE TABLE %s (" % (opts.table_name,)
  num_bigint_cols = opts.columns - opts.num_string_columns
  assert(num_bigint_cols > 0)
  for i in range(opts.columns):
    coltype = 'STRING'
    if i < num_bigint_cols: coltype = 'BIGINT'
    if i > 0: create_table_ddl += ', '
    create_table_ddl += "f%d %s" % (i, coltype)
    if i == 0: create_table_ddl += ' PRIMARY KEY'
  create_table_ddl += ") PARTITION BY HASH(f0) PARTITIONS %d STORED AS KUDU "  % (opts.partitions, )
  create_table_ddl += "TBLPROPERTIES ('kudu.num_tablet_replicas' = '%d')" % (opts.replication_factor, )

  # echo $create_table_ddl | impala-shell -f - || exit $?
  cmd = 'echo "%s" | impala-shell -f -' % (create_table_ddl, )
  print(cmd)
  if not opts.dryrun:
    print(check_output(cmd, shell=True))

def load_table(opts):
  """ Load a table with data using the DistributedDataGenerator spark job """
  print("--------------------------------------")
  print("Loading table %s" % (opts.table_name,))
  print("--------------------------------------")
  print_timestamp()
  # Example invocation:
  # spark2-submit --num-executors 10 --class org.apache.kudu.spark.tools.DistributedDataGenerator kudu-spark2-tools_2.11-1.8.0-SNAPSHOT.jar --type random --num-rows 10000000 --num-tasks 20 impala::default.mpercy_test3 vc1320.halxg.cloudera.com
  CLASS_NAME = 'org.apache.kudu.spark.tools.DistributedDataGenerator'
  row_size_bytes = opts.num_string_columns * opts.string_field_len + (opts.columns - opts.num_string_columns) * 8
  num_rows = opts.table_data_size_mb * 1024 * 1024 / row_size_bytes
  cmd = "%s --num-executors %d --class %s %s --type %s --num-rows %d --num-tasks %d %s %s" % \
    (opts.spark_submit_command, opts.num_executors, CLASS_NAME, opts.kudu_spark_tools_jar,
     opts.load_policy, num_rows, opts.num_tasks, opts.table_prefix + opts.table_name, opts.master_addresses)
  print(cmd)
  if not opts.dryrun:
    print(check_output(cmd, shell=True))

def backup_table(opts):
  print("--------------------------------------")
  print("Backing up table %s" % (opts.table_name,))
  print("--------------------------------------")
  print_timestamp()
  CLASS_NAME = "org.apache.kudu.backup.KuduBackup"
  cmd = "%s --num-executors %d --class %s %s --kuduMasterAddresses %s --path %s %s" % \
    (opts.spark_submit_command, opts.num_executors, CLASS_NAME, opts.kudu_backup_jar,
     opts.master_addresses, opts.backup_path, opts.table_prefix + opts.table_name)
  print(cmd)
  if not opts.dryrun:
    print(check_output(cmd, shell=True))

def restore_table(opts):
  print("--------------------------------------")
  print("Restoring table %s" % (opts.table_name,))
  print("--------------------------------------")
  print_timestamp()
  CLASS_NAME = "org.apache.kudu.backup.KuduRestore"
  cmd = "%s --num-executors %d --class %s %s --kuduMasterAddresses %s --path %s %s" % \
    (opts.spark_submit_command, opts.num_executors, CLASS_NAME, opts.kudu_backup_jar,
     opts.master_addresses, opts.backup_path, opts.table_prefix + opts.table_name)
  print(cmd)
  if not opts.dryrun:
    print(check_output(cmd, shell=True))

def print_timestamp():
  print(datetime.datetime.now().isoformat())

def check_output(*popenargs, **kwargs):
  r"""Run command with arguments and return its output as a byte string.
  Backported from Python 2.7 as it's implemented as pure python on stdlib.
  >>> check_output(['/usr/bin/python', '--version'])
  Python 2.6.2
  """
  process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
  output, unused_err = process.communicate()
  retcode = process.poll()
  if retcode:
    cmd = kwargs.get("args")
    if cmd is None:
      cmd = popenargs[0]
    error = subprocess.CalledProcessError(retcode, cmd)
    error.output = output
    raise error
  return output

def parse_args():
  """ Parse command-line arguments """
  parser = argparse.ArgumentParser(description='Run a Kudu backup and restore performance test',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-n', '--dryrun', action='store_true',
                      help='Do not execute any commands, only print what would be executed')
  parser.add_argument('-m', '--master-addresses', required=True, help='The Kudu master addresses')
  parser.add_argument('-p', '--backup-path', required=True, help='The Hadoop-compatible path at which to store the backup')
  parser.add_argument('table_name', help='The name of the Kudu table to create')
  parser.add_argument('-x', '--num-executors', type=int, default=10, help='Number of YARN executors')
  parser.add_argument('-t', '--num-tasks', type=int, default=20, help='Number of Spark tasks to create')
  parser.add_argument('--columns', type=int, default=10, help='The number of columns in the Kudu table')
  parser.add_argument('--num-string-columns', type=int, default=9,
                      help='The number of string columns in the table; the rest will be bigints')
  parser.add_argument('--partitions', type=int, default=10,
                      help='The number of hash partitions of the table. This script only supports hash partitions')
  parser.add_argument('--table-data-size-mb', type=int, default=1024,
                      help='The uncompressed data size of the table, in MB')
  parser.add_argument('--replication-factor', type=int, default=1, help='The replication factor of the table')
  parser.add_argument('--backup-file-format', default='parquet', help='The file format of the backup: must be parquet')
  parser.add_argument('--delete-on-success', type=bool, choices=[True, False], default=True,
                      help='Whether to delete the created Kudu tables after a successful test run')
  parser.add_argument('--create-table', type=bool, choices=[True, False], default=True,
                      help='Whether to create the table for loading')
  parser.add_argument('--load-table', type=bool, choices=[True, False], default=True,
                      help='Whether to load the table with data')
  parser.add_argument('--load-policy', default='sequential', choices=['sequential', 'random'],
                      help='The data loading policy for the data generator')
  parser.add_argument('--string-field-len', type=int, default=128,
                      help='The length, in bytes, of generated string column values')
  parser.add_argument('--spark-submit-command', default='spark-submit', help='The name of the spark-submit binary')
  parser.add_argument('--kudu-spark-tools-jar', default='kudu-spark2-tools_2.11-1.8.0.jar',
                      help='The path to the kudu-spark-tools jar (for --load-table)')
  parser.add_argument('--kudu-backup-jar', default='kudu-backup2_2.11-1.8.0.jar',
                      help='The path to the kudu-backup jar')
  parser.add_argument('--table-prefix', default='impala::default.', help='Kudu table name prefix in the Hive metastore')
  return parser.parse_args()

def main():
  print_timestamp()
  print("Starting perf test...")

  opts = parse_args()
  start = timeit.default_timer()

  stats = {}
  if opts.create_table:
    create_table(opts)
  create_table_done_time = timeit.default_timer()
  stats['create_table'] = create_table_done_time - start

  if opts.load_table:
    load_table(opts)
  load_table_done_time = timeit.default_timer()
  stats['load_table'] = load_table_done_time - create_table_done_time

  backup_table(opts)
  backup_table_done_time = timeit.default_timer()
  stats['backup_table'] = backup_table_done_time - load_table_done_time

  restore_table(opts)
  restore_table_done_time = timeit.default_timer()
  stats['restore_table'] = restore_table_done_time - backup_table_done_time

  end = timeit.default_timer()

  print_timestamp()
  print("Ending perf test")
  print("Time elapsed: %s s" % (end - start,))

  print("")
  print("--------------------------------------")
  print("Stats:")
  print("--------------------------------------")
  print(json.dumps(stats, sort_keys=True,
                   indent=4, separators=(',', ': ')))

if __name__ == "__main__":
  main()
