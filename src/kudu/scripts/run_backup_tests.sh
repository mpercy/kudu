#!/bin/bash -xe
#####################################################################

# Configurations to test
# - Number of partitions per tablet server: 50 (x 9 tablet servers = 450 partitions total)
# - Number of columns per table: 10, 75, 300
# - Column type distributions: 10% strings, 90% strings (1KB strings)
# - Total (uncompressed) data size of a table: 500GB, 5TB, 50TB
# - Read parallelism per tserver: # cpus, # partitions

# Fixed variables.
NUM_TABLET_SERVERS=9 # On the Flash cluster
VCPUS_PER_TABLET_SERVER=32 # On vc1306.halxg.cloudera.com (Flash cluster)
PARTITIONS_PER_TABLET_SERVER=50 # Arbitrarily chosen

# Calculated variables.
NUM_PARTITIONS=$(($NUM_TABLET_SERVERS * $PARTITIONS_PER_TABLET_SERVER))

# Called Python program should not buffer output.
export PYTHONUNBUFFERED=y

# Future variables, but fixed for now.
TABLE_DATA_SIZE_MB=$((500 * 1024)) # TODO(mpercy): Also: (5 * 1024 * 1024) and (50 * 1024 * 1024).
NUM_EXECUTORS=$(($NUM_TABLET_SERVERS * $NUM_PARTITIONS)) # TODO(mpercy): Also test $NUM_TABLET_SERVERS * $VCPUS_PER_TABLET_SERVER
NUM_TASKS=$(($NUM_EXECUTORS * 10)) # TODO(mpercy): Test varying this.

CMD=./backup-perf.py
TABLE_NAME=mpercy_test20
BACKUP_BASE=hdfs:///user/mpercy/kudu-backup-tests

OPTS="--spark-submit-command spark2-submit --kudu-spark-tools-jar kudu-spark2-tools_2.11-1.8.0-SNAPSHOT.jar --kudu-backup-jar kudu-backup2_2.11-1.8.0-SNAPSHOT.jar -m vc1320.halxg.cloudera.com"
OPTS="$OPTS --num-executors $NUM_EXECUTORS --num-tasks $NUM_TASKS --partitions $NUM_PARTITIONS --table-data-size-mb=$TABLE_DATA_SIZE_MB"

for NUM_COLUMNS in 10 75 300; do
  for STRING_COL_FRACTION in .1 .9; do
    NUM_STR_COLS=$(perl -e 'print $NUM_COLUMNS * $STRING_COL_FRACTION, "\n";')
    OPTS="$OPTS --num-string-columns $NUM_STR_COLS"

    # Create the table but don't back it up.
    time $CMD $OPTS --backup-table false --restore-table false --drop-created-table false \
                    --drop-restored-table false -p $BACKUP_BASE \
                    $TABLE_NAME

    # Back up and restore the table several times to get a reasonable average.
    for ATTEMPT_NUM in 1 2 3; do
      TS="$(date '+%Y%m%d-%H%M%S')"
      time $CMD $OPTS --create-table false --load-table false --drop-created-table false \
                      -p $BACKUP_BASE/$TS $TABLE_NAME
    done

    # Drop the created table.
    time $CMD $OPTS --create-table false --load-table false --backup-table false \
                    --restore-table false --drop-restored-table false \
                    --drop-created-table true -p $BACKUP_BASE \
                    $TABLE_NAME
  done
done

exit 0
