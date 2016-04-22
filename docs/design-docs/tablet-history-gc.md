<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Tablet history garbage collection

This document describes the removal of what we call "ancient history" from the
tablet. As described in the [Kudu Tablet design doc](tablet.md), Kudu stores
the history of changes over time in order to support scans at a particular
snapshot. Eventually, to reclaim space, we want to stop storing that history.

## Ancient history mark

The ancient history mark is a HybridTime timestamp prior to which history is
considered "ancient". Ancient data is made unavailable and removed from disk.
The ancient history mark is defined by a property called the "tablet history
max age". This property will be specified as part of the table creation
process, and a default is configured as a _gflag_ called
`tablet_history_default_max_age_sec`, which defaults to 1 week.

## Consistency and read visibility

An attempt to open a snapshot scan prior to the ancient history mark will be rejected.

When doing actual garbage collection, in order not to disrupt existing open
scanners, if there is an open scanner with a snapshot timestamp earlier than
the current ancient history mark, we will use the timestamp of that scanner as
the _effective_ ancient history mark for the purposes of history GC.

## Implementation: Removing old update history

In an update-heavy workload, many redo records (and after compaction, many undo
records) tend to accumulate per row. Removing old undo records takes place as
part of the major delta compaction maintenance task.

## Implementation: Removing old deleted rows

### Merging compactions

When a merging compaction is run, two or more RowSets are merged into one and
their constituent row ids are reassigned. During this process, if a row marked
as "deleted" has a deletion timestamp prior to the ancient history mark, that
row will be skipped when writing the new rowset. This process will permanently
remove all traces of the row and the space will be reclaimed from disk.

### Row GC maintenance task

In cases where a merging tablet compaction is not run, we still want to remove
old rows. Therefore we will create a new tablet maintenance task called Row GC,
with the explicit purpose ot removing old rows. This task will run a special
type of merging compaction that does not actually require merging multiple
RowSets, but will only run on a single RowSet.

Defining the Row GC task will require a scoring function that takes into
account the number of deleted rows in a tablet and the resulting number of
bytes that we can reclaim as a result of deleting old rows.
