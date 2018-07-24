package org.apache.kudu.mapreduce.tools;

import org.apache.kudu.client.PartialRow;

/**
 * Interface for an object that knows how to generate data for a particular column.
 */
public interface ColumnDataGenerator {

  /**
   * Add data to the given row for the column represented by this object.
   * @param row The row to add the generated field data to
   */
  void generateColumnData(PartialRow row);
}
