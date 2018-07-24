package org.apache.kudu.mapreduce.tools;

import com.google.common.base.Charsets;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;

public class SequentialDataGenerator implements ColumnDataGenerator {
  private final int index;
  private final Type type;
  private final int stringFieldLen;
  private long value;

  public SequentialDataGenerator(int index, Type type, long initialValue, int stringFieldLen) {
    this.index = index;
    this.type = type;
    this.stringFieldLen = stringFieldLen;
    this.value = initialValue;
  }

  @Override
  public void generateColumnData(PartialRow row) {
    switch (type) {
      case INT8:
        row.addByte(index, (byte)value++);
        return;
      case INT16:
        row.addShort(index, (short)value++);
        return;
      case INT32:
        row.addInt(index, (int)value++);
        return;
      case INT64:
      case UNIXTIME_MICROS:
        row.addLong(index, value++);
        return;
      case BINARY: {
        String str = String.format("%0" + stringFieldLen + "d", value++);
        row.addBinary(index, str.getBytes(Charsets.UTF_8));
        return;
      }
      case STRING: {
        String str = String.format("%0" + stringFieldLen + "d", value++);
        row.addString(index, str);
        return;
      }
      case BOOL:
        row.addBoolean(index, value++ % 2 == 1);
        return;
      case FLOAT:
        row.addFloat(index, (float)value++);
        return;
      case DOUBLE:
        row.addDouble(index, (double)value++);
        return;
      case DECIMAL:
        row.addDecimal(index, new BigDecimal(value++));
        return;
      default:
        throw new UnsupportedOperationException("Unknown type " + type);
    }

  }
}
