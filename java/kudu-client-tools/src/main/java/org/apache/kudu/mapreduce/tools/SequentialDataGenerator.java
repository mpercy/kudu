package org.apache.kudu.mapreduce.tools;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Arrays;

public class SequentialDataGenerator implements ColumnDataGenerator {
  private final int index;
  private final Type type;
  private final int stringFieldLen;
  private final byte[] zeros;
  private long value;

  public SequentialDataGenerator(int index, Type type, long initialValue, int stringFieldLen) {
    this.index = index;
    this.type = type;
    this.stringFieldLen = stringFieldLen;
    this.zeros = new byte[stringFieldLen];
    Arrays.fill(zeros, (byte) '0');
    this.value = initialValue;
  }

  /** Returns a zero-suffixed stringification of the value member variable. Post-increments 'value' */
  private byte[] getAndIncrementValueAsByteArray() {
    byte[] buf = Arrays.copyOf(zeros, zeros.length);
    byte[] val = String.valueOf(value).getBytes(Charset.forName("UTF-8"));
    System.arraycopy(val, 0, buf, 0, Math.min(val.length, buf.length));
    value++;
    return buf;
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
      case BINARY:
        row.addBinary(index, getAndIncrementValueAsByteArray());
        return;
      case STRING:
        row.addString(index, new String(getAndIncrementValueAsByteArray(), Charset.forName("UTF-8")));
        return;
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
