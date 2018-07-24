package org.apache.kudu.mapreduce.tools;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import javax.xml.bind.DatatypeConverter;
import java.math.BigDecimal;
import java.util.Random;

/**
 * For use by LoadRandomData.scala
 */
@InterfaceAudience.LimitedPrivate({"LoadRandomData"})
@InterfaceStability.Evolving
public class RandomDataGenerator implements ColumnDataGenerator {
    private final Random rng;
    private final int index;
    private final Type type;
    private final int stringFieldLen;

    /**
     * Instantiate a random data generator for a specific field.
     * @param index The numerical index of the column in the row schema
     * @param type The type of the data at index {@code index}
     * @param stringFieldLen The number of random bytes to generate for each BINARY or STRING field
     */
    public RandomDataGenerator(int index, Type type, int stringFieldLen) {
        this.rng = new Random();
        this.index = index;
        this.type = type;
        this.stringFieldLen = stringFieldLen;
    }

    @Override
    public void generateColumnData(PartialRow row) {
        switch (type) {
            case INT8:
                row.addByte(index, (byte) rng.nextInt(Byte.MAX_VALUE));
                return;
            case INT16:
                row.addShort(index, (short)rng.nextInt(Short.MAX_VALUE));
                return;
            case INT32:
                row.addInt(index, rng.nextInt(Integer.MAX_VALUE));
                return;
            case INT64:
            case UNIXTIME_MICROS:
                row.addLong(index, rng.nextLong());
                return;
            case BINARY: {
                byte bytes[] = new byte[stringFieldLen];
                rng.nextBytes(bytes);
                row.addBinary(index, bytes);
                return;
            }
            case STRING: {
                // Base64-encode randomized STRING fields.
                // We only need to generate 6 bits per base64 character instead
                // of the usual 8. After we encode we get stringFieldLen bytes.
                byte bytes[] = new byte[stringFieldLen * 3 / 4];
                rng.nextBytes(bytes);
                // java.util.Base64 is a Java 8 API so we can't use it yet.
                String encoded = DatatypeConverter.printBase64Binary(bytes);
                row.addString(index, encoded);
                return;
            }
            case BOOL:
                row.addBoolean(index, rng.nextBoolean());
                return;
            case FLOAT:
                row.addFloat(index, rng.nextFloat());
                return;
            case DOUBLE:
                row.addDouble(index, rng.nextDouble());
                return;
            case DECIMAL:
                row.addDecimal(index, new BigDecimal(rng.nextDouble()));
                return;
            default:
                throw new UnsupportedOperationException("Unknown type " + type);
        }
    }
}
