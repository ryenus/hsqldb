/* Copyright (c) 2001-2007, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.rowio;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.CachedRow;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.Type;

/**
 *  Provides methods for writing the data for a row to a
 *  byte array. The new format of data consists of mainly binary values
 *  and is not compatible with v.1.6.x databases.
 *
 * @author sqlbob@users (RMP)
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.0
 */
public class RowOutputBinary extends RowOutputBase {

    private static final int INT_STORE_SIZE = 4;
    int                      storageSize;

    public RowOutputBinary() {
        super();
    }

    public RowOutputBinary(int initialSize) {
        super(initialSize);
    }

    /**
     *  Constructor used for network transmission of result sets
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBinary(byte[] buffer) {
        super(buffer);
    }

// fredt@users - comment - methods for writing column type, name and data size
    public void writeIntData(int i, int position) {

        int temp = count;

        count = position;

        writeInt(i);

        if (count < temp) {
            count = temp;
        }
    }

    public void writeEnd() {

        // fredt - this value is used in 1.7.0 when reading back, for a
        // 'data integrity' check
        // has been removed in 1.7.2 as compatibility is no longer necessary
        // writeInt(pos);
        for (; count < storageSize; ) {
            this.write(0);
        }
    }

    public void writeSize(int size) {

        storageSize = size;

        writeInt(size);
    }

    public void writeType(int type) {
        writeShort(type);
    }

    public void writeString(String s) {

        int temp = count;

        writeInt(0);
        StringConverter.writeUTF(s, this);
        writeIntData(count - temp - 4, temp);
    }

    /**
     *  Calculate the size of byte array required to store a row.
     *
     * @param  row - a database row
     * @return  size of byte array
     * @exception  HsqlException When data is inconsistent
     */
    public int getSize(CachedRow row) {

        Object[] data  = row.getData();
        Type[]   types = row.getTable().getColumnTypes();
        int      cols  = row.getTable().getColumnCount();

        return INT_STORE_SIZE + getSize(data, cols, types);
    }

    public static int getRowSize(CachedRow row) {

        Object[] data  = row.getData();
        Type[]   types = row.getTable().getColumnTypes();
        int      cols  = row.getTable().getColumnCount();

        return getSize(data, cols, types);
    }

// fredt@users - comment - methods used for writing each SQL type
    protected void writeFieldType(Type type) {
        write(1);
    }

    protected void writeNull(Type type) {
        write(0);
    }

    protected void writeChar(String s, Type t) {
        writeString(s);
    }

    protected void writeSmallint(Number o) {
        writeShort(o.intValue());
    }

    protected void writeInteger(Number o) {
        writeInt(o.intValue());
    }

    protected void writeBigint(Number o) {
        writeLong(o.longValue());
    }

    protected void writeReal(Double o) {
        writeLong(Double.doubleToLongBits((o.doubleValue())));
    }

    protected void writeDecimal(BigDecimal o, Type type) {

        int        scale   = o.scale();
        BigInteger bigint  = JavaSystem.unscaledValue(o);
        byte[]     bytearr = bigint.toByteArray();

        writeByteArray(bytearr);
        writeInt(scale);
    }

    protected void writeBoolean(Boolean o) {
        write(o.booleanValue() ? 1
                               : 0);
    }

    protected void writeDate(Date o, Type type) {
        writeLong(o.getTime());
    }

    protected void writeTime(TimeData o, Type type) {
        writeInt((int) o.getSeconds());
        writeInt(o.getNanos());
    }

    protected void writeTimestamp(Timestamp o, Type type) {
        writeLong(o.getTime());
        writeInt(o.getNanos());
    }

    protected void writeYearMonthInterval(IntervalMonthData o, Type type) {
        this.writeBytes(type.convertToString(o));
    }

    protected void writeDaySecondInterval(IntervalSecondData o, Type type) {
        this.writeBytes(type.convertToString(o));
    }


    protected void writeOther(JavaObjectData o) {
        writeByteArray(o.getBytes());
    }

    protected void writeBinary(BinaryData o) {
        writeByteArray(o.getBytes());
    }

    protected void writeClob(ClobData o, Type type) {
        writeString(type.convertToString(o));

    }

    protected void writeBlob(BlobData o, Type type) {
        writeByteArray(o.getBytes());
    }


// fredt@users - comment - helper and conversion methods
    public void writeByteArray(byte[] b) {
        writeInt(b.length);
        write(b, 0, b.length);
    }

    // fredt@users - comment - helper and conversion methods
        public void writeCharArray(char[] c) {
            writeInt(c.length);
            write(c, 0, c.length);
        }

    /**
     *  Calculate the size of byte array required to store a row.
     *
     * @param  data - the row data
     * @param  l - number of data[] elements to include in calculation
     * @param  type - array of java.sql.Types values
     * @return size of byte array
     * @exception  HsqlException when data is inconsistent
     */
    private static int getSize(Object[] data, int l, Type[] types) {

        int s = 0;

        for (int i = 0; i < l; i++) {
            Object o = data[i];

            s += 1;    // type or null

            if (o != null) {
                switch (types[i].type) {

                    case Types.SQL_ALL_TYPES :
                        throw Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "RowOutputBinary");
                    case Types.SQL_CHAR :
                    case Types.SQL_VARCHAR :
                    case Types.VARCHAR_IGNORECASE :
                        s += 4;
                        s += StringConverter.getUTFSize((String) o);
                        break;

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                        s += 2;
                        break;

                    case Types.SQL_INTEGER :
                        s += 4;
                        break;

                    case Types.SQL_BIGINT :
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        s += 8;
                        break;

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        s += 8;

                        BigDecimal bigdecimal = (BigDecimal) o;
                        BigInteger bigint =
                            JavaSystem.unscaledValue(bigdecimal);

                        s += bigint.toByteArray().length;
                        break;

                    case Types.SQL_BOOLEAN :
                        s += 1;
                        break;

                    case Types.SQL_DATE :
                    case Types.SQL_TIME :
                        s += 8;
                        break;

                    case Types.SQL_TIMESTAMP :
                        s += 12;
                        break;

                    case Types.SQL_BINARY :
                    case Types.SQL_VARBINARY :
                        s += 4;
                        s += ((BinaryData) o).length();
                        break;

                    case Types.OTHER :
                        JavaObjectData jo = (JavaObjectData) o;

                        s += 4;
                        s += jo.getBytesLength();
                        break;

                    default :
                        Trace.printSystemOut(
                            Trace.FUNCTION_NOT_SUPPORTED + " "
                            + types[i].getName());
                }
            }
        }

        return s;
    }

    /**
     * @param  extra amount of extra space
     */
    public void ensureRoom(int extra) {
        super.ensureRoom(extra);
    }

    public void reset() {

        super.reset();

        storageSize = 0;
    }

    public void reset(int newSize) {

        super.reset(newSize);

        storageSize = 0;
    }

    public void setBuffer(byte[] buffer) {

        buf = buffer;

        reset();
    }
}
