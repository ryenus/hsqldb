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
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.Column;
import org.hsqldb.Table;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.Type;

/**
 * Base class for writing the data for a database row in different formats.
 * Defines the methods that are independent of storage format and declares
 * the format-dependent methods that subclasses should define.
 *
 * @author sqlbob@users (RMP)
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.0
 */
abstract class RowOutputBase extends HsqlByteArrayOutputStream
implements RowOutputInterface {

    public static final int CACHED_ROW_160 = 0;
    public static final int CACHED_ROW_170 = 1;

    // the last column in a table is an ID that should not be written to file
    protected boolean skipSystemId = false;

    /**
     *  Constructor used for persistent storage of a Table row
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase() {
        super();
    }

    /**
     *  Constructor used for result sets
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase(int initialSize) {
        super(initialSize);
    }

    /**
     *  Constructor used for network transmission of result sets
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase(byte[] buffer) {
        super(buffer);
    }

// fredt@users - comment - methods for writing Result column type, name and data size
    public abstract void writeEnd();

    public abstract void writeSize(int size);

    public abstract void writeType(int type);

    public abstract void writeIntData(int i, int position);

    public abstract void writeString(String s);

// fredt@users - comment - methods used for writing each SQL type
    protected void writeFieldPrefix() {}

    protected abstract void writeFieldType(Type type);

    protected abstract void writeNull(Type type);

    protected abstract void writeChar(String s, Type t);

    protected abstract void writeSmallint(Number o);

    protected abstract void writeInteger(Number o);

    protected abstract void writeBigint(Number o);

    protected abstract void writeReal(Double o);

    protected abstract void writeDecimal(BigDecimal o, Type type);

    protected abstract void writeBoolean(Boolean o);

    protected abstract void writeDate(Date o, Type type);

    protected abstract void writeTime(TimeData o, Type type);

    protected abstract void writeTimestamp(Timestamp o, Type type);

    protected abstract void writeYearMonthInterval(IntervalMonthData o, Type type);

    protected abstract void writeDaySecondInterval(IntervalSecondData o, Type type);

    protected abstract void writeOther(JavaObjectData o);

    protected abstract void writeBinary(BinaryData o);

    protected abstract void writeClob(ClobData o, Type type);

    protected abstract void writeBlob(BlobData o, Type type);

    public void writeRow(Object[] data, Table t) {

        writeSize(0);
        writeData(data, t);
        writeIntData(size(), 0);
    }

    /**
     *  This method is called to write data for a table.
     *
     * @param  data
     * @param  t
     * @throws  IOException
     */
    public void writeData(Object[] data, Table t) {

        Type[] types = t.getColumnTypes();
        int    l     = t.getColumnCount();

        writeData(l, types, data, null, null);
    }

    /**
     *  This method is called to write data for a Result.
     *
     * @param  l
     * @param  types
     * @param  data
     * @param cols
     * @param primarykeys
     * @throws  IOException
     */
    public void writeData(int l, Type[] types, Object[] data,
                          HashMappedList cols, int[] primaryKeys) {

        boolean hasPK = primaryKeys != null && primaryKeys.length != 0;
        int     limit = hasPK ? primaryKeys.length
                              : l;

        for (int i = 0; i < limit; i++) {
            int    j = hasPK ? primaryKeys[i]
                             : i;
            Object o = data[j];
            Type    t = types[j];

            if (cols != null) {
                Column col = (Column) cols.get(j);

                writeFieldPrefix();
                writeString(col.columnName.statementName);
            }

            if (o == null) {
                writeNull(t);

                continue;
            }

            writeFieldType(t);

            switch (t.type) {

                case Types.SQL_ALL_TYPES :
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION,
                        "RowOutputBase");
                case Types.SQL_CHAR :
                case Types.SQL_VARCHAR :
                case Types.VARCHAR_IGNORECASE :
                    writeChar((String) o, t);
                    break;

                case Types.TINYINT :
                case Types.SQL_SMALLINT :
                    writeSmallint((Number) o);
                    break;

                case Types.SQL_INTEGER :
                    writeInteger((Number) o);
                    break;

                case Types.SQL_BIGINT :
                    writeBigint((Number) o);
                    break;

                case Types.SQL_REAL :
                case Types.SQL_FLOAT :
                case Types.SQL_DOUBLE :
                    writeReal((Double) o);
                    break;

                case Types.SQL_NUMERIC :
                case Types.SQL_DECIMAL :
                    writeDecimal((BigDecimal) o, t);
                    break;

                case Types.SQL_BOOLEAN :
                    writeBoolean((Boolean) o);
                    break;

                case Types.SQL_DATE :
                    writeDate((Date) o, t);
                    break;

                case Types.SQL_TIME :
                    writeTime((TimeData) o, t);
                    break;

                case Types.SQL_TIMESTAMP :
                    writeTimestamp((Timestamp) o, t);
                    break;

                case Types.OTHER :
                    writeOther((JavaObjectData) o);
                    break;

                case Types.SQL_BLOB :
                    writeBlob((BlobData) o, t);
                    break;

                case Types.SQL_CLOB :
                    writeClob((ClobData) o, t);
                    break;

                case Types.SQL_BINARY :
                case Types.SQL_VARBINARY :
                    writeBinary((BinaryData) o);
                    break;

                default :
                    throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                             t.getName());
            }
        }
    }

    // returns the underlying HsqlByteArrayOutputStream
    public HsqlByteArrayOutputStream getOutputStream() {
        return this;
    }
}
