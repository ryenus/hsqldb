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

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataMemory;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataMemory;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.Type;

/**
 *  Provides methods for reading the data for a row from a
 *  byte array. The format of data is that used for storage of cached
 *  tables by v.1.6.x databases, apart from strings.
 *
 * @author sqlbob@users (RMP)
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.0
 */
public class RowInputBinary extends RowInputBase
implements org.hsqldb.rowio.RowInputInterface {

    private RowOutputBinary out;

    public RowInputBinary() {
        super();
    }

    public RowInputBinary(byte[] buf) {
        super(buf);
    }

    /**
     * uses the byte[] buffer from out. At each reset, the buffer is set
     * to the current one for out.
     */
    public RowInputBinary(RowOutputBinary out) {

        super(out.getBuffer());

        this.out = out;
    }

    public int readType() throws IOException {
        return readShort();
    }

    public String readString() throws IOException {

        int    length = readInt();
        String s      = StringConverter.readUTF(buf, pos, length);

        s   = ValuePool.getString(s);
        pos += length;

        return s;
    }

    protected boolean checkNull() throws IOException {

        int b = readByte();

        return b == 0 ? true
                      : false;
    }

    protected String readChar(Type type) throws IOException {
        return readString();
    }

    protected Integer readSmallint() throws IOException, HsqlException {
        return ValuePool.getInt(readShort());
    }

    protected Integer readInteger() throws IOException, HsqlException {
        return ValuePool.getInt(readInt());
    }

    protected Long readBigint() throws IOException, HsqlException {
        return ValuePool.getLong(readLong());
    }

    protected Double readReal() throws IOException, HsqlException {
        return ValuePool.getDouble(readLong());
    }

    protected BigDecimal readDecimal() throws IOException, HsqlException {

        byte[]     bytes  = readByteArray();
        int        scale  = readInt();
        BigInteger bigint = new BigInteger(bytes);

        return ValuePool.getBigDecimal(new BigDecimal(bigint, scale));
    }

    protected Boolean readBit() throws IOException, HsqlException {
        return readBoolean() ? Boolean.TRUE
                             : Boolean.FALSE;
    }

    protected TimeData readTime(Type type) throws IOException, HsqlException {
        return new TimeData(readInt(), readInt());
    }

    protected Date readDate(Type type) throws IOException, HsqlException {

        long date = readLong();

        return ValuePool.getDate(date);
    }

    protected Timestamp readTimestamp(Type type)
    throws IOException, HsqlException {
        return HsqlDateTime.timestampValue(readLong(), readInt());
    }

    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException, HsqlException {

        int months = readInt();

        return new IntervalMonthData(months, (IntervalType) type);
    }

    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException, HsqlException {

        int seconds = readInt();
        int nanos   = readInt();

        return new IntervalSecondData(seconds, nanos, (IntervalType) type);
    }

    protected Object readOther() throws IOException, HsqlException {
        return new JavaObjectData(readByteArray());
    }

    protected BinaryData readBinary() throws IOException, HsqlException {
        return new BinaryData(readByteArray(), false);
    }

    protected ClobData readClob() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        return new ClobDataMemory(s.toCharArray(), false);
    }

    protected BlobData readBlob() throws IOException, HsqlException {

        byte[] bytes = readByteArray();

        return new BlobDataMemory(bytes, false);
    }

    // helper methods
    public byte[] readByteArray() throws IOException {

        byte[] b = new byte[readInt()];

        readFully(b);

        return b;
    }

    public char[] readCharArray() throws IOException {

        char[] c = new char[readInt()];

        if (count - pos < c.length) {
            pos = count;

            throw new EOFException();
        }

        for (int i = 0; i < c.length; i++) {
            int ch1 = buf[pos++] & 0xff;
            int ch2 = buf[pos++] & 0xff;

            c[i] = (char) ((ch1 << 8) + (ch2));
        }

        return c;
    }

    /**
     *  Used to reset the row, ready for Result data to be written into the
     *  byte[] buffer by an external routine.
     *
     */
    public void resetRow(int rowsize) {

        if (out != null) {
            out.reset(rowsize);

            buf = out.getBuffer();
        }

        super.reset();
    }

    /**
     *  Used to reset the row, ready for a new db row to be written into the
     *  byte[] buffer by an external routine.
     *
     */
    public void resetRow(int filepos, int rowsize) throws IOException {

        if (out != null) {
            out.reset(rowsize);

            buf = out.getBuffer();
        }

        super.resetRow(filepos, rowsize);
    }
}