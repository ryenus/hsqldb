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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataMemory;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataMemory;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.Type;

/**
 *  Class for reading the data for a database row in text table format.
 *
 * @author sqlbob@users (RMP)
 * @version 1.8.0
 * @since 1.7.0
 */
public class RowInputText extends RowInputBase implements RowInputInterface {

    // text table specific
    private String    fieldSep;
    private String    varSep;
    private String    longvarSep;
    private int       fieldSepLen;
    private int       varSepLen;
    private int       longvarSepLen;
    private boolean   fieldSepEnd;
    private boolean   varSepEnd;
    private boolean   longvarSepEnd;
    private int       textLen;
    protected String  text;
    protected int     line;
    protected int     field;
    protected int     next = 0;
    protected boolean allQuoted;

    /**
     * fredt@users - comment - in future may use a custom subclasse of
     * InputStream to read the data.
     *
     * author: sqlbob@users (RMP)
     */
    public RowInputText(String fieldSep, String varSep, String longvarSep,
                        boolean allQuoted) {

        super(new byte[0]);

        //-- Newline indicates that field should match to end of line.
        if (fieldSep.endsWith("\n")) {
            fieldSepEnd = true;
            fieldSep    = fieldSep.substring(0, fieldSep.length() - 1);
        }

        if (varSep.endsWith("\n")) {
            varSepEnd = true;
            varSep    = varSep.substring(0, varSep.length() - 1);
        }

        if (longvarSep.endsWith("\n")) {
            longvarSepEnd = true;
            longvarSep    = longvarSep.substring(0, longvarSep.length() - 1);
        }

        this.allQuoted  = allQuoted;
        this.fieldSep   = fieldSep;
        this.varSep     = varSep;
        this.longvarSep = longvarSep;
        fieldSepLen     = fieldSep.length();
        varSepLen       = varSep.length();
        longvarSepLen   = longvarSep.length();
    }

    public void setSource(String text, int pos, int byteSize) {

        size      = byteSize;
        this.text = text;
        textLen   = text.length();
        filePos   = pos;
        next      = 0;

        line++;

        field = 0;
    }

    protected String getField(String sep, int sepLen,
                              boolean isEnd) throws IOException {

        String s = null;

        try {
            int start = next;

            field++;

            if (isEnd) {
                if ((next >= textLen) && (sepLen > 0)) {
                    throw Trace.error(Trace.TextDatabaseRowInput_getField);
                } else if (text.endsWith(sep)) {
                    next = textLen - sepLen;
                } else {
                    throw Trace.error(Trace.TextDatabaseRowInput_getField2);
                }
            } else {
                next = text.indexOf(sep, start);

                if (next == -1) {
                    next = textLen;
                }
            }

            if (start > next) {
                start = next;
            }

            s    = text.substring(start, next);
            next += sepLen;
            s    = s.trim();

            if (s.length() == 0) {
                s = null;
            }
        } catch (Exception e) {
            throw new IOException(
                Trace.getMessage(
                    Trace.TextDatabaseRowInput_getField3, true, new Object[] {
                new Integer(field), e.toString()
            }));
        }

        return s;
    }

    public String readString() throws IOException {
        return getField(fieldSep, fieldSepLen, fieldSepEnd);
    }

    private String readVarString() throws IOException {
        return getField(varSep, varSepLen, varSepEnd);
    }

    /**
     * Obsoleted in 1.9.0
     */
    private String readLongVarString() throws IOException {
        return getField(longvarSep, longvarSepLen, longvarSepEnd);
    }

    public short readShort() throws IOException {
        return (short) readInt();
    }

    public int readInt() throws IOException {

        String s = readString();

        if (s == null) {
            return 0;
        }

        s = s.trim();

        if (s.length() == 0) {
            return 0;
        }

        return Integer.parseInt(s);
    }

    public long readLong() throws IOException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "RowInputText");
    }

    public int readType() throws IOException {
        return 0;
    }

    protected boolean checkNull() {

        // Return null on each column read instead.
        return false;
    }

    protected String readChar(Type type) throws IOException {

        switch (type.type) {

            case Types.SQL_CHAR :
                return readString();

            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                return readVarString();

            default :
                return readLongVarString();
        }
    }

    protected Integer readSmallint() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return Integer.valueOf(s);
    }

    protected Integer readInteger() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return Integer.valueOf(s);
    }

    protected Long readBigint() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return Long.valueOf(s);
    }

    protected Double readReal() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return Double.valueOf(s);
    }

    protected BigDecimal readDecimal() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return new BigDecimal(s);
    }

    protected TimeData readTime(Type type) throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return (TimeData) type.convertToTypeLimits(
            DateTimeType.newTime(s).value);
    }

    protected Date readDate(Type type) throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return HsqlDateTime.dateValue(s);
    }

    protected Timestamp readTimestamp(Type type)
    throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return HsqlDateTime.timestampValue(s);
    }

    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return (IntervalMonthData) ((IntervalType) type).newInterval(s);
    }

    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return (IntervalSecondData) ((IntervalType) type).newInterval(s);
    }

    protected Boolean readBit() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return s.equalsIgnoreCase("TRUE") ? Boolean.TRUE
                                          : Boolean.FALSE;
    }

    protected Object readOther() throws IOException, HsqlException {

        byte[] data;
        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        data = StringConverter.hexToByteArray(s);

        return new JavaObjectData(data);
    }

    protected BinaryData readBinary() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return new BinaryData(StringConverter.hexToByteArray(s), false);
    }

    protected ClobData readClob() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return new ClobDataMemory(s.toCharArray(), false);
    }

    protected BlobData readBlob() throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        byte[] bytes = StringConverter.hexToByteArray(s);

        return new BlobDataMemory(bytes, false);
    }

    public int getLineNumber() {
        return line;
    }

    public void skippedLine() {
        line++;
    }

    public void reset() {

        text    = "";
        textLen = 0;
        filePos = 0;
        next    = 0;
        field   = 0;
        line    = 0;
    }
}
