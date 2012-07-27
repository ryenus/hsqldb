/* Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.Scanner;
import org.hsqldb.Tokens;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

/**
 * Class for reading the data for a database row from the script file.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.7.3
 */
public class RowInputTextLog extends RowInputBase
implements RowInputInterface {

    Scanner scanner;
    String  tableName  = null;
    String  schemaName = null;
    int     statementType;
    Object  value;

    public RowInputTextLog() {

        super(new byte[0]);

        scanner = new Scanner();
    }

    public void setSource(String text) throws HsqlException {

        scanner.reset(text);

        statementType = ScriptReaderBase.ANY_STATEMENT;

        scanner.scanNext();

        String s = scanner.getString();

        if (s.equals(Tokens.T_INSERT)) {
            statementType = ScriptReaderBase.INSERT_STATEMENT;

            scanner.scanNext();
            scanner.scanNext();

            tableName = scanner.getString();

            scanner.scanNext();
        } else if (s.equals(Tokens.T_DELETE)) {
            statementType = ScriptReaderBase.DELETE_STATEMENT;

            scanner.scanNext();
            scanner.scanNext();

            tableName = scanner.getString();
        } else if (s.equals(Tokens.T_COMMIT)) {
            statementType = ScriptReaderBase.COMMIT_STATEMENT;
        } else if (s.equals(Tokens.T_SET)) {
            scanner.scanNext();

            if (Tokens.T_SCHEMA.equals(scanner.getString())) {
                scanner.scanNext();

                schemaName    = scanner.getString();
                statementType = ScriptReaderBase.SET_SCHEMA_STATEMENT;
            }
        }
    }

    public int getStatementType() {
        return statementType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    protected void readField() {

        readFieldPrefix();
        scanner.scanNext();

        value = scanner.getValue();
    }

    protected void readNumberField(Type type) {

        readFieldPrefix();
        scanner.scanNext();

        boolean minus = scanner.getTokenType() == Tokens.MINUS;

        if (minus) {
            scanner.scanNext();
        }

        value = scanner.getValue();

        if (minus) {
            try {
                value = ((NumberType) scanner.getDataType()).negate(value);
            } catch (HsqlException e) {}
        }
    }

    protected void readFieldPrefix() {

        scanner.scanNext();

        if (statementType == ScriptReaderBase.DELETE_STATEMENT) {
            scanner.scanNext();
            scanner.scanNext();
        }
    }

    public String readString() throws IOException {

        readField();

        return (String) value;
    }

    public short readShort() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public int readInt() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public long readLong() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public int readType() throws IOException {
        return 0;
    }

    protected boolean checkNull() {

        // Return null on each column read instead.
        return false;
    }

    protected String readChar(Type type) throws IOException {

        readField();

        return (String) value;
    }

    protected Integer readSmallint() throws IOException, HsqlException {

        readNumberField(Type.SQL_SMALLINT);

        return (Integer) value;
    }

    protected Integer readInteger() throws IOException, HsqlException {

        readNumberField(Type.SQL_INTEGER);

        return (Integer) value;
    }

    protected Long readBigint() throws IOException, HsqlException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        return ValuePool.getLong(((Number) value).longValue());
    }

    protected Double readReal() throws IOException, HsqlException {

        readNumberField(Type.SQL_DOUBLE);

        if (value == null) {
            return null;
        }

/*
        if (tokenizer.isGetThis(Token.T_DIVIDE)) {
            s = tokenizer.getString();

            // parse simply to ensure it's a number
            double ii = JavaSystem.parseDouble(s);

            if (i == 0E0) {
                i = Double.NaN;
            } else if (i == -1E0) {
                i = Double.NEGATIVE_INFINITY;
            } else if (i == 1E0) {
                i = Double.POSITIVE_INFINITY;
            }
        }
*/
        return (Double) value;
    }

    protected BigDecimal readDecimal(Type type)
    throws IOException, HsqlException {

        readNumberField(type);

        if (value == null) {
            return null;
        }

        return (BigDecimal) type.convertToDefaultType(null, value);
    }

    protected TimeData readTime(Type type) throws IOException, HsqlException {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newTime((String) value);
    }

    protected TimestampData readDate(Type type)
    throws IOException, HsqlException {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newDate((String) value);
    }

    protected TimestampData readTimestamp(Type type)
    throws IOException, HsqlException {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newTimestamp((String) value);
    }

    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException, HsqlException {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalMonthData) scanner.newInterval((String) value,
                (IntervalType) type);
    }

    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException, HsqlException {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalSecondData) scanner.newInterval((String) value,
                (IntervalType) type);
    }

    protected Boolean readBoole() throws IOException, HsqlException {

        readField();

        return (Boolean) value;
    }

    protected Object readOther() throws IOException, HsqlException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryString();

        value = scanner.getValue();

        return new JavaObjectData(((BinaryData) value).getBytes());
    }

    protected BinaryData readBit() throws IOException, HsqlException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBitString();

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected BinaryData readBinary() throws IOException, HsqlException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryString();

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected ClobData readClob() throws IOException, HsqlException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new ClobDataID(id);
    }

    protected BlobData readBlob() throws IOException, HsqlException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new BlobDataID(id, 0);
    }
}