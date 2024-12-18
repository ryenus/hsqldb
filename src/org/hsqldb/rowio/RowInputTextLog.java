/* Copyright (c) 2001-2025, The HSQL Development Group
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

import org.hsqldb.Scanner;
import org.hsqldb.Session;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.map.ValuePool;
import org.hsqldb.scriptio.StatementLineTypes;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

/**
 * Class for reading the data for a database row from the script file.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.7.3
 */
public class RowInputTextLog extends RowInputBase implements RowInputInterface {

    Scanner scanner;
    String  tableName  = null;
    String  schemaName = null;
    int     statementType;
    Object  value;
    boolean noSeparators;

    public RowInputTextLog() {
        super(new byte[0]);

        scanner = new Scanner();
    }

    public void setSource(Session session, String text) {

        scanner.reset(session, text);

        statementType = StatementLineTypes.ANY_STATEMENT;

        scanner.scanNext();

        int tokenType = scanner.getTokenType();

        switch (tokenType) {

            case Tokens.INSERT : {
                statementType = StatementLineTypes.INSERT_STATEMENT;

                scanner.scanNext();

                // scanner.getTokenType() == Tokens.INTO;
                scanner.scanNext();

                tableName = scanner.getString();

                scanner.scanNext();
                break;
            }

            case Tokens.DELETE : {
                statementType = StatementLineTypes.DELETE_STATEMENT;

                scanner.scanNext();

                // scanner.getTokenType() == Tokens.FROM;
                scanner.scanNext();

                tableName = scanner.getString();
                break;
            }

            case Tokens.COMMIT : {
                statementType = StatementLineTypes.COMMIT_STATEMENT;
                break;
            }

            case Tokens.SET : {
                scanner.scanNext();

                tokenType = scanner.getTokenType();

                if (tokenType == Tokens.SCHEMA) {
                    scanner.scanNext();

                    schemaName    = scanner.getString();
                    statementType = StatementLineTypes.SET_SCHEMA_STATEMENT;
                }

                break;
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

        boolean minus = scanner.getTokenType() == Tokens.MINUS_OP;

        if (minus) {
            scanner.scanNext();
        }

        value = scanner.getValue();

        if (minus) {
            try {
                value = scanner.getDataType().negate(value);
            } catch (HsqlException e) {}
        }
    }

    protected void readFieldPrefix() {

        if (!noSeparators) {
            scanner.scanNext();

            if (statementType == StatementLineTypes.DELETE_STATEMENT) {
                scanner.scanNext();
                scanner.scanNext();
            }
        }
    }

    public String readString() {
        readField();

        return (String) value;
    }

    public char readChar() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputTextLog");
    }

    public byte readByte() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputTextLog");
    }

    public short readShort() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputTextLog");
    }

    public int readInt() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputTextLog");
    }

    public long readLong() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputTextLog");
    }

    public int readType() {
        return 0;
    }

    protected boolean readNull() {

        // Return null on each column read instead.
        return false;
    }

    protected String readChar(Type type) {
        readField();

        return (String) value;
    }

    protected Integer readSmallint() {
        readNumberField(Type.SQL_SMALLINT);

        return (Integer) value;
    }

    protected Integer readInteger() {

        readNumberField(Type.SQL_INTEGER);

        if (value instanceof Long) {
            value = Type.SQL_INTEGER.convertToDefaultType(null, value);
        }

        return (Integer) value;
    }

    protected Long readBigint() {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (Long) Type.SQL_BIGINT.convertToDefaultType(null, value);
        }

        return ValuePool.getLong(((Number) value).longValue());
    }

    protected Double readReal() {

        readNumberField(Type.SQL_DOUBLE);

        if (value == null) {
            return null;
        }

        if (scanner.scanSpecialIdentifier(Tokens.T_DIVIDE_OP)) {
            scanner.scanNext();

            Object divisor = scanner.getValue();
            double i       = ((Number) divisor).doubleValue();

            if (i == 0) {
                if (((Number) value).doubleValue() == 1E0) {
                    i = Double.POSITIVE_INFINITY;
                } else if (((Number) value).doubleValue() == -1E0) {
                    i = Double.NEGATIVE_INFINITY;
                } else if (((Number) value).doubleValue() == 0E0) {
                    i = Double.NaN;
                } else {
                    throw Error.error(ErrorCode.X_42585);
                }
            } else {
                throw Error.error(ErrorCode.X_42585);
            }

            value = Double.valueOf(i);
        }

        return (Double) value;
    }

    protected BigDecimal readDecimal(Type type) {

        readNumberField(type);

        if (value == null) {
            return null;
        }

        BigDecimal bd = (BigDecimal) type.convertToDefaultType(null, value);

        return bd;
    }

    protected TimeData readTime(Type type) {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newTime((String) value);
    }

    protected TimestampData readDate(Type type) {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newDate((String) value);
    }

    protected TimestampData readTimestamp(Type type) {

        readField();

        if (value == null) {
            return null;
        }

        return scanner.newTimestamp((String) value);
    }

    protected IntervalMonthData readYearMonthInterval(Type type) {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalMonthData) scanner.newInterval(
            (String) value,
            (IntervalType) type);
    }

    protected IntervalSecondData readDaySecondInterval(Type type) {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalSecondData) scanner.newInterval(
            (String) value,
            (IntervalType) type);
    }

    protected Boolean readBoole() {

        readFieldPrefix();
        scanner.scanNext();

        String token = scanner.getString();

        value = null;

        if (token.equalsIgnoreCase(Tokens.T_TRUE)) {
            value = Boolean.TRUE;
        } else if (token.equalsIgnoreCase(Tokens.T_FALSE)) {
            value = Boolean.FALSE;
        }

        return (Boolean) value;
    }

    protected Object readOther() {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BINARY_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return new JavaObjectData(((BinaryData) value).getBytes());
    }

    protected BinaryData readBit() {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBitStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BIT_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected BinaryData readUUID() {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanUUIDStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BINARY_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected BinaryData readBinary() {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BINARY_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected ClobData readClob() {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new ClobDataID(id);
    }

    protected BlobData readBlob() {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new BlobDataID(id);
    }

    protected Object[] readArray(Type type) {

        type = type.collectionBaseType();

        readFieldPrefix();
        scanner.scanNext();

        String token = scanner.getString();

        value = null;

        if (token.equalsIgnoreCase(Tokens.T_NULL)) {
            return null;
        } else if (!token.equalsIgnoreCase(Tokens.T_ARRAY)) {
            throw Error.error(ErrorCode.X_42584);
        }

        scanner.scanNext();

        token = scanner.getString();

        if (!token.equalsIgnoreCase(Tokens.T_LEFTBRACKET)) {
            throw Error.error(ErrorCode.X_42584);
        }

        HsqlArrayList<Object> list = new HsqlArrayList<>();

        noSeparators = true;

        for (int i = 0; ; i++) {
            if (scanner.scanSpecialIdentifier(Tokens.T_RIGHTBRACKET)) {
                break;
            }

            if (i > 0) {
                if (!scanner.scanSpecialIdentifier(Tokens.T_COMMA)) {
                    throw Error.error(ErrorCode.X_42584);
                }
            }

            Object value = readData(type);

            list.add(value);
        }

        noSeparators = false;

        Object[] data = new Object[list.size()];

        list.toArray(data);

        return data;
    }
}
