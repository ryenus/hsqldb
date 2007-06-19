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


package org.hsqldb.types;

import org.hsqldb.Expression;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.store.BitMap;

/**
 *
 * Operations allowed on BIT strings are CONCAT, SUBSTRING, POSITION,
 * BIT_LENGTH and OCTECT_LENGTH
 *
 */
public class BitType extends BinaryType {

    public BitType(int type, long precision) {
        super(type, precision);
    }

    public int displaySize() {
        return (int) precision;
    }

    public int getJDBCTypeNumber() {
        return Types.BIT;
    }

    public String getJDBCClassName() {
        return "[B";
    }

    public int getSQLGenericTypeNumber() {
        return type;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getNameString() {
        return Token.T_BIT;
    }

    public String getDefinition() {
        return Token.T_BIT;
    }

    public boolean isBitType() {
        return true;
    }

    public boolean requiresPrecision() {
        return type == Types.SQL_BIT_VARYING;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return precision >= other.precision ? this
                                                : other;
        }

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
                return precision >= other.precision ? this
                                                    : getBitType(type,
                                                    other.precision);

            case Types.SQL_BIT_VARYING :
                return other.precision >= precision ? other
                                                    : getBitType(other.type,
                                                    precision);

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return other.getAggregateType(this);

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    /**
     * Returns type for concat
     */
    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {

        if (operation != Expression.CONCAT) {
            return getAggregateType(other);
        }

        Type newType;
        long otherPrecision = other.precision;

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BIT :
                newType = this;
                break;

            case Types.SQL_BIT_VARYING :
                newType = other;
                break;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return other.getCombinedType(this, operation);

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        return getBitType(newType.type, precision + otherPrecision);
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        if (precision == 0) {
            return a;
        }

        long len = ((BlobData) a).bitLength();

        if (len == precision) {
            return a;
        } else if (len > precision) {
            if (getRightTrimSize((BlobData) a) > precision) {
                throw Trace.error(Trace.STRING_DATA_TRUNCATION);
            }

            switch (type) {

                case Types.SQL_BIT :
                case Types.SQL_BIT_VARYING : {
                    byte[] data =
                        ((BlobData) a).getBytes(0, (int) (precision + 7) / 8);

                    return new BinaryData(data, precision);
                }
            }
        } else {
            if (type == Types.SQL_BIT) {
                byte[] data = new byte[(int) (precision + 7) / 8];

                System.arraycopy(((BlobData) a).getBytes(), 0, data, 0,
                                 ((BlobData) a).getBytes().length);

                return new BinaryData(data, precision);
            }
        }

        return a;
    }

    public Object castToType(Session session, Object a,
                             Type otherType) throws HsqlException {

        if (a == null) {
            return null;
        }

        a = convertToType(session, a, otherType);

        switch (type) {

            case Types.SQL_BINARY : {
                BinaryData b = (BinaryData) a;

                if (b.bitLength() > precision) {
                    byte[] data = b.getBytes(0, (int) (precision + 7) / 8);

                    for (int i = (int) precision; i < data.length * 8; i++) {
                        BitMap.unset(data, i);
                    }

                    b = new BinaryData(data, precision);
                } else if (b.bitLength() < precision) {
                    byte[] data = (byte[]) ArrayUtil.resizeArray(b.getBytes(),
                        (int) (precision + 7) / 8);

                    b = new BinaryData(data, precision);
                }

                return b;
            }
            case Types.SQL_VARBINARY : {
                BinaryData b = (BinaryData) a;

                if (b.length() > precision) {
                    byte[] data = b.getBytes(0, (int) (precision + 7) / 8);

                    for (int i = (int) precision; i < data.length * 8; i++) {
                        BitMap.unset(data, i);
                    }

                    b = new BinaryData(data, precision);
                }

                return b;
            }
            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return null;
        }

        if (otherType.isBitType()) {
            return convertToTypeLimits(a);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToBitString(((BlobData) a).getBytes(),
                (int) ((BlobData) a).bitLength());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.byteArrayToSQLHexString(
            ((BinaryData) a).getBytes());
    }

// todo
    public long position(BlobData data, BlobData otherData, Type otherType,
                         long offset) throws HsqlException {

        if (data == null || otherData == null) {
            return -1L;
        }

        long otherLength = ((BlobData) data).bitLength();

        if (offset + otherLength > data.bitLength()) {
            return -1;
        }

//        return data.position(otherData, offset);
        throw Trace.error(Trace.UNSUPPORTED_INTERNAL_OPERATION);
    }

    public BlobData substring(BlobData data, long offset, long length,
                              boolean hasLength) throws HsqlException {

        long end;
        long dataLength = data.bitLength();

        if (hasLength) {
            end = offset + length;
        } else {
            end = dataLength > offset ? dataLength
                                      : offset;
        }

        if (end < offset) {
            throw Trace.error(Trace.SQL_DATA_SUBSTRING_ERROR);
        }

        if (offset > end || end < 0) {

            // return zero length data
            offset = 0;
            end    = 0;
        }

        if (offset < 0) {
            offset = 0;
        }

        if (end > dataLength) {
            end = dataLength;
        }

        length = end - offset;

        byte[] dataBytes = data.getBytes();
        byte[] bytes     = new byte[(int) (length + 7) / 8];

        for (int i = (int) offset; i < end; i++) {
            if (BitMap.isSet(dataBytes, i)) {
                BitMap.set(bytes, i - (int) offset);
            }
        }

        return new BinaryData(bytes, length);
    }

    int getRightTrimSize(BlobData data) {

        int    i     = (int) data.bitLength() - 1;
        byte[] bytes = data.getBytes();

        for (; i >= 0; i--) {
            if (BitMap.isSet(bytes, i)) {
                break;
            }
        }

        return i + 1;
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        long length = ((BlobData) a).bitLength() + ((BlobData) b).bitLength();

        if (length > Integer.MAX_VALUE) {

            // todo throw
        }

        byte[] bData = ((BlobData) b).getBytes();

        int aLength = ((BlobData) a).getBytes().length;
        int bLength = ((BlobData) b).getBytes().length;
        byte[] bytes = new byte[(int) (length + 7) / 8];

        System.arraycopy(((BlobData) a).getBytes(), 0, bytes, 0,
                         aLength);

        for (int i = 0; i < bLength; i++) {
            if (BitMap.isSet(bData, i)) {
                BitMap.set(bytes, aLength + i);
            }
        }


        return new BinaryData(bytes, length);
    }

    public long size(Object a) {

        if (a == null) {
            return -1;
        }

        return ((BlobData) a).length();
    }

    /**
     * todo check and adjust max precision
     */
    public static BinaryType getBitType(int type, long precision) {

        switch (type) {

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return new BitType(type, precision);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "BinaryType");
        }
    }
}
