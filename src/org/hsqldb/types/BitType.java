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

    public boolean requiresPrecision() {
        return type == Types.SQL_BIT_VARYING;
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (precision == 0) {
            return a;
        }

        long len = ((BlobData) a).length() * 8;

        if (len == precision) {
            return a;
        } else if (len > precision) {
            if (getRightTrimSize((BlobData) a) > precision) {
                throw Trace.error(Trace.STRING_DATA_TRUNCATION);
            }

            switch (type) {

                case Types.SQL_BINARY :
                case Types.SQL_VARBINARY : {
                    byte[] data = ((BlobData) a).getBytes(0, (int) precision);

                    return new BinaryData(data, false);
                }
                case Types.SQL_BLOB : {
                    byte[] data = ((BlobData) a).getBytes(0, (int) precision);

                    return new BlobDataMemory(data, false);
                }
            }
        } else {
            if (type == Types.SQL_BINARY) {
                byte[] data = new byte[(int) precision];

                System.arraycopy(((BlobData) a).getBytes(), 0, data, 0,
                                 (int) len);

                return new BinaryData(data, false);
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

                if (b.length() > precision) {
                    byte[] data = b.getBytes(0, (int) precision);

                    b = new BinaryData(data, false);
                } else if (b.length() < precision) {
                    byte[] data = (byte[]) ArrayUtil.resizeArray(b.getBytes(),
                        (int) precision);

                    b = new BinaryData(data, false);
                }

                return b;
            }
            case Types.SQL_VARBINARY : {
                BinaryData b = (BinaryData) a;

                if (b.length() > precision) {
                    byte[] data = b.getBytes(0, (int) precision);

                    b = new BinaryData(data, false);
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

        if (otherType.isBinaryType()) {
            return convertToTypeLimits(a);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        } else if (a instanceof BinaryData) {
            return a;
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToBitString(
            ((BlobData) a).getBytes(), (int) ((BlobData) a).bitLength());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.byteArrayToSQLHexString(
            ((BinaryData) a).getBytes());
    }

    public long position(BlobData data, BlobData otherData, Type otherType,
                         long offset) throws HsqlException {

        if (data == null || otherData == null) {
            return -1L;
        }

        long otherLength = ((BlobData) data).length();

        if (offset + otherLength > data.length()) {
            return -1;
        }

        return data.position(otherData, offset);
    }

    public BlobData substring(BlobData data, long offset, long length,
                              boolean hasLength) throws HsqlException {

        long end;
        long dataLength = data.length();

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

        // change method signature to take long
        byte[] bytes = ((BlobData) data).getBytes(offset, (int) length);

        if (data instanceof BinaryData) {
            return new BinaryData(bytes, false);
        } else {
            return new BlobDataMemory(bytes, false);
        }
    }

    int getRightTrimSize(BlobData data) {

        int endindex = super.getRightTrimSize(data);

        if (endindex == 0) {
            return 0;
        }

        endindex--;

        byte[] bytes   = data.getBytes();
        byte   endbyte = bytes[endindex];
        int    bits    = 0;

        for (int i = 7; i >= 0; i--) {
            if (BitMap.isSet(endbyte, i)) {
                bits = i + 1;

                break;
            }
        }

        return endindex * 8 + bits;
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        return new BinaryData((BlobData) a, (BlobData) b);
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
