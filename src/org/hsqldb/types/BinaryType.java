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

/**
 * Type implementation for BINARY, VARBINARY, etc.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class BinaryType extends Type {

    public BinaryType(int type, long precision) {
        super(type, precision, 0);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeNumber() {
        return type == Types.SQL_BINARY ? Types.BINARY
                                        : Types.VARBINARY;
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
        return type == Types.SQL_BINARY ? Token.T_BINARY
                                        : Token.T_VARBINARY;
    }

    public String getDefinition() {
        return type == Types.SQL_BINARY ? Token.T_BINARY
                                        : Token.T_VARBINARY;
    }

    public boolean isBinaryType() {
        return true;
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return type == Types.SQL_VARBINARY;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return precision >= other.precision ? this
                                                : other;
        }

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BINARY :
                return precision >= other.precision ? this
                                                    : getBinaryType(type,
                                                    other.precision);

            case Types.SQL_VARBINARY :
                if (type == Types.SQL_BLOB) {
                    return precision >= other.precision ? this
                                                        : getBinaryType(type,
                                                        other.precision);
                } else {
                    return other.precision >= precision ? other
                                                        : getBinaryType(
                                                        other.type,
                                                        precision);
                }
            case Types.SQL_BLOB :
                return other.precision >= precision ? other
                                                    : getBinaryType(
                                                    other.type, precision);

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

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_BINARY :
                newType = this;
                break;

            case Types.SQL_VARBINARY :
                newType = (type == Types.SQL_BLOB) ? this
                                                   : other;
                break;

            case Types.SQL_BLOB :
                newType = other;
                break;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        return getBinaryType(newType.type, precision + other.precision);
    }

    public int compare(Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (a instanceof BinaryData && b instanceof BinaryData) {
            int i = compareTo(((BinaryData) a).getBytes(),
                              ((BinaryData) b).getBytes());

            return (i == 0) ? 0
                            : (i < 0 ? -1
                                     : 1);
        }

        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BinaryType");
    }

    /*
     * Compares a <CODE>byte[]</CODE> with another specified
     * <CODE>byte[]</CODE> for order.  Returns a negative integer, zero,
     * or a positive integer as the first object is less than, equal to, or
     * greater than the specified second <CODE>byte[]</CODE>.<p>
     *
     * @param o1 the first byte[] to be compared
     * @param o2 the second byte[] to be compared
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    static int compareTo(byte[] o1, byte[] o2) {

        int len  = o1.length;
        int lenb = o2.length;

        for (int i = 0; ; i++) {
            int a = 0;
            int b = 0;

            if (i < len) {
                a = ((int) o1[i]) & 0xff;
            } else if (i >= lenb) {
                return 0;
            }

            if (i < lenb) {
                b = ((int) o2[i]) & 0xff;
            }

            if (a > b) {
                return 1;
            }

            if (b > a) {
                return -1;
            }
        }
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (precision == 0) {
            return a;
        }

        long len = ((BlobData) a).length();

        if (len == precision) {
            return a;
        } else if (len > precision) {
            if (getRightTrimSize((BlobData) a, 0) > precision) {
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

        return StringConverter.byteArrayToHex(((BlobData) a).getBytes());
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

    private int getRightTrimSize(BlobData data, int trim) {

        byte[] bytes    = data.getBytes();
        int    endindex = bytes.length;

        for (--endindex; endindex >= 0 && bytes[endindex] == trim;
                endindex--) {}

        return ++endindex;
    }

    public BlobData trim(Session session, BlobData data, int trim,
                         boolean leading,
                         boolean trailing) throws HsqlException {

        if (data == null) {
            return null;
        }

        byte[] bytes    = ((BlobData) data).getBytes();
        int    endindex = bytes.length;

        if (trailing) {
            for (--endindex; endindex >= 0 && bytes[endindex] == trim;
                    endindex--) {}

            endindex++;
        }

        int startindex = 0;

        if (leading) {
            while (startindex < endindex && bytes[startindex] == trim) {
                startindex++;
            }
        }

        byte[] newBytes = bytes;

        if (startindex != 0 || endindex != bytes.length) {
            newBytes = new byte[endindex - startindex];

            System.arraycopy(bytes, startindex, newBytes, 0,
                             endindex - startindex);
        }

        if (type == Types.SQL_BLOB) {
            BlobData blob = new BlobDataMemory(newBytes, newBytes == bytes);

            blob.setId(session.getLobId());
            session.database.lobManager.addBlob(blob);

            return blob;
        } else {
            return new BinaryData(newBytes, newBytes == bytes);
        }
    }

    public BlobData overlay(Session session, BlobData data, BlobData overlay,
                            long offset, long length,
                            boolean hasLength) throws HsqlException {

        if (data == null || overlay == null) {
            return null;
        }

        if (!hasLength) {
            length = ((BlobData) overlay).length();
        }

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY : {
                BinaryData binary =
                    new BinaryData(substring(data, 0, offset, true), overlay);

                binary = new BinaryData(binary,
                                        substring(data, offset + length, 0,
                                                  false));

                return binary;
            }
            case Types.SQL_BLOB : {
                BlobData blob =
                    new BlobDataMemory(substring(data, 0, offset, true),
                                       overlay);

                blob = new BlobDataMemory(blob,
                                          substring(data, offset + length, 0,
                                                    false));

                blob.setId(session.getLobId());
                session.database.lobManager.addBlob(blob);

                return blob;
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "BinaryType");
        }
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        if (type == Types.SQL_BLOB) {
            BlobData blob = new BlobDataMemory((BlobData) a, (BlobData) b);

            blob.setId(session.getLobId());
            session.database.lobManager.addBlob(blob);

            return blob;
        } else {
            return new BinaryData((BlobData) a, (BlobData) b);
        }
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
    public static BinaryType getBinaryType(int type, long precision) {

        switch (type) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return new BinaryType(type, precision);

            case Types.SQL_BLOB :
                return new BlobType(precision);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "BinaryType");
        }
    }
}
