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

import java.math.BigDecimal;

import org.hsqldb.Collation;
import org.hsqldb.Expression;
import org.hsqldb.HsqlException;
import org.hsqldb.Library;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.ValuePool;

/**
 * Type implementation for CHARACTER, VARCHAR, etc.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class CharacterType extends Type {

    Collation                     collation;
    boolean                       isEqualIdentical;
    final static int              defaultCharPrecision = 1;
    public static final Collation defaultCollation     = new Collation();
    public final static CharacterType sqlIdentifierType =
        new CharacterType(Types.SQL_VARCHAR, 128L);

    public CharacterType(Collation collation, int type, long precision) {

        super(type, precision, 0);

        this.collation = defaultCollation;
        isEqualIdentical = this.collation.isEqualAlwaysIdentical()
                           && type != Types.VARCHAR_IGNORECASE;
    }

    public CharacterType(int type, long precision) {

        super(type, precision, 0);

        this.collation = defaultCollation;
        isEqualIdentical = this.collation.isEqualAlwaysIdentical()
                           && type != Types.VARCHAR_IGNORECASE;
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeNumber() {

        switch (type) {

            case Types.SQL_CHAR :
                return Types.CHAR;

            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                return Types.VARCHAR;

            case Types.SQL_CLOB :
                return Types.CLOB;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }

    public String getJDBCClassName() {
        return "java.lang.String";
    }

    public int getSQLGenericTypeNumber() {
        return type == Types.SQL_CHAR ? type
                                      : Types.SQL_VARCHAR;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getNameString() {

        switch (type) {

            case Types.SQL_CHAR :
                return Token.T_CHARACTER;

            case Types.SQL_VARCHAR :
                return Token.T_VARCHAR;

            case Types.VARCHAR_IGNORECASE :
                return Token.T_VARCHAR_IGNORECASE;

            case Types.SQL_CLOB :
                return Token.T_CLOB;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }

    public String getDefinition() {

        if (precision == 0) {
            return getNameString();
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getNameString());
        sb.append('(');
        sb.append(precision);
        sb.append(')');

        return sb.toString();
    }

    public boolean isCharacterType() {
        return true;
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return type == Types.SQL_VARCHAR || type == Types.VARCHAR_IGNORECASE;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return precision >= other.precision ? this
                                                : other;
        }

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_CHAR :
                return precision >= other.precision ? this
                                                    : getCharacterType(type,
                                                    other.precision);

            case Types.SQL_VARCHAR :
                if (type == Types.SQL_CLOB
                        || type == Types.VARCHAR_IGNORECASE) {
                    return precision >= other.precision ? this
                                                        : getCharacterType(
                                                        type, other.precision);
                } else {
                    return other.precision >= precision ? other
                                                        : getCharacterType(
                                                        other.type, precision);
                }
            case Types.VARCHAR_IGNORECASE :
                if (type == Types.SQL_CLOB) {
                    return precision >= other.precision ? this
                                                        : getCharacterType(
                                                        type, other.precision);
                } else {
                    return other.precision >= precision ? other
                                                        : getCharacterType(
                                                        other.type, precision);
                }
            case Types.SQL_CLOB :
                return other.precision >= precision ? other
                                                    : getCharacterType(
                                                    other.type, precision);

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    /**
     * For concatenation
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

            case Types.SQL_CHAR :
                newType = this;
                break;

            case Types.SQL_VARCHAR :
                newType =
                    (type == Types.SQL_CLOB || type == Types
                        .VARCHAR_IGNORECASE) ? this
                                             : other;
                break;

            case Types.VARCHAR_IGNORECASE :
                newType = type == Types.SQL_CLOB ? this
                                                 : other;
                break;

            case Types.SQL_CLOB :
                newType = other;
                break;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        return getCharacterType(newType.type, precision + other.precision);
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

        String as          = (String) a;
        String bs          = (String) b;
        int    la          = as.length();
        int    lb          = bs.length();
        int    shortLength = la > lb ? lb
                                     : la;
        int    result;

        if (type == Types.VARCHAR_IGNORECASE) {
            result = collation.compareIgnoreCase(as.substring(0, shortLength),
                                                 bs.substring(0, shortLength));
        } else {
            result = collation.compare(as.substring(0, shortLength),
                                       bs.substring(0, shortLength));
        }

        if (la == lb || result != 0) {
            return result;
        }

        if (la > lb) {
            as = as.substring(shortLength, la);
            bs = ValuePool.getSpaces(la - shortLength);
        } else {
            as = ValuePool.getSpaces(lb - shortLength);
            bs = bs.substring(shortLength, lb);
        }

        if (type == Types.VARCHAR_IGNORECASE) {
            return collation.compareIgnoreCase(as, bs);
        } else {
            return collation.compare(as, bs);
        }
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (precision == 0) {
            return a;
        }

        if (a == null) {
            return a;
        }

        switch (type) {

            case Types.SQL_CHAR : {
                int slen = ((String) a).length();

                if (slen > precision) {
                    if (Library.rtrim((String) a).length() <= precision) {
                        return ((String) a).substring(0, (int) precision);
                    } else {
                        throw Trace.error(Trace.STRING_DATA_TRUNCATION);
                    }
                }

                char[] b = new char[(int) precision];

                ((String) a).getChars(0, slen, b, 0);

                for (int i = slen; i < precision; i++) {
                    b[i] = ' ';
                }

                return new String(b);
            }
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                int slen = ((String) a).length();

                if (slen > precision) {
                    if (StringUtil.rTrimSize((String) a) <= precision) {
                        return ((String) a).substring(0, (int) precision);
                    } else {
                        throw Trace.error(Trace.STRING_DATA_TRUNCATION);
                    }
                }

                return a;
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }

    public Object castToType(Session session, Object a,
                             Type otherType) throws HsqlException {

        if (a == null) {
            return a;
        }

        switch (otherType.type) {

            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                if (precision != 0 && ((String) a).length() > precision) {
                    if (Library.rtrim((String) a).length() <= precision) {
                        session.addWarning(Trace.error(Trace.GENERIC_WARNING));
                    }

                    return ((String) a).substring(0, (int) precision);
                }

                return convertToTypeLimits(a);

            case Types.SQL_CLOB :
                if (precision != 0 && ((ClobData) a).length() > precision) {
                    String s = ((ClobData) a).toString();

                    if (Library.rtrim(s).length() <= precision) {
                        session.addWarning(Trace.error(Trace.GENERIC_WARNING));
                    }

                    return ((String) a).substring(0, (int) precision);
                }

                return convertToTypeLimits(a);

            case Types.SQL_BLOB :
                throw Trace.error(Trace.INVALID_CONVERSION);
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY : {
                String s = otherType.convertToString(a);

                return convertToTypeLimits(s);
            }
            default : {
                String s = otherType.convertToString(a);

                return convertToTypeLimits(s);
            }
        }
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return a;
        }

        switch (otherType.type) {

            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                return convertToTypeLimits(a);

            case Types.SQL_CLOB : {
                String s = ((ClobData) a).toString();

                return convertToTypeLimits(s);
            }
            case Types.SQL_BLOB :
                throw Trace.error(Trace.INVALID_CONVERSION);
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY : {
                String s = otherType.convertToString(a);

                return convertToTypeLimits(s);
            }
            default : {
                String s = otherType.convertToString(a);

                return convertToTypeLimits(s);
            }
        }
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (a instanceof Boolean) {
            return convertToType(null, a, Type.SQL_BOOLEAN);
        } else if (a instanceof BigDecimal) {
            a = JavaSystem.toString((BigDecimal) a);

            return convertToType(null, a, Type.SQL_VARCHAR);
        } else if (a instanceof Number) {
            a = a.toString();    // use shortcut

            return convertToType(null, a, Type.SQL_VARCHAR);
        } else if (a instanceof String) {
            return convertToType(null, a, Type.SQL_VARCHAR);
        } else if (a instanceof java.sql.Date) {
            return convertToType(null, a, Type.SQL_DATE);
        } else if (a instanceof java.sql.Time) {
            return convertToType(null, a, Type.SQL_TIME);
        } else if (a instanceof java.sql.Timestamp) {
            return convertToType(null, a, Type.SQL_TIMESTAMP);
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_CHAR : {
                int slen = ((String) a).length();

                if (precision == 0 || slen == precision) {
                    return (String) a;
                }

                char[] b = new char[(int) precision];

                ((String) a).getChars(0, slen, b, 0);

                for (int i = slen; i < precision; i++) {
                    b[i] = ' ';
                }

                return new String(b);
            }
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                return (String) a;
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        String s = convertToString(a);

        return StringConverter.toQuotedString(s, '\'', true);
    }

    public boolean isEqualIdentical() {
        return isEqualIdentical;
    }

    public long position(Object data, Object otherData, Type otherType,
                         long offset) throws HsqlException {

        if (data == null || otherData == null) {
            return -1L;
        }

        if (otherType.type == Types.SQL_CLOB) {
            long otherLength = ((ClobData) data).length();

            if (offset + otherLength > ((String) data).length()) {
                return -1;
            }

            String otherString = ((ClobData) otherData).getSubString(0,
                (int) otherLength);

            return ((String) data).indexOf(otherString, (int) offset);
        } else if (otherType.isCharacterType()) {
            long otherLength = ((String) data).length();

            if (offset + otherLength > ((String) data).length()) {
                return -1;
            }

            return ((String) data).indexOf((String) otherData, (int) offset);
        } else {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "CharacterType");
        }
    }

    public Object substring(Session session, Object data, long offset,
                            long length,
                            boolean hasLength) throws HsqlException {

        long end;
        long dataLength = type == Types.SQL_CLOB ? ((ClobData) data).length()
                                                 : ((String) data).length();

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

        if (data instanceof String) {
            return ((String) data).substring((int) offset,
                                             (int) (offset + length));
        } else if (data instanceof ClobData) {

            // change method signature to take long
            String result = ((ClobData) data).getSubString(offset,
                (int) length);
            ClobData clob = new ClobDataMemory(result);

            clob.setId(session.getLobId());
            session.database.lobManager.addClob(clob);

            return clob;
        } else {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "CharacterType");
        }
    }

    /**
     * Memory limits apply to Upper and Lower implementations with Clob data
     */
    public Object upper(Session session, Object data) throws HsqlException {

        if (data == null) {
            return null;
        }

        if (type == Types.SQL_CLOB) {
            String result = ((ClobData) data).getSubString(0,
                (int) ((ClobData) data).length());

            result = collation.toUpperCase(result);

            ClobData clob = new ClobDataMemory(result);

            clob.setId(session.getLobId());
            session.database.lobManager.addClob(clob);

            return clob;
        }

        return collation.toUpperCase((String) data);
    }

    public Object lower(Session session, Object data) throws HsqlException {

        if (data == null) {
            return null;
        }

        if (type == Types.SQL_CLOB) {
            String result = ((ClobData) data).getSubString(0,
                (int) ((ClobData) data).length());

            result = collation.toLowerCase(result);

            ClobData clob = new ClobDataMemory(result);

            clob.setId(session.getLobId());
            session.database.lobManager.addClob(clob);

            return clob;
        }

        return collation.toLowerCase((String) data);
    }

    public Object trim(Session session, Object data, int trim,
                       boolean leading,
                       boolean trailing) throws HsqlException {

        if (data == null) {
            return null;
        }

        String s;

        if (type == Types.SQL_CLOB) {
            s = ((ClobData) data).getSubString(
                0, (int) ((ClobData) data).length());
        } else {
            s = (String) data;
        }

        int endindex = s.length();

        if (trailing) {
            for (--endindex; endindex >= 0 && s.charAt(endindex) == trim;
                    endindex--) {}

            endindex++;
        }

        int startindex = 0;

        if (leading) {
            while (startindex < endindex && s.charAt(startindex) == trim) {
                startindex++;
            }
        }

        if (startindex == 0 && endindex == s.length()) {}
        else {
            s = s.substring(startindex, endindex);
        }

        if (type == Types.SQL_CLOB) {
            ClobData clob = new ClobDataMemory(s);

            clob.setId(session.getLobId());
            session.database.lobManager.addClob(clob);

            return clob;
        } else {
            return s;
        }
    }

    public Object overlay(Session session, Object data, Object overlay,
                          long offset, long length,
                          boolean hasLength) throws HsqlException {

        if (data == null || overlay == null) {
            return null;
        }

        if (!hasLength) {
            length = type == Types.SQL_CLOB ? ((ClobData) overlay).length()
                                            : ((String) overlay).length();
        }

        Object temp = concat(null, substring(session, data, 0, offset, true),
                             overlay);

        return concat(null, temp,
                      substring(session, data, offset + length, 0, false));
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        String left;
        String right;

        if (a instanceof ClobData) {
            left = ((ClobData) a).getSubString(
                0, (int) ((ClobData) a).length());
        } else {
            left = (String) a;
        }

        if (b instanceof ClobData) {
            right =
                ((ClobData) b).getSubString(0, (int) ((ClobData) b).length());
        } else {
            right = (String) b;
        }

        if (type == Types.SQL_CLOB) {
            ClobData clob = new ClobDataMemory(left + right);

            clob.setId(session.getLobId());
            session.database.lobManager.addClob(clob);

            return clob;
        } else {
            return left + right;
        }
    }

    public long size(Object data) throws HsqlException {

        if (type == Types.SQL_CLOB) {
            return ((ClobData) data).length();
        }

        return ((String) data).length();
    }

/*
    public static Object concat(Object a, Object b) {

        if (a == null || b == null) {
            return null;
        }

        return a.toString() + b.toString();
    }
*/
    public static Type getCharacterType(int type, long precision) {

        switch (type) {

            case Types.SQL_VARCHAR :
            case Types.SQL_CHAR :
            case Types.VARCHAR_IGNORECASE :
                return new CharacterType(type, (int) precision);

            case Types.SQL_CLOB :
                return new ClobType(precision);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }
}
