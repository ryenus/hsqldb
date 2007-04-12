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
import java.math.BigInteger;

import org.hsqldb.Expression;
import org.hsqldb.HsqlException;
import org.hsqldb.Library;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.ValuePool;

/**
 * Type instance for all NUMBER types.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class NumberType extends Type {

    static final int tinyintPrecision        = 3;
    static final int smallintPrecision       = 5;
    static final int integerPrecision        = 10;
    static final int bigintPrecision         = 19;
    static final int doublePrecision         = 0;
    static final int defaultNumericPrecision = 400;

    //
    static final int TINYINT_WIDTH  = 8;
    static final int SMALLINT_WIDTH = 16;
    static final int INTEGER_WIDTH  = 32;
    static final int BIGINT_WIDTH   = 64;
    static final int DOUBLE_WIDTH   = 128;    // nominal width
    static final int DECIMAL_WIDTH  = 256;    // nominal width
    public static final Type SQL_NUMERIC_DEFAULT_INT =
        new NumberType(Types.NUMERIC, defaultNumericPrecision, 0);

    //
    final int typeWidth;

    public NumberType(int type, long precision, int scale) {

        super(type, precision, scale);

        switch (type) {

            case Types.TINYINT :
                typeWidth = TINYINT_WIDTH;
                break;

            case Types.SQL_SMALLINT :
                typeWidth = SMALLINT_WIDTH;
                break;

            case Types.SQL_INTEGER :
                typeWidth = INTEGER_WIDTH;
                break;

            case Types.SQL_BIGINT :
                typeWidth = BIGINT_WIDTH;
                break;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                typeWidth = DOUBLE_WIDTH;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                typeWidth = DECIMAL_WIDTH;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public int displaySize() {

        switch (type) {

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                if (scale == 0) {
                    if (precision == 0) {
                        return 646456995;    // precision + "-.".length()}
                    }

                    return (int) precision + 1;
                }

                return (int) precision + 2;

            case Types.SQL_FLOAT :
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return 23;                   // String.valueOf(-Double.MAX_VALUE).length();

            case Types.SQL_BIGINT :
                return 20;                   // decimal precision + "-".length();

            case Types.SQL_INTEGER :
                return 11;                   // decimal precision + "-".length();

            case Types.SQL_SMALLINT :
                return 6;                    // decimal precision + "-".length();

            case Types.TINYINT :
                return 4;                    // decimal precision + "-".length();

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public int getJDBCTypeNumber() {
        return type == Types.SQL_BIGINT ? Types.BIGINT
                                        : type;
    }

    public int getSQLGenericTypeNumber() {
        return type;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getJDBCClassName() {

        switch (type) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return "java.lang.Integer";

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return "java.lang.Double";

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return "java.math.BigDecimal";

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public String getName() {

        switch (type) {

            case Types.TINYINT :
                return Token.T_TINYINT;

            case Types.SQL_SMALLINT :
                return Token.T_SMALLINT;

            case Types.SQL_INTEGER :
                return Token.T_INTEGER;

            case Types.SQL_BIGINT :
                return Token.T_BIGINT;

            case Types.SQL_REAL :
                return Token.T_REAL;

            case Types.SQL_FLOAT :
                return Token.T_FLOAT;

            case Types.SQL_DOUBLE :
                return Token.T_DOUBLE;

            case Types.SQL_NUMERIC :
                return Token.T_NUMERIC;

            case Types.SQL_DECIMAL :
                return Token.T_DECIMAL;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public String getDefinition() {

        switch (type) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                StringBuffer sb = new StringBuffer(16);

                sb.append(getName());
                sb.append('(');
                sb.append(precision);
                sb.append(',');
                sb.append(scale);
                sb.append(')');

                return sb.toString();

            default :
                return getName();
        }
    }

    public boolean isNumberType() {
        return true;
    }

    public boolean isIntegralType() {

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return false;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return scale == 0;

            default :
                return true;
        }
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (this == other) {
            return this;
        }

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                break;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        if (typeWidth == DOUBLE_WIDTH) {
            return this;
        }

        if (((NumberType) other).typeWidth == DOUBLE_WIDTH) {
            return other;
        }

        if (typeWidth <= BIGINT_WIDTH
                && ((NumberType) other).typeWidth <= BIGINT_WIDTH) {
            return (typeWidth > ((NumberType) other).typeWidth) ? this
                                                                : other;
        }

        int newScale = scale > other.scale ? scale
                                           : other.scale;
        long newDigits = precision - scale > other.precision - other.scale
                         ? precision - scale
                         : other.precision - other.scale;

        return getNumberType(Types.SQL_NUMERIC, newDigits + newScale,
                             newScale);
    }

    /**
     *  Returns a SQL type "wide" enough to represent the result of the
     *  expression.<br>
     *  A type is "wider" than the other if it can represent all its
     *  numeric values.<BR>
     *  Arithmetic operation terms are promoted to a type that can
     *  represent the resulting values and avoid incorrect results.<p>
     *  FLOAT/REAL/DOUBLE used in an operation results in the same type,
     *  regardless of the type of the other operand.
     *  When the result or the expression is converted to the
     *  type of the target column for storage, an exception is thrown if the
     *  resulting value cannot be stored in the column<p>
     *  Types narrower than INTEGER (int) are promoted to
     *  INTEGER. The order of promotion is as follows<p>
     *
     *  INTEGER, BIGINT, NUMERIC/DECIMAL<p>
     *
     *  TINYINT and SMALLINT in any combination return INTEGER<br>
     *  TINYINT/SMALLINT/INTEGER and INTEGER return BIGINT<br>
     *  TINYINT/SMALLINT/INTEGER and BIGINT return NUMERIC/DECIMAL<br>
     *  BIGINT and BIGINT return NUMERIC/DECIMAL<br>
     *  REAL/FLOAT/DOUBLE and any type return REAL/FLOAT/DOUBLE<br>
     *  NUMERIC/DECIMAL any type other than REAL/FLOAT/DOUBLE returns NUMERIC/DECIMAL<br>
     *  In the case of NUMERIC/DECIMAL returned, the result precision is always
     *  large enough to express any value result, while the scale depends on the
     *  operation:<br>
     *  For ADD/SUBTRACT/DIVIDE, the scale is the larger of the two<br>
     *  For MULTIPLY, the scale is the sum of the two scales<br>
     */
    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {

        if (other.type == Types.SQL_ALL_TYPES) {
            other = this;
        }

        switch (operation) {


            case Expression.ADD :
                if (other.isIntervalType()) {
                    return other;
                }
                break;

            case Expression.DIVIDE :
            case Expression.SUBTRACT :
                break;

            case Expression.MULTIPLY :
                if (other.isIntervalType()) {
                    return other;
                }
                break;

            default :
                // all derivatives of equality ops or comparison ops
                return getAggregateType(other);
        }

        if (!other.isNumberType()) {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }

        if (typeWidth == DOUBLE_WIDTH
                || ((NumberType) other).typeWidth == DOUBLE_WIDTH) {
            return NumberType.SQL_DOUBLE;
        }

        int sum = typeWidth + ((NumberType) other).typeWidth;

        if (sum <= INTEGER_WIDTH) {
            return NumberType.SQL_INTEGER;
        }

        if (sum <= BIGINT_WIDTH) {
            return NumberType.SQL_BIGINT;
        }

        int  newScale;
        long newDigits;

        switch (operation) {

            case Expression.DIVIDE :
            case Expression.SUBTRACT :
            case Expression.ADD :
                newScale = scale > other.scale ? scale
                                               : other.scale;
                newDigits = precision - scale > other.precision - other.scale
                            ? precision - scale
                            : other.precision - other.scale;

                newDigits++;
                break;

            case Expression.MULTIPLY :
                newDigits = precision - scale + other.precision - other.scale;
                newScale  = scale + other.scale;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }

        return getNumberType(Types.SQL_DECIMAL, newScale + newDigits,
                             newScale);
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

        switch (type) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return (ai > bi) ? 1
                                 : (bi > ai ? -1
                                            : 0);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return (longa > longb) ? 1
                                       : (longb > longa ? -1
                                                        : 0);
            }
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return (ad > bd) ? 1
                                 : (bd > ad ? -1
                                            : 0);
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                int i = ((BigDecimal) a).compareTo((BigDecimal) b);

                return (i == 0) ? 0
                                : (i < 0 ? -1
                                         : 1);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    /** @todo - review if range enforcement / java type conversion is necessary */
    public Object convertToTypeLimits(Object a) throws HsqlException {

        switch (type) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return a;

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return a;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                BigDecimal dec = (BigDecimal) a;

                if (scale != dec.scale()) {
                    dec = dec.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
                }

                int valuePrecision = JavaSystem.precision(dec);

                if (valuePrecision > precision) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return dec;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (otherType.isIntervalType()) {
            int endType = ((IntervalType) otherType).endIntervalType;

            switch (endType) {

                case Types.SQL_INTERVAL_YEAR :
                case Types.SQL_INTERVAL_MONTH :
                case Types.SQL_INTERVAL_DAY :
                case Types.SQL_INTERVAL_HOUR :
                case Types.SQL_INTERVAL_MINUTE : {
                    Integer value = ValuePool.getInt(
                        ((IntervalType) otherType).convertToInt(a));

                    return convertToType(session, value, Type.SQL_INTEGER);
                }
                case Types.SQL_INTERVAL_SECOND : {
                    long seconds = ((IntervalSecondData) a).units;
                    long nanos   = ((IntervalSecondData) a).nanos;
                    BigDecimal value =
                        DateTimeIntervalType.getSecondPart(seconds, nanos,
                                                           otherType.scale);

                    return convertToType(
                        session, value,
                        DateTimeIntervalType.extractSecondType);
                }
            }
        }

        switch (otherType.type) {

            case Types.SQL_CLOB :
                a = ((ClobData) a).getSubString(
                    0L, (int) ((ClobData) a).length());
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                a = Library.trim((String) a, " ", true, true);

                switch (this.type) {

                    case Types.TINYINT :
                        try {
                            int value = Byte.parseByte((String) a);

                            return ValuePool.getInt(value);
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    case Types.SQL_SMALLINT :
                        try {
                            int value = Short.parseShort((String) a);

                            return ValuePool.getInt(value);
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    case Types.SQL_INTEGER :
                        try {
                            int value = Integer.parseInt((String) a);

                            return ValuePool.getInt(value);
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    case Types.SQL_BIGINT :
                        try {
                            long l = Long.parseLong((String) a);

                            return ValuePool.getLong(l);
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        try {
                            double d = Double.parseDouble((String) a);

                            return ValuePool.getDouble(
                                Double.doubleToLongBits(d));
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        try {
                            a = new BigDecimal((String) a);

                            return convertToTypeLimits(a);
                        } catch (NumberFormatException e) {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }
                    default :
                        throw Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "NumberType");
                }
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                break;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        switch (this.type) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return convertToInt(a, this.type);

            case Types.SQL_BIGINT :
                return convertToLong(a);

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return convertToDouble(a);

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                BigDecimal value = convertToDecimal(a);

                return convertToTypeLimits(value);
        }

        return null;
    }

    /**
     * Converts a value to the type of the default instance of this type
     */
    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        Type type;

        if (a instanceof Number) {
            if (a instanceof BigInteger) {
                a = new BigDecimal((BigInteger) a);
            } else if (a instanceof Float) {
                a = new Double(((Float) a).doubleValue());
            } else if (a instanceof Byte) {
                a = ValuePool.getInt(((Byte) a).intValue());
            } else if (a instanceof Short) {
                a = ValuePool.getInt(((Short) a).intValue());
            }

            if (a instanceof Integer) {
                type = Type.SQL_INTEGER;
            } else if (a instanceof Long) {
                type = Type.SQL_BIGINT;
            } else if (a instanceof Double) {
                type = Type.SQL_DOUBLE;
            } else if (a instanceof BigDecimal) {
                type = Type.SQL_DECIMAL;
            } else {
                throw Trace.error(Trace.INVALID_CONVERSION);
            }
        } else if (a instanceof String) {
            type = Type.SQL_VARCHAR;
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }

        return convertToType(null, a, type);
    }

    /**
     * Type narrowing from DOUBLE/DECIMAL/NUMERIC to BIGINT / INT / SMALLINT / TINYINT
     * following SQL rules. When conversion is from a non-integral type,
     * digits to the right of the decimal point are lost.
     */

    /**
     * Converter from a numeric object to Integer. Input is checked to be
     * within range represented by the given number type.
     */
    static Integer convertToInt(Object a, int type) throws HsqlException {

        int value;

        if (a instanceof Integer) {
            if (type == Types.SQL_INTEGER) {
                return (Integer) a;
            }

            value = ((Integer) a).intValue();
        } else if (a instanceof Long) {
            long temp = ((Long) a).longValue();

            if (Integer.MAX_VALUE < temp || temp < Integer.MIN_VALUE) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            value = (int) temp;
        } else if (a instanceof BigDecimal) {
            BigInteger bi = ((BigDecimal) a).toBigInteger();

            if (bi.compareTo(MAX_INT) > 0 || bi.compareTo(MIN_INT) < 0) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            value = bi.intValue();
        } else if (a instanceof Double || a instanceof Float) {
            double d = ((Number) a).doubleValue();

            if (Double.isInfinite(d) || Double.isNaN(d)
                    || d >= (double) Integer.MAX_VALUE + 1
                    || d <= (double) Integer.MIN_VALUE - 1) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            value = (int) d;
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }

        if (type == Types.TINYINT) {
            if (Byte.MAX_VALUE < value || value < Byte.MIN_VALUE) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        } else if (type == Types.SQL_SMALLINT) {
            if (Short.MAX_VALUE < value || value < Short.MIN_VALUE) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }

        return ValuePool.getInt(value);
    }

    /**
     * Converter from a numeric object to Long. Input is checked to be
     * within range represented by Long.
     */
    static Long convertToLong(Object a) throws HsqlException {

        if (a instanceof Integer) {
            return ValuePool.getLong(((Integer) a).intValue());
        } else if (a instanceof Long) {
            return (Long) a;
        } else if (a instanceof BigDecimal) {
            BigInteger bi = ((BigDecimal) a).toBigInteger();

            if (bi.compareTo(MAX_LONG) > 0 || bi.compareTo(MIN_LONG) < 0) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getLong(bi.longValue());
        } else if (a instanceof Double || a instanceof Float) {
            double d = ((Number) a).doubleValue();

            if (Double.isInfinite(d) || Double.isNaN(d)
                    || d >= (double) Long.MAX_VALUE + 1
                    || d <= (double) Long.MIN_VALUE - 1) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getLong((long) d);
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    /**
     * Converter from a numeric object to Double. Input is checked to be
     * within range represented by Double
     */
    private static Double convertToDouble(Object a) throws HsqlException {

        double value;

        if (a instanceof java.lang.Double) {
            return (Double) a;
        } else if (a instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) a;

            value = bd.doubleValue();

            int        signum = bd.signum();
            BigDecimal bdd    = new BigDecimal(value + signum);

            if (bdd.compareTo(bd) != signum) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        } else {
            value = ((Number) a).doubleValue();
        }

        return ValuePool.getDouble(Double.doubleToLongBits(value));
    }

    private static BigDecimal convertToDecimal(Object a)
    throws HsqlException {

        if (a instanceof BigDecimal) {
            return (BigDecimal) a;
        } else if (a instanceof Integer || a instanceof Long) {
            return BigDecimal.valueOf(((Number) a).longValue());
        } else if (a instanceof Double) {
            double value = ((Number) a).doubleValue();

            if (Double.isInfinite(value) || Double.isNaN(value)) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return new BigDecimal(value);
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (this.type) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                return a.toString();

            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                double value = ((Double) a).doubleValue();

                // todo - java 5 format change
                if (value == Double.NEGATIVE_INFINITY) {
                    return "-1E0/0";
                }

                if (value == Double.POSITIVE_INFINITY) {
                    return "1E0/0";
                }

                if (Double.isNaN(value)) {
                    return "0E0/0E0";
                }

                String s = Double.toString(value);

                // ensure the engine treats the value as a DOUBLE, not DECIMAL
                if (s.indexOf('E') < 0) {
                    s = s.concat("E0");
                }

                return s;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return JavaSystem.toString((BigDecimal) a);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public String convertToSQLString(Object a) {
        if (a == null) {
            return "NULL";
        }

        return convertToString(a);
    }

    public int compareToTypeRange(Object o) {

        if (!(o instanceof Number)) {
            return 0;
        }

        if (o instanceof Integer || o instanceof Long) {
            long temp = ((Number) o).longValue();
            int  min;
            int  max;

            switch (type) {

                case Types.TINYINT :
                    min = Byte.MIN_VALUE;
                    max = Byte.MAX_VALUE;
                    break;

                case Types.SQL_SMALLINT :
                    min = Short.MIN_VALUE;
                    max = Short.MAX_VALUE;
                    break;

                case Types.SQL_INTEGER :
                    min = Integer.MIN_VALUE;
                    max = Integer.MAX_VALUE;
                    break;

                case Types.SQL_BIGINT :
                    return 0;

                case Types.SQL_DECIMAL :
                case Types.SQL_NUMERIC :
                default :
                    return 0;
            }

            if (max < temp) {
                return 1;
            }

            if (temp < min) {
                return -1;
            }

            return 0;
        } else {
            try {
                o = convertToLong(o);

                return compareToTypeRange(o);
            } catch (HsqlException e) {
                if (e.getErrorCode() == -Trace.NUMERIC_VALUE_OUT_OF_RANGE) {
                    if (o instanceof BigDecimal) {
                        return ((BigDecimal) o).signum();
                    } else if (o instanceof Double) {
                        return ((Double) o).doubleValue() > 0 ? 1
                                                              : -1;
                    }
                }
            }
        }

        return 0;
    }

    public Object add(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad + bd));

//                return new Double(ad + bd);
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(a);
                b = convertToDefaultType(b);

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.add(bbd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai + bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa + longb);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Object subtract(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad - bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(a);
                b = convertToDefaultType(b);

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.subtract(bbd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai - bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa - longb);
            }
            default :
        }

        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "NumberType");
    }

    public Object multiply(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad * bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(a);
                b = convertToDefaultType(b);

                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.multiply(bbd);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai * bi);
            }
            case Types.SQL_BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa * longb);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Object divide(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad / bd));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                a = convertToDefaultType(a);
                b = convertToDefaultType(b);

                BigDecimal abd   = (BigDecimal) a;
                BigDecimal bbd   = (BigDecimal) b;
                int        scale = abd.scale() > bbd.scale() ? abd.scale()
                                                             : bbd.scale();

                return (bbd.signum() == 0) ? null
                                           : abd.divide(bbd, scale,
                                           BigDecimal.ROUND_HALF_DOWN);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                if (bi == 0) {
                    throw Trace.error(Trace.DIVISION_BY_ZERO);
                }

                return ValuePool.getInt(ai / bi);
            }
            case Types.SQL_BIGINT : {
                long al = ((Number) a).longValue();
                long bl = ((Number) b).longValue();

                if (bl == 0) {
                    throw Trace.error(Trace.DIVISION_BY_ZERO);
                }

                return ValuePool.getLong(al / bl);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Type getIntegralType() {

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                return SQL_NUMERIC_DEFAULT_INT;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return scale == 0 ? this
                                  : new NumberType(type, precision, 0);

            default :
                return this;
        }
    }

    public boolean isNegative(Object a) throws HsqlException {

        if (a == null) {
            return false;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = ((Number) a).doubleValue();

                return ad < 0;
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return ((BigDecimal) a).signum() < 0;

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                return ((Number) a).intValue() < 0;

            case Types.SQL_BIGINT :
                return ((Number) a).longValue() < 0;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Object absolute(Object a) throws HsqlException {
        return isNegative(a) ? negate(a)
                             : a;
    }

    public Object negate(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = -((Number) a).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return ((BigDecimal) a).negate();

            case Types.TINYINT : {
                int value = ((Number) a).intValue();

                if (value == Byte.MIN_VALUE) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_SMALLINT : {
                int value = ((Number) a).intValue();

                if (value == Short.MIN_VALUE) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_INTEGER : {
                int value = ((Number) a).intValue();

                if (value == Integer.MIN_VALUE) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getInt(-value);
            }
            case Types.SQL_BIGINT : {
                long value = ((Number) a).longValue();

                if (value == Long.MIN_VALUE) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getLong(-value);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    public Object ceiling(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double ad = Math.ceil(((Double) a).doubleValue());

                if (Double.isInfinite(ad)) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getDouble(Double.doubleToLongBits(ad));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal value = ((BigDecimal) a).setScale(0,
                    BigDecimal.ROUND_CEILING);

                if (JavaSystem.precision(value) > precision) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }
            }
            default :
                return a;
        }
    }

    public Object floor(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE : {
                double value = Math.floor(((Double) a).doubleValue());

                if (Double.isInfinite(value)) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }

                return ValuePool.getDouble(Double.doubleToLongBits(value));
            }
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                BigDecimal value = ((BigDecimal) a).setScale(0,
                    BigDecimal.ROUND_FLOOR);

                if (JavaSystem.precision(value) > precision) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                }
            }
            default :
                return a;
        }
    }

    public static NumberType getNumberType(int type, long precision,
                                           int scale) {

        switch (type) {

            case Types.SQL_INTEGER :
                return SQL_INTEGER;

            case Types.SQL_SMALLINT :
                return SQL_SMALLINT;

            case Types.SQL_BIGINT :
                return SQL_BIGINT;

            case Types.TINYINT :
                return TINYINT;

            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return SQL_DOUBLE;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                return new NumberType(type, precision, scale);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberType");
        }
    }

    static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    static final BigInteger MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
    static final BigInteger MIN_INT = BigInteger.valueOf(Integer.MIN_VALUE);
    static final BigDecimal BIG_DECIMAL_0 = new BigDecimal(0.0);
    static final BigDecimal BIG_DECIMAL_1 = new BigDecimal(1.0);
}
