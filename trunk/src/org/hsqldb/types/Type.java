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

import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.BitMap;

/**
 * Base class for type objects.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public abstract class Type {

    public final int type;
    final long       precision;
    final int        scale;

    Type(int type, long precision, int scale) {

        this.type      = type;
        this.precision = (int) precision;
        this.scale     = scale;
    }

    public long size() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    public abstract int displaySize();

    /**
     * Returns the JDBC type number of type, if it exists,
     * otherwise the HSQLDB / SQL type.
     */
    public abstract int getJDBCTypeNumber();

    /**
     * Returns the JDBC class name of type, if it exists,
     * otherwise the HSQLDB class name.
     */
    public abstract String getJDBCClassName();

    /**
     * Returns the generic SQL CLI type number of type, if it exists,
     * otherwise the HSQLDB type. The generic type is returned for DATETIME
     * and INTERVAL types.
     */
    public abstract int getSQLGenericTypeNumber();

    /**
     * Returns the specific SQL CLI type number of type, if it exists,
     * otherwise the HSQLDB type
     */
    public abstract int getSQLSpecificTypeNumber();

    /**
     * Returns the name of the type
     */
    public abstract String getNameString();

    /**
     * Returns the full definition of the type, including parameters
     */
    public abstract String getDefinition();

    public abstract int compare(Object a, Object b);

    public abstract Object convertToTypeLimits(Object a) throws HsqlException;

    /**
     * Explicit casts are handled by this method.
     * SQL standard 6.12 rules for enforcement of size, precision and scale
     * are implemented. For CHARACTER values, it performs truncation in all
     * cases of long strings.
     */
    public Object castToType(Session session, Object a,
                             Type type) throws HsqlException {
        return convertToType(session, a, type);
    }

    /**
     * Same as castToType except for CHARACTER values. Perform string
     * truncation of trailing spaces only. For other long strings, it raises
     * an exception.
     */
    public abstract Object convertToType(Session session, Object a,
                                         Type type) throws HsqlException;

    /**
     * Converts the object to the given type. Used for JDBC conversions.
     */
    public abstract Object convertToDefaultType(Object o) throws HsqlException;

    public abstract String convertToString(Object a);

    public abstract String convertToSQLString(Object a);

    public Type getParentType() {
        return null;
    }

    public boolean isDistinctType() {
        return false;
    }

    public boolean isStructuredType() {
        return false;
    }

    public boolean isDomainType() {
        return false;
    }

    public boolean isCharacterType() {
        return false;
    }

    public boolean isNumberType() {
        return false;
    }

    public boolean isIntegralType() {
        return false;
    }

    public boolean isDateTimeType() {
        return false;
    }

    public boolean isIntervalType() {
        return false;
    }

    public boolean isBinaryType() {
        return false;
    }

    public boolean isBooleanType() {
        return false;
    }

    public boolean isLobType() {
        return false;
    }

    public boolean isBitType() {
        return false;
    }

    public boolean acceptsPrecision() {
        return false;
    }

    public boolean requiresPrecision() {
        return false;
    }

    public boolean acceptsFractionalPrecision() {
        return false;
    }

    public boolean acceptsScale() {
        return false;
    }

    /**
     * Common type used in comparison opertions. other must be comparable
     * with this.
     */
    public abstract Type getAggregateType(Type other) throws HsqlException;

    /**
     * Result type of combining values of two types in different opertions.
     * other type is not allways comparable with this, but a operation should
     * be valid without any explicit CAST
     */
    public abstract Type getCombinedType(Type other,
                                         int operation) throws HsqlException;

    public int compareToTypeRange(Object o) {
        return 0;
    }

    /**
     * All arithmetic ops are called on the pre-determined Type object of the result
     */
    public Object add(Object a, Object b) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
    }

    public Object subtract(Object a, Object b) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
    }

    public Object multiply(Object a, Object b) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
    }

    public Object divide(Object a, Object b) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
    }

    public boolean equals(Object other) {

        if (other instanceof Type) {
            return ((Type) other).type == type
                   && ((Type) other).precision == precision
                   && ((Type) other).scale == scale;
        }

        return false;
    }

    // null type
    public static final Type SQL_ALL_TYPES = NullType.getNullType();

    // character types
    public static final Type SQL_CHAR = new CharacterType(Types.SQL_CHAR, 0);
    public static final Type SQL_VARCHAR = new CharacterType(Types.SQL_VARCHAR,
        0);
    public static final Type SQL_VARCHAR_MAX_WIDTH =
        new CharacterType(Types.SQL_VARCHAR, 0);    // todo - needs max implementation defined width. used for parameters
    public static final ClobType SQL_CLOB = new ClobType();
    public static final Type VARCHAR_IGNORECASE =
        new CharacterType(Types.VARCHAR_IGNORECASE, 0);

    // binary types
    public static final BinaryType SQL_BINARY =
        new BinaryType(Types.SQL_BINARY, 0);
    public static final BinaryType SQL_VARBINARY =
        new BinaryType(Types.SQL_VARBINARY, 0);
    public static final BlobType SQL_BLOB = new BlobType();

    // other type
    public static final OtherType OTHER = OtherType.getOtherType();

    // boolean type
    public static final BooleanType SQL_BOOLEAN = BooleanType.getBooleanType();

    // number types
    public static final NumberType SQL_NUMERIC =
        new NumberType(Types.SQL_NUMERIC, NumberType.defaultNumericPrecision,
                       0);
    public static final NumberType SQL_DECIMAL =
        new NumberType(Types.SQL_DECIMAL, NumberType.defaultNumericPrecision,
                       0);
    public static final NumberType SQL_DOUBLE =
        new NumberType(Types.SQL_DOUBLE, 0, 0);

    //
    public static final NumberType TINYINT = new NumberType(Types.TINYINT,
        NumberType.tinyintPrecision, 0);
    public static final NumberType SQL_SMALLINT =
        new NumberType(Types.SQL_SMALLINT, NumberType.smallintPrecision, 0);
    public static final NumberType SQL_INTEGER =
        new NumberType(Types.SQL_INTEGER, NumberType.integerPrecision, 0);
    public static final NumberType SQL_BIGINT =
        new NumberType(Types.SQL_BIGINT, NumberType.bigintPrecision, 0);

    // date time
    public static final DateTimeType SQL_DATE =
        new DateTimeType(Types.SQL_DATE, 0);
    public static final DateTimeType SQL_TIME =
        new DateTimeType(Types.SQL_TIME,
                         DateTimeType.defaultTimeFractionPrecision);
    public static final DateTimeType SQL_TIMESTAMP =
        new DateTimeType(Types.SQL_TIMESTAMP,
                         DateTimeType.defaultTimestampFractionPrecision);
    public static final DateTimeType SQL_TIMESTAMP_NO_FRACTION =
        new DateTimeType(Types.SQL_TIMESTAMP, 0);

    // interval
    public static final IntervalType SQL_INTERVAL_YEAR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MONTH =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MONTH,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_HOUR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_MINUTE,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_SECOND =
        IntervalType.newIntervalType(
            Types.SQL_INTERVAL_SECOND, IntervalType.defaultIntervalPrecision,
            IntervalType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_SECOND_MAX_FRACTION =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_SECOND,
                                     IntervalType.defaultIntervalPrecision,
                                     IntervalType.maxFractionPrecision);
    public static final IntervalType SQL_INTERVAL_YEAR_TO_MONTH =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_HOUR =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_HOUR,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_DAY_TO_MINUTE,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_DAY_TO_SECOND =
        IntervalType.newIntervalType(
            Types.SQL_INTERVAL_DAY_TO_SECOND,
            IntervalType.defaultIntervalPrecision,
            IntervalType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_HOUR_TO_MINUTE =
        IntervalType.newIntervalType(Types.SQL_INTERVAL_HOUR_TO_MINUTE,
                                     IntervalType.defaultIntervalPrecision, 0);
    public static final IntervalType SQL_INTERVAL_HOUR_TO_SECOND =
        IntervalType.newIntervalType(
            Types.SQL_INTERVAL_HOUR_TO_SECOND,
            IntervalType.defaultIntervalPrecision,
            IntervalType.defaultIntervalFractionPrecision);
    public static final IntervalType SQL_INTERVAL_MINUTE_TO_SECOND =
        IntervalType.newIntervalType(
            Types.SQL_INTERVAL_MINUTE_TO_SECOND,
            IntervalType.defaultIntervalPrecision,
            IntervalType.defaultIntervalFractionPrecision);

    /**
     * For literals, supports only the types returned by Tokenizer
     */
    public static Type getValueType(int type, Object value) {

        long precision = 0;
        int  scale     = 0;

        if (value != null) {
            switch (type) {

                case Types.SQL_INTEGER :
                case Types.SQL_BIGINT :
                case Types.SQL_DOUBLE :
                case Types.SQL_BOOLEAN :
                case Types.SQL_ALL_TYPES :
                    break;

                case Types.SQL_CHAR :
                    precision = value.toString().length();
                    break;

                case Types.SQL_VARBINARY :
                    precision = ((BinaryData) value).length();
                    break;

                case Types.SQL_BIT :
                    precision = ((BinaryData) value).bitLength();
                    break;

                case Types.SQL_NUMERIC :
                    precision = JavaSystem.precision((BigDecimal) value);
                    scale     = ((BigDecimal) value).scale();
                    break;

                default :
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION, "Type");
            }
        }

        try {
            return getType(type, 0, precision, scale);
        } catch (HsqlException e) {

            // exception should never be thrown
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Type");
        }
    }

    public static Type getDefaultType(int type) {

        try {
            return getType(type, 0, 0, 0);
        } catch (HsqlException e) {

            // exception should never be thrown
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Type");
        }
    }

    public static int getHSQLDBTypeCode(int jdbcTypeNumber)
    throws HsqlException {

        switch (jdbcTypeNumber) {

            case Types.BIGINT :
                return Types.SQL_BIGINT;

            case Types.LONGVARCHAR :
                return Types.SQL_VARCHAR;

            case Types.CLOB :
                return Types.SQL_CLOB;

            case Types.BINARY :
                return Types.SQL_BINARY;

            case Types.BIT :
                return Types.SQL_BIT_VARYING;

            case Types.VARBINARY :
            case Types.LONGVARBINARY :
                return Types.SQL_VARBINARY;

            case Types.BLOB :
                return Types.SQL_BLOB;

            default :
                return jdbcTypeNumber;
        }
    }

    /**
     * translate an internal type number to JDBC type number
     * if a type is not supported internally, it is returned without translation
     *
     * @param i int
     * @return int
     */
    public static int getJDBCTypeCode(int type) {

        switch (type) {

            case Types.SQL_BLOB :
                return Types.BLOB;

            case Types.SQL_CLOB :
                return Types.CLOB;

            case Types.SQL_BIGINT :
                return Types.BIGINT;

            case Types.SQL_BINARY :
                return Types.BINARY;

            case Types.SQL_VARBINARY :
                return Types.VARBINARY;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return Types.BIT;

            default :
                return type;
        }
    }

    /**
     * Enforces precision and scale limits on type
     */
    public static Type getType(int type, int collation, long precision,
                               int scale) throws HsqlException {

        switch (type) {

            case Types.SQL_ALL_TYPES :
                return null;

//                return SQL_ALL_TYPES; // needs changes to Expression type resolution
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
            case Types.SQL_CLOB :
                return CharacterType.getCharacterType(type, precision);

            case Types.SQL_INTEGER :
                return SQL_INTEGER;

            case Types.SQL_SMALLINT :
                return SQL_SMALLINT;

            case Types.SQL_BIGINT :
                return SQL_BIGINT;

            case Types.TINYINT :
                return TINYINT;

            case Types.SQL_FLOAT :
                if (precision > 53) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE,
                                      "" + precision);
                }
            case Types.SQL_REAL :
            case Types.SQL_DOUBLE :
                return SQL_DOUBLE;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                if (precision == 0) {
                    precision = NumberType.defaultNumericPrecision;
                }

                return NumberType.getNumberType(type, precision, scale);

            case Types.SQL_BOOLEAN :
                return SQL_BOOLEAN;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                return BinaryType.getBinaryType(type, precision);

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return BitType.getBitType(type, precision);

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP :
                return DateTimeType.getDateTimeType(type, scale);

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND :
                return IntervalType.getIntervalType(type, precision, scale);

            case Types.OTHER :
                return OTHER;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Type");
        }
    }

    public static Type getAggregatedType(Type add,
                                         Type existing) throws HsqlException {

        if (existing == null || existing.type == Types.SQL_ALL_TYPES) {
            return add;
        }

        if (add == null || add.type == Types.SQL_ALL_TYPES) {
            return existing;
        }

        return existing.getAggregateType(add);
    }

    public static IntValueHashMap typeAliases;
    public static IntValueHashMap typeNames;

    static {
        typeNames = new IntValueHashMap(37);

        typeNames.put("CHARACTER", Types.SQL_CHAR);
        typeNames.put("VARCHAR", Types.SQL_VARCHAR);
        typeNames.put("VARCHAR_IGNORECASE", Types.VARCHAR_IGNORECASE);
        typeNames.put("DATE", Types.SQL_DATE);
        typeNames.put("TIME", Types.SQL_TIME);
        typeNames.put("TIMESTAMP", Types.SQL_TIMESTAMP);
        typeNames.put("INTERVAL", Types.SQL_INTERVAL);
        typeNames.put("TINYINT", Types.TINYINT);
        typeNames.put("SMALLINT", Types.SQL_SMALLINT);
        typeNames.put("INTEGER", Types.SQL_INTEGER);
        typeNames.put("BIGINT", Types.SQL_BIGINT);
        typeNames.put("REAL", Types.SQL_REAL);
        typeNames.put("FLOAT", Types.SQL_FLOAT);
        typeNames.put("DOUBLE", Types.SQL_DOUBLE);
        typeNames.put("NUMERIC", Types.SQL_NUMERIC);
        typeNames.put("DECIMAL", Types.SQL_DECIMAL);
        typeNames.put("BOOLEAN", Types.SQL_BOOLEAN);
        typeNames.put("BINARY", Types.SQL_BINARY);
        typeNames.put("VARBINARY", Types.SQL_VARBINARY);
        typeNames.put("CLOB", Types.SQL_CLOB);
        typeNames.put("BLOB", Types.SQL_BLOB);
        typeNames.put("BIT", Types.SQL_BIT);
        typeNames.put("OTHER", Types.OTHER);

        //
        typeAliases = new IntValueHashMap(67, 1);

        typeAliases.put("CHAR", Types.SQL_CHAR);
/*
        typeAliases.put("CHAR VARYING", Types.SQL_VARCHAR);
        typeAliases.put("CHARACTER VARYING", Types.SQL_VARCHAR);
        typeAliases.put("CHARACTER LARGE OBJECT", Types.SQL_CLOB);
 */
        typeAliases.put("INT", Types.SQL_INTEGER);
        typeAliases.put("DEC", Types.SQL_DECIMAL);
        typeAliases.put("LONGVARCHAR", Types.SQL_VARCHAR);
        typeAliases.put("DATETIME", Types.SQL_TIMESTAMP);
        typeAliases.put("LONGVARBINARY", Types.SQL_VARBINARY);
        typeAliases.put("OBJECT", Types.OTHER);
    }

    public static int getTypeNr(String name) {

        int i = typeNames.get(name, Integer.MIN_VALUE);

        if (i == Integer.MIN_VALUE) {
            i = typeAliases.get(name, Integer.MIN_VALUE);
        }

        return i;
    }

    /**
     * convertJavaToSQLType
     *
     * @param value Object
     * @param dataType Type
     * @return Object
     */
    public static Object convertJavaToSQLType(Object a, Type dataType) {

        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        } else if (a instanceof java.sql.Time) {
            return new TimeData(((java.sql.Time) a).getTime());
        }

        return a;
    }

    public static boolean isSupportedSQLType(int typeNumber) {

        try {
            if (getDefaultType(typeNumber) != null) {
                return true;
            }
        } catch (RuntimeException e) {
            return false;
        }

        return false;
    }
}
