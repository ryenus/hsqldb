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
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.Expression;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.ValuePool;

/**
 * Type instance for various typs of INTERVAL.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class IntervalType extends DateTimeIntervalType {

    int internalPrecision;
    int startIntervalType;
    int endIntervalType;
    int startPartIndex;
    int endPartIndex;

    private IntervalType(int type, long precision, int scale) {
        super(type, precision, scale);
    }

    public int displaySize() {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                return (int) precision;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                return (int) precision + 3;

            case Types.SQL_INTERVAL_MONTH :
                return (int) precision;

            case Types.SQL_INTERVAL_DAY :
                return (int) precision;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                return (int) precision + 3;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                return (int) precision + 6;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                return (int) precision + 9 + (scale == 0 ? 0
                                                         : scale + 1);

            case Types.SQL_INTERVAL_HOUR :
                return (int) precision;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                return (int) precision + 3;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                return (int) precision + 6 + (scale == 0 ? 0
                                                         : scale + 1);

            case Types.SQL_INTERVAL_MINUTE :
                return (int) precision;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                return (int) precision + 3 + (scale == 0 ? 0
                                                         : scale + 1);

            case Types.SQL_INTERVAL_SECOND :
                return (int) precision + (scale == 0 ? 0
                                                     : scale + 1);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public int getJDBCTypeNumber() {

        // no JDBC number is available
        return type;
    }

    public String getJDBCClassName() {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return IntervalMonthData.class.getName();

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
                return org.hsqldb.types.IntervalSecondData.class.getName();

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public int getSQLGenericTypeNumber() {
        return Types.SQL_INTERVAL;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getName() {
        return getName(type);
    }

    static String getName(int type) {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                return "INTERVAL YEAR";

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                return "INTERVAL YEAR TO MONTH";

            case Types.SQL_INTERVAL_MONTH :
                return "INTERVAL MONTH";

            case Types.SQL_INTERVAL_DAY :
                return "INTERVAL DAY";

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                return "INTERVAL DAY TO HOUR";

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                return "INTERVAL DAY TO MINUTE";

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                return "INTERVAL DAY TO SECOND";

            case Types.SQL_INTERVAL_HOUR :
                return "INTERVAL HOUR";

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                return "INTERVAL HOUR TO MINUTE";

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                return "INTERVAL HOUR TO SECOND";

            case Types.SQL_INTERVAL_MINUTE :
                return "INTERVAL MINUTE";

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                return "INTERVAL MINUTE TO SECOND";

            case Types.SQL_INTERVAL_SECOND :
                return "INTERVAL SECOND";

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public String getDefinition() {

        if (precision == defaultIntervalPrecision
                && (endIntervalType != Types.SQL_INTERVAL_SECOND
                    || scale == defaultIntervalFractionPrecision)) {
            return getName();
        }

        StringBuffer sb = new StringBuffer(32);

        sb.append(getName(startIntervalType));

        if (type == Types.SQL_INTERVAL_SECOND) {
            sb.append('(');
            sb.append(precision);

            if (scale != defaultIntervalFractionPrecision) {
                sb.append(',');
                sb.append(scale);
            }

            sb.append(')');

            return sb.toString();
        }

        if (precision != defaultIntervalPrecision) {
            sb.append('(');
            sb.append(precision);
            sb.append(')');
        }

        if (startIntervalType != endIntervalType) {
            sb.append(' ');
            sb.append(Token.T_TO);
            sb.append(' ');
            sb.append(Token.SQL_INTERVAL_FIELD_NAMES[endPartIndex]);

            if (endIntervalType == Types.SQL_INTERVAL_SECOND
                    && scale != defaultIntervalFractionPrecision) {
                sb.append('(');
                sb.append(scale);
                sb.append(')');
            }
        }

        return sb.toString();
    }

    public boolean isIntervalType() {
        return true;
    }

    public boolean isYearMonthIntervalType() {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return true;

            default :
                return false;
        }
    }

    public boolean isDaySecondIntervalType() {

        switch (type) {

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
                return true;

            default :
                return false;
        }
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean acceptsFractionalPrecision() {
        return endIntervalType == Types.SQL_INTERVAL_SECOND;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            if (precision >= other.precision && scale >= other.scale) {
                return this;
            } else if (precision <= other.precision && scale <= other.scale) {
                return other;
            }
        }

        if (!other.isIntervalType()) {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        int startType = ((IntervalType) other).startIntervalType
                        > startIntervalType ? startIntervalType
                                            : ((IntervalType) other)
                                                .startIntervalType;
        int endType =
            ((IntervalType) other).endIntervalType > endIntervalType
            ? ((IntervalType) other).endIntervalType
            : endIntervalType;
        int  newType      = getCombinedIntervalType(startType, endType);
        long newPrecision = precision > other.precision ? precision
                                                        : other.precision;
        int  newScale     = scale > other.scale ? scale
                                                : other.scale;

        try {
            return getIntervalType(newType, startType, endType, newPrecision,
                                   newScale);
        } catch (RuntimeException e) {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {

        switch (operation) {


            case Expression.MULTIPLY :
            case Expression.DIVIDE :
                if (other.isNumberType()) {
                    return this;
                }
                break;

            case Expression.ADD :
                if (other.isDateTimeType()) {
                    return other;
                } else if (other.isIntervalType()) {
                    return getAggregateType(other);
                }
                break;

            case Expression.SUBTRACT :
                if (other.isIntervalType()) {
                    return getAggregateType(other);
                }
                break;

            default :
                return getAggregateType(other);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
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

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return ((IntervalMonthData) a).compareTo(
                    (IntervalMonthData) b);

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
                return ((IntervalSecondData) a).compareTo(
                    (IntervalSecondData) b);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (a instanceof IntervalMonthData) {
            IntervalMonthData im = (IntervalMonthData) a;

            if (im.units > getIntervalValueLimit()) {
                throw Trace.error(Trace.STRING_DATA_TRUNCATION);
            }
        } else if (a instanceof IntervalSecondData) {
            IntervalSecondData is = (IntervalSecondData) a;

            if (is.units > getIntervalValueLimit()) {
                throw Trace.error(Trace.STRING_DATA_TRUNCATION);
            }

            int divisor = nanoScaleFactors[scale];

            is.nanos = (is.nanos / divisor) * divisor;
        }

        return a;
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return null;
        }

        switch (otherType.type) {

            case Types.SQL_CLOB :
                a = a.toString();
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                return newInterval((String) a);
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL : {
                int value =
                    NumberType.convertToInt(a, Types.SQL_INTEGER).intValue();

                switch (this.endIntervalType) {

                    case Types.SQL_INTERVAL_YEAR :
                        return IntervalMonthData.newIntervalYear(value * 12,
                                (IntervalType) otherType);

                    case Types.SQL_INTERVAL_MONTH :
                        return IntervalMonthData.newIntervalMonth(value,
                                (IntervalType) otherType);

                    case Types.SQL_INTERVAL_DAY :
                        return IntervalSecondData.newIntervalDay(value * 24
                                * 60 * 60, (IntervalType) otherType);

                    case Types.SQL_INTERVAL_HOUR :
                        return IntervalSecondData.newIntervalHour(value * 60
                                * 60, (IntervalType) otherType);

                    case Types.SQL_INTERVAL_MINUTE :
                        return IntervalSecondData.newIntervalMinute(value
                                * 60, (IntervalType) otherType);

                    case Types.SQL_INTERVAL_SECOND : {
                        int nanos = 0;

                        if (a instanceof BigDecimal) {
                            BigDecimal b = ((BigDecimal) a).setScale(
                                maxFractionPrecision);
                            BigInteger i = JavaSystem.unscaledValue(b);
                            Long l =
                                (Long) Type.SQL_BIGINT.convertToDefaultType(
                                    i);

                            nanos =
                                (int) (l.longValue()
                                       % DateTimeType.nanoScaleFactors[0]);
                        }

                        return new IntervalSecondData(
                            value, nanos, (IntervalType) otherType);
                    }
                    default :
                        throw Trace.error(Trace.INVALID_CONVERSION);
                }
            }
            case Types.SQL_INTERVAL_YEAR : {
                int months = (((IntervalMonthData) a).units / 12) * 12;

                return new IntervalMonthData(months, this);
            }
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH : {
                int months = ((IntervalMonthData) a).units;

                return new IntervalMonthData(months, this);
            }
            case Types.SQL_INTERVAL_DAY : {
                int seconds = ((IntervalSecondData) a).units;

                seconds = (seconds / DateTimeType.dayToSecondFactors[0])
                          * DateTimeType.dayToSecondFactors[0];
            }
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_HOUR : {
                int seconds = ((IntervalSecondData) a).units;

                seconds = (seconds / DateTimeType.dayToSecondFactors[1])
                          * DateTimeType.dayToSecondFactors[1];
            }
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_MINUTE : {
                int seconds = ((IntervalSecondData) a).units;

                seconds = (seconds / DateTimeType.dayToSecondFactors[0])
                          * DateTimeType.dayToSecondFactors[0];
            }
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND : {
                int seconds = ((IntervalSecondData) a).units;
                int nanos   = 0;

                if (scale > 0) {
                    nanos =
                        (nanos / (DateTimeType.precisionFactors[scale - 1]))
                        * (DateTimeType.precisionFactors[scale - 1]);
                }

                new IntervalSecondData(seconds, nanos, this);
            }
            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        if (a instanceof String) {
            return convertToType(null, a, Type.SQL_VARCHAR);
        } else {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                return intervalMonthToString(a);

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
                return intervalSecondToString(a);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return convertToString(a);
    }

    public Object add(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                int months = ((IntervalMonthData) a).units
                             + ((IntervalMonthData) b).units;

                return new IntervalMonthData(months, this);

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
                long seconds = ((IntervalSecondData) a).units
                               + ((IntervalSecondData) b).units;
                long nanos = ((IntervalSecondData) a).nanos
                             + ((IntervalSecondData) b).nanos;

                return new IntervalSecondData(seconds, nanos, this, true);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public Object subtract(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                if (a instanceof IntervalMonthData
                        && b instanceof IntervalMonthData) {
                    int months = ((IntervalMonthData) a).units
                                 - ((IntervalMonthData) b).units;

                    return new IntervalMonthData(months, this);
                } else if (a instanceof Date && b instanceof Date) {
                    int months = DateTimeType.subtractMonths((Date) a,
                        (Date) b);

                    return new IntervalMonthData(months, this);
                } else if (a instanceof Timestamp && b instanceof Timestamp) {
                    int months = DateTimeType.subtractMonths((Timestamp) a,
                        (Timestamp) b);

                    return new IntervalMonthData(months, this);
                }

                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
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
                if (a instanceof IntervalSecondData
                        && b instanceof IntervalSecondData) {
                    long seconds = ((IntervalSecondData) a).units
                                   - ((IntervalSecondData) b).units;
                    long nanos = ((IntervalSecondData) a).nanos
                                 - ((IntervalSecondData) b).nanos;

                    return new IntervalSecondData(seconds, nanos, this, true);
                } else if (a instanceof Date && b instanceof Date) {
                    long seconds =
                        (((Date) a).getTime() - ((Date) b).getTime()) / 1000;

                    return new IntervalSecondData(seconds, 0, this);
                } else if (a instanceof TimeData && b instanceof TimeData) {
                    long seconds =
                        (((TimeData) a).getTime() - ((TimeData) b).getTime())
                        / 1000;
                    long nanos = ((TimeData) a).nanos - ((TimeData) b).nanos;

                    return new IntervalSecondData(seconds, nanos, this, true);
                } else if (a instanceof Timestamp && b instanceof Timestamp) {
                    long seconds =
                        (((Timestamp) a).getTime() - ((Timestamp) b)
                            .getTime()) / 1000;
                    long nanos = ((Timestamp) a).getNanos()
                                 - ((Timestamp) b).getNanos();

                    return new IntervalSecondData(seconds, nanos, this, true);
                }

                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public Object multiply(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        if (a instanceof Number) {
            Object temp = a;

            a = b;
            b = temp;
        }

        switch (endIntervalType) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_MONTH : {
                Integer months =
                    ValuePool.getInt(((IntervalMonthData) a).units);
                Object result = extractSecondType.multiply(months, b);

                return convertToType(null, result, extractSecondType);
            }
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_SECOND : {
                Integer seconds =
                    ValuePool.getInt(((IntervalSecondData) a).units);
                Object result = extractSecondType.multiply(seconds, b);

                return getIntervalType(this, maxIntervalPrecision,
                                       this.scale).convertToType(null,
                                           result, extractSecondType);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public Object divide(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (endIntervalType) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_MONTH : {
                Integer months =
                    ValuePool.getInt(((IntervalMonthData) a).units);
                Object result = extractSecondType.divide(months, b);

                return convertToType(null, result, extractSecondType);
            }
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_SECOND : {
                Integer seconds =
                    ValuePool.getInt(((IntervalSecondData) a).units);
                Object result = extractSecondType.divide(seconds, b);

                return getIntervalType(this, maxIntervalPrecision,
                                       this.scale).convertToType(null,
                                           result, extractSecondType);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "CharacterType");
        }
    }

    String intervalMonthToString(Object a) {

        StringBuffer buffer = new StringBuffer(8);
        int          months = ((IntervalMonthData) a).units;

        if (months < 0) {
            months = -months;

            buffer.append('-');
        }

        for (int i = startPartIndex; i <= endPartIndex; i++) {
            int  factor = DateTimeType.yearToMonthFactors[i];
            long part   = months / factor;

            if (i == startPartIndex) {
                int zeros = (int) precision
                            - getPrecisionExponent((int) part);

                for (int j = 0; j < zeros; j++) {
                    buffer.append('0');
                }
            } else if (part < 10) {
                buffer.append('0');
            }

            buffer.append(part);

            months = months % factor;

            if (i < endPartIndex) {
                buffer.append((char) DateTimeType.yearToMonthSeparators[i]);
            }
        }

        return buffer.toString();
    }

    String intervalSecondToString(Object a) {

        StringBuffer buffer  = new StringBuffer(64);
        long         seconds = ((IntervalSecondData) a).units;
        int          nanos   = ((IntervalSecondData) a).nanos;

        if (seconds < 0) {
            seconds = -seconds;

            buffer.append('-');
        }

        for (int i = startPartIndex; i <= endPartIndex; i++) {
            int  factor = DateTimeType.dayToSecondFactors[i];
            long part   = seconds / factor;

            if (i == startPartIndex) {
                int zeros = (int) precision
                            - getPrecisionExponent((int) part);

                for (int j = 0; j < zeros; j++) {
                    buffer.append('0');
                }
            } else if (part < 10) {
                buffer.append('0');
            }

            buffer.append(part);

            seconds = seconds % factor;

            if (i < endPartIndex) {
                buffer.append((char) DateTimeType.dayToSecondSeparators[i]);
            }
        }

        if (scale != 0) {
            buffer.append((char) DateTimeType
                .dayToSecondSeparators[DateTimeType.FRACTION_PART_INDEX - 1]);
        }

        for (int i = 0; i < scale; i++) {
            int digit = nanos / DateTimeType.precisionFactors[i];

            nanos -= digit * DateTimeType.precisionFactors[i];

            buffer.append(digit);
        }

        return buffer.toString();
    }

    public int getStartIntervalType() {
        return startIntervalType;
    }

    public int getEndIntervalType() {
        return endIntervalType;
    }

    public static IntervalType newIntervalType(int type, long precision,
            int fractionPrecision) {

        int startType = getStartIntervalType(type);
        int endType   = getEndIntervalType(type);

        return newIntervalType(type, startType, endType, precision,
                               fractionPrecision);
    }

    public static IntervalType newIntervalType(int type, int startType,
            int endType, long precision, int fractionPrecision) {

        IntervalType newType = new IntervalType(type, precision,
            fractionPrecision);

        newType.internalPrecision = 0;
        newType.startIntervalType = startType;
        newType.endIntervalType   = endType;
        newType.startPartIndex    = intervalIndexMap.get(startType);
        newType.endPartIndex      = intervalIndexMap.get(endType);

        return newType;
    }

    public static IntervalType getIntervalType(IntervalType type,
            long precision, int fractionalPrecision) {

        if (type.precision >= precision
                && type.scale >= fractionalPrecision) {
            return type;
        }

        return getIntervalType(type.type, precision, fractionalPrecision);
    }

    public static IntervalType getIntervalType(int type, long precision,
            int fractionPrecision) {

        int startType = getStartIntervalType(type);
        int endType   = getEndIntervalType(type);

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision);
    }

    public static IntervalType getIntervalType(int startIndex, int endIndex,
            long precision, int fractionPrecision) throws HsqlException {

        if (startIndex == -1 || endIndex == -1) {

            // todo right message
            throw Trace.error(Trace.UNEXPECTED_TOKEN);
        }

        if (startIndex > endIndex) {

            // todo right message
            throw Trace.error(Trace.UNEXPECTED_TOKEN);
        }

        if (startIndex <= DateTimeType.MONTH_INDEX
                && endIndex > DateTimeType.MONTH_INDEX) {

            // todo right message -- mixing YEAR_MONTH and other types
            throw Trace.error(Trace.UNEXPECTED_TOKEN);
        }

        int startType = DateTimeType.intervalParts[startIndex];
        int endType   = DateTimeType.intervalParts[endIndex];
        int type      = DateTimeType.intervalTypes[startIndex][endIndex];

        if (precision == 0 || precision > DateTimeType.maxIntervalPrecision
                || fractionPrecision > DateTimeType.maxFractionPrecision) {
            throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
        }

        if (precision == -1) {
            precision = DateTimeType.defaultIntervalPrecision;
        }

        if (fractionPrecision == -1) {
            if (endType == Types.SQL_INTERVAL_SECOND) {
                fractionPrecision =
                    DateTimeType.defaultIntervalFractionPrecision;
            } else {
                fractionPrecision = 0;
            }
        }

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision);
    }

    public static IntervalType getIntervalType(int type, int startType,
            int endType, long precision, int fractionPrecision) {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_YEAR;
                }
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_YEAR_TO_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_MONTH :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_DAY :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY;
                }
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY_TO_HOUR;
                }
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_DAY_TO_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                if (precision == DateTimeType.defaultIntervalPrecision
                        && fractionPrecision
                           == DateTimeType.defaultDatetimeFractionPrecision) {
                    return SQL_INTERVAL_DAY_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_HOUR :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_HOUR;
                }
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_HOUR_TO_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE :
                if (precision == DateTimeType.defaultIntervalPrecision) {
                    return SQL_INTERVAL_HOUR_TO_MINUTE;
                }
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                if (precision == DateTimeType.defaultIntervalPrecision
                        && fractionPrecision
                           == DateTimeType.defaultDatetimeFractionPrecision) {
                    return SQL_INTERVAL_HOUR_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                if (precision == DateTimeType.defaultIntervalPrecision
                        && fractionPrecision
                           == DateTimeType.defaultDatetimeFractionPrecision) {
                    return SQL_INTERVAL_MINUTE_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_SECOND :
                if (precision == DateTimeType.defaultIntervalPrecision
                        && fractionPrecision
                           == DateTimeType.defaultDatetimeFractionPrecision) {
                    return SQL_INTERVAL_SECOND;
                }
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }

        return newIntervalType(type, startType, endType, precision,
                               fractionPrecision);
    }

    public Object newInterval(String s) throws HsqlException {

        byte[] separators;
        int[]  factors;
        int[]  limits;
        int    firstPart;
        int    lastPart;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                separators = yearToMonthSeparators;
                factors    = yearToMonthFactors;
                limits     = yearToMonthLimits;
                firstPart  = 0;
                lastPart   = 0;
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                separators = yearToMonthSeparators;
                factors    = yearToMonthFactors;
                limits     = yearToMonthLimits;
                firstPart  = 0;
                lastPart   = 1;
                break;

            case Types.SQL_INTERVAL_MONTH :
                separators = yearToMonthSeparators;
                factors    = yearToMonthFactors;
                limits     = yearToMonthLimits;
                firstPart  = 1;
                lastPart   = 1;
                break;

            case Types.SQL_INTERVAL_DAY :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 0;
                lastPart   = 0;
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 0;
                lastPart   = 1;
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 0;
                lastPart   = 2;
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 0;
                lastPart   = 4;
                break;

            case Types.SQL_INTERVAL_HOUR :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 1;
                lastPart   = 1;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 1;
                lastPart   = 2;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 1;
                lastPart   = 4;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 2;
                lastPart   = 2;
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 2;
                lastPart   = 4;
                break;

            case Types.SQL_INTERVAL_SECOND :
                separators = dayToSecondSeparators;
                factors    = dayToSecondFactors;
                limits     = dayToSecondLimits;
                firstPart  = 3;
                lastPart   = 4;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DataTime");
        }

        int     fraction      = 0;
        int     totalValue    = 0;
        int     currentValue  = 0;
        boolean negate        = false;
        int     i             = 0;
        int     currentPart   = firstPart;
        boolean isYearMonth   = separators == yearToMonthSeparators;
        int     currentDigits = 0;

        for (; i < s.length(); i++) {
            if (s.charAt(i) != ' ') {
                break;
            }
        }

        if (s.charAt(i) == '-') {
            negate = true;

            i++;
        }

        for (; currentPart <= lastPart; i++) {
            boolean endOfPart = false;

            if (i > s.length()) {
                break;
            } else if (i == s.length()) {
                if (currentPart == lastPart) {
                    endOfPart = true;
                } else if (currentPart == FRACTION_PART_INDEX - 1) {
                    endOfPart = true;
                } else {

                    // parts missing
                    throw Trace.error(Trace.UNEXPECTED_TOKEN);
                }
            } else {
                int character = s.charAt(i);

                if (character >= '0' && character <= '9') {
                    int digit = character - '0';

                    currentValue *= 10;
                    currentValue += digit;

                    currentDigits++;
                } else if (character == separators[currentPart]) {
                    endOfPart = true;
                } else if (character == ' ' && currentPart == lastPart) {
                    endOfPart = true;
                } else {
                    throw Trace.error(Trace.UNEXPECTED_TOKEN);
                }
            }

            if (endOfPart) {
                if (currentPart == firstPart) {
                    if (currentValue >= precisionLimits[(int) precision]
                            || currentDigits > precision) {

                        // todo better message
                        throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                    }

                    if (currentDigits == 0) {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN);
                    }

                    totalValue += currentValue * factors[currentPart];
                } else if (currentPart == FRACTION_PART_INDEX) {
                    if (currentDigits > maxFractionPrecision) {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN);
                    }

                    if (currentDigits > 0) {
                        fraction = currentValue
                                   * precisionFactors[currentDigits - 1];
                    }
                } else {
                    if (currentValue >= limits[currentPart]) {
                        throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                    }

                    if (currentDigits != 2) {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN);
                    }

                    totalValue += currentValue * factors[currentPart];
                }

                currentPart++;

                currentValue  = 0;
                currentDigits = 0;
            }
        }

        for (; i < s.length(); i++) {
            if (s.charAt(i) != ' ') {
                throw Trace.error(Trace.UNEXPECTED_TOKEN);
            }
        }

        if (negate) {
            totalValue = -totalValue;
        }

        if (isYearMonth) {
            return new IntervalMonthData((int) totalValue, this);
        } else {
            return new IntervalSecondData(totalValue, fraction, this);
        }
    }

    public static int getStartIntervalType(int type) {

        int startType;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                startType = Types.SQL_INTERVAL_YEAR;
                break;

            case Types.SQL_INTERVAL_MONTH :
                startType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                startType = Types.SQL_INTERVAL_DAY;
                break;

            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                startType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                startType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_SECOND :
                startType = Types.SQL_INTERVAL_SECOND;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }

        return startType;
    }

    public static int getEndIntervalType(int type) {

        int endType;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                endType = Types.SQL_INTERVAL_YEAR;
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                endType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_MONTH :
                endType = Types.SQL_INTERVAL_MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
                endType = Types.SQL_INTERVAL_DAY;
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                endType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_HOUR :
                endType = Types.SQL_INTERVAL_HOUR;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                endType = Types.SQL_INTERVAL_MINUTE;
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_INTERVAL_SECOND :
                endType = Types.SQL_INTERVAL_SECOND;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }

        return endType;
    }

    public static Type getCombinedIntervalType(IntervalType type1,
            IntervalType type2) {

        int startType = type2.startIntervalType > type1.startIntervalType
                        ? type1.startIntervalType
                        : type2.startIntervalType;
        int endType = type2.endIntervalType > type1.endIntervalType
                      ? type2.endIntervalType
                      : type1.endIntervalType;
        int  type              = getCombinedIntervalType(startType, endType);
        long precision = type1.precision > type2.precision ? type1.precision
                                                           : type2.precision;
        int  fractionPrecision = type1.scale > type2.scale ? type1.scale
                                                           : type2.scale;

        return getIntervalType(type, startType, endType, precision,
                               fractionPrecision);
    }

    public static int getCombinedIntervalType(int startType, int endType) {

        if (startType == endType) {
            return startType;
        }

        switch (startType) {

            case Types.SQL_INTERVAL_YEAR :
                if (endType == Types.SQL_INTERVAL_MONTH) {
                    return Types.SQL_INTERVAL_YEAR_TO_MONTH;
                }
                break;

            case Types.SQL_INTERVAL_DAY :
                switch (endType) {

                    case Types.SQL_INTERVAL_HOUR :
                        return Types.SQL_INTERVAL_DAY_TO_HOUR;

                    case Types.SQL_INTERVAL_MINUTE :
                        return Types.SQL_INTERVAL_DAY_TO_MINUTE;

                    case Types.SQL_INTERVAL_SECOND :
                        return Types.SQL_INTERVAL_DAY_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_HOUR :
                switch (endType) {

                    case Types.SQL_INTERVAL_MINUTE :
                        return Types.SQL_INTERVAL_HOUR_TO_MINUTE;

                    case Types.SQL_INTERVAL_SECOND :
                        return Types.SQL_INTERVAL_HOUR_TO_SECOND;
                }
                break;

            case Types.SQL_INTERVAL_MINUTE :
                if (endType == Types.SQL_INTERVAL_SECOND) {
                    return Types.SQL_INTERVAL_MINUTE_TO_SECOND;
                }
                break;

            default :
        }

        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "IntervalType");
    }

    int getIntervalValueLimit() {

        int limit;

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                limit = DateTimeType.precisionLimits[(int) precision] * 12;
                break;

            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
                limit = DateTimeType.precisionLimits[(int) precision] * 12;
                limit += 12;
                break;

            case Types.SQL_INTERVAL_MONTH :
                limit = DateTimeType.precisionLimits[(int) precision];
                break;

            case Types.SQL_INTERVAL_DAY :
                limit = DateTimeType.precisionLimits[(int) precision] * 24
                        * 60 * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_HOUR :
                limit = DateTimeType.precisionLimits[(int) precision] * 24
                        * 60 * 60;
                limit += 24 * 60 * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
                limit = DateTimeType.precisionLimits[(int) precision] * 24
                        * 60 * 60;
                limit += 24 * 60 * 60 + 60 * 60;
                break;

            case Types.SQL_INTERVAL_DAY_TO_SECOND :
                limit = DateTimeType.precisionLimits[(int) precision] * 24
                        * 60 * 60;
                limit += 24 * 60 * 60 + 60 * 60 + 60;
                break;

            case Types.SQL_INTERVAL_HOUR :
                limit = DateTimeType.precisionLimits[(int) precision] * 60
                        * 60;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
                limit = DateTimeType.precisionLimits[(int) precision] * 60
                        * 60;
                limit += 60 * 60 + 60;
                break;

            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
                limit = DateTimeType.precisionLimits[(int) precision] * 60
                        * 60;
                limit += 60;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                limit = DateTimeType.precisionLimits[(int) precision] * 60;
                break;

            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
                limit = DateTimeType.precisionLimits[(int) precision] * 60;
                limit += 60;
                break;

            case Types.SQL_INTERVAL_SECOND :
                limit = DateTimeType.precisionLimits[(int) precision];
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Type");
        }

        return limit;
    }

    public int getPart(Object interval, int part) {

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                return ((IntervalMonthData) interval).units / 12;

            case Types.SQL_INTERVAL_MONTH :
                return ((IntervalMonthData) interval).units % 12;

            case Types.SQL_INTERVAL_DAY :
                return ((IntervalSecondData) interval).units / (24 * 60 * 60);

            case Types.SQL_INTERVAL_HOUR : {
                int val = ((IntervalSecondData) interval).units
                          % (24 * 60 * 60);

                return val / (60 * 60);
            }
            case Types.SQL_INTERVAL_MINUTE : {
                int val = ((IntervalSecondData) interval).units % (60 * 60);

                return val / 60;
            }
            case Types.SQL_INTERVAL_SECOND :
                return ((IntervalSecondData) interval).units % 60;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public BigDecimal getSecondPart(Object interval) {

        long seconds = ((IntervalSecondData) interval).units % 60;
        int  nanos   = 0;

        nanos = ((IntervalSecondData) interval).nanos;

        return getSecondPart(seconds, nanos, scale);
    }

    public int convertToInt(Object interval) {

        switch (endIntervalType) {

            case Types.SQL_INTERVAL_YEAR :
                return ((IntervalMonthData) interval).units / 12;

            case Types.SQL_INTERVAL_MONTH :
                return ((IntervalMonthData) interval).units;

            case Types.SQL_INTERVAL_DAY : {
                long seconds = ((IntervalSecondData) interval).units;

                return (int) (seconds / DateTimeType.dayToSecondFactors[0]);
            }
            case Types.SQL_INTERVAL_HOUR : {
                long seconds = ((IntervalSecondData) interval).units;

                return (int) (seconds / DateTimeType.dayToSecondFactors[1]);
            }
            case Types.SQL_INTERVAL_MINUTE : {
                long seconds = ((IntervalSecondData) interval).units;

                return (int) (seconds / DateTimeType.dayToSecondFactors[2]);
            }
            case Types.SQL_INTERVAL_SECOND :
                long seconds = ((IntervalSecondData) interval).units;

                return (int) seconds;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public static void main(String[] args) {

        IntervalType t = null;
        Object       i = null;
        String       s = null;

        try {
            s = "200 10";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                             4, 0);
            i = t.newInterval(s);
            s = "200 10:12:12.456789";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 6);
            i = t.newInterval(s);
            s = " 200 10:12:12.456789  ";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 7);
            i = t.newInterval(s);
            s = " 200 10:12:12.";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);
            s = " 200 10:12:12. ";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);
            s = " 200 10:12:12";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);
            s = " 200 10:0:12";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);
        } catch (HsqlException e) {
            System.out.println(s);
        }

        try {
            s = "20000 10";    // first part too long
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                             4, 0);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = "2000 90";    // other part too large
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                                             4, 0);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = "200 10:12:123.456789";    // other part to long
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = " 200 10:12 12.456789  ";    // bad separator
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = " 200 10:12:12 456789  ";    // bad separator
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = " 200 10:12:12 .";    // bad separator
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}

        try {
            s = " 20000 10:12:12. ";    // first part too long
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = t.newInterval(s);

            System.out.println(s);
        } catch (HsqlException e) {}
    }
}
