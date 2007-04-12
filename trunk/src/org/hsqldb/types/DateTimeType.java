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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.hsqldb.Expression;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.Library;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.StringUtil;

/**
 * Type instance for DATE, TIME and TIMESTAMP.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class DateTimeType extends DateTimeIntervalType {

    public DateTimeType(int type, int scale) {
        super(type, 0, scale);
    }

    protected DateTimeType(int type, int precision, int scale) {
        super(type, 0, scale);
    }

    public int displaySize() {

        switch (type) {

            case Types.SQL_DATE :
                return 10;

            case Types.SQL_TIME :
                return 8 + (scale == 0 ? 0
                                       : scale + 1);

            case Types.SQL_TIMESTAMP :
                return 19 + (scale == 0 ? 0
                                        : scale + 1);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public int getJDBCTypeNumber() {

        // JDBC numbers happen to be the same as SQL
        return type;
    }

    public String getJDBCClassName() {

        switch (type) {

            case Types.SQL_DATE :
                return "java.sql.Date";

            case Types.SQL_TIME :
                return "org.hsqldb.types.TimeData";

            case Types.SQL_TIMESTAMP :
                return "java.sql.Timestamp";

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public int getSQLGenericTypeNumber() {
        return Types.SQL_DATETIME;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getName() {

        switch (type) {

            case Types.SQL_DATE :
                return Token.T_DATE;

            case Types.SQL_TIME :
                return Token.T_TIME;

            case Types.SQL_TIMESTAMP :
                return Token.T_TIMESTAMP;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public String getDefinition() {

        if (type == Types.SQL_DATE
                || scale == defaultDatetimeFractionPrecision) {
            return getName();
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getName());
        sb.append('(');
        sb.append(scale);
        sb.append(')');

        return sb.toString();
    }

    public boolean isDateTimeType() {
        return true;
    }

    public boolean acceptsFractionalPrecision() {
        return type == Types.SQL_TIME || type == Types.SQL_TIMESTAMP;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return scale >= other.scale ? this
                                        : other;
        }

        switch (other.type) {

            case Types.SQL_ALL_TYPES :
                return this;

            case Types.SQL_DATE :
                if (other.type == Types.SQL_TIMESTAMP) {
                    return other;
                }
                break;

            case Types.SQL_TIME :
                break;

            case Types.SQL_TIMESTAMP :
                if (other.type == Types.SQL_DATE) {
                    return this;
                }
                break;

            default :
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {

        switch (operation) {

            case Expression.EQUAL :
            case Expression.GREATER :
            case Expression.GREATER_EQUAL :
            case Expression.SMALLER_EQUAL :
            case Expression.SMALLER :
            case Expression.NOT_EQUAL :
            case Expression.ALTERNATIVE :
                return getAggregateType(other);

            case Expression.MULTIPLY :
            case Expression.DIVIDE :
                break;

            case Expression.ADD :
            case Expression.SUBTRACT :
                if (other.isIntervalType()) {
                    return this;
                }
                break;

            default :
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

            case Types.SQL_DATE :
                return HsqlDateTime.compare((Date) a, (Date) b);

            case Types.SQL_TIME :
                return HsqlDateTime.compare((TimeData) a, (TimeData) b);

            case Types.SQL_TIMESTAMP :
                return HsqlDateTime.compare((Timestamp) a, (Timestamp) b);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (scale == maxFractionPrecision) {
            return a;
        }

        switch (type) {

            case Types.SQL_DATE :
                return a;

            case Types.SQL_TIME : {
                TimeData ti       = (TimeData) a;
                int      nanos    = ti.getNanos();
                int      divisor  = nanoScaleFactors[scale];
                int      newNanos = (nanos / divisor) * divisor;

                ti.setNanos(newNanos);

                return ti;
            }
            case Types.SQL_TIMESTAMP : {
                Timestamp ts       = (Timestamp) a;
                int       nanos    = ts.getNanos();
                int       divisor  = nanoScaleFactors[scale];
                int       newNanos = (nanos / divisor) * divisor;

                ts.setNanos(newNanos);

                return ts;
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return a;
        }

        switch (otherType.type) {

            case Types.SQL_CLOB :
                a = a.toString();
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                a = Library.trim((String) a, " ", true, true);

                switch (this.type) {

                    case Types.SQL_DATE :
                        a = HsqlDateTime.dateValue((String) a);

                        return convertToTypeLimits(a);

                    case Types.SQL_TIME :
                        a = newTime((String) a).value;

                        return convertToTypeLimits(a);

                    case Types.SQL_TIMESTAMP :
                        a = HsqlDateTime.timestampValue((String) a);

                        return convertToTypeLimits(a);
                }
            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP :
                break;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION);
        }

        switch (this.type) {

            case Types.SQL_DATE :
                switch (otherType.type) {

                    case Types.SQL_DATE :
                        return a;

                    case Types.SQL_TIMESTAMP :
                        return HsqlDateTime.getNormalisedDate((Timestamp) a);

                    default :
                        throw Trace.error(Trace.INVALID_CONVERSION);
                }
            case Types.SQL_TIME :
                switch (otherType.type) {

                    case Types.SQL_TIME :
                        return convertToTypeLimits(a);

                    case Types.SQL_TIMESTAMP :
                        Timestamp ts = (Timestamp) a;
                        long ms =
                            HsqlDateTime.convertToNormalisedTime(ts.getTime());

                        a = new TimeData(ms, ts.getNanos());

                        return convertToTypeLimits(a);

                    default :
                        throw Trace.error(Trace.INVALID_CONVERSION);
                }
            case Types.SQL_TIMESTAMP : {
                switch (otherType.type) {

                    case Types.SQL_TIME :
                        long millis = session == null
                                      ? HsqlDateTime.getNormalisedDate(
                                          System.currentTimeMillis())
                                      : session.getCurrentDate().getTime();

                        millis += ((TimeData) a).getTime();
                        a      = HsqlDateTime.getTimestamp(millis);

                        return convertToTypeLimits(a);

                    case Types.SQL_TIMESTAMP :
                        return convertToTypeLimits(a);

                    case Types.SQL_DATE :
                        return HsqlDateTime.getNormalisedTimestamp((Date) a);

                    default :
                        throw Trace.error(Trace.INVALID_CONVERSION);
                }
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        if (a instanceof String) {
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

            case Types.SQL_DATE :
                return HsqlDateTime.getDateString((Date) a, null);

            case Types.SQL_TIME :
                return HsqlDateTime.getTimeString((TimeData) a, null)
                       + StringUtil.toZeroPaddedString(((TimeData) a).nanos,
                                                       9, scale);

            case Types.SQL_TIMESTAMP :
                return HsqlDateTime.getTimestampString((Timestamp) a, scale);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.toQuotedString(convertToString(a), '\'',
                                              false);
    }

    public Object add(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_DATE :
                if (b instanceof IntervalMonthData) {
                    return addMonths((Date) a, ((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((Date) a,
                                      (int) ((IntervalSecondData) b).units);
                }
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION, "IntervalType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      (int) ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths((Timestamp) a,
                                     ((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((Timestamp) a,
                                      (int) ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public Object subtract(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.SQL_DATE :
                if (b instanceof IntervalMonthData) {
                    return addMonths((Date) a,
                                     -((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((Date) a,
                                      -(int) ((IntervalSecondData) b).units);
                }
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION, "IntervalType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      -(int) ((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths((Timestamp) a,
                                     -((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((Timestamp) a,
                                      -(int) ((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    public int getPart(Object dateTime, int part) {

        int calendarPart;

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                calendarPart = Calendar.YEAR;
                break;

            case Types.SQL_INTERVAL_MONTH :
                return HsqlDateTime.getDateTimePart((java.util.Date) dateTime, Calendar.MONTH)
                       + 1;

            case Types.SQL_INTERVAL_DAY :
                calendarPart = Calendar.DAY_OF_MONTH;
                break;

            case Types.SQL_INTERVAL_HOUR :
                calendarPart = Calendar.HOUR;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                calendarPart = Calendar.MINUTE;
                break;

            case Types.SQL_INTERVAL_SECOND :
                calendarPart = Calendar.SECOND;
                break;

            case DAY_OF_WEEK :
                calendarPart = Calendar.DAY_OF_WEEK;
                break;

            case WEEK_OF_YEAR :
                calendarPart = Calendar.WEEK_OF_YEAR;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }

        return HsqlDateTime.getDateTimePart((java.util.Date) dateTime,
                                            calendarPart);
    }

    public BigDecimal getSecondPart(Object dateTime) {

        long seconds = getPart(dateTime, Types.SQL_INTERVAL_SECOND);
        int  nanos   = 0;

        if (type == Types.SQL_TIMESTAMP) {
            nanos = ((Timestamp) dateTime).getNanos();
        } else if (type == Types.SQL_TIME) {
            nanos = ((TimeData) dateTime).getNanos();
        }

        return getSecondPart(seconds, nanos, scale);
    }

    public static DateTimeType getDateTimeType(int type,
            int scale) throws HsqlException {

        if (scale > DateTimeType.maxFractionPrecision) {
            throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
        }

        if (scale != DateTimeType.defaultDatetimeFractionPrecision) {
            return new DateTimeType(type, scale);
        }

        switch (type) {

            case Types.SQL_DATE :
                return SQL_DATE;

            case Types.SQL_TIME :
                return SQL_TIME;

            case Types.SQL_TIMESTAMP :
                return SQL_TIMESTAMP;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "DateTimeType");
        }
    }

    // experimental
    public boolean canAdd(Type other) {

        if (!other.isIntervalType()) {
            return false;
        }

        int start = ((IntervalType) other).startIntervalType;
        int end   = ((IntervalType) other).endIntervalType;

        switch (type) {

            case Types.SQL_DATE :
                if (start > Types.SQL_INTERVAL_DAY) {
                    return false;
                }
            case Types.SQL_TIME :
                if (end < Types.SQL_INTERVAL_HOUR) {
                    return false;
                }
            case Types.SQL_TIMESTAMP :
        }

        return true;
    }

    public static Boolean overlaps(Session session, Object[] a, Type[] ta,
                                   Object[] b,
                                   Type[] tb) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        if (a[0] == null || b[0] == null) {
            return null;
        }

        if (a[1] == null) {
            a[1] = a[0];
        }

        if (b[1] == null) {
            b[1] = b[0];
        }

        Type commonType = ta[0].getCombinedType(tb[0], Expression.EQUAL);

        a[0] = commonType.castToType(session, a[0], ta[0]);
        b[0] = commonType.castToType(session, b[0], tb[0]);

        if (ta[1].isIntervalType()) {
            a[1] = commonType.add(a[0], a[1]);
        } else {
            a[1] = commonType.castToType(session, a[1], ta[1]);
        }

        if (tb[1].isIntervalType()) {
            b[1] = commonType.add(b[0], b[1]);
        } else {
            b[1] = commonType.castToType(session, b[1], tb[1]);
        }

        if (commonType.compare(a[0], a[1]) > 0) {
            Object temp = a[0];

            a[0] = a[1];
            a[1] = temp;
        }

        if (commonType.compare(b[0], b[1]) > 0) {
            Object temp = b[0];

            b[0] = b[1];
            b[1] = temp;
        }

        if (commonType.compare(a[0], b[0]) > 0) {
            Object[] temp = a;

            a = b;
            b = temp;
        }

        if (commonType.compare(a[1], b[0]) > 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    //
    private static Calendar tempCalGMT =
        new GregorianCalendar(TimeZone.getTimeZone("GMT"));

    public static int subtractMonths(java.util.Date a,
                                     java.util.Date b) throws HsqlException {

        synchronized (tempCalGMT) {
            boolean negate = false;

            if (b.after(a)) {
                negate = true;

                java.util.Date temp = a;

                a = b;
                b = temp;
            }

            tempCalGMT.setTimeInMillis(b.getTime());

            int months = tempCalGMT.get(Calendar.MONTH);
            int years  = tempCalGMT.get(Calendar.YEAR);

            tempCalGMT.setTimeInMillis(a.getTime());
            tempCalGMT.add(Calendar.MONTH, -months);
            tempCalGMT.add(Calendar.YEAR, -years);

            months = tempCalGMT.get(Calendar.MONTH);
            years  = tempCalGMT.get(Calendar.YEAR);
            months += years * 12;

            if (negate) {
                months = -months;
            }

            return months;
        }
    }

    public static Date addMonths(Date date, int months) throws HsqlException {

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(date.getTime());
            tempCalGMT.add(Calendar.MONTH, months);

            Date da = new Date(tempCalGMT.getTimeInMillis());

            // check range
            return da;
        }
    }

    public static Date addSeconds(Date date,
                                  int seconds) throws HsqlException {

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(date.getTime());
            tempCalGMT.add(Calendar.SECOND, seconds);

            Date da = new Date(tempCalGMT.getTimeInMillis());

            // check range
            return da;
        }
    }

    public static TimeData addSeconds(TimeData source, int seconds,
                                      int nanos) throws HsqlException {

        nanos   += source.getNanos();
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += DateTimeType.limitNanoseconds;

            seconds--;
        }

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(source.getTime());
            tempCalGMT.add(Calendar.SECOND, seconds);

            seconds = (int) (tempCalGMT.getTimeInMillis() / 1000);

            TimeData ti = new TimeData(seconds, nanos);

            // check range
            return ti;
        }
    }

    public static Timestamp addMonths(Timestamp source,
                                      int months) throws HsqlException {

        int n = source.getNanos();

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(source.getTime());
            tempCalGMT.add(Calendar.MONTH, months);

            Timestamp ts = new Timestamp(tempCalGMT.getTimeInMillis());

            ts.setNanos(n);

            return ts;
        }
    }

    public static Timestamp addSeconds(Timestamp source, int seconds,
                                       int nanos) throws HsqlException {

        nanos   += source.getNanos();
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += limitNanoseconds;

            seconds--;
        }

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(source.getTime());
            tempCalGMT.add(Calendar.SECOND, seconds);

            Timestamp ts = new Timestamp(tempCalGMT.getTimeInMillis());

            ts.setNanos(nanos);

            return ts;
        }
    }

    /**
     * todo - This returns a GMT time value, whereas a local time may be needed
     * in ops
     */
    public static TypedData newTime(String s) throws HsqlException {

        byte[] separators    = dayToSecondSeparators;
        int[]  factors       = dayToSecondFactors;
        int[]  limits        = dayToSecondLimits;
        int    firstPart     = 1;
        int    lastPart      = 4;
        int    fraction      = 0;
        long   totalValue    = 0;
        int    currentValue  = 0;
        int    i             = 0;
        int    currentPart   = firstPart;
        int    currentDigits = 0;
        int    scale         = 0;

        for (; i < s.length(); i++) {
            if (s.charAt(i) != ' ') {
                break;
            }
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
                if (currentPart == FRACTION_PART_INDEX) {
                    if (currentDigits > DateTimeType.maxFractionPrecision) {
                        throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                    }

                    if (currentDigits > 0) {
                        fraction = currentValue
                                   * precisionFactors[currentDigits - 1];
                    }

                    scale = currentDigits;
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

        DateTimeType type = DateTimeType.getDateTimeType(Types.SQL_TIME,
            scale);

        return new TypedData(new TimeData((int) totalValue, fraction), type);
    }

    public static int getScale(String s) {

        int i = s.indexOf('.');

        return i < 0 ? 0
                     : s.length() - i - 1;
    }
}
