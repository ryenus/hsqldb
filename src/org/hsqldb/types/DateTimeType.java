/* Copyright (c) 2001-2022, The HSQL Development Group
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
import java.text.SimpleDateFormat;

//#ifdef JAVA8
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

//#endif JAVA8
import java.util.Calendar;
import java.util.Date;

import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;

import java.util.TimeZone;

/**
 * Type subclass for DATE, TIME and TIMESTAMP.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
public final class DateTimeType extends DTIType {

    public final boolean withTimeZone;
    private final String nameString;
    public static final long epochSeconds =
        HsqlDateTime.getDateSeconds("1-01-01");
    public static final TimestampData epochTimestamp =
        new TimestampData(epochSeconds);
    public static final long epochLimitSeconds =
        HsqlDateTime.getDateSeconds("10000-01-01");
    public static final TimestampData epochLimitTimestamp =
        new TimestampData(epochLimitSeconds);

    // this is used for the lifetime of the JVM - it should not be altered
    public final static TimeZone systemTimeZone = TimeZone.getDefault();

    public DateTimeType(int typeGroup, int type, int scale) {

        super(typeGroup, type, 0, scale);

        withTimeZone = type == Types.SQL_TIME_WITH_TIME_ZONE
                       || type == Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
        nameString = getNameStringPrivate();
    }

    public int displaySize() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return 10;

            case Types.SQL_TIME :
                return 8 + (scale == 0 ? 0
                                       : scale + 1);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return 8 + (scale == 0 ? 0
                                       : scale + 1) + 6;

            case Types.SQL_TIMESTAMP :
                return 19 + (scale == 0 ? 0
                                        : scale + 1);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return 19 + (scale == 0 ? 0
                                        : scale + 1) + 6;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public int getJDBCTypeCode() {

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return Types.TIME_WITH_TIMEZONE;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return Types.TIMESTAMP_WITH_TIMEZONE;

            default :

                // JDBC numbers happen to be the same as SQL
                return typeCode;
        }
    }

    public Class getJDBCClass() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return java.sql.Date.class;

            case Types.SQL_TIME :
                return java.sql.Time.class;

            case Types.SQL_TIMESTAMP :
                return java.sql.Timestamp.class;

//#ifdef JAVA8
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return OffsetTime.class;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return OffsetDateTime.class;

//#else
/*
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return java.sql.Time.class;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return java.sql.Timestamp.class;
*/

//#endif JAVA8
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public String getJDBCClassName() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return "java.sql.Date";

            case Types.SQL_TIME :
                return "java.sql.Time";

            case Types.SQL_TIMESTAMP :
                return "java.sql.Timestamp";

//#ifdef JAVA8
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return "java.time.OffsetTime";

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return "java.time.OffsetDateTime";

//#else
/*
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return "java.sql.Time";

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return "java.sql.Timestamp";
*/

//#endif JAVA8
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public int getJDBCPrecision() {
        return this.displaySize();
    }

    public int getSQLGenericTypeCode() {
        return Types.SQL_DATETIME;
    }

    public String getNameString() {
        return nameString;
    }

    public boolean canCompareDirect(Type otherType) {
        return typeCode == otherType.typeCode;
    }

    private String getNameStringPrivate() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return Tokens.T_DATE;

            case Types.SQL_TIME :
                return Tokens.T_TIME;

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return Tokens.T_TIME + ' ' + Tokens.T_WITH + ' '
                       + Tokens.T_TIME + ' ' + Tokens.T_ZONE;

            case Types.SQL_TIMESTAMP :
                return Tokens.T_TIMESTAMP;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return Tokens.T_TIMESTAMP + ' ' + Tokens.T_WITH + ' '
                       + Tokens.T_TIME + ' ' + Tokens.T_ZONE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public String getDefinition() {

        String token;

        switch (typeCode) {

            case Types.SQL_DATE :
                return Tokens.T_DATE;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (scale == DTIType.defaultTimeFractionPrecision) {
                    return getNameString();
                }

                token = Tokens.T_TIME;
                break;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (scale == DTIType.defaultTimestampFractionPrecision) {
                    return getNameString();
                }

                token = Tokens.T_TIMESTAMP;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }

        StringBuilder sb = new StringBuilder(16);

        sb.append(token);
        sb.append('(');
        sb.append(scale);
        sb.append(')');

        if (withTimeZone) {
            sb.append(' ' + Tokens.T_WITH + ' ' + Tokens.T_TIME + ' '
                      + Tokens.T_ZONE);
        }

        return sb.toString();
    }

    public boolean isDateTimeType() {
        return true;
    }

    public boolean isDateOrTimestampType() {

        switch (typeCode) {

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return true;

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return false;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public boolean isTimestampType() {

        switch (typeCode) {

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return true;

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return false;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public boolean isTimeType() {

        switch (typeCode) {

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_DATE :
                return false;

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return true;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public boolean isDateTimeTypeWithZone() {
        return withTimeZone;
    }

    public boolean acceptsFractionalPrecision() {
        return typeCode != Types.SQL_DATE;
    }

    public Type getAggregateType(Type other) {

        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        // DATE with DATE returned here
        if (typeCode == other.typeCode) {
            return scale >= other.scale ? this
                                        : other;
        }

        if (other.typeCode == Types.SQL_ALL_TYPES) {
            return this;
        }

        if (other.isCharacterType()) {
            return other.getAggregateType(this);
        }

        if (!other.isDateTimeType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        DateTimeType otherType = (DateTimeType) other;

        // DATE with TIME caught here
        if (otherType.startIntervalType > endIntervalType
                || startIntervalType > otherType.endIntervalType) {
            throw Error.error(ErrorCode.X_42562);
        }

        int     newType = typeCode;
        int     scale   = this.scale > otherType.scale ? this.scale
                                                       : otherType.scale;
        boolean zone    = withTimeZone || otherType.withTimeZone;
        int startType = otherType.startIntervalType > startIntervalType
                        ? startIntervalType
                        : otherType.startIntervalType;

        if (startType == Types.SQL_INTERVAL_HOUR) {
            newType = zone ? Types.SQL_TIME_WITH_TIME_ZONE
                           : Types.SQL_TIME;
        } else {
            newType = zone ? Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                           : Types.SQL_TIMESTAMP;
        }

        return getDateTimeType(newType, scale);
    }

    public Type getCombinedType(Session session, Type other, int operation) {

        switch (operation) {

            case OpTypes.EQUAL :
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.SMALLER :
            case OpTypes.NOT_EQUAL : {
                if (typeCode == other.typeCode) {
                    return this;
                }

                if (other.typeCode == Types.SQL_ALL_TYPES) {
                    return this;
                }

                if (!other.isDateTimeType()) {
                    throw Error.error(ErrorCode.X_42562);
                }

                DateTimeType otherType = (DateTimeType) other;

                // DATE with TIME caught here
                if (otherType.startIntervalType > endIntervalType
                        || startIntervalType > otherType.endIntervalType) {
                    throw Error.error(ErrorCode.X_42562);
                }

                int     newType = typeCode;
                int     scale   = this.scale > otherType.scale ? this.scale
                                                               : otherType
                                                                   .scale;
                boolean zone    = withTimeZone || otherType.withTimeZone;
                int startType = otherType.startIntervalType
                                > startIntervalType ? startIntervalType
                                                    : otherType
                                                        .startIntervalType;

                if (startType == Types.SQL_INTERVAL_HOUR) {
                    newType = zone ? Types.SQL_TIME_WITH_TIME_ZONE
                                   : Types.SQL_TIME;
                } else {
                    newType = zone ? Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                                   : Types.SQL_TIMESTAMP;
                }

                return getDateTimeType(newType, scale);
            }
            case OpTypes.ADD :
            case OpTypes.SUBTRACT :
                if (other.isIntervalType()) {
                    if (typeCode != Types.SQL_DATE && other.scale > scale) {
                        return getDateTimeType(typeCode, other.scale);
                    }

                    return this;
                } else if (other.isDateTimeType()) {
                    if (operation == OpTypes.SUBTRACT) {
                        if (other.typeComparisonGroup == typeComparisonGroup) {
                            if (typeCode == Types.SQL_DATE) {
                                return Type.SQL_INTERVAL_DAY_MAX_PRECISION;
                            } else {
                                return Type
                                    .SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION;
                            }
                        }
                    }
                } else if (other.isNumberType()) {
                    return this;
                }
                break;

            default :
        }

        throw Error.error(ErrorCode.X_42562);
    }

    public int compare(Session session, Object a, Object b) {

        long diff;

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        switch (typeCode) {

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE : {
                diff = ((TimeData) a).seconds - ((TimeData) b).seconds;

                if (diff == 0) {
                    diff = ((TimeData) a).nanos - ((TimeData) b).nanos;
                }

                return diff == 0 ? 0
                                 : diff > 0 ? 1
                                            : -1;
            }
            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                diff = ((TimestampData) a).seconds
                       - ((TimestampData) b).seconds;

                if (diff == 0) {
                    diff = ((TimestampData) a).nanos
                           - ((TimestampData) b).nanos;
                }

                return diff == 0 ? 0
                                 : diff > 0 ? 1
                                            : -1;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_DATE :
                return a;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME : {
                TimeData ti       = (TimeData) a;
                int      nanos    = ti.nanos;
                int      newNanos = scaleNanos(nanos);

                if (newNanos == nanos) {
                    return ti;
                }

                return new TimeData(ti.seconds, newNanos, ti.zone);
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                TimestampData ts       = (TimestampData) a;
                int           nanos    = ts.nanos;
                int           newNanos = scaleNanos(nanos);

                if (ts.seconds > epochLimitSeconds) {
                    throw Error.error(ErrorCode.X_22008);
                }

                if (newNanos == nanos) {
                    return ts;
                }

                return new TimestampData(ts.seconds, newNanos, ts.zone);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    int scaleNanos(int nanos) {

        int divisor = nanoScaleFactors[scale];

        return (nanos / divisor) * divisor;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return a;
        }

        switch (otherType.typeCode) {

            case Types.SQL_CLOB :
                a = Type.SQL_VARCHAR.convertToType(session, a, otherType);

            //fall through
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
                switch (this.typeCode) {

                    case Types.SQL_DATE :
                    case Types.SQL_TIME_WITH_TIME_ZONE :
                    case Types.SQL_TIME :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    case Types.SQL_TIMESTAMP : {
                        try {
                            return session.getScanner()
                                .convertToDatetimeInterval(session,
                                                           (String) a, this);
                        } catch (HsqlException e) {
                            return convertToDatetimeSpecial(session,
                                                            (String) a, this);
                        }
                    }
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                break;

            default :
                throw Error.error(ErrorCode.X_42561);
        }

        switch (this.typeCode) {

            case Types.SQL_DATE :
                switch (otherType.typeCode) {

                    case Types.SQL_DATE :
                        return a;

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        long seconds = ((TimestampData) a).seconds
                                       + ((TimestampData) a).zone;

                        seconds = DateTimeType.toDateSeconds(seconds);

                        return new TimestampData(seconds);
                    }
                    case Types.SQL_TIMESTAMP : {
                        long seconds = ((TimestampData) a).seconds;

                        seconds = DateTimeType.toDateSeconds(seconds);

                        return new TimestampData(seconds);
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIME_WITH_TIME_ZONE :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIME : {
                        TimeData ti          = (TimeData) a;
                        int      zoneSeconds = session.getZoneSeconds();

                        return new TimeData(ti.seconds - zoneSeconds,
                                            scaleNanos(ti.nanos), zoneSeconds);
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = toTimeSeconds(ts.seconds);

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.nanos), ts.zone);
                    }
                    case Types.SQL_TIMESTAMP : {
                        TimestampData ts          = (TimestampData) a;
                        int           zoneSeconds = session.getZoneSeconds();
                        long          seconds     = ts.seconds - zoneSeconds;

                        seconds = toTimeSeconds(seconds);

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.nanos), zoneSeconds);
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIME :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti = (TimeData) a;

                        return new TimeData(ti.seconds + ti.zone,
                                            scaleNanos(ti.nanos), 0);
                    }
                    case Types.SQL_TIME :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = ts.seconds + ts.zone;

                        seconds = toTimeSeconds(seconds);

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.nanos), 0);
                    }
                    case Types.SQL_TIMESTAMP :
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = toTimeSeconds(ts.seconds);

                        return new TimeData((int) (seconds),
                                            scaleNanos(ts.nanos));

                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().seconds
                                       + ti.seconds;

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.nanos),
                                                 ti.zone);
                    }
                    case Types.SQL_TIME : {
                        TimeData ti          = (TimeData) a;
                        int      zoneSeconds = session.getZoneSeconds();
                        long seconds = session.getCurrentDate().seconds
                                       + ti.seconds - zoneSeconds;

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.nanos),
                                                 zoneSeconds);
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_TIMESTAMP : {
                        if (!(a instanceof TimestampData)) {
                            throw Error.error(ErrorCode.X_42561);
                        }

                        TimestampData ts          = (TimestampData) a;
                        int           zoneSeconds = session.getZoneSeconds();
                        long          seconds     = ts.seconds - zoneSeconds;

                        return new TimestampData(seconds,
                                                 scaleNanos(ts.nanos),
                                                 zoneSeconds);
                    }
                    case Types.SQL_DATE : {
                        if (!(a instanceof TimestampData)) {
                            throw Error.error(ErrorCode.X_42561);
                        }

                        TimestampData ts          = (TimestampData) a;
                        int           zoneSeconds = session.getZoneSeconds();

                        return new TimestampData(ts.seconds, 0, zoneSeconds);
                    }
                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            case Types.SQL_TIMESTAMP :
                switch (otherType.typeCode) {

                    case Types.SQL_TIME_WITH_TIME_ZONE : {
                        TimeData ti          = (TimeData) a;
                        int      zoneSeconds = session.getZoneSeconds();
                        long seconds = session.getCurrentDate().seconds
                                       + ti.seconds - zoneSeconds;

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.nanos),
                                                 zoneSeconds);
                    }
                    case Types.SQL_TIME : {
                        TimeData ti = (TimeData) a;
                        long seconds = session.getCurrentDate().seconds
                                       + ti.seconds;

                        return new TimestampData(seconds,
                                                 scaleNanos(ti.nanos));
                    }
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        TimestampData ts      = (TimestampData) a;
                        long          seconds = ts.seconds + ts.zone;

                        return new TimestampData(seconds,
                                                 scaleNanos(ts.nanos));
                    }
                    case Types.SQL_TIMESTAMP :
                        return convertToTypeLimits(session, a);

                    case Types.SQL_DATE :
                        return a;

                    default :
                        throw Error.error(ErrorCode.X_42561);
                }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        Type otherType = a instanceof TimeData ? Type.SQL_TIME
                                               : Type.SQL_TIMESTAMP;

        return convertToType(session, a, otherType);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        Calendar calendar       = session.getCalendar();
        long     seconds        = 0;
        int      nanos          = 0;
        int      zoneSeconds    = 0;
        boolean  hasZone        = false;
        boolean  isJavaUtilDate = a instanceof java.util.Date;
        boolean  isTimeObject   = false;
        boolean  isDateObject   = false;

        if (isJavaUtilDate) {
            if (a instanceof java.sql.Time) {
                isTimeObject = true;
            } else if (a instanceof java.sql.Date) {
                isDateObject = true;
            }

            long millis = ((java.util.Date) a).getTime();

            if (isDateObject) {
                millis = HsqlDateTime.getNormalisedDate(calendar, millis);
            }

            seconds     = millis / 1000;
            zoneSeconds = calendar.getTimeZone().getOffset(millis) / 1000;

            if (a instanceof java.sql.Timestamp) {
                nanos = ((java.sql.Timestamp) a).getNanos();
            } else {
                nanos = (int) (millis % 1000);
            }

//#ifdef JAVA8
        } else if (a instanceof java.time.LocalDate) {
            LocalDate ld = (LocalDate) a;
            setDateComponents(calendar, ld);

            long millis = calendar.getTimeInMillis();
            seconds = millis / 1000;
            zoneSeconds = calendar.getTimeZone().getOffset(millis) / 1000;
            nanos = 0;
            isDateObject = true;
        } else if (a instanceof OffsetDateTime) {
            OffsetDateTime odt = (OffsetDateTime) a;

            seconds = odt.toEpochSecond();
            zoneSeconds = odt.get(ChronoField.OFFSET_SECONDS);
            nanos = odt.getNano();
            hasZone = true;
        } else if (a instanceof ZonedDateTime) {
            ZonedDateTime zdt = (ZonedDateTime) a;
            seconds = zdt.toEpochSecond();
            zoneSeconds = zdt.get(ChronoField.OFFSET_SECONDS);
            nanos = zdt.getNano();
            hasZone = true;
        } else if (a instanceof java.time.LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) a;

            setDateTimeComponents(calendar, ldt);

            long millis = calendar.getTimeInMillis();
            seconds = millis / 1000;
            zoneSeconds = calendar.getTimeZone().getOffset(millis) / 1000;
            nanos = ldt.getNano();
        } else if (a instanceof java.time.Instant) {
            Instant ins = (Instant) a;

            long millis;
            seconds = ins.getEpochSecond();
            millis = seconds * 1000;
            zoneSeconds = calendar.getTimeZone().getOffset(millis) / 1000;
            nanos = ins.getNano();
        } else if (a instanceof java.time.OffsetTime) {
            OffsetTime ot = (OffsetTime) a;

            seconds = ot.toLocalTime().toSecondOfDay();
            zoneSeconds = ot.get(ChronoField.OFFSET_SECONDS);
            nanos = ot.getNano();
            isTimeObject = true;
            hasZone = true;
        } else if (a instanceof java.time.LocalTime) {
            LocalTime lt = (LocalTime) a;

            seconds = lt.toSecondOfDay();
            nanos  = lt.getNano();
            isTimeObject = true;

//#endif JAVA8
        } else {
            throw Error.error(ErrorCode.X_42561);
        }

        if (!withTimeZone) {
            seconds     += zoneSeconds;
            zoneSeconds = 0;
        }

        switch (typeCode) {

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE : {
                if (isDateObject) {
                    throw Error.error(ErrorCode.X_42561);
                }

                seconds = toTimeSeconds(seconds);
                nanos   = DateTimeType.normaliseFraction(nanos, scale);

                return new TimeData((int) seconds, nanos, zoneSeconds);
            }
            case Types.SQL_DATE : {
                if (isTimeObject) {
                    throw Error.error(ErrorCode.X_42561);
                }

                seconds = toDateSeconds(seconds);

                return new TimestampData(seconds);
            }
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                if (isTimeObject) {
                    throw Error.error(ErrorCode.X_42561);
                }

                nanos = DateTimeType.normaliseFraction(nanos, scale);

                return new TimestampData(seconds, nanos, zoneSeconds);
            }
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertSQLToJavaGMT(SessionInterface session, Object a) {

        long millis;

        switch (typeCode) {

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                millis = ((TimeData) a).seconds * 1000L;
                millis += ((TimeData) a).nanos / 1000000;

                return new java.sql.Time(millis);

            case Types.SQL_DATE :
                millis = ((TimestampData) a).seconds * 1000;

                return new java.sql.Date(millis);

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                millis = ((TimestampData) a).seconds * 1000;

                java.sql.Timestamp value = new java.sql.Timestamp(millis);

                value.setNanos(((TimestampData) a).nanos);

                return value;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_TIME : {
                Calendar cal = session.getCalendar();
                long millis = HsqlDateTime.convertMillisToCalendar(cal,
                    ((TimeData) a).getMillis());

                millis = HsqlDateTime.getNormalisedTime(cal, millis);

                return new java.sql.Time(millis);
            }
            case Types.SQL_TIME_WITH_TIME_ZONE : {

//#ifdef JAVA8
                TimeData   ts   = (TimeData) a;
                ZoneOffset zone = ZoneOffset.ofTotalSeconds(ts.zone);
                long millis =
                    HsqlDateTime.getNormalisedTime((ts.seconds + ts.zone)
                                                   * 1_000L);
                long       nanos = millis * 1_000_000L;
                LocalTime  ldt   = LocalTime.ofNanoOfDay(nanos + ts.nanos);

                return OffsetTime.of(ldt, zone);

//#else
/*
                long millis = ((TimeData) a).getMillis();

                return new java.sql.Time(millis);
*/

//#endif JAVA8
            }
            case Types.SQL_DATE : {
                Calendar cal = session.getCalendar();
                long millis = HsqlDateTime.convertMillisToCalendar(cal,
                    ((TimestampData) a).getMillis());

                // millis = HsqlDateTime.getNormalisedDate(cal, millis);
                return new java.sql.Date(millis);
            }
            case Types.SQL_TIMESTAMP : {
                Calendar cal = session.getCalendar();
                long millis = HsqlDateTime.convertMillisToCalendar(cal,
                    ((TimestampData) a).getMillis());
                java.sql.Timestamp value = new java.sql.Timestamp(millis);

                value.setNanos(((TimestampData) a).nanos);

                return value;
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {

//#ifdef JAVA8
                TimestampData ts   = (TimestampData) a;
                ZoneOffset    zone = ZoneOffset.ofTotalSeconds(ts.zone);
                LocalDateTime ldt = LocalDateTime.ofEpochSecond(ts.seconds,
                    ts.nanos, zone);

                return OffsetDateTime.of(ldt, zone);

//#else
/*
                long               millis = ((TimestampData) a).getMillis();
                java.sql.Timestamp value  = new java.sql.Timestamp(millis);

                value.setNanos(((TimestampData) a).nanos);

                return value;
*/

//#endif JAVA8
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public static int normaliseTime(int seconds) {

        while (seconds < 0) {
            seconds += 24 * 60 * 60;
        }

        if (seconds >= 24 * 60 * 60) {
            seconds %= 24 * 60 * 60;
        }

        return seconds;
    }

    public String convertToString(Object a) {

        boolean       zone = false;
        String        s;
        StringBuilder sb;

        if (a == null) {
            return null;
        }

        switch (typeCode) {

            case Types.SQL_DATE :
                return HsqlDateTime.getDateString(((TimestampData) a).seconds);

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME : {
                TimeData t       = (TimeData) a;
                int      seconds = normaliseTime(t.seconds + t.zone);

                s = intervalSecondToString(seconds, t.nanos, false);

                if (!withTimeZone) {
                    return s;
                }

                sb = new StringBuilder(s);
                s = Type.SQL_INTERVAL_HOUR_TO_MINUTE.intervalSecondToString(
                    ((TimeData) a).zone, 0, true);

                sb.append(s);

                return sb.toString();
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                TimestampData ts = (TimestampData) a;
                String tss = HsqlDateTime.getTimestampString(ts.seconds
                    + ts.zone, ts.nanos, scale);

                if (withTimeZone) {
                    s = Type.SQL_INTERVAL_HOUR_TO_MINUTE
                        .intervalSecondToString(((TimestampData) a).zone, 0,
                                                true);
                    tss += s;
                }

                return tss;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        StringBuilder sb = new StringBuilder(32);

        switch (typeCode) {

            case Types.SQL_DATE :
                sb.append(Tokens.T_DATE);
                break;

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                sb.append(Tokens.T_TIME);
                break;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                sb.append(Tokens.T_TIMESTAMP);
                break;
        }

        sb.append(StringConverter.toQuotedString(convertToString(a), '\'',
                false));

        return sb.toString();
    }

    public void convertToJSON(Object a, StringBuilder sb) {

        if (a == null) {
            sb.append("null");

            return;
        }

        sb.append('"');
        sb.append(convertToString(a));
        sb.append('"');
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == Types.SQL_ALL_TYPES) {
            return true;
        }

        if (otherType.isCharacterType()) {
            return true;
        }

        if (!otherType.isDateTimeType()) {
            return false;
        }

        if (otherType.typeCode == Types.SQL_DATE) {
            return typeCode != Types.SQL_TIME;
        } else if (otherType.typeCode == Types.SQL_TIME) {
            return typeCode != Types.SQL_DATE;
        }

        return true;
    }

    public int canMoveFrom(Type otherType) {

        if (otherType == this) {
            return ReType.keep;
        }

        if (typeCode == otherType.typeCode) {
            return scale >= otherType.scale ? ReType.keep
                                            : ReType.change;
        }

        return -1;
    }

    public Object add(Session session, Object a, Object b, Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        if (otherType.isNumberType()) {
            if (typeCode == Types.SQL_DATE) {
                b = ((NumberType) otherType).floor(b);
            }

            b = Type.SQL_INTERVAL_SECOND_MAX_PRECISION.multiply(
                IntervalSecondData.oneDay, b);
        }

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "DateTimeType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths(session, (TimestampData) a,
                                     ((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimestampData) a,
                                      ((IntervalSecondData) b).units,
                                      ((IntervalSecondData) b).nanos);
                }
                break;

            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public Object subtract(Session session, Object a, Object b,
                           Type otherType) {

        if (a == null || b == null) {
            return null;
        }

        if (otherType.isNumberType()) {
            if (typeCode == Types.SQL_DATE) {
                b = ((NumberType) otherType).floor(b);
            }

            b = Type.SQL_INTERVAL_SECOND_MAX_PRECISION.multiply(
                IntervalSecondData.oneDay, b);
        }

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME :
                if (b instanceof IntervalMonthData) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "DateTimeType");
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimeData) a,
                                      -((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP :
                if (b instanceof IntervalMonthData) {
                    return addMonths(session, (TimestampData) a,
                                     -((IntervalMonthData) b).units);
                } else if (b instanceof IntervalSecondData) {
                    return addSeconds((TimestampData) a,
                                      -((IntervalSecondData) b).units,
                                      -((IntervalSecondData) b).nanos);
                }
                break;

            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public static double convertToDouble(Object a) {

        double seconds;
        double fraction;

        if (a instanceof TimeData) {
            seconds  = ((TimeData) a).seconds;
            fraction = ((TimeData) a).nanos / 1000000000d;
        } else {
            seconds  = ((TimestampData) a).seconds;
            fraction = ((TimestampData) a).nanos / 1000000000d;
        }

        return seconds + fraction;
    }

    public TimestampData convertFromDouble(Session session, double value) {

        long units = (long) value;
        int  nanos = (int) ((value - units) * limitNanoseconds);

        return getDateTimeValue(session, units, nanos);
    }

    public Object truncate(Session session, Object a, int part) {

        if (a == null) {
            return null;
        }

        long     millis   = getMillis(a);
        Calendar calendar = session.getCalendarGMT();

        millis = HsqlDateTime.getTruncatedPart(calendar, millis, part);
        millis -= getZoneMillis(a);

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
                millis = HsqlDateTime.getNormalisedTime(calendar, millis);

            //fall through
            case Types.SQL_TIME : {
                return new TimeData((int) (millis / 1000), 0,
                                    ((TimeData) a).zone);
            }
            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                return new TimestampData(millis / 1000, 0,
                                         ((TimestampData) a).zone);
            }
            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public Object round(Session session, Object a, int part) {

        if (a == null) {
            return null;
        }

        long     millis   = getMillis(a);
        Calendar calendar = session.getCalendarGMT();

        millis = HsqlDateTime.getRoundedPart(calendar, millis, part);
        millis -= getZoneMillis(a);

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIME : {
                millis = HsqlDateTime.getNormalisedTime(millis);

                return new TimeData((int) (millis / 1000), 0,
                                    ((TimeData) a).zone);
            }
            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP : {
                return new TimestampData(millis / 1000, 0,
                                         ((TimestampData) a).zone);
            }
            default :
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
    }

    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof DateTimeType) {
            return super.equals(other)
                   && ((DateTimeType) other).withTimeZone == withTimeZone;
        }

        return false;
    }

    public int getPart(Session session, Object dateTime, int part) {

        int calendarPart;
        int increment = 0;
        int divisor   = 1;

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                calendarPart = Calendar.YEAR;
                break;

            case Types.SQL_INTERVAL_MONTH :
                increment    = 1;
                calendarPart = Calendar.MONTH;
                break;

            case Types.SQL_INTERVAL_DAY :
            case Types.DTI_DAY_OF_MONTH :
                calendarPart = Calendar.DAY_OF_MONTH;
                break;

            case Types.SQL_INTERVAL_HOUR :
                calendarPart = Calendar.HOUR_OF_DAY;
                break;

            case Types.SQL_INTERVAL_MINUTE :
                calendarPart = Calendar.MINUTE;
                break;

            case Types.SQL_INTERVAL_SECOND :
                calendarPart = Calendar.SECOND;
                break;

            case Types.DTI_DAY_OF_WEEK :
                calendarPart = Calendar.DAY_OF_WEEK;
                break;

            case Types.DTI_WEEK_OF_YEAR :
            case Types.DTI_ISO_WEEK_OF_YEAR :
                calendarPart = Calendar.WEEK_OF_YEAR;
                break;

            case Types.DTI_SECONDS_MIDNIGHT : {
                if (typeCode == Types.SQL_TIME
                        || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {}
                else {
                    try {
                        Type target = withTimeZone
                                      ? Type.SQL_TIME_WITH_TIME_ZONE
                                      : Type.SQL_TIME;

                        dateTime = target.castToType(session, dateTime, this);
                    } catch (HsqlException e) {}
                }

                return ((TimeData) dateTime).seconds;
            }
            case Types.DTI_TIMEZONE_HOUR :
                if (typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    return ((TimestampData) dateTime).zone / 3600;
                } else {
                    return ((TimeData) dateTime).zone / 3600;
                }
            case Types.DTI_TIMEZONE_MINUTE :
                if (typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    return ((TimestampData) dateTime).zone / 60 % 60;
                } else {
                    return ((TimeData) dateTime).zone / 60 % 60;
                }
            case Types.DTI_TIMEZONE :
                if (typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    return ((TimestampData) dateTime).zone / 60;
                } else {
                    return ((TimeData) dateTime).zone / 60;
                }
            case Types.DTI_QUARTER :
                increment    = 1;
                divisor      = 3;
                calendarPart = Calendar.MONTH;
                break;

            case Types.DTI_DAY_OF_YEAR :
                calendarPart = Calendar.DAY_OF_YEAR;
                break;

            case Types.DTI_MILLISECOND :
                if (this.isDateOrTimestampType()) {
                    return ((TimestampData) dateTime).nanos / 1000000;
                } else {
                    return ((TimeData) dateTime).nanos / 1000000;
                }
            case Types.DTI_NANOSECOND :
                if (this.isDateOrTimestampType()) {
                    return ((TimestampData) dateTime).nanos;
                } else {
                    return ((TimeData) dateTime).nanos;
                }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "DateTimeType - " + part);
        }

        long millis = getMillis(dateTime);

        return HsqlDateTime.getDateTimePart(session.getCalendarGMT(), millis, calendarPart)
               / divisor + increment;
    }

    public Object addMonthsSpecial(Session session, Object dateTime,
                                   int months) {

        TimestampData ts     = (TimestampData) dateTime;
        Calendar      cal    = session.getCalendarGMT();
        long          millis = (ts.seconds + ts.zone) * 1000;
        boolean       lastDay;

        HsqlDateTime.setTimeInMillis(cal, millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, -1);

        lastDay = millis == cal.getTimeInMillis();

        HsqlDateTime.setTimeInMillis(cal, millis);
        cal.add(Calendar.MONTH, months);

        if (lastDay) {
            cal.set(Calendar.DAY_OF_MONTH, 1);
            cal.add(Calendar.MONTH, 1);
            cal.add(Calendar.DAY_OF_MONTH, -1);
        }

        millis = cal.getTimeInMillis();

        return new TimestampData(millis / 1000, 0, 0);
    }

    public Object getLastDayOfMonth(Session session, Object dateTime) {

        TimestampData ts     = (TimestampData) dateTime;
        Calendar      cal    = session.getCalendarGMT();
        long          millis = (ts.seconds + ts.zone) * 1000;

        HsqlDateTime.setTimeInMillis(cal, millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.add(Calendar.MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, -1);

        millis = cal.getTimeInMillis();

        return new TimestampData(millis / 1000, 0, 0);
    }

    long getMillis(Object dateTime) {

        long millis;

        if (typeCode == Types.SQL_TIME
                || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            millis =
                (((TimeData) dateTime).seconds + ((TimeData) dateTime).zone)
                * 1000L;
        } else {
            millis =
                (((TimestampData) dateTime)
                    .seconds + ((TimestampData) dateTime).zone) * 1000;
        }

        return millis;
    }

    long getZoneMillis(Object dateTime) {

        long millis;

        if (typeCode == Types.SQL_TIME
                || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            millis = ((TimeData) dateTime).zone * 1000L;
        } else {
            millis = ((TimestampData) dateTime).zone * 1000L;
        }

        return millis;
    }

    public BigDecimal getSecondPart(Session session, Object dateTime) {

        long seconds = getPart(session, dateTime, Types.SQL_INTERVAL_SECOND);
        int  nanos   = 0;

        if (typeCode == Types.SQL_TIMESTAMP
                || typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
            nanos = ((TimestampData) dateTime).nanos;
        } else if (typeCode == Types.SQL_TIME
                   || typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            nanos = ((TimeData) dateTime).nanos;
        }

        return getSecondPart(seconds, nanos);
    }

    public String getPartString(Session session, Object dateTime, int part) {

        String javaPattern = "";

        switch (part) {

            case Types.DTI_DAY_NAME :
                javaPattern = "EEEE";
                break;

            case Types.DTI_MONTH_NAME :
                javaPattern = "MMMM";
                break;
        }

        SimpleDateFormat format = session.getSimpleDateFormatGMT();

        try {
            format.applyPattern(javaPattern);
        } catch (Exception e) {}

        Date date = (Date) convertSQLToJavaGMT(session, dateTime);

        return format.format(date);
    }

    /**
     * Session derivatives of CURRENT_TIMESTAMP
     */
    public static TimestampData toLocalTimestampValue(
            TimestampData tsWithZone) {
        return new TimestampData(tsWithZone.seconds + tsWithZone.zone,
                                 tsWithZone.nanos);
    }

    public static TimestampData toCurrentDateValue(TimestampData tsWithZone) {

        long seconds = toDateSeconds(tsWithZone.seconds + tsWithZone.zone);

        return new TimestampData(seconds);
    }

    public static TimeData toCurrentTimeValue(TimestampData tsWithZone) {

        int seconds = toTimeSeconds(tsWithZone.seconds + tsWithZone.zone);

        return new TimeData(seconds, tsWithZone.nanos);
    }

    public static TimeData toCurrentTimeWithZoneValue(
            TimestampData tsWithZone) {

        int seconds = toTimeSeconds(tsWithZone.seconds);

        return new TimeData(seconds, tsWithZone.nanos, tsWithZone.zone);
    }

    public TimestampData getDateTimeValue(SessionInterface session,
                                          long seconds, int nanos) {

        nanos = DateTimeType.normaliseFraction(nanos, scale);

        switch (typeCode) {

            case Types.SQL_DATE :
                seconds = toDateSeconds(seconds);

                return new TimestampData(seconds);

            case Types.SQL_TIMESTAMP :
                return new TimestampData(seconds, nanos);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return new TimestampData(seconds, nanos,
                                         session.getZoneSeconds());

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public static DateTimeType getDateTimeType(int type, int scale) {

        if (scale > DTIType.maxFractionPrecision) {
            throw Error.error(ErrorCode.X_42592);
        }

        switch (type) {

            case Types.SQL_DATE :
                return SQL_DATE;

            case Types.SQL_TIME :
                if (scale == DTIType.defaultTimeFractionPrecision) {
                    return SQL_TIME;
                }

                return new DateTimeType(Types.SQL_TIME, type, scale);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                if (scale == DTIType.defaultTimeFractionPrecision) {
                    return SQL_TIME_WITH_TIME_ZONE;
                }

                return new DateTimeType(Types.SQL_TIME, type, scale);

            case Types.SQL_TIMESTAMP :
                if (scale == DTIType.defaultTimestampFractionPrecision) {
                    return SQL_TIMESTAMP;
                }

                if (scale == 0) {
                    return SQL_TIMESTAMP_NO_FRACTION;
                }

                return new DateTimeType(Types.SQL_TIMESTAMP, type, scale);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                if (scale == DTIType.defaultTimestampFractionPrecision) {
                    return SQL_TIMESTAMP_WITH_TIME_ZONE;
                }

                return new DateTimeType(Types.SQL_TIMESTAMP, type, scale);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeType");
        }
    }

    public static Object changeZoneToUTC(Object a) {

        if (a instanceof TimestampData) {
            TimestampData ts = (TimestampData) a;

            if (ts.zone != 0) {
                return new TimestampData(ts.seconds, ts.nanos);
            }
        }

        if (a instanceof TimeData) {
            TimeData ts = (TimeData) a;

            if (ts.zone != 0) {
                return new TimeData(ts.seconds, ts.nanos);
            }
        }

        return a;
    }

    public Object changeZone(Session session, Object a, Type otherType,
                             int targetZone, int localZone) {

        Calendar calendar = session.getCalendarGMT();

        if (a == null) {
            return null;
        }

        if (targetZone > DTIType.timezoneSecondsLimit
                || -targetZone > DTIType.timezoneSecondsLimit) {
            throw Error.error(ErrorCode.X_22009);
        }

        switch (typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE : {
                TimeData value = (TimeData) a;

                if (otherType.isDateTimeTypeWithZone()) {
                    if (value.zone != targetZone) {
                        return new TimeData(value.seconds, value.nanos,
                                            targetZone);
                    }
                } else {
                    int seconds = value.seconds - localZone;

                    seconds =
                        (int) (HsqlDateTime.getNormalisedTime(calendar, seconds * 1000L)
                               / 1000);

                    return new TimeData(seconds, value.nanos, targetZone);
                }

                break;
            }
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                TimestampData value   = (TimestampData) a;
                long          seconds = value.seconds;

                if (!otherType.isDateTimeTypeWithZone()) {
                    seconds -= localZone;
                }

                if (value.seconds != seconds || value.zone != targetZone) {
                    return new TimestampData(seconds, value.nanos, targetZone);
                }

                break;
            }
        }

        return a;
    }

    public boolean canAdd(IntervalType other) {
        return other.startPartIndex >= startPartIndex
               && other.endPartIndex <= endPartIndex;
    }

    public int getSqlDateTimeSub() {

        switch (typeCode) {

            case Types.SQL_DATE :
                return 1;

            case Types.SQL_TIME :
                return 2;

            case Types.SQL_TIMESTAMP :
                return 3;

            default :
                return 0;
        }
    }

    /**
     * For temporal predicate operations on periods, we need to make sure we
     * compare data of the same types.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return The common data type of the boundaries of the two limits.
     *         null if any of the two periods is null or if the first limit of
     *         any period is null.
     *
     * @since 2.3.4
     */
    public static Type normalizeInput(Session session, Object[] a, Type[] ta,
                                      Object[] b, Type[] tb,
                                      boolean pointOfTime) {

        if (a == null || b == null) {
            return null;
        }

        if (a[0] == null || b[0] == null) {
            return null;
        }

        if (a[1] == null) {
            return null;
        }

        if (!pointOfTime && b[1] == null) {
            return null;
        }

        if (!ta[0].isDateTimeType() || !tb[0].isDateTimeType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        Type commonType = SQL_TIMESTAMP_WITH_TIME_ZONE;

        a[0] = commonType.castToType(session, a[0], ta[0]);
        b[0] = commonType.castToType(session, b[0], tb[0]);

        if (ta[1].isIntervalType()) {
            a[1] = commonType.add(session, a[0], a[1], ta[1]);
        } else {
            a[1] = commonType.castToType(session, a[1], ta[1]);
        }

        if (tb[1].isIntervalType()) {
            b[1] = commonType.add(session, b[0], b[1], tb[1]);
        } else {
            if (pointOfTime) {
                b[1] = b[0];
            } else {
                b[1] = commonType.castToType(session, b[1], tb[1]);
            }
        }

        if (commonType.compare(session, a[0], a[1]) >= 0) {
            Object temp = a[0];

            a[0] = a[1];
            a[1] = temp;
        }

        if (!pointOfTime && commonType.compare(session, b[0], b[1]) >= 0) {
            Object temp = b[0];

            b[0] = b[1];
            b[1] = temp;
        }

        return commonType;
    }

    /**
     * For temporal predicate operations on periods, we need to make sure we
     * compare data of the same types.
     * We also switch the period boundaries if the first entry is after the
     * second one.
     * <p>
     * Important: when this method returns, the boundaries of the periods may
     * have been changed.
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return The common data type of the boundaries of the two limits.
     *         null if any of the two periods is null or if the first limit of
     *         any period is null.
     *
     * @since 2.3.4
     */
    public static Type normalizeInputRelaxed(Session session, Object[] a,
            Type[] ta, Object[] b, Type[] tb) {

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

        Type commonType = ta[0].getCombinedType(session, tb[0], OpTypes.EQUAL);

        a[0] = commonType.castToType(session, a[0], ta[0]);
        b[0] = commonType.castToType(session, b[0], tb[0]);

        if (ta[1].isIntervalType()) {
            a[1] = commonType.add(session, a[0], a[1], ta[1]);
        } else {
            a[1] = commonType.castToType(session, a[1], ta[1]);
        }

        if (tb[1].isIntervalType()) {
            b[1] = commonType.add(session, b[0], b[1], tb[1]);
        } else {
            b[1] = commonType.castToType(session, b[1], tb[1]);
        }

        if (commonType.compare(session, a[0], a[1]) > 0) {
            Object temp = a[0];

            a[0] = a[1];
            a[1] = temp;
        }

        if (commonType.compare(session, b[0], b[1]) > 0) {
            Object temp = b[0];

            b[0] = b[1];
            b[1] = temp;
        }

        return commonType;
    }

    /**
     * The predicate "a OVERLAPS b" applies when both a and b are either period
     * names or period constructors.
     * This predicate returns True if the two periods have at least one time
     * point in common, i.e, if a[0] < b[1] and
     * a[1] > b[0]. This predicate is commutative: "a OVERLAPS B" must return
     * the same result of "b OVERLAPS a"
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if the two periods overlaps,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean overlaps(Session session, Object[] a, Type[] ta,
                                   Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[0], b[0]) > 0) {
            Object[] temp = a;

            a = b;
            b = temp;
        }

        if (commonType.compare(session, a[1], b[0]) > 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "a OVERLAPS b" applies when both a and b are rows.
     * This predicate returns True if the two periods have at least one time
     * point in common, i.e, if a[0] < b[1] and
     * a[1] > b[0]. This predicate is commutative: "a OVERLAPS B" must return
     * the same result of "b OVERLAPS a"
     * <p>
     * Important: when this method returns, the boundaries of the periods may
     * have been changed.
     *
     * @param session  The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if the two periods overlaps,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean overlapsRelaxed(Session session, Object[] a,
                                          Type[] ta, Object[] b, Type[] tb) {

        Type commonType = normalizeInputRelaxed(session, a, ta, b, tb);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[0], b[0]) > 0) {
            Object[] temp = a;

            a = b;
            b = temp;
        }

        if (commonType.compare(session, a[1], b[0]) > 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "a PRECEDES b" applies when both a and b are either period
     * names or period constructors.
     * In this case, the predicate returns True if the end value of a is less
     * than or equal to the start value of b, i.e., if ae <= as.
     * <p>
     *
     * @param session  The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a precedes period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean precedes(Session session, Object[] a, Type[] ta,
                                   Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[1], b[0]) <= 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "x IMMEDIATELY PRECEDES y" applies when both x and y are either period names or
     * period constructors. In this case, the predicate returns True if the end value of x is equal to the start value
     * of y, i.e., if xe = ys.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a immediately precedes period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean immediatelyPrecedes(Session session, Object[] a,
            Type[] ta, Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[1], b[0]) == 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "x IMMEDIATELY SUCCEEDS y" applies when both x and y are either period names or
     * period constructors. In this case, the predicate returns True if the start value of x is equal to the end value
     * of y, i.e., if xs = ye.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a immediately succeeds period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean immediatelySucceeds(Session session, Object[] a,
            Type[] ta, Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[0], b[1]) == 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "x SUCCEEDS y" applies when both x and y are either period names or period constructors.
     * In this case, the predicate returns True if the start value of x is greater than or equal to the end value of y,
     * i.e., if xs >= ye.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a succeeds period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean succeeds(Session session, Object[] a, Type[] ta,
                                   Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[0], b[1]) >= 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "x EQUALS y" applies when both x and y are either period names or period constructors.
     * This predicate returns True if the two periods have every time point in common, i.e., if xs = ys and xe = ye.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a equals period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean equals(Session session, Object[] a, Type[] ta,
                                 Object[] b, Type[] tb) {

        Type commonType = normalizeInput(session, a, ta, b, tb, false);

        if (commonType == null) {
            return null;
        }

        if (commonType.compare(session, a[0], b[0]) == 0
                && commonType.compare(session, a[1], b[1]) == 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    /**
     * The predicate "x CONTAINS y" applies when<br>
     * a) both x and y are either period names or period constructors. In this case, the predicate returns True if
     * x contains every time point in y, i.e., if xs <= ys and xe >= ye.<br>
     * b) x is either a period name or a period constructor and y is a datetime value expression. In this case, the
     * predicate returns True if x contains y, i.e., if xs <= y and xe > y.
     * <p>
     * The <i>b</i> part of this definition is not supported yet. In order to get the same result, one have to specify
     * a period with the same date time value for the period start and end.
     * <p>
     *
     * @param session The session
     * @param a First period to compare
     * @param ta Types of the first period
     * @param b Second period to compare
     * @param tb Type of the second period
     *
     * @return {@link Boolean#TRUE} if period a contains period b,
     *          else {@link Boolean#FALSE}
     */
    public static Boolean contains(Session session, Object[] a, Type[] ta,
                                   Object[] b, Type[] tb,
                                   boolean pointOfTime) {

        Type commonType = normalizeInput(session, a, ta, b, tb, pointOfTime);

        if (commonType == null) {
            return null;
        }

        int compareStart = commonType.compare(session, a[0], b[0]);
        int compareEnd   = commonType.compare(session, a[1], b[1]);

        if (compareStart <= 0 && compareEnd >= 0) {

            // if the end of the two period are equals, period a does not
            // contain period b if it is defined by a single point in time
            if (pointOfTime) {
                if (compareEnd == 0) {
                    return Boolean.FALSE;
                }
            }

            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }

    public static BigDecimal subtractMonthsSpecial(Session session,
            TimestampData a, TimestampData b) {

        long    s1    = (a.seconds + a.zone) * 1000;
        long    s2    = (b.seconds + b.zone) * 1000;
        boolean minus = false;

        if (s1 < s2) {
            minus = true;

            long temp = s1;

            s1 = s2;
            s2 = temp;
        }

        s1 = HsqlDateTime.getNormalisedDate(session.getCalendarGMT(), s1);
        s2 = HsqlDateTime.getNormalisedDate(session.getCalendarGMT(), s2);

        Calendar cal = session.getCalendarGMT();

        cal.setTimeInMillis(s1);

        int lastDay1;
        int months1 = cal.get(Calendar.MONTH) + cal.get(Calendar.YEAR) * 12;
        int day1    = cal.get(Calendar.DAY_OF_MONTH);

        cal.set(Calendar.DAY_OF_MONTH, 1);

        long millis = cal.getTimeInMillis();

        cal.add(Calendar.MONTH, 1);

        millis = cal.getTimeInMillis();

        cal.add(Calendar.DAY_OF_MONTH, -1);

        millis   = cal.getTimeInMillis();
        lastDay1 = cal.get(Calendar.DAY_OF_MONTH);

        cal.setTimeInMillis(s2);

        int lastDay2;
        int months2 = cal.get(Calendar.MONTH) + cal.get(Calendar.YEAR) * 12;
        int day2    = cal.get(Calendar.DAY_OF_MONTH);

        cal.set(Calendar.DAY_OF_MONTH, 1);

        millis = cal.getTimeInMillis();

        cal.add(Calendar.MONTH, 1);

        millis = cal.getTimeInMillis();

        cal.add(Calendar.DAY_OF_MONTH, -1);

        millis   = cal.getTimeInMillis();
        lastDay2 = cal.get(Calendar.DAY_OF_MONTH);

        double months;
        double days;

        if (day1 == day2 || (day1 == lastDay1 && day2 == lastDay2)) {
            months = months1 - months2;

            if (minus) {
                months = -months;
            }

            return BigDecimal.valueOf(months);
        } else if (day2 > day1) {
            months = months1 - months2 - 1;
            days   = lastDay2 - day2 + day1;
            months += days / 31;

            if (minus) {
                months = -months;
            }

            return BigDecimal.valueOf(months);
        } else {
            months = months1 - months2;
            days   = day1 - day2;
            months += days / 31;

            if (minus) {
                months = -months;
            }

            return BigDecimal.valueOf(months);
        }
    }

    public static int subtractMonths(Session session, TimestampData a,
                                     TimestampData b, boolean isYear) {

        Calendar calendar = session.getCalendarGMT();
        boolean  negate   = false;

        if (b.seconds > a.seconds) {
            negate = true;

            TimestampData temp = a;

            a = b;
            b = temp;
        }

        calendar.setTimeInMillis(a.seconds * 1000);

        int months = calendar.get(Calendar.MONTH);
        int years  = calendar.get(Calendar.YEAR);

        calendar.setTimeInMillis(b.seconds * 1000);

        months -= calendar.get(Calendar.MONTH);
        years  -= calendar.get(Calendar.YEAR);

        if (isYear) {
            months = years * 12;
        } else {
            if (months < 0) {
                months += 12;

                years--;
            }

            months += years * 12;
        }

        if (negate) {
            months = -months;
        }

        return months;
    }

    public static TimeData addSeconds(TimeData source, long seconds,
                                      int nanos) {

        nanos   += source.nanos;
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += DTIType.limitNanoseconds;

            seconds--;
        }

        seconds += source.seconds;
        seconds %= (24 * 60 * 60);

        return new TimeData((int) seconds, nanos, source.zone);
    }

    /* @todo - overflow */
    public static TimestampData addMonths(Session session,
                                          TimestampData source, int months) {

        int      n   = source.nanos;
        Calendar cal = session.getCalendarGMT();

        HsqlDateTime.setTimeInMillis(cal, source.seconds * 1000);
        cal.add(Calendar.MONTH, months);

        return new TimestampData(cal.getTimeInMillis() / 1000, n, source.zone);
    }

    public static TimestampData addSeconds(TimestampData source, long seconds,
                                           int nanos) {

        nanos   += source.nanos;
        seconds += nanos / limitNanoseconds;
        nanos   %= limitNanoseconds;

        if (nanos < 0) {
            nanos += limitNanoseconds;

            seconds--;
        }

        long newSeconds = source.seconds + seconds;

        return new TimestampData(newSeconds, nanos, source.zone);
    }

    public static TimestampData convertToDatetimeSpecial(
            SessionInterface session, String s, DateTimeType type) {

        switch (type.typeCode) {

            case Types.SQL_TIMESTAMP :
                if (session instanceof Session
                        && ((Session) session).database.sqlSyntaxOra) {
                    String pattern;

                    switch (s.length()) {

                        case 8 :
                        case 9 : {
                            pattern = "DD-MON-YY";

                            break;
                        }
                        case 10 :
                        case 11 : {
                            pattern = "DD-MON-YYYY";

                            break;
                        }
                        case 19 :
                        case 20 : {
                            pattern = "DD-MON-YYYY HH24:MI:SS";

                            break;
                        }
                        default :

                        // if (s.length() > 20)
                        {
                            pattern = "DD-MON-YYYY HH24:MI:SS.FF";

                            break;
                        }
                    }

                    SimpleDateFormat format = session.getSimpleDateFormatGMT();

                    return HsqlDateTime.toDate(s, pattern, format, true);
                }

            // fall through
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            default :
        }

        throw Error.error(ErrorCode.X_22007);
    }

    public static TimestampData nextDayOfWeek(Session session,
            TimestampData d, int day) {

        Calendar cal = session.getCalendarGMT();

        cal.setTimeInMillis(d.getMillis());

        int start = cal.get(Calendar.DAY_OF_WEEK);

        if (start >= day) {
            day += 7;
        }

        int diff = day - start;

        cal.add(Calendar.DAY_OF_MONTH, diff);

        long millis = cal.getTimeInMillis();

        millis = HsqlDateTime.getNormalisedDate(cal, millis);

        return new TimestampData(millis / 1000);
    }

    public static int getDayOfWeek(String name) {

        if (name.length() > 0) {
            int c = Character.toUpperCase(name.charAt(0));

            switch (c) {

                case 'M' :
                    return 2;

                case 'T' :
                    if (name.length() < 2) {
                        break;
                    }

                    if (Character.toUpperCase(name.charAt(1)) == 'U') {
                        return 3;
                    } else if (Character.toUpperCase(name.charAt(1)) == 'H') {
                        return 5;
                    }
                    break;

                case 'W' :
                    return 4;

                case 'F' :
                    return 6;

                case 'S' :
                    if (name.length() < 2) {
                        break;
                    }

                    if (Character.toUpperCase(name.charAt(1)) == 'A') {
                        return 7;
                    } else if (Character.toUpperCase(name.charAt(1)) == 'U') {
                        return 1;
                    }
                    break;
            }
        }

        throw Error.error(ErrorCode.X_22007, name);
    }

    static int toTimeSeconds(long seconds) {

        int timeSeconds = (int) (seconds % secondsInDay);

        if (timeSeconds < 0) {
            timeSeconds += secondsInDay;
        }

        return timeSeconds;
    }

    static long toDateSeconds(long seconds) {

        long timeSeconds = seconds % secondsInDay;

        if (timeSeconds < 0) {
            timeSeconds += secondsInDay;
        }

        return seconds - timeSeconds;
    }

    public static TimestampData getSysDateTimestamp() {

        long millis  = System.currentTimeMillis();
        long seconds = millis / 1000;
        int  offset  = systemTimeZone.getOffset(millis) / 1000;

        return new TimestampData(seconds + offset);
    }

    public static TimestampData getSystemTimestampWithZone() {
        return getTimestampWithZone(systemTimeZone);
    }

//#ifdef JAVA8
    public static TimestampData getTimestampWithZone(TimeZone zone) {
        Instant instant = Instant.now();

        long     seconds = instant.getEpochSecond();
        int      nanos   = (instant.getNano() / 1000) * 1000;

        int zoneSeconds = zone.getOffset(seconds * 1000)/ 1000;

        return new TimestampData(seconds, nanos, zoneSeconds);
    }

    public static TimestampData getSystemTimestampUTC() {
        Instant instant = Instant.now();

        long     seconds = instant.getEpochSecond();
        int      nanos   = (instant.getNano() / 1000) * 1000;

        return new TimestampData(seconds, nanos);
    }

    public static void setDateTimeComponents(Calendar calendar, LocalDateTime ldt) {
        calendar.clear();
        calendar.set(Calendar.YEAR, ldt.getYear());
        calendar.set(Calendar.MONTH, ldt.getMonthValue() - 1);
        calendar.set(Calendar.DAY_OF_MONTH, ldt.getDayOfMonth());
        calendar.set(Calendar.HOUR_OF_DAY, ldt.getHour());
        calendar.set(Calendar.MINUTE, ldt.getMinute());
        calendar.set(Calendar.SECOND, ldt.getSecond());
    }

    public static void setDateComponents(Calendar calendar, LocalDate ldt) {
        calendar.clear();
        calendar.set(Calendar.YEAR, ldt.getYear());
        calendar.set(Calendar.MONTH, ldt.getMonthValue() - 1);
        calendar.set(Calendar.DAY_OF_MONTH, ldt.getDayOfMonth());
    }

    public static void setTimeComponents(Calendar calendar, LocalTime ldt) {
        calendar.clear();
        calendar.set(Calendar.HOUR_OF_DAY, ldt.getHour());
        calendar.set(Calendar.MINUTE, ldt.getMinute());
        calendar.set(Calendar.SECOND, ldt.getSecond());
    }

//#else
/*
    public static TimestampData getTimestampWithZone(TimeZone zone) {

        long millis      = System.currentTimeMillis();
        long seconds     = millis / 1000;
        int  nanos       = (int) (millis % 1000) * 1000000;
        int  zoneSeconds = zone.getOffset(millis) / 1000;

        return new TimestampData(seconds, nanos, zoneSeconds);
    }

    public static TimestampData getSystemTimestampUTC() {

        long millis  = System.currentTimeMillis();
        long seconds = millis / 1000;
        int  nanos   = (int) (millis % 1000) * 1000000;

        return new TimestampData(seconds, nanos);
    }
*/

//#endif JAVA8
}
