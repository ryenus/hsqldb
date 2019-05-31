/* Copyright (c) 2001-2019, The HSQL Development Group
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

import org.hsqldb.Session;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.IntKeyIntValueHashMap;

/**
 * Common elements for Type instances for DATETIME and INTERVAL.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 1.9.0
 */
public abstract class DTIType extends Type {

    public static final byte[] yearToSecondSeparators       = {
        '-', '-', ' ', ':', ':', '.'
    };
    public static final int[]  yearToSecondFactors          = {
        12, 1, 24 * 60 * 60, 60 * 60, 60, 1, 0
    };
    public static final int[]  yearToSecondLimits           = {
        0, 12, 0, 24, 60, 60, 1000000000
    };
    public static final int    INTERVAL_MONTH_INDEX         = 1;
    public static final int    INTERVAL_SECOND_INDEX        = 5;
    public static final int    INTERVAL_FRACTION_PART_INDEX = 6;
    static final long[]        precisionLimits              = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
        1000000000, 10000000000L, 100000000000L, 1000000000000L
    };
    static final int[] precisionFactors = {
        100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1
    };
    static final int[] nanoScaleFactors = {
        1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10,
        1
    };
    public static final int timezoneSecondsLimit = 14 * 60 * 60;
    static final int[]      intervalParts        = {
        Types.SQL_INTERVAL_YEAR, Types.SQL_INTERVAL_MONTH,
        Types.SQL_INTERVAL_DAY, Types.SQL_INTERVAL_HOUR,
        Types.SQL_INTERVAL_MINUTE, Types.SQL_INTERVAL_SECOND
    };
    static final int[][] intervalTypes = {
        {
            Types.SQL_INTERVAL_YEAR, Types.SQL_INTERVAL_YEAR_TO_MONTH, 0, 0, 0,
            0
        }, {
            0, Types.SQL_INTERVAL_MONTH, 0, 0, 0, 0
        }, {
            0, 0, Types.SQL_INTERVAL_DAY, Types.SQL_INTERVAL_DAY_TO_HOUR,
            Types.SQL_INTERVAL_DAY_TO_MINUTE, Types.SQL_INTERVAL_DAY_TO_SECOND
        }, {
            0, 0, 0, Types.SQL_INTERVAL_HOUR,
            Types.SQL_INTERVAL_HOUR_TO_MINUTE,
            Types.SQL_INTERVAL_HOUR_TO_SECOND
        }, {
            0, 0, 0, 0, Types.SQL_INTERVAL_MINUTE,
            Types.SQL_INTERVAL_MINUTE_TO_SECOND
        }, {
            0, 0, 0, 0, 0, Types.SQL_INTERVAL_SECOND
        },
    };
    static final IntKeyIntValueHashMap intervalIndexMap =
        new IntKeyIntValueHashMap();

    static {
        intervalIndexMap.put(Types.SQL_INTERVAL_YEAR, 0);
        intervalIndexMap.put(Types.SQL_INTERVAL_MONTH, 1);
        intervalIndexMap.put(Types.SQL_INTERVAL_DAY, 2);
        intervalIndexMap.put(Types.SQL_INTERVAL_HOUR, 3);
        intervalIndexMap.put(Types.SQL_INTERVAL_MINUTE, 4);
        intervalIndexMap.put(Types.SQL_INTERVAL_SECOND, 5);
    }

    //
    public final int startIntervalType;
    public final int endIntervalType;

    //
    public final int startPartIndex;
    public final int endPartIndex;

    protected DTIType(int typeGroup, int type, long precision, int scale,
                      int startIntervalType, int endIntervalType) {

        super(typeGroup, type, precision, scale);

        this.startIntervalType = startIntervalType;
        this.endIntervalType   = endIntervalType;
        startPartIndex         = intervalIndexMap.get(startIntervalType);
        endPartIndex           = intervalIndexMap.get(endIntervalType);
    }

    protected DTIType(int typeGroup, int type, long precision, int scale) {

        super(typeGroup, type, precision, scale);

        switch (type) {

            case Types.SQL_DATE :
                startIntervalType = Types.SQL_INTERVAL_YEAR;
                endIntervalType   = Types.SQL_INTERVAL_DAY;
                break;

            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                startIntervalType = Types.SQL_INTERVAL_HOUR;
                endIntervalType   = Types.SQL_INTERVAL_SECOND;
                break;

            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                startIntervalType = Types.SQL_INTERVAL_YEAR;
                endIntervalType   = Types.SQL_INTERVAL_SECOND;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DTIType");
        }

        startPartIndex = intervalIndexMap.get(startIntervalType);
        endPartIndex   = intervalIndexMap.get(endIntervalType);
    }

    String intervalSecondToString(long seconds, int nanos, boolean signed) {

        StringBuilder sb = new StringBuilder(64);

        if (seconds < 0) {
            seconds = -seconds;

            sb.append('-');
        } else if (signed) {
            sb.append('+');
        }

        for (int i = startPartIndex; i <= endPartIndex; i++) {
            int  factor = DTIType.yearToSecondFactors[i];
            long part   = seconds / factor;

            if (i == startPartIndex) {
                int startDigits = precision == 0 ? 2
                                                 : (int) precision;
                int zeros       = startDigits - getPrecisionExponent(part);
/*
                for (int j = 0; j < zeros; j++) {
                    buffer.append('0');
                }
*/
            } else if (part < 10) {
                sb.append('0');
            }

            sb.append(part);

            seconds %= factor;

            if (i < endPartIndex) {
                sb.append((char) DTIType.yearToSecondSeparators[i]);
            }
        }

        if (scale != 0) {
            sb.append((char) DTIType
                .yearToSecondSeparators[DTIType.INTERVAL_FRACTION_PART_INDEX - 1]);
        }

        if (nanos < 0) {
            nanos = -nanos;
        }

        for (int i = 0; i < scale; i++) {
            int digit = nanos / DTIType.precisionFactors[i];

            nanos -= digit * DTIType.precisionFactors[i];

            sb.append(digit);
        }

        return sb.toString();
    }

    public int getStartIntervalType() {
        return startIntervalType;
    }

    public int getEndIntervalType() {
        return endIntervalType;
    }

    public Type getExtractType(int part) {

        switch (part) {

            case Types.DTI_DAY_NAME :
            case Types.DTI_MONTH_NAME :
            case Types.DTI_QUARTER :
            case Types.DTI_DAY_OF_MONTH :
            case Types.DTI_DAY_OF_YEAR :
            case Types.DTI_DAY_OF_WEEK :
            case Types.DTI_WEEK_OF_YEAR :
                if (!isDateTimeType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (part == Types.DTI_DAY_NAME
                        || part == Types.DTI_MONTH_NAME) {
                    return Type.SQL_VARCHAR;
                } else {
                    return Type.SQL_INTEGER;
                }
            case Types.SQL_INTERVAL_SECOND :
                if (part == startIntervalType || part == endIntervalType) {

                    // type is INTERVAL SECOND
                    // or TIMESTAMP or INTERVAL x TO SECOND
                    if (scale == 0) {
                        return Type.SQL_BIGINT;
                    }

                    return new NumberType(Types.SQL_DECIMAL,
                                          maxIntervalSecondPrecision + scale,
                                          scale);
                }

                throw Error.error(ErrorCode.X_42561);
            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_MONTH :
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_MINUTE :
                if (part < startIntervalType || part > endIntervalType) {
                    throw Error.error(ErrorCode.X_42561);
                }

                return Type.SQL_INTEGER;

            // used for DATEPART function
            case Types.DTI_MILLISECOND :
            case Types.DTI_MICROSECOND :
            case Types.DTI_NANOSECOND :
                return Type.SQL_BIGINT;

            case Types.DTI_SECONDS_MIDNIGHT :
                if (!isDateTimeType()
                        || endIntervalType < Types.SQL_INTERVAL_SECOND) {
                    throw Error.error(ErrorCode.X_42561);
                }

                return Type.SQL_INTEGER;

            case Types.DTI_TIMEZONE :
            case Types.DTI_TIMEZONE_HOUR :
            case Types.DTI_TIMEZONE_MINUTE :
                if (typeCode != Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                        && typeCode != Types.SQL_TIME_WITH_TIME_ZONE) {
                    throw Error.error(ErrorCode.X_42561);
                }

                return Type.SQL_INTEGER;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DTIType");
        }
    }

    public static int normaliseFraction(int fraction, int precision) {
        return (fraction / nanoScaleFactors[precision])
               * nanoScaleFactors[precision];
    }

    public static int normaliseFraction(int fraction, int digits,
                                        int precision) {

        fraction *= DTIType.nanoScaleFactors[digits];

        return (fraction / nanoScaleFactors[precision])
               * nanoScaleFactors[precision];
    }

    static int getPrecisionExponent(long value) {

        int i = 1;

        for (; i < precisionLimits.length; i++) {
            if (value < precisionLimits[i]) {
                break;
            }
        }

        return i;
    }

    public static int getFieldNameTypeForToken(int token) {

        switch (token) {

            case Tokens.YEAR :
                return Types.SQL_INTERVAL_YEAR;

            case Tokens.MONTH :
                return Types.SQL_INTERVAL_MONTH;

            case Tokens.DAY :
                return Types.SQL_INTERVAL_DAY;

            case Tokens.HOUR :
                return Types.SQL_INTERVAL_HOUR;

            case Tokens.MINUTE :
                return Types.SQL_INTERVAL_MINUTE;

            case Tokens.SECOND :
                return Types.SQL_INTERVAL_SECOND;

            case Tokens.MILLISECOND :
                return Types.DTI_MILLISECOND;

            case Tokens.MICROSECOND :
                return Types.DTI_MICROSECOND;

            case Tokens.NANOSECOND :
                return Types.DTI_NANOSECOND;

            case Tokens.TIMEZONE_HOUR :
                return Types.DTI_TIMEZONE_HOUR;

            case Tokens.TIMEZONE_MINUTE :
                return Types.DTI_TIMEZONE_MINUTE;

            case Tokens.TIMEZONE :
                return Types.DTI_TIMEZONE;

            case Tokens.DAY_NAME :
                return Types.DTI_DAY_NAME;

            case Tokens.MONTH_NAME :
                return Types.DTI_MONTH_NAME;

            case Tokens.QUARTER :
                return Types.DTI_QUARTER;

            case Tokens.DAY_OF_MONTH :
                return Types.DTI_DAY_OF_MONTH;

            case Tokens.DAY_OF_WEEK :
                return Types.DTI_DAY_OF_WEEK;

            case Tokens.DAY_OF_YEAR :
                return Types.DTI_DAY_OF_YEAR;

            case Tokens.WEEK_OF_YEAR :
                return Types.DTI_WEEK_OF_YEAR;

            case Tokens.SECONDS_MIDNIGHT :
                return Types.DTI_SECONDS_MIDNIGHT;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DTIType");
        }
    }

    public static String getFieldNameTokenForType(int type) {

        switch (type) {

            case Types.SQL_INTERVAL_YEAR :
                return Tokens.T_YEAR;

            case Types.SQL_INTERVAL_MONTH :
                return Tokens.T_MONTH;

            case Types.SQL_INTERVAL_DAY :
                return Tokens.T_DAY;

            case Types.SQL_INTERVAL_HOUR :
                return Tokens.T_HOUR;

            case Types.SQL_INTERVAL_MINUTE :
                return Tokens.T_MINUTE;

            case Types.SQL_INTERVAL_SECOND :
                return Tokens.T_SECOND;

            case Types.DTI_TIMEZONE_HOUR :
                return Tokens.T_TIMEZONE_HOUR;

            case Types.DTI_TIMEZONE_MINUTE :
                return Tokens.T_TIMEZONE_MINUTE;

            case Types.DTI_DAY_NAME :
                return Tokens.T_DAY_NAME;

            case Types.DTI_MONTH_NAME :
                return Tokens.T_MONTH_NAME;

            case Types.DTI_QUARTER :
                return Tokens.T_QUARTER;

            case Types.DTI_DAY_OF_MONTH :
                return Tokens.T_DAY_OF_MONTH;

            case Types.DTI_DAY_OF_WEEK :
                return Tokens.T_DAY_OF_WEEK;

            case Types.DTI_DAY_OF_YEAR :
                return Tokens.T_DAY_OF_YEAR;

            case Types.DTI_WEEK_OF_YEAR :
                return Tokens.T_WEEK_OF_YEAR;

            case Types.DTI_SECONDS_MIDNIGHT :
                return Tokens.T_SECONDS_MIDNIGHT;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DTIType");
        }
    }

    public static boolean isValidDatetimeRange(Type a, Type b) {

        if (!a.isDateTimeType()) {
            return false;
        }

        if (b.isDateTimeType()) {
            if ((a.typeCode == Types.SQL_TIME && b.typeCode == Types.SQL_DATE)
                    || (a.typeCode == Types.SQL_DATE
                        && b.typeCode == Types.SQL_TIME)) {
                return false;
            }

            return true;
        }

        if (b.isIntervalType()) {
            return ((DateTimeType) a).canAdd((IntervalType) b);
        }

        return false;
    }

    public static final int defaultTimeFractionPrecision      = 0;
    public static final int defaultTimestampFractionPrecision = 6;
    public static final int defaultIntervalPrecision          = 2;
    public static final int defaultIntervalFractionPrecision  = 6;
    public static final int maxIntervalPrecision              = 9;
    public static final int maxIntervalSecondPrecision        = 12;
    public static final int maxFractionPrecision              = 9;
    public static final int limitNanoseconds                  = 1000000000;

    public abstract int getPart(Session session, Object dateTime, int part);

    public abstract BigDecimal getSecondPart(Session session, Object dateTime);

    BigDecimal getSecondPart(long seconds, long nanos) {

        seconds *= DTIType.precisionLimits[scale];
        seconds += nanos / DTIType.nanoScaleFactors[scale];

        return BigDecimal.valueOf(seconds, scale);
    }
}
