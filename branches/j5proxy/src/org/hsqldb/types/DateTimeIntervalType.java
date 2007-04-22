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

import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.IntKeyIntValueHashMap;

/**
 * Common elements for Type instances for DATETIME and INTERVAL.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public abstract class DateTimeIntervalType extends Type {

    protected DateTimeIntervalType(int type, long precision, int scale) {
        super(type, precision, scale);
    }

    static final byte[] yearToMonthSeparators = { '-' };
    static final byte[] dayToSecondSeparators = {
        ' ', ':', ':', '.'
    };
    static final int[]  yearToMonthFactors    = {
        12, 1
    };
    static final int[]  dayToSecondFactors    = {
        24 * 60 * 60, 60 * 60, 60, 1, 0
    };

//    static final int[]  hourToSecondFactors   = {
//        60 * 60, 60, 1, 0
//    };
    static final int[] yearToMonthLimits   = {
        0, 12
    };
    static final int[] dayToSecondLimits   = {
        0, 24, 60, 60, 1000000000
    };
    static final int[] hourToSecondLimits  = {
        0, 24, 60, 60, 1000000000
    };
    static final int   MONTH_INDEX         = 1;
    static final int   FRACTION_PART_INDEX = 4;
    static final int[] precisionLimits     = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
        1000000000
    };
    static final int[] precisionFactors = {
        100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1
    };
    static int[]       nanoScaleFactors = {
        1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100,
        10, 1
    };
    static final int[] intervalParts = {
        Types.SQL_INTERVAL_YEAR, Types.SQL_INTERVAL_MONTH,
        Types.SQL_INTERVAL_DAY, Types.SQL_INTERVAL_HOUR,
        Types.SQL_INTERVAL_MINUTE, Types.SQL_INTERVAL_SECOND
    };
    static final int[][] intervalTypes = {
        {
            Types.SQL_INTERVAL_YEAR, Types.SQL_INTERVAL_YEAR_TO_MONTH, 0, 0,
            0, 0
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
        intervalIndexMap.put(Types.SQL_INTERVAL_DAY, 0);
        intervalIndexMap.put(Types.SQL_INTERVAL_HOUR, 1);
        intervalIndexMap.put(Types.SQL_INTERVAL_MINUTE, 2);
        intervalIndexMap.put(Types.SQL_INTERVAL_SECOND, 3);
    }

    static final int TIMEZONE_HOUR   = Types.SQL_TYPE_NUMBER_LIMIT + 1;
    static final int TIMEZONE_MINUTE = Types.SQL_TYPE_NUMBER_LIMIT + 2;
    static final int DAY_OF_WEEK     = Types.SQL_TYPE_NUMBER_LIMIT + 3;
    static final int WEEK_OF_YEAR    = Types.SQL_TYPE_NUMBER_LIMIT + 4;

    static int getPrecisionExponent(int value) {

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

            case Token.YEAR :
                return Types.SQL_INTERVAL_YEAR;

            case Token.MONTH :
                return Types.SQL_INTERVAL_MONTH;

            case Token.DAY :
                return Types.SQL_INTERVAL_DAY;

            case Token.HOUR :
                return Types.SQL_INTERVAL_HOUR;

            case Token.MINUTE :
                return Types.SQL_INTERVAL_MINUTE;

            case Token.SECOND :
                return Types.SQL_INTERVAL_SECOND;

            case Token.TIMEZONE_HOUR :
                return TIMEZONE_HOUR;

            case Token.TIMEZONE_MINUTE :
                return TIMEZONE_MINUTE;

            case Token.DAY_OF_WEEK :
                return DAY_OF_WEEK;

            case Token.WEEK_OF_YEAR :
                return WEEK_OF_YEAR;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "IntervalType");
        }
    }

    public static final int defaultDatetimeFractionPrecision = 0;
    public static final int defaultIntervalPrecision         = 2;
    public static final int defaultIntervalFractionPrecision = 6;
    public static final int maxIntervalPrecision             = 4;
    public static final int maxFractionPrecision             = 9;
    public static final int limitNanoseconds                 = 1000000000;
    public static final NumberType extractSecondType =
        new NumberType(Types.SQL_NUMERIC, 18, maxFractionPrecision);

    abstract public int getPart(Object dateTime, int part);

    abstract public BigDecimal getSecondPart(Object dateTime);

    static BigDecimal getSecondPart(long seconds, long nanos, int scale) {

        seconds *= DateTimeIntervalType.precisionLimits[scale];
        seconds += nanos / DateTimeIntervalType.nanoScaleFactors[scale];

        return BigDecimal.valueOf(seconds, scale);
    }
}
