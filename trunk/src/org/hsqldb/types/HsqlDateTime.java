/* Copyright (c) 2001-2024, The HSQL Development Group
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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.StringUtil;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * collection of static methods to convert Date and Timestamp strings
 * into corresponding Java objects and perform other Calendar related
 * operation.<p>
 *
 * From version 2.0.0, HSQLDB supports TIME ZONE with datetime types. The
 * values are stored internally as UTC seconds from 1970, regardless of the
 * time zone of the JVM, and converted as and when required, to the local
 * timezone.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.7.0
 */
public class HsqlDateTime {

    public static final Locale  defaultLocale    = Locale.UK;
    private static final Calendar tempCalDefault = new GregorianCalendar();
    private static final Calendar tempCalGMT = new GregorianCalendar(
        TimeZone.getTimeZone("GMT"),
        defaultLocale);
    private static final String sdfdPattern      = "yyyy-MM-dd";
    private static final SimpleDateFormat sdfd = new SimpleDateFormat(
        sdfdPattern,
        defaultLocale);
    private static final String sdftsPattern     = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat sdfts = new SimpleDateFormat(
        sdftsPattern,
        defaultLocale);
    private static final String sdftsSysPattern  = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final Date   sysDate          = new Date();

    static {
        TimeZone.getDefault();
        tempCalGMT.setLenient(false);
        sdfd.setCalendar(
            new GregorianCalendar(TimeZone.getTimeZone("GMT"), defaultLocale));
        sdfd.setLenient(false);
        sdfts.setCalendar(
            new GregorianCalendar(TimeZone.getTimeZone("GMT"), defaultLocale));
        sdfts.setLenient(false);
    }

    public static long getDateSeconds(String s) {

        try {
            synchronized (sdfd) {
                Date d = sdfd.parse(s);

                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007, e);
        }
    }

    public static String getDateString(long seconds) {

        synchronized (sdfd) {
            sysDate.setTime(seconds * 1000);

            return sdfd.format(sysDate);
        }
    }

    public static long getTimestampSeconds(String s) {

        try {
            synchronized (sdfts) {
                Date d = sdfts.parse(s);

                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007, e);
        }
    }

    public static String getTimestampString(
            long seconds,
            int nanos,
            int scale) {

        synchronized (sdfts) {
            sysDate.setTime(seconds * 1000);

            String ts = sdfts.format(sysDate);

            if (scale > 0) {
                ts += '.' + StringUtil.toZeroPaddedString(nanos, 9, scale);
            }

            return ts;
        }
    }

    public static String getTimestampString(long millis) {

        synchronized (sdfts) {
            sysDate.setTime(millis);

            return sdfts.format(sysDate);
        }
    }

    private static void resetToDate(Calendar cal) {

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }

    private static void resetToTime(Calendar cal) {

        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
    }

    public static long convertMillisToCalendar(Calendar calendar, long millis) {

        synchronized (tempCalGMT) {
            synchronized (calendar) {
                calendar.clear();
                tempCalGMT.setTimeInMillis(millis);
                calendar.set(
                    tempCalGMT.get(Calendar.YEAR),
                    tempCalGMT.get(Calendar.MONTH),
                    tempCalGMT.get(Calendar.DAY_OF_MONTH),
                    tempCalGMT.get(Calendar.HOUR_OF_DAY),
                    tempCalGMT.get(Calendar.MINUTE),
                    tempCalGMT.get(Calendar.SECOND));

                return calendar.getTimeInMillis();
            }
        }
    }

    public static long convertMillisFromCalendar(
            Calendar sourceCalendar,
            Calendar targetClendar,
            long millis) {

        synchronized (targetClendar) {
            synchronized (sourceCalendar) {
                targetClendar.clear();
                sourceCalendar.setTimeInMillis(millis);
                targetClendar.set(
                    sourceCalendar.get(Calendar.YEAR),
                    sourceCalendar.get(Calendar.MONTH),
                    sourceCalendar.get(Calendar.DAY_OF_MONTH),
                    sourceCalendar.get(Calendar.HOUR_OF_DAY),
                    sourceCalendar.get(Calendar.MINUTE),
                    sourceCalendar.get(Calendar.SECOND));

                return targetClendar.getTimeInMillis();
            }
        }
    }

    public static long convertSecondsFromCalendar(
            Calendar sourceCalendar,
            Calendar targetClendar,
            long seconds) {

        synchronized (targetClendar) {
            synchronized (sourceCalendar) {
                targetClendar.clear();
                sourceCalendar.setTimeInMillis(seconds * 1000);
                targetClendar.set(
                    sourceCalendar.get(Calendar.YEAR),
                    sourceCalendar.get(Calendar.MONTH),
                    sourceCalendar.get(Calendar.DAY_OF_MONTH),
                    sourceCalendar.get(Calendar.HOUR_OF_DAY),
                    sourceCalendar.get(Calendar.MINUTE),
                    sourceCalendar.get(Calendar.SECOND));

                return targetClendar.getTimeInMillis() / 1000;
            }
        }
    }

    public static long getNormalisedTime(long t) {
        return getNormalisedTime(tempCalGMT, t);
    }

    public static long getNormalisedTime(Calendar calendar, long t) {

        synchronized (calendar) {
            calendar.setTimeInMillis(t);
            resetToTime(calendar);

            return calendar.getTimeInMillis();
        }
    }

    public static long getNormalisedDate(long d) {
        return getNormalisedDate(tempCalGMT, d);
    }

    public static long getNormalisedDate(Calendar calendar, long t) {

        synchronized (calendar) {
            calendar.setTimeInMillis(t);
            resetToDate(calendar);

            return calendar.getTimeInMillis();
        }
    }

    public static int getZoneSeconds() {
        return getZoneSeconds(tempCalDefault);
    }

    public static int getZoneSeconds(Calendar calendar) {
        return (calendar.get(
            Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET)) / 1000;
    }

    /**
     * truncates millisecond date object
     */
    public static long getTruncatedPart(Calendar calendar, long m, int part) {

        synchronized (calendar) {
            calendar.setTimeInMillis(m);

            switch (part) {

                case Types.DTI_ISO_WEEK_OF_YEAR : {
                    int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);

                    if (dayWeek == 1) {
                        dayWeek = 8;
                    }

                    calendar.add(Calendar.DAY_OF_YEAR, 2 - dayWeek);
                    resetToDate(calendar);
                    break;
                }

                case Types.DTI_WEEK_OF_YEAR : {
                    int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);

                    calendar.add(Calendar.DAY_OF_YEAR, 1 - dayWeek);
                    resetToDate(calendar);
                    break;
                }

                case Types.DTI_QUARTER : {
                    int month = calendar.get(Calendar.MONTH);

                    month = (month / 3) * 3;

                    zeroFromPart(calendar, Types.SQL_INTERVAL_MONTH);
                    calendar.set(Calendar.MONTH, month);
                    break;
                }

                default : {
                    zeroFromPart(calendar, part);
                    break;
                }
            }

            return calendar.getTimeInMillis();
        }
    }

    /**
     * rounded millisecond date object
     */
    public static long getRoundedPart(Calendar calendar, long m, int part) {

        synchronized (calendar) {
            calendar.setTimeInMillis(m);

            switch (part) {

                case Types.SQL_INTERVAL_YEAR :
                    if (calendar.get(Calendar.MONTH) > 6) {
                        calendar.add(Calendar.YEAR, 1);
                    }

                    break;

                case Types.SQL_INTERVAL_MONTH :
                    if (calendar.get(Calendar.DAY_OF_MONTH) > 15) {
                        calendar.add(Calendar.MONTH, 1);
                    }

                    break;

                case Types.SQL_INTERVAL_DAY :
                    if (calendar.get(Calendar.HOUR_OF_DAY) > 11) {
                        calendar.add(Calendar.DAY_OF_MONTH, 1);
                    }

                    break;

                case Types.SQL_INTERVAL_HOUR :
                    if (calendar.get(Calendar.MINUTE) > 29) {
                        calendar.add(Calendar.HOUR_OF_DAY, 1);
                    }

                    break;

                case Types.SQL_INTERVAL_MINUTE :
                    if (calendar.get(Calendar.SECOND) > 29) {
                        calendar.add(Calendar.MINUTE, 1);
                    }

                    break;

                case Types.SQL_INTERVAL_SECOND :
                    if (calendar.get(Calendar.MILLISECOND) > 499) {
                        calendar.add(Calendar.SECOND, 1);
                    }

                    break;

                case Types.DTI_WEEK_OF_YEAR : {
                    int dayYear = calendar.get(Calendar.DAY_OF_YEAR);
                    int year    = calendar.get(Calendar.YEAR);
                    int week    = calendar.get(Calendar.WEEK_OF_YEAR);
                    int day     = calendar.get(Calendar.DAY_OF_WEEK);

                    calendar.clear();
                    calendar.set(Calendar.YEAR, year);

                    if (day > 3) {
                        week++;
                    }

                    if (week == 1 && (dayYear > 356 || dayYear < 7)) {
                        calendar.set(Calendar.DAY_OF_YEAR, dayYear);

                        while (true) {
                            if (calendar.get(Calendar.DAY_OF_WEEK) == 1) {
                                return calendar.getTimeInMillis();
                            }

                            calendar.add(Calendar.DAY_OF_YEAR, -1);
                        }
                    }

                    calendar.set(Calendar.WEEK_OF_YEAR, week);

                    return calendar.getTimeInMillis();
                }
            }

            zeroFromPart(calendar, part);

            return calendar.getTimeInMillis();
        }
    }

    public static void zeroFromPart(Calendar cal, int part) {

        switch (part) {

            case Types.SQL_INTERVAL_YEAR :
                cal.set(Calendar.MONTH, 0);

            // fall through
            case Types.SQL_INTERVAL_MONTH :
                cal.set(Calendar.DAY_OF_MONTH, 1);

            // fall through
            case Types.SQL_INTERVAL_DAY :
                cal.set(Calendar.HOUR_OF_DAY, 0);

            // fall through
            case Types.SQL_INTERVAL_HOUR :
                cal.set(Calendar.MINUTE, 0);

            // fall through
            case Types.SQL_INTERVAL_MINUTE :
                cal.set(Calendar.SECOND, 0);

            // fall through
            case Types.SQL_INTERVAL_SECOND :
                cal.set(Calendar.MILLISECOND, 0);

            // fall through
            default :
        }
    }

    private static final IntValueHashMap<String> shortNameToIntervalTypeCode =
        new IntValueHashMap<>();

    static {
        shortNameToIntervalTypeCode.put("YYYY", Types.SQL_INTERVAL_YEAR);
        shortNameToIntervalTypeCode.put("YY", Types.SQL_INTERVAL_YEAR);
        shortNameToIntervalTypeCode.put("MONTH", Types.SQL_INTERVAL_MONTH);
        shortNameToIntervalTypeCode.put("MON", Types.SQL_INTERVAL_MONTH);
        shortNameToIntervalTypeCode.put("MM", Types.SQL_INTERVAL_MONTH);
        shortNameToIntervalTypeCode.put("WW", Types.DTI_WEEK_OF_YEAR);
        shortNameToIntervalTypeCode.put("IW", Types.DTI_ISO_WEEK_OF_YEAR);
        shortNameToIntervalTypeCode.put("DDD", Types.SQL_INTERVAL_DAY);
        shortNameToIntervalTypeCode.put("DD", Types.SQL_INTERVAL_DAY);
        shortNameToIntervalTypeCode.put("HH24", Types.SQL_INTERVAL_HOUR);
        shortNameToIntervalTypeCode.put("HH12", Types.SQL_INTERVAL_HOUR);
        shortNameToIntervalTypeCode.put("HH", Types.SQL_INTERVAL_HOUR);
        shortNameToIntervalTypeCode.put("MI", Types.SQL_INTERVAL_MINUTE);
        shortNameToIntervalTypeCode.put("SS", Types.SQL_INTERVAL_SECOND);
    }

    public static int toStandardIntervalPart(String id) {
        return shortNameToIntervalTypeCode.get(id, -1);
    }

    /**
     * Timestamp String generator
     */
    public static class SystemTimeString {

        private Date date = new Date();
        private SimpleDateFormat dateFormat = new SimpleDateFormat(
            sdftsSysPattern);

        public SystemTimeString() {

            dateFormat.setCalendar(
                new GregorianCalendar(TimeZone.getTimeZone("GMT"),
                                      defaultLocale));
            dateFormat.setLenient(false);
        }

        public synchronized String getTimestampString() {
            date.setTime(System.currentTimeMillis());

            return dateFormat.format(date);
        }
    }
}
