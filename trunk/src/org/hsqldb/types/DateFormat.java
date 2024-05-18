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

import java.text.ParsePosition;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;

import java.util.Arrays;
import java.util.Locale;

/**
 * Parses and formats date time objects with the given pattern.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 2.7.3
 */
public class DateFormat {

    public static final Locale    defaultLocale  = Locale.UK;
    //J-

    private static final char[][] dateTokens     = {
            { 'R', 'R', 'R', 'R' }, { 'I', 'Y', 'Y', 'Y' }, { 'Y', 'Y', 'Y', 'Y' },
            { 'I', 'Y' }, { 'Y', 'Y' },
            { 'B', 'C' }, { 'B', '.', 'C', '.' }, { 'A', 'D' }, { 'A', '.', 'D', '.' },
            { 'M', 'O', 'N' }, { 'M', 'O', 'N', 'T', 'H' },
            { 'M', 'M' },
            { 'D', 'A', 'Y' }, { 'D', 'Y' }, { 'D' },
            { 'W' }, { 'I', 'W' }, { 'D', 'D' }, { 'D', 'D', 'D' },
            { 'H', 'H', '2', '4' }, { 'H', 'H', '1', '2' }, { 'H', 'H' },
            { 'M', 'I' }, { 'S', 'S' },
            { 'A', 'M' }, { 'P', 'M' }, { 'A', '.', 'M', '.' }, { 'P', '.', 'M', '.' },
            { 'F', 'F', '1' }, { 'F', 'F', '2' },  { 'F', 'F', '3' }, { 'F', 'F', '4' },  { 'F', 'F', '5' }, { 'F', 'F', '6' },   { 'F', 'F', '7' }, { 'F', 'F', '8' }, { 'F', 'F', '9' },
            { 'F', 'F' },
            { 'T', 'Z' },
    };

    private static final String[] javaDateTokens = {
            "uuuu", "YYYY", "uuuu",
            "YY", "uu",
            "G", "G", "G", "G",
            "MMM", "MMMM",
            "MM",
            "EEEE", "EE", "F",
            "W", "ww", "dd", "D",
            "HH", "KK", "HH",
            "mm", "ss",
            "a", "a", "a", "a",
            "[S]", "[SS]", "[SSS]", "[SSSS]", "[SSSSS]", "[SSSSSS]", "[SSSSSSS]", "[SSSSSSSS]", "[SSSSSSSSS]",
            "SSSSSS",
            "xxxxx"
    };

    private static final char[] fixedFraction = { 'F', 'F' };
    private static final String variableFraction = "[[SSSSSSSSS][SSSSSSSS][SSSSSSS][SSSSSS][SSSSS][SSSS][SSS][SS][S]]";


    //J+

    public static DateTimeFormatter toFormatter(String pattern, boolean parse) {

        try {
            String javaPattern               = toJavaDatePattern(
                pattern,
                parse);
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

            builder.parseCaseInsensitive();
            builder.parseLenient();
            builder.appendPattern(javaPattern);

            DateTimeFormatter dtf = builder.toFormatter(defaultLocale);

            return dtf;
        } catch (Exception e) {
            throw Error.error(e, ErrorCode.X_22007, e.toString());
        }
    }

    public static TimestampData toDate(
            DateTimeType dataType,
            String string,
            DateTimeFormatter formatter) {

        long seconds;
        int  nanos = 0;
        int  zone  = 0;

        try {
            ParsePosition    ppos = new ParsePosition(0);
            TemporalAccessor ta   = formatter.parse(string, ppos);

            switch (dataType.typeCode) {

                case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    if (ta.isSupported(ChronoField.OFFSET_SECONDS)) {
                        OffsetDateTime odt = OffsetDateTime.from(ta);

                        seconds = odt.toEpochSecond();
                        nanos   = odt.getNano();
                        zone    = odt.getOffset().getTotalSeconds();
                        break;
                    }

                // fall through
                case Types.SQL_TIMESTAMP :
                    if (ta.isSupported(ChronoField.SECOND_OF_MINUTE)) {
                        LocalDateTime ldt = LocalDateTime.from(ta);

                        seconds = ldt.toEpochSecond(ZoneOffset.UTC);
                        nanos   = ldt.getNano();
                        break;
                    }

                // fall through
                case Types.DATE :
                    LocalDate ld = LocalDate.from(ta);

                    seconds = ld.toEpochDay() * DTIType.secondsInDay;
                    break;

                default :
                    throw Error.error(ErrorCode.X_42561);
            }

            nanos = DTIType.normaliseFraction(nanos, dataType.scale);

            return new TimestampData(seconds, nanos, zone);
        } catch (Exception e) {
            throw Error.error(e, ErrorCode.X_22007, e.toString());
        }
    }

    public static TimestampData toDate(
            DateTimeType dataType,
            String string,
            String pattern) {
        DateTimeFormatter dtf = toFormatter(pattern, true);

        return toDate(dataType, string, dtf);
    }

    public static String toFormattedDate(
            DateTimeType dataType,
            Object dateTime,
            DateTimeFormatter formatter) {

        try {
            Temporal dt;

            switch (dataType.typeCode) {

                case Types.DATE :
                case Types.SQL_TIMESTAMP :
                case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    TimestampData ts = (TimestampData) dateTime;
                    LocalDateTime ldt = LocalDateTime.ofEpochSecond(
                        ts.seconds + ts.zone,
                        ts.nanos,
                        ZoneOffset.UTC);

                    if (dataType.typeCode
                            == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                        dt = OffsetDateTime.of(
                            ldt,
                            ZoneOffset.ofTotalSeconds(ts.zone));
                    } else {
                        dt = ldt;
                    }

                    break;

                case Types.SQL_TIME :
                case Types.SQL_TIME_WITH_TIME_ZONE :
                    TimeData ti = (TimeData) dateTime;
                    LocalTime lt = LocalTime.ofNanoOfDay(
                        (ti.seconds + ti.zone) * DTIType.nanosInSecond
                        + ti.nanos);

                    if (dataType.typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
                        dt = OffsetTime.of(
                            lt,
                            ZoneOffset.ofTotalSeconds(ti.zone));
                    } else {
                        dt = lt;
                    }

                    break;

                default :
                    throw Error.error(ErrorCode.X_42561);
            }

            String result = formatter.format(dt);

            return result;
        } catch (Exception e) {
            throw Error.error(e, ErrorCode.X_22007, e.toString());
        }
    }

    public static String toFormattedDate(
            DateTimeType dataType,
            Object dateTime,
            String pattern) {
        DateTimeFormatter dtf = toFormatter(pattern, false);

        return toFormattedDate(dataType, dateTime, dtf);
    }

    /** Indicates end-of-input */
    private static final char   e          = 0xffff;
    private static final String javaPrefix = "JAVA:";

    /**
     * Converts the given format into a pattern accepted by {@code java.time.DateTimeFormatter}
     *
     * @param format date format
     * @param parse false for formatting
     */
    public static String toJavaDatePattern(String format, boolean parse) {

        int           len = format.length();
        char          ch;
        StringBuilder sb        = new StringBuilder(len);
        Tokenizer     tokenizer = new Tokenizer();

        if (format.startsWith(javaPrefix)) {
            return format.substring(javaPrefix.length());
        }

        for (int i = 0; i <= len; i++) {
            ch = (i == len)
                 ? e
                 : format.charAt(i);

            if (tokenizer.isInQuotes()) {
                if (tokenizer.isQuoteChar(ch)) {
                    ch = '\'';
                } else if (ch == '\'') {

                    // double the single quote
                    sb.append(ch);
                }

                sb.append(ch);
                continue;
            }

            if (!tokenizer.next(ch, i)) {
                if (tokenizer.consumed) {
                    int    index = tokenizer.getLastMatch();
                    String s     = javaDateTokens[index];

                    if (Arrays.equals(dateTokens[index], fixedFraction)) {
                        if (parse) {
                            s = variableFraction;
                        }
                    }

                    sb.append(s);

                    i = tokenizer.matchOffset;
                } else {
                    if (tokenizer.isQuoteChar(ch)) {
                        ch = '\'';

                        sb.append(ch);
                    } else if (tokenizer.isLiteral(ch)) {
                        sb.append(ch);
                    } else if (ch == e) {

                        //
                    } else {
                        throw Error.error(
                            ErrorCode.X_22007,
                            format.substring(i));
                    }
                }

                tokenizer.reset();
            }
        }

        if (tokenizer.isInQuotes()) {
            throw Error.error(ErrorCode.X_22007);
        }

        String javaPattern = sb.toString();

        return javaPattern;
    }

    /**
     * This class can match 64 tokens at maximum.
     */
    static class Tokenizer {

        private int     lastMatched;
        private int     matchOffset;
        private int     offset;
        private long    state;
        private boolean consumed;
        private boolean isInQuotes;
        private boolean matched;

        //
        private final char     quoteChar;
        private final char[]   literalChars;
        private static char[]  defaultLiterals = new char[] {
            ' ', ',', '-', '.', '/', ':', ';'
        };
        private final char[][] tokens;

        public Tokenizer() {

            quoteChar    = '\"';
            literalChars = defaultLiterals;
            tokens       = dateTokens;

            reset();
        }

        /**
         * Resets for next reuse.
         */
        public void reset() {

            lastMatched = -1;
            offset      = -1;
            state       = 0;
            consumed    = false;
            matched     = false;
        }

        /**
         * Returns the length of a token to match.
         */
        public int length() {
            return offset;
        }

        /**
         * Returns an index of the last matched token.
         */
        public int getLastMatch() {
            return lastMatched;
        }

        /**
         * Indicates whether the last character has been consumed by the matcher.
         */
        public boolean isConsumed() {
            return consumed;
        }

        /**
         * Indicates whether the last character has been matched by the matcher.
         */
        public boolean wasMatched() {
            return matched;
        }

        /**
         * Indicates if tokenizing a quoted string
         */
        public boolean isInQuotes() {
            return isInQuotes;
        }

        /**
         * returns true if character is the quote char and sets state
         */
        public boolean isQuoteChar(char ch) {

            if (quoteChar == ch) {
                isInQuotes = !isInQuotes;

                return true;
            }

            return false;
        }

        /**
         * Returns true if ch is in the list of literals
         */
        public boolean isLiteral(char ch) {
            return ArrayUtil.isInSortedArray(ch, literalChars);
        }

        /**
         * Checks whether the specified bit is not set.
         *
         * @param bit numbered from high bit
         */
        private boolean isZeroBit(int bit) {
            return (state & (1L << bit)) == 0;
        }

        /**
         * Sets the specified bit.
         * @param bit numbered from high bit
         */
        private void setBit(int bit) {
            state |= (1L << bit);
        }

        /**
         * Matches the specified character against tokens.
         *
         * @param ch character
         * @param position in the string
         */
        public boolean next(char ch, int position) {

            int index = ++offset;
            int len   = offset + 1;
            int left  = 0;

            matched = false;

            for (int i = tokens.length; --i >= 0; ) {
                if (isZeroBit(i)) {
                    if (tokens[i][index] == Character.toUpperCase(ch)) {
                        if (tokens[i].length == len) {
                            setBit(i);

                            lastMatched = i;
                            consumed    = true;
                            matched     = true;
                            matchOffset = position;
                        } else {
                            ++left;
                        }
                    } else {
                        setBit(i);
                    }
                }
            }

            return left > 0;
        }
    }
}
