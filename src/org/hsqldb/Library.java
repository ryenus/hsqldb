/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb;

import java.util.Locale;

import org.hsqldb.persist.HsqlDatabaseProperties;

/**
 * Since the introduction of SQL built-in functions and rewrite of
 * OpenGroup CLI functions as SQL functions, most previous methods have been
 * removed. The remaining methods are called when SQL functions are executed.
 *
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class Library {

    private Library() {}

    /**
     * Retrieves a <em>magically</em> rounded </code>double</code> value produced
     * from the given <code>double</code> value.  This method provides special
     * handling for numbers close to zero and performs rounding only for
     * numbers within a specific range, returning  precisely the given value
     * if it does not lie in this range. <p>
     *
     * Special handling includes: <p>
     *
     * <UL>
     * <LI> input in the interval -0.0000000000001..0.0000000000001 returns 0.0
     * <LI> input outside the interval -1000000000000..1000000000000 returns
     *      input unchanged
     * <LI> input is converted to String form
     * <LI> input with a <code>String</code> form length greater than 16 returns
     *      input unchaged
     * <LI> <code>String</code> form with last four characters of '...000x' where
     *      x != '.' is converted to '...0000'
     * <LI> <code>String</code> form with last four characters of '...9999' is
     *      converted to '...999999'
     * <LI> the <code>java.lang.Double.doubleValue</code> of the <code>String</code>
     *      form is returned
     * </UL>
     * @author Thomas Mueller (Hypersonic SQL Group)
     * @param d the double value for which to retrieve the <em>magically</em>
     *      rounded value
     * @return the <em>magically</em> rounded value produced
     */
    public static double roundMagic(double d) {

        // this function rounds numbers in a good way but slow:
        // - special handling for numbers around 0
        // - only numbers <= +/-1000000000000
        // - convert to a string
        // - check the last 4 characters:
        // '000x' becomes '0000'
        // '999x' becomes '999999' (this is rounded automatically)
        if ((d < 0.0000000000001) && (d > -0.0000000000001)) {
            return 0.0;
        }

        if ((d > 1000000000000.) || (d < -1000000000000.)) {
            return d;
        }

        StringBuffer sb = new StringBuffer();

        sb.append(d);

        int len = sb.length();

        if (len < 16) {
            return d;
        }

        char cx = sb.charAt(len - 1);
        char c1 = sb.charAt(len - 2);
        char c2 = sb.charAt(len - 3);
        char c3 = sb.charAt(len - 4);

        if ((c1 == '0') && (c2 == '0') && (c3 == '0') && (cx != '.')) {
            sb.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9') && (cx != '.')) {
            sb.setCharAt(len - 1, '9');
            sb.append('9');
            sb.append('9');
        }

        return Double.valueOf(sb.toString()).doubleValue();
    }

    /**
     * Returns the given <code>double</code> value, rounded to the given
     * <code>int</code> places right of the decimal point. If
     * the supplied rounding place value is negative, rounding is performed
     * to the left of the decimal point, using its magnitude (absolute value).
     * @param d the value to be rounded
     * @param p the rounding place value
     * @return <code>d</code> rounded
     */
    public static double round(double d, int p) {

        double f = Math.pow(10., p);

        return Math.round(d * f) / f;
    }

    // STRING FUNCTIONS

    /**
     * Returns a count of the characters that do not match when comparing
     * the 4 digit numeric SOUNDEX character sequences for the
     * given <code>String</code> objects.  If either <code>String</code> object is
     * <code>null</code>, zero is returned.
     * @param s1 the first <code>String</code>
     * @param s2 the second <code>String</code>
     * @return the number of differences between the <code>SOUNDEX</code> of
     *      <code>s1</code> and the <code>SOUNDEX</code> of <code>s2</code>
     */

// fredt@users 20020305 - patch 460907 by fredt - soundex
    public static int difference(String s1, String s2) {

        /** @todo: check if this is the standard algorithm */
        if ((s1 == null) || (s2 == null)) {
            return 0;
        }

        s1 = soundex(s1);
        s2 = soundex(s2);

        int e = 0;

        for (int i = 0; i < 4; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                e++;
            }
        }

        return e;
    }

    /**
     * Returns the starting position of the first occurrence of
     * the given <code>search</code> <code>String</code> object within
     * the given <code>String</code> object, <code>s</code>.
     *
     * The search for the first occurrence of <code>search</code> begins with
     * the first character position in <code>s</code>, unless the optional
     * argument, <code>start</code>, is specified (non-null). If
     * <code>start</code> is specified, the search begins with the character
     * position indicated by the value of <code>start</code>, where the
     * first character position in <code>s</code> is indicated by the value 1.
     * If <code>search</code> is not found within <code>s</code>, the
     * value 0 is returned.
     * @param search the <code>String</code> occurence to find in <code>s</code>
     * @param s the <code>String</code> within which to find the first
     *      occurence of <code>search</code>
     * @param start the optional character position from which to start
     *      looking in <code>s</code>
     * @return the one-based starting position of the first occurrence of
     *      <code>search</code> within <code>s</code>, or 0 if not found
     */
    public static int locate(String search, String s, Integer start) {

        if (s == null || search == null) {
            return 0;
        }

        int i = (start == null) ? 0
                                : start.intValue() - 1;

        return s.indexOf(search, (i < 0) ? 0
                                         : i) + 1;
    }

// fredt@users 20011010 - patch 460907 by fredt - soundex

    /**
     * Returns a four character code representing the sound of the given
     * <code>String</code>. Non-ASCCI characters in the
     * input <code>String</code> are ignored. <p>
     *
     * This method was
     * rewritten for HSQLDB by fredt@users to comply with the description at
     * <a href="http://www.archives.gov/genealogy/census/soundex.html">
     * http://www.archives.gov/genealogy/census/soundex.html </a>.<p>
     * @param s the <code>String</code> for which to calculate the 4 character
     *      <code>SOUNDEX</code> value
     * @return the 4 character <code>SOUNDEX</code> value for the given
     *      <code>String</code>
     */
    public static String soundex(String s) {

        if (s == null) {
            return s;
        }

        s = s.toUpperCase(Locale.ENGLISH);

        int    len       = s.length();
        char[] b         = new char[] {
            '0', '0', '0', '0'
        };
        char   lastdigit = '0';

        for (int i = 0, j = 0; i < len && j < 4; i++) {
            char c = s.charAt(i);
            char newdigit;

            if ("AEIOUY".indexOf(c) != -1) {
                newdigit = '7';
            } else if (c == 'H' || c == 'W') {
                newdigit = '8';
            } else if ("BFPV".indexOf(c) != -1) {
                newdigit = '1';
            } else if ("CGJKQSXZ".indexOf(c) != -1) {
                newdigit = '2';
            } else if (c == 'D' || c == 'T') {
                newdigit = '3';
            } else if (c == 'L') {
                newdigit = '4';
            } else if (c == 'M' || c == 'N') {
                newdigit = '5';
            } else if (c == 'R') {
                newdigit = '6';
            } else {
                continue;
            }

            if (j == 0) {
                b[j++]    = c;
                lastdigit = newdigit;
            } else if (newdigit <= '6') {
                if (newdigit != lastdigit) {
                    b[j++]    = newdigit;
                    lastdigit = newdigit;
                }
            } else if (newdigit == '7') {
                lastdigit = newdigit;
            }
        }

        return new String(b, 0, 4);
    }

    /**
     * Retrieves the full version number of this database product. <p>
     *
     * @return database version number as a <code>String</code> object
     * @since 1.8.0.4
     */
    public static String getDatabaseFullProductVersion() {
        return HsqlDatabaseProperties.THIS_FULL_VERSION;
    }

    /**
     * Retrieves the name of this database product. <p>
     *
     * @return database product name as a <code>String</code> object
     * @since 1.7.2
     */
    public static String getDatabaseProductName() {
        return HsqlDatabaseProperties.PRODUCT_NAME;
    }

    /**
     * Retrieves the version number of this database product. <p>
     *
     * @return database version number as a <code>String</code> object
     * @since 1.7.2
     */
    public static String getDatabaseProductVersion() {
        return HsqlDatabaseProperties.THIS_VERSION;
    }

    /**
     * Retrieves the major version number of this database. <p>
     *
     * @return the database's major version as an <code>int</code> value
     * @since 1.7.2
     */
    public static int getDatabaseMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     * Retrieves the major version number of this database. <p>
     *
     * @return the database's major version as an <code>int</code> value
     * @since 1.7.2
     */
    public static int getDatabaseMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }
}
