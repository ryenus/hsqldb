package org.hsqldb.lib;

import java.lang.reflect.Array;

/** Provides a collection of convenience methods for processing and
 * creating objects with <code>String</code> value components.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class StringUtil {

    /**
     * If necessary, adds zeros to the beginning of a value so that the total
     * length matches the given precision, otherwise trims the right digits.
     * Then if maxSize is smaller than precision, trims the right digits to
     * maxSize. Negative values are treated as positive
     */
    public static String toZeroPaddedString(long value, int precision,
            int maxSize) {

        StringBuffer buffer = new StringBuffer();

        if (value < 0) {
            value = -value;
        }

        String s = Long.toString(value);

        if (s.length() > precision) {
            s = s.substring(precision);
        }

        for (int i = s.length(); i < precision; i++) {
            buffer.append('0');
        }

        buffer.append(s);

        if (maxSize < precision) {
            buffer.setLength(maxSize);
        }

        return buffer.toString();
    }

    public static String toPaddedString(String source, int length, char pad,
                                        boolean trailing) {

        int len = source.length();

        if (len >= length) {
            return source;
        }

        StringBuffer buffer = new StringBuffer(length);

        if (trailing) {
            buffer.append(source);
        }

        for (int i = len; i < length; i++) {
            buffer.append(pad);
        }

        if (!trailing) {
            buffer.append(source);
        }

        return buffer.toString();
    }

    /**
     * Returns a string with non alphanumeric chars converted to the
     * substitute character. A digit first character is also converted.
     * By sqlbob@users
     * @param source string to convert
     * @param substitute character to use
     * @return converted string
     */
    public static String toLowerSubset(String source, char substitute) {

        int          len = source.length();
        StringBuffer src = new StringBuffer(len);
        char         ch;

        for (int i = 0; i < len; i++) {
            ch = source.charAt(i);

            if (!Character.isLetterOrDigit(ch)) {
                src.append(substitute);
            } else if ((i == 0) && Character.isDigit(ch)) {
                src.append(substitute);
            } else {
                src.append(Character.toLowerCase(ch));
            }
        }

        return src.toString();
    }

    /**
     * Builds a bracketed CSV list from the array
     * @param array an array of Objects
     * @return string
     */
    public static String arrayToString(Object array) {

        int          len  = Array.getLength(array);
        int          last = len - 1;
        StringBuffer sb   = new StringBuffer(2 * (len + 1));

        sb.append('{');

        for (int i = 0; i < len; i++) {
            sb.append(Array.get(array, i));

            if (i != last) {
                sb.append(',');
            }
        }

        sb.append('}');

        return sb.toString();
    }

    /**
     * Builds a CSV list from the specified String[], separator string and
     * quote string. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <li> No attempt is made to escape the quote character sequence if it is
     *      found internal to a list element.
     * <ul>
     * @return a CSV list
     * @param separator the <code>String</code> to use as the list element separator
     * @param quote the <code>String</code> with which to quote the list elements
     * @param s array of <code>String</code> objects
     */
    public static String getList(String[] s, String separator, String quote) {

        int          len = s.length;
        StringBuffer b   = new StringBuffer(len * 16);

        for (int i = 0; i < len; i++) {
            b.append(quote);
            b.append(s[i]);
            b.append(quote);

            if (i + 1 < len) {
                b.append(separator);
            }
        }

        return b.toString();
    }

    /**
     * Builds a CSV list from the specified int[], <code>separator</code>
     * <code>String</code> and <code>quote</code> <code>String</code>. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <ul>
     * @return a CSV list
     * @param s the array of int values
     * @param separator the <code>String</code> to use as the separator
     * @param quote the <code>String</code> with which to quote the list elements
     */
    public static String getList(int[] s, String separator, String quote) {

        int          len = s.length;
        StringBuffer b   = new StringBuffer(len * 8);

        for (int i = 0; i < len; i++) {
            b.append(quote);
            b.append(s[i]);
            b.append(quote);

            if (i + 1 < len) {
                b.append(separator);
            }
        }

        return b.toString();
    }

    /**
     * Builds a CSV list from the specified String[][], separator string and
     * quote string. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Uses only the first element in each subarray.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <li> No attempt is made to escape the quote character sequence if it is
     *      found internal to a list element.
     * <ul>
     * @return a CSV list
     * @param separator the <code>String</code> to use as the list element separator
     * @param quote the <code>String</code> with which to quote the list elements
     * @param s the array of <code>String</code> array objects
     */
    public static String getList(String[][] s, String separator,
                                 String quote) {

        int          len = s.length;
        StringBuffer b   = new StringBuffer(len * 16);

        for (int i = 0; i < len; i++) {
            b.append(quote);
            b.append(s[i][0]);
            b.append(quote);

            if (i + 1 < len) {
                b.append(separator);
            }
        }

        return b.toString();
    }

    /**
     * Appends a pair of string to the string buffer, using the separator between
     * and terminator at the end
     * @param b the buffer
     * @param s1 first string
     * @param s2 second string
     * @param separator separator string
     * @param terminator terminator string
     */
    public static void appendPair(StringBuffer b, String s1, String s2,
                                  String separator, String terminator) {

        b.append(s1);
        b.append(separator);
        b.append(s2);
        b.append(terminator);
    }

    /**
     * Checks if text is empty (characters <= space)
     * @author: Nitin Chauhan
     * @return boolean true if text is null or empty, false otherwise
     * @param s java.lang.String
     */
    public static boolean isEmpty(String s) {

        int i = s == null ? 0
                          : s.length();

        while (i > 0) {
            if (s.charAt(--i) > ' ') {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the size of substring that does not contain any trailing spaces
     * @param s the string
     * @return trimmed size
     */
    public static int rTrimSize(String s) {

        int i = s.length();

        while (i > 0) {
            i--;

            if (s.charAt(i) != ' ') {
                return i + 1;
            }
        }

        return 0;
    }

    /**
     * Skips any spaces at or after start and returns the index of first
     * non-space character;
     * @param s the string
     * @param start index to start
     * @return index of first non-space
     */
    public static int skipSpaces(String s, int start) {

        int limit = s.length();
        int i     = start;

        for (; i < limit; i++) {
            if (s.charAt(i) != ' ') {
                break;
            }
        }

        return i;
    }

    /**
     * Splits the string into an array, using the separator. If separator is
     * not found in the string, the whole string is returned in the array.
     *
     * @param s the string
     * @param separator the separator
     * @return array of strings
     */
    public static String[] split(String s, String separator) {

        HsqlArrayList list      = new HsqlArrayList();
        int           currindex = 0;

        for (boolean more = true; more; ) {
            int nextindex = s.indexOf(separator, currindex);

            if (nextindex == -1) {
                nextindex = s.length();
                more      = false;
            }

            list.add(s.substring(currindex, nextindex));

            currindex = nextindex + separator.length();
        }

        return (String[]) list.toArray(new String[list.size()]);
    }
}
