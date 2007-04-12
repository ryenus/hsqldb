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
 * Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UTFDataFormatException;

/**
 * Collection of static methods for converting strings between different
 * formats and to and from byte arrays.<p>
 *
 * New class, with extensively enhanced and rewritten Hypersonic code.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author fredt@users
 * @version 1.8.0
 * @since 1.7.2
 */

// fredt@users 20020328 - patch 1.7.0 by fredt - error trapping
public class StringConverter {

    private static final byte[] HEXBYTES = {
        (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4',
        (byte) '5', (byte) '6', (byte) '7', (byte) '8', (byte) '9',
        (byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
    };
    private static final String HEXINDEX = "0123456789abcdef0123456789ABCDEF";

    /**
     * Converts a String into a byte array by using a big-endian two byte
     * representation of each char value in the string.
     */
    byte[] stringToFullByteArray(String s) {

        int    length = s.length();
        byte[] buffer = new byte[length * 2];
        int    c;

        for (int i = 0; i < length; i++) {
            c                 = s.charAt(i);
            buffer[i * 2]     = (byte) ((c & 0x0000ff00) >> 8);
            buffer[i * 2 + 1] = (byte) (c & 0x000000ff);
        }

        return buffer;
    }

    /**
     * Compacts a hexadecimal string into a byte array
     *
     *
     * @param s hexadecimal string
     *
     * @return byte array for the hex string
     * @throws IOException
     */
    public static byte[] hexToByteArray(String s) throws IOException {

        int     l    = s.length();
        byte[]  data = new byte[l / 2 + (l % 2)];
        int     n,
                b    = 0;
        boolean high = true;
        int     i    = 0;

        for (int j = 0; j < l; j++) {
            char c = s.charAt(j);

            if (c == ' ') {
                continue;
            }

            n = HEXINDEX.indexOf(c);

            if (n == -1) {
                throw new IOException(
                    "hexadecimal string contains non hex character");    //NOI18N
            }

            if (high) {
                b    = (n & 0xf) << 4;
                high = false;
            } else {
                b         += (n & 0xf);
                high      = true;
                data[i++] = (byte) b;
            }
        }

        if (!high) {
            throw new IOException(
                "hexadecimal string with odd number of characters");    //NOI18N
        }

        if (i < data.length) {
            data = (byte[]) ArrayUtil.resizeArray(data, i);
        }

        return data;
    }

    /**
     * Converts a byte array into a hexadecimal string
     *
     *
     * @param b byte array
     *
     * @return hex string
     */
    public static String byteArrayToHex(byte[] b) {

        int    len = b.length;
        char[] s   = new char[len * 2];

        for (int i = 0, j = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s[j++] = (char) HEXBYTES[c >> 4 & 0xf];
            s[j++] = (char) HEXBYTES[c & 0xf];
        }

        return new String(s);
    }

    /**
     * Converts a byte array into an SQL hexadecimal string
     *
     *
     * @param b byte array
     *
     * @return hex string
     */
    public static String byteArrayToHexString(byte[] b) {

        int    len = b.length;
        char[] s   = new char[len * 2 + 2];

        s[0] = '\'';

        int j = 1;

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s[j++] = (char) HEXBYTES[c >> 4 & 0xf];
            s[j++] = (char) HEXBYTES[c & 0xf];
        }

        s[j] = '\'';

        return new String(s);
    }

    /**
     * Converts a byte array into an SQL hexadecimal string
     *
     *
     * @param b byte array
     *
     * @return hex string
     */
    public static String byteArrayToSQLHexString(byte[] b) {

        int    len = b.length;
        char[] s   = new char[len * 2 + 3];

        s[0] = 'X';
        s[1] = '\'';

        int j = 2;

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s[j++] = (char) HEXBYTES[c >> 4 & 0xf];
            s[j++] = (char) HEXBYTES[c & 0xf];
        }

        s[j] = '\'';

        return new String(s);
    }

    /**
     * Converts a byte array into hexadecimal characters
     * which are written as ASCII to the given output stream.
     *
     * @param o output stream
     * @param b byte array
     */
    public static void writeHex(byte[] o, int from, byte[] b) {

        int len = b.length;

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            o[from++] = HEXBYTES[c >> 4 & 0xf];
            o[from++] = HEXBYTES[c & 0xf];
        }
    }

    public static String byteToString(byte[] b, String charset) {

        try {
            return (charset == null) ? new String(b)
                                     : new String(b, charset);
        } catch (Exception e) {}

        return null;
    }

    /**
     * Converts a Unicode string into UTF8 then convert into a hex string
     *
     *
     * @param s normal Unicode string
     *
     * @return hex string representation of UTF8 encoding of the input
     */
    public static String unicodeToHexString(String s) {

        HsqlByteArrayOutputStream bout = new HsqlByteArrayOutputStream();

        writeUTF(s, bout);

        return byteArrayToHex(bout.toByteArray());
    }

// fredt@users 20011120 - patch 450455 by kibu@users - modified
// method return type changed to HsqlStringBuffer with spare
// space for end-of-line characters -- to reduce String concatenation

    /**
     * Hsqldb specific encoding used only for log files.
     *
     * The SQL statements that need to be written to the log file (input) are
     * Java Unicode strings. input is converted into a 7bit escaped ASCII
     * string (output)with the following transformations.
     * All characters outside the 0x20-7f range are converted to a
     * escape sequence and added to output.
     * If a backslash character is immdediately followed by 'u', the
     * backslash character is converted to escape sequence and
     * added to output.
     * All the remaining characters in input are added to output without
     * conversion.
     *
     * The escape sequence is backslash, letter u, xxxx, where xxxx
     * is the hex representation of the character code.
     * (fredt@users)
     *
     * @param b output stream to wite to
     * @param s Java Unicode string
     *
     * @return number of bytes written out
     *
     */
    public static int unicodeToAscii(HsqlByteArrayOutputStream b, String s,
                                     boolean doubleSingleQuotes) {

        int count = 0;

        if ((s == null) || (s.length() == 0)) {
            return 0;
        }

        int len = s.length();

        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);

            if (c == '\\') {
                if ((i < len - 1) && (s.charAt(i + 1) == 'u')) {
                    b.write(c);    // encode the \ as unicode, so 'u' is ignored
                    b.write('u');
                    b.write('0');
                    b.write('0');
                    b.write('5');
                    b.write('c');

                    count += 6;
                } else {
                    b.write(c);

                    count++;
                }
            } else if ((c >= 0x0020) && (c <= 0x007f)) {
                b.write(c);        // this is 99%

                count++;

                if (c == '\'' && doubleSingleQuotes) {
                    b.write(c);

                    count++;
                }
            } else {
                b.write('\\');
                b.write('u');
                b.write(HEXBYTES[(c >> 12) & 0xf]);
                b.write(HEXBYTES[(c >> 8) & 0xf]);
                b.write(HEXBYTES[(c >> 4) & 0xf]);
                b.write(HEXBYTES[c & 0xf]);

                count += 6;
            }
        }

        return count;
    }

// fredt@users 20020522 - fix for 557510 - backslash bug
// this legacy bug resulted from forward reading the input when a backslash
// was present and manifested itself when a backslash was followed
// immdediately by a character outside the 0x20-7f range in a database field.

    /**
     * Hsqldb specific decoding used only for log files.
     *
     * This method converts the 7 bit escaped ASCII strings in a log file
     * back into Java Unicode strings. See unicodeToAccii() above,
     *
     * @param s encoded ASCII string in byte array
     * @param offset position of first byte
     * @param length number of bytes to use
     *
     * @return Java Unicode string
     */
    public static String asciiToUnicode(byte[] s, int offset, int length) {

        if (length == 0) {
            return "";
        }

        char[] b = new char[length];
        int    j = 0;

        for (int i = 0; i < length; i++) {
            byte c = s[offset + i];

            if (c == '\\' && i < length - 5) {
                byte c1 = s[offset + i + 1];

                if (c1 == 'u') {
                    i++;

                    // 4 characters read should always return 0-15
                    int k = HEXINDEX.indexOf(s[offset + (++i)]) << 12;

                    k      += HEXINDEX.indexOf(s[offset + (++i)]) << 8;
                    k      += HEXINDEX.indexOf(s[offset + (++i)]) << 4;
                    k      += HEXINDEX.indexOf(s[offset + (++i)]);
                    b[j++] = (char) k;
                } else {
                    b[j++] = (char) c;
                }
            } else {
                b[j++] = (char) c;
            }
        }

        return new String(b, 0, j);
    }

    public static String asciiToUnicode(String s) {

        if ((s == null) || (s.indexOf("\\u") == -1)) {
            return s;
        }

        int    len = s.length();
        char[] b   = new char[len];
        int    j   = 0;

        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);

            if (c == '\\' && i < len - 5) {
                char c1 = s.charAt(i + 1);

                if (c1 == 'u') {
                    i++;

                    // 4 characters read should always return 0-15
                    int k = HEXINDEX.indexOf(s.charAt(++i)) << 12;

                    k      += HEXINDEX.indexOf(s.charAt(++i)) << 8;
                    k      += HEXINDEX.indexOf(s.charAt(++i)) << 4;
                    k      += HEXINDEX.indexOf(s.charAt(++i));
                    b[j++] = (char) k;
                } else {
                    b[j++] = c;
                }
            } else {
                b[j++] = c;
            }
        }

        return new String(b, 0, j);
    }

    public static String readUTF(byte[] bytearr, int offset,
                                 int length) throws IOException {

        char[] buf = new char[length];

        return readUTF(bytearr, offset, length, buf);
    }

    public static String readUTF(byte[] bytearr, int offset, int length,
                                 char[] buf) throws IOException {

        int bcount = 0;
        int c, char2, char3;
        int count = 0;

        while (count < length) {
            c = (int) bytearr[offset + count];

            if (bcount == buf.length) {
                buf = (char[]) ArrayUtil.resizeArray(buf, length);
            }

            if (c > 0) {

                /* 0xxxxxxx*/
                count++;

                buf[bcount++] = (char) c;

                continue;
            }

            c &= 0xff;

            switch (c >> 4) {

                case 12 :
                case 13 :

                    /* 110x xxxx   10xx xxxx*/
                    count += 2;

                    if (count > length) {
                        throw new UTFDataFormatException();
                    }

                    char2 = (int) bytearr[offset + count - 1];

                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException();
                    }

                    buf[bcount++] = (char) (((c & 0x1F) << 6)
                                            | (char2 & 0x3F));
                    break;

                case 14 :

                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;

                    if (count > length) {
                        throw new UTFDataFormatException();
                    }

                    char2 = (int) bytearr[offset + count - 2];
                    char3 = (int) bytearr[offset + count - 1];

                    if (((char2 & 0xC0) != 0x80)
                            || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException();
                    }

                    buf[bcount++] = (char) (((c & 0x0F) << 12)
                                            | ((char2 & 0x3F) << 6)
                                            | ((char3 & 0x3F) << 0));
                    break;

                default :

                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException();
            }
        }

        // The number of chars produced may be less than length
        return new String(buf, 0, bcount);
    }

    /**
     * Writes a string to the specified DataOutput using UTF-8 encoding in a
     * machine-independent manner.
     * <p>
     * @param      str   a string to be written.
     * @param      out   destination to write to
     * @return     The number of bytes written out.
     */
    public static int writeUTF(String str, HsqlByteArrayOutputStream out) {

        int strlen = str.length();
        int c,
            count  = 0;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);

            if (c >= 0x0001 && c <= 0x007F) {
                out.write(c);

                count++;
            } else if (c > 0x07FF) {
                out.write(0xE0 | ((c >> 12) & 0x0F));
                out.write(0x80 | ((c >> 6) & 0x3F));
                out.write(0x80 | ((c >> 0) & 0x3F));

                count += 3;
            } else {
                out.write(0xC0 | ((c >> 6) & 0x1F));
                out.write(0x80 | ((c >> 0) & 0x3F));

                count += 2;
            }
        }

        return count;
    }

    public static int getUTFSize(String s) {

        int len = (s == null) ? 0
                              : s.length();
        int l   = 0;

        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);

            if ((c >= 0x0001) && (c <= 0x007F)) {
                l++;
            } else if (c > 0x07FF) {
                l += 3;
            } else {
                l += 2;
            }
        }

        return l;
    }

    /**
     * Using a Reader and a Writer, returns a String from an InputStream.
     */
    public static String inputStreamToString(InputStream x,
            int length) throws IOException {

        InputStreamReader in        = new InputStreamReader(x);
        StringWriter      writer    = new StringWriter();
        int               blocksize = 8 * 1024;
        char[]            buffer    = new char[blocksize];

        for (int left = length; left > 0; ) {
            int read = in.read(buffer, 0, left > blocksize ? blocksize
                                                           : left);

            if (read == -1) {
                break;
            }

            writer.write(buffer, 0, read);

            left -= read;
        }

        writer.close();

        return writer.toString();
    }

// fredt@users 20020130 - patch 497872 by Nitin Chauhan - use byte[] of exact size

    /**
     * Returns the quoted version of the string using the quotechar argument.
     * doublequote argument indicates whether each instance of quotechar
     * inside the string is doubled.<p>
     *
     * null string argument returns null. If the caller needs the literal
     * "NULL" it should created it itself <p>
     *
     * The reverse conversion is handled in Tokenizer.java
     */
    public static String toQuotedString(String s, char quoteChar,
                                        boolean extraQuote) {

        if (s == null) {
            return null;
        }

        int    count = extraQuote ? count(s, quoteChar)
                                  : 0;
        int    len   = s.length();
        char[] b     = new char[2 + count + len];
        int    i     = 0;
        int    j     = 0;

        b[j++] = quoteChar;

        for (; i < len; i++) {
            char c = s.charAt(i);

            b[j++] = c;

            if (extraQuote && c == quoteChar) {
                b[j++] = c;
            }
        }

        b[j] = quoteChar;

        return new String(b);
    }

// TODO: CBB further review and testing...
//    private static final int MAX_TO_QUOTED_BUFFER_SIZE = 64 * 1024;
//    private static final char[] scb = new char[MAX_TO_QUOTED_BUFFER_SIZE];
//    private static StringBuffer ssb = new StringBuffer();
//
//    /**
//     * Returns the quoted version of the string using the quoteChar argument.
//     * extraQuote argument indicates whether each instance of quoteChar
//     * inside the string is doubled.<p>
//     *
//     * A null string argument returns null. If the caller needs the literal
//     * "NULL" it should handle the case directly. <p>
//     *
//     * The reverse conversion is handled in Tokenizer.java. <p>
//     *
//     * This version is up to 2x faster than the original for degenerate cases,
//     * i.e. when extraQuote is false or when there are zero occurences of
//     * quoteChar in s.  It is also up to 1.5x faster when the length
//     * of the resulting value is less than or equal to the compiled
//     * MAX_TO_QUOTED_BUFFER_SIZE. <p>
//     *
//     * For all-quoted TEXT tables, experiments involving hundreds of thousands
//     * of insertions, even under several hundred threads of execution, indicate
//     * an average 20% speedup for the degenerate cases when using the JDBC batch
//     * facility on a table with one identity column and three varchar
//     * columns with average data length between 80 and 100 characters per
//     * field. <p>
//     *
//     * For all-quoted TEXT tables, similar experiments over a range of other
//     * field data lengths and distribution of internal quote characters indicate
//     * performance at least identical to the original method, with best case
//     * approaching 10% speed up over the original method. <p>
//     */
//    public static String toQuotedString(final String s,
//                                        final char quoteChar,
//                                        final boolean extraQuote) {
//        if (s == null) {
//            return null;
//        }
//
//        if (!extraQuote) {
//            // Then we don't need to double up internal quote chars at all,
//            // so emperically, the following is the optimal performing
//            // technique discovered so far.
//            //
//            // Tests indicate that even under serveral hundred threads,
//            // this is up to 2X faster (and burns 33% less memory) than
//            // the original method.
//            synchronized(ssb) {
//                final int len = 2 + s.length();
//
//                ssb.ensureCapacity(len);
//                ssb.setLength(0);
//
//                String out = ssb.append(quoteChar)
//                                .append(s)
//                                .append(quoteChar)
//                                .toString();
//
//                // Put a limit on the long-term heap consumed by
//                // trying to optimize this method.
////#ifdef JAVA5
///*
//                if (len > MAX_TO_QUOTED_BUFFER_SIZE) {
//                    ssb.setLength(0);
//                    ssb.trimToSize();
//                }
//*/
//
////#else
//                if (len > MAX_TO_QUOTED_BUFFER_SIZE) {
//                    ssb = new StringBuffer();
//                }
//
////#endif JAVA5
//
//                return out;
//            }
//        }
//
//        final int count = count(s, quoteChar);
//
//        if (count == 0) {
//            // Then we don't need to double up internal quote chars at all,
//            // so emperically, the following is the optimal performing
//            // technique discovered so far.
//            //
//            // Tests indicate that even under serveral hundred threads,
//            // this is up to 2X faster (and burns 33% less memory) than
//            // the original method.
//            synchronized(ssb) {
//                final int len = 2 + s.length();
//
//                ssb.ensureCapacity(len);
//                ssb.setLength(0);
//
//                String out = ssb.append(quoteChar)
//                                .append(s)
//                                .append(quoteChar)
//                                .toString();
//
//                // Put a limit on the long-term heap consumed by
//                // trying to optimize this method.
////#ifdef JAVA5
///*
//                if (len > MAX_TO_QUOTED_BUFFER_SIZE) {
//                    ssb.setLength(0);
//                    ssb.trimToSize();
//                }
//*/
//
////#else
//                if (len > MAX_TO_QUOTED_BUFFER_SIZE) {
//                    ssb = new StringBuffer();
//                }
//
////#endif JAVA5
//
//                return out;
//            }
//        } else {
//            // we need to double up some internal quote chars
//
//            final int slen = s.length();
//            final int len  = 2 + count + slen;
//
//            if (len <= scb.length) {
//                // then we can (re)use our static buffer to avoid
//                // excess heap allocation, which can speed
//                // things up noticably
//
//                synchronized(scb) {
//                    final char[] b = scb;
//
//                    b[0]     = quoteChar;
//                    b[len-1] = quoteChar;
//
//                    for (int i = 0, j = 1 ; i < slen ; i++) {
//                        final char c = s.charAt(i);
//
//                        b[j++] = c;
//
//                        if (c == '\'') {
//                            b[j++] = c;
//                        }
//                    }
//
//                    return new String(b, 0, len);
//                }
//            } else {
//                // the output string is larger than our static buffer
//                final char[] b = new char[len];
//
//                b[0]     = quoteChar;
//                b[len-1] = quoteChar;
//
//                for (int i = 0, j = 1 ; i < slen ; i++) {
//                    final char c = s.charAt(i);
//
//                    b[j++] = c;
//
//                    if (c == '\'') {
//                        b[j++] = c;
//                    }
//                }
//
//                return new String(b, 0, len);
//            }
//        }
//    }

    /**
     * Counts Character c in String s
     *
     * @param String s
     *
     * @return int count
     */
    static int count(final String s, final char c) {

        int pos   = 0;
        int count = 0;

        if (s != null) {
            while ((pos = s.indexOf(c, pos)) > -1) {
                count++;
                pos++;
            }
        }

        return count;
    }
}
