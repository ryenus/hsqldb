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


package org.hsqldb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.Reader;
import java.io.StringReader;

import java.sql.Clob;
import java.sql.SQLException;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.java.JavaSystem;

// campbell-burnet@users 2004-03/04-xx - doc 1.7.2 - javadocs updated; methods put in
//                                            correct (historical, interface
//                                            declared) order
// campbell-burnet@users 2004-03/04-xx - patch 1.7.2 - null check for constructor (a
//                                              null CLOB value is Java null,
//                                              not a Clob object with null
//                                              data);moderate thread safety;
//                                              simplification; optimization
//                                              of operations between jdbcClob
//                                              instances
// campbell-burnet@users 2005-12-07    - patch 1.8.0.x - initial JDBC 4.0 support work
// campbell-burnet@users 2006-05-22    - doc   1.9.0 - full synch up to JAVA 1.6 (Mustang) Build 84
//                              - patch 1.9.0 - setAsciiStream &
//                                              setCharacterStream improvement
// patch 1.9.0
// - full synch up to JAVA 1.6 (Mustang) b90
// - better bounds checking

/**
 * The mapping in the Java programming language
 * for the SQL {@code CLOB} type.
 * An SQL {@code CLOB} is a built-in type
 * that stores a Character Large Object as a column value in a row of
 * a database table.
 * By default drivers implement a {@code Clob} object using an SQL
 * {@code locator(CLOB)}, which means that a {@code Clob} object
 * contains a logical pointer to the SQL {@code CLOB} data rather than
 * the data itself. A {@code Clob} object is valid for the duration
 * of the transaction in which it was created.
 * <P>The {@code Clob} interface provides methods for getting the
 * length of an SQL {@code CLOB} (Character Large Object) value,
 * for materializing a {@code CLOB} value on the client, and for
 * searching for a substring or {@code CLOB} object within a
 * {@code CLOB} value.
 * Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement}, such as
 * {@code getClob} and {@code setClob} allow a programmer to
 * access an SQL {@code CLOB} value.  In addition, this interface
 * has methods for updating a {@code CLOB} value.
 * <p>
 * All methods on the {@code Clob} interface must be
 * fully implemented if the JDBC driver supports the data type.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 * Previous to 2.0, the HSQLDB driver did not implement Clob using an SQL
 * locator(CLOB).  That is, an HSQLDB Clob object did not contain a logical
 * pointer to SQL CLOB data; rather it directly contained a representation of
 * the data (a String). As a result, an HSQLDB Clob object was itself
 * valid beyond the duration of the transaction in which is was created,
 * although it did not necessarily represent a corresponding value
 * on the database. Also, the interface methods for updating a CLOB value
 * were unsupported, with the exception of the truncate method,
 * in that it could be used to truncate the local value. <p>
 *
 * Starting with 2.0, the HSQLDB driver fully supports both local and remote
 * SQL CLOB data implementations, meaning that an HSQLDB Clob object <em>may</em>
 * contain a logical pointer to remote SQL CLOB data (see {@link JDBCClobClient
 * JDBCClobClient}) or it may directly contain a local representation of the
 * data (as implemented in this class).  In particular, when the product is built
 * under JDK 1.6+ and the Clob instance is constructed as a result of calling
 * JDBCConnection.createClob(), then the resulting Clob instance is initially
 * disconnected (is not bound to the transaction scope of the vending Connection
 * object), the data is contained directly and all interface methods for
 * updating the CLOB value are supported for local use until the first
 * invocation of free(); otherwise, an HSQLDB Clob's implementation is
 * determined at runtime by the driver, it is typically not valid beyond
 * the duration of the transaction in which is was created, and there no
 * standard way to query whether it represents a local or remote value.
 *
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.3
 * @since JDK 1.2, HSQLDB 1.7.2
 */
public class JDBCClob implements Clob {

    /**
     * Retrieves the number of characters
     * in the {@code CLOB} value
     * designated by this {@code Clob} object.
     *
     * @return length of the {@code CLOB} in characters
     * @throws SQLException if there is an error accessing the
     *            length of the {@code CLOB} value
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     *         does not support this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long length() throws SQLException {
        return getData().length();
    }

    /**
     * Retrieves a copy of the specified substring
     * in the {@code CLOB} value
     * designated by this {@code Clob} object.
     * The substring begins at position
     * {@code pos} and has up to {@code length} consecutive
     * characters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * The official specification above is ambiguous in that it does not
     * precisely indicate the policy to be observed when
     * {@code pos > this.length() - length}.  One policy would be to retrieve
     * the characters from {@code pos} to {@code this.length()}.  Another would
     * be to throw an exception. This class observes the second policy. <p>
     *
     * <b>Note</b><p>
     *
     * This method uses {@link java.lang.String#substring(int, int)}.
     * <p>
     * Depending on implementation (typically JDK 6 and earlier releases), the
     * returned value may be sharing the underlying (and possibly much larger)
     * character buffer. Depending on factors such as hardware acceleration for
     * array copies, the average length and number of sub-strings taken, and so
     * on, this <em>may or may not</em> result in faster operation and
     * non-trivial memory savings. On the other hand, Oracle / OpenJDK 7, it
     * was decided that the memory leak implications outweigh the benefits
     * of buffer sharing for most use cases on modern hardware.
     * <p>
     * It is left up to any client of this method to determine if this is a
     * potential factor relative to the target runtime and to decide how to
     * handle space-time trade-offs (i.e. whether to make an isolated copy of
     * the returned substring or risk that more memory remains allocated than
     * is absolutely required).
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the first character of the substring to be extracted.
     *            The first character is at position 1.
     * @param length the number of consecutive characters to be copied;
     *         the value for length must be 0 or greater
     * @return a {@code String} that is the specified substring in
     *         the {@code CLOB} value designated by this {@code Clob} object
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value; if pos is less than 1 or length is
     *            less than 0
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     *          does not support this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public String getSubString(
            final long pos,
            final int length)
            throws SQLException {

        final String data = getData();
        final int    dlen = data.length();

        if (pos == MIN_POS && length == dlen) {
            return data;
        }

        if (pos < MIN_POS || pos > dlen) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        final long index = pos - 1;

        if (length < 0 || length > dlen - index) {
            throw JDBCUtil.outOfRangeArgument("length: " + length);
        }

        return data.substring((int) index, (int) index + length);
    }

    /**
     * Retrieves the {@code CLOB} value designated by this {@code Clob}
     * object as a {@code java.io.Reader} object (or as a stream of
     * characters).
     *
     * @return a {@code java.io.Reader} object containing the
     *         {@code CLOB} data
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     * does not support this method
     * @see #setCharacterStream
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public java.io.Reader getCharacterStream() throws SQLException {
        return new StringReader(getData());
    }

    /**
     * Retrieves the {@code CLOB} value designated by this {@code Clob}
   * object as an ascii stream.
     *
     * @return a {@code java.io.InputStream} object containing the
     *         {@code CLOB} data
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setAsciiStream
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public java.io.InputStream getAsciiStream() throws SQLException {

        try {
            return new ByteArrayInputStream(
                getData().getBytes(JavaSystem.CS_US_ASCII));
        } catch (Throwable e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the character position at which the specified substring
     * {@code searchstr} appears in the SQL {@code CLOB} value
     * represented by this {@code Clob} object.  The search
     * begins at position {@code start}.
     *
     * @param searchstr the substring for which to search
   * @param start the position at which to begin searching;
   *        the first position is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *         {@code CLOB} value or if start is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     * does not support this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(
            final String searchstr,
            long start)
            throws SQLException {

        final String data = getData();

        if (start < MIN_POS) {
            throw JDBCUtil.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null || start > MAX_POS) {
            return -1;
        }

        final int position = data.indexOf(searchstr, (int) start - 1);

        return (position == -1)
               ? -1
               : position + 1;
    }

    /**
     * Retrieves the character position at which the specified
     * {@code Clob} object {@code searchstr} appears in this
     * {@code Clob} object.  The search begins at position
     * {@code start}.
     *
     * @param searchstr the {@code Clob} object for which to search
     * @param start the position at which to begin searching; the first
     *              position is 1
     * @return the position at which the {@code Clob} object appears
     *              or -1 if it is not present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if start is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     *         does not support this method
     * @since JDK 1.2, HSQLDB 1.7.2
     */
    public long position(
            final Clob searchstr,
            final long start)
            throws SQLException {

        final String data = getData();

        if (start < MIN_POS) {
            throw JDBCUtil.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null) {
            return -1;
        }

        final long dlen       = data.length();
        final long sslen      = searchstr.length();
        final long startIndex = start - 1;

        // This is potentially much less expensive than materializing a large
        // substring from some other vendor's CLOB.  Indeed, we should probably
        // do the comparison piecewise, using an in-memory buffer (or temp-files
        // when available), if it is detected that the input CLOB is very long.
        if (startIndex > dlen - sslen) {
            return -1;
        }

        // by now, we know sslen and startIndex are both < Integer.MAX_VALUE
        String pattern;

        if (searchstr instanceof JDBCClob) {
            pattern = ((JDBCClob) searchstr).getData();
        } else {
            pattern = searchstr.getSubString(1L, (int) sslen);
        }

        final int index = data.indexOf(pattern, (int) startIndex);

        return (index == -1)
               ? -1
               : index + 1;
    }

    //---------------------------- jdbc 3.0 -----------------------------------

    /**
     * Writes the given Java {@code String} to the {@code CLOB}
     * value that this {@code Clob} object designates at the position
     * {@code pos}. The string will overwrite the existing characters
     * in the {@code Clob} object starting at the position
     * {@code pos}.  If the end of the {@code Clob} value is reached
     * while writing the given string, then the length of the {@code Clob}
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for {@code pos}
     * is greater than the length+1 of the {@code CLOB} value then the
     * behavior is undefined. Some JDBC drivers may throw an
     * {@code SQLException} while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 2.0 this feature is supported. <p>
     *
     * When built under JDK 1.6+ and the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in the
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propagate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * No attempt is made to ensure precise thread safety. Instead, volatile
     * member field and local variable snapshot isolation semantics are
     * implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, if an application may perform concurrent JDBCClob
     * modifications and the integrity of the application depends on total order
     * Clob modification semantics, then such operations should be synchronized
     * on an appropriate monitor.<p>
     *
     * When the value specified for {@code pos} is greater then the
     * length+1, then the CLOB value is extended in length to accept the
     * written characters and the undefined region up to @{code pos} is filled
     * with with space (' ') characters.
     *
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to the {@code CLOB}
     *         value that this {@code Clob} object represents;
     *         the first position is 1.
     * @param str the string to be written to the {@code CLOB}
     *        value that this {@code Clob} designates
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if pos is less than 1
     *
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     * does not support this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int setString(long pos, String str) throws SQLException {

        return setString(
            pos,
            str,
            0,
            str == null
            ? 0
            : str.length());
    }

    /**
     * Writes {@code len} characters of {@code str}, starting
     * at character {@code offset}, to the {@code CLOB} value
     * that this {@code Clob} represents.
     * The string will overwrite the existing characters
     * in the {@code Clob} object starting at the position
     * {@code pos}.  If the end of the {@code Clob} value is reached
     * while writing the given string, then the length of the {@code Clob}
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for {@code pos}
     * is greater than the length+1 of the {@code CLOB} value then the
     * behavior is undefined. Some JDBC drivers may throw an
     * {@code SQLException} while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 2.0 this feature is supported. <p>
     *
     * When the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propagate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * If the value specified for {@code pos}
     * is greater than the length of the {@code CLOB} value, then
     * the {@code CLOB} value is extended in length to accept the
     * written characters and the undefined region up to {@code pos} is
     * filled with space (' ') characters.<p>
     *
     * No attempt is made to ensure precise thread safety. Instead, volatile
     * member field and local variable snapshot isolation semantics are
     * implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, if an application may perform concurrent JDBCClob
     * modifications and the integrity of the application depends on total order
     * Clob modification semantics, then such operations should be synchronized
     * on an appropriate monitor.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to this
     *        {@code CLOB} object; The first position  is 1
     * @param str the string to be written to the {@code CLOB}
     *        value that this {@code Clob} object represents
     * @param offset the offset into {@code str} to start reading
     *        the characters to be written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if pos is less than 1
     *
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     *         does not support this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public int setString(
            final long pos,
            final String str,
            final int offset,
            final int len)
            throws SQLException {

        checkReadonly();

        final String data = getData();

        if (str == null) {
            throw JDBCUtil.nullArgument("str");
        }

        final int strlen = str.length();
        final int dlen   = data.length();
        final int ipos   = (int) (pos - 1);

        if (offset == 0 && len == strlen && ipos == 0 && len >= dlen) {
            setData(str);

            return len;
        }

        if (offset < 0 || offset > strlen) {
            throw JDBCUtil.outOfRangeArgument("offset: " + offset);
        }

        if (len < 0 || len > strlen - offset) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }

        if (pos < MIN_POS || (pos - MIN_POS) > (Integer.MAX_VALUE - len)) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        final long endPos = (pos + len);
        char[]     chars;

        if (pos > dlen) {

            // 1.)  'datachars' + '\32\32\32...' + substring
            chars = new char[(int) endPos - 1];

            data.getChars(0, dlen, chars, 0);

            for (int i = dlen; i < ipos; i++) {
                chars[i] = ' ';
            }

            str.getChars(offset, offset + len, chars, ipos);
        } else if (endPos > dlen) {

            // 2.)  'datach...' + substring
            chars = new char[(int) endPos - 1];

            data.getChars(0, ipos, chars, 0);
            str.getChars(offset, offset + len, chars, ipos);
        } else {

            // 3.)  'dat' + substring + 'rs'
            chars = new char[dlen];

            data.getChars(0, ipos, chars, 0);
            str.getChars(offset, offset + len, chars, ipos);

            final int dataOffset = ipos + len;

            data.getChars(dataOffset, dlen, chars, dataOffset);
        }

        setData(new String(chars));

        return len;
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * {@code CLOB} value that this {@code Clob} object represents,
     * starting at position {@code pos}.  Characters written to the stream
     * will overwrite the existing characters
     * in the {@code Clob} object starting at the position
     * {@code pos}.  If the end of the {@code Clob} value is reached
     * while writing characters to the stream, then the length of the {@code Clob}
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for {@code pos}
     * is greater than the length+1 of the {@code CLOB} value then the
     * behavior is undefined. Some JDBC drivers may throw an
     * {@code SQLException} while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 2.0 this feature is supported. <p>
     *
     * When the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propagate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updatable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * The data written to the stream does not appear in this
     * Clob until the stream is closed. <p>
     *
     * When the stream is closed, if the value specified for {@code pos}
     * is greater than the length of the {@code CLOB} value, then
     * the {@code CLOB} value is extended in length to accept the
     * written characters and the undefined region up to {@code pos} is
     * filled with space (' ') characters. <p>
     *
     * Also, no attempt is made to ensure precise thread safety. Instead,
     * volatile member field and local variable snapshot isolation semantics
     * are implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, if an application may perform concurrent JDBCClob
     * modifications and the integrity of the application depends on total order
     * Clob modification semantics, then such operations should be synchronized
     * on an appropriate monitor.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param pos the position at which to start writing to this
     *        {@code CLOB} object; The first position is 1
     * @return the stream to which ASCII encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if pos is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getAsciiStream
     *
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public java.io.OutputStream setAsciiStream(
            final long pos)
            throws SQLException {

        checkReadonly();
        checkClosed();

        if (pos < MIN_POS || pos > MAX_POS) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.ByteArrayOutputStream() {

            boolean closed = false;
            public synchronized void close() throws java.io.IOException {

                if (closed) {
                    return;
                }

                closed = true;

                final byte[] bytes  = super.buf;
                final int    length = super.count;

                super.buf   = null;
                super.count = 0;

                try {
                    final String str = new String(
                        bytes,
                        0,
                        length,
                        JavaSystem.CS_US_ASCII);

                    JDBCClob.this.setString(pos, str);
                } catch (Throwable e) {
                    throw JavaSystem.toIOException(e);
                }
            }
        };
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters
     * to the {@code CLOB} value that this {@code Clob} object
     * represents, at position {@code pos}. Characters written to the stream
     * will overwrite the existing characters
     * in the {@code Clob} object starting at the position
     * {@code pos}.  If the end of the {@code Clob} value is reached
     * while writing characters to the stream, then the length of the {@code Clob}
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for {@code pos}
     * is greater than the length+1 of the {@code CLOB} value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * {@code SQLException} while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 2.0 this feature is supported. <p>
     *
     * When the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Clob instances. To propagate the Clob value to a database
     * in this case, it is required to supply the Clob instance to an updating
     * or inserting setXXX method of a Prepared or Callable Statement, or to
     * supply the Clob instance to an updateXXX method of an updateable
     * ResultSet. <p>
     *
     * <b>Implementation Notes:</b><p>
     *
     * The data written to the stream does not appear in this
     * Clob until the stream is closed. <p>
     *
     * When the stream is closed, if the value specified for {@code pos}
     * is greater than the length of the {@code CLOB} value, then
     * the {@code CLOB} value is extended in length to accept the
     * written characters and the undefined region up to {@code pos} is
     * filled with space (' ') characters. <p>
     *
     * Also, no attempt is made to ensure precise thread safety. Instead,
     * volatile member field and local variable snapshot isolation semantics
     * are implemented.  This is expected to eliminate most issues related
     * to race conditions, with the possible exception of concurrent
     * invocation of free(). <p>
     *
     * In general, if an application may perform concurrent JDBCClob
     * modifications and the integrity of the application depends on
     * total order Clob modification semantics, then such operations
     * should be synchronized on an appropriate monitor.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param  pos the position at which to start writing to the
     *        {@code CLOB} value; The first position is 1
     *
     * @return a stream to which Unicode encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if {@code pos} is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     * does not support this method
     * @see #getCharacterStream
     *
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public java.io.Writer setCharacterStream(
            final long pos)
            throws SQLException {

        checkReadonly();
        checkClosed();

        if (pos < MIN_POS || pos > MAX_POS) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.StringWriter() {

            private boolean closed = false;
            public synchronized void close() throws java.io.IOException {

                if (closed) {
                    return;
                }

                closed = true;

                final StringBuffer sb = super.getBuffer();

                try {
                    JDBCClob.this.setStringBuffer(pos, sb, 0, sb.length());
                } catch (SQLException se) {
                    throw JavaSystem.toIOException(se);
                } finally {
                    sb.setLength(0);
                    sb.trimToSize();
                }
            }
        };
    }

    /**
     * Truncates the {@code CLOB} value that this {@code Clob}
     * designates to have a length of {@code len}
     * characters.
     * <p>
     * <b>Note:</b> If the value specified for {@code pos}
     * is greater than the length+1 of the {@code CLOB} value then the
     * behavior is undefined. Some JDBC drivers may throw an
     * {@code SQLException} while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 2.0 this feature is fully supported. <p>
     *
     * When the Clob instance is constructed as a
     * result of calling JDBCConnection.createClob(), this operation affects
     * only the client-side value; it has no effect upon a value stored in a
     * database because JDBCConnection.createClob() constructs disconnected,
     * initially empty Blob instances. To propagate the truncated Clob value to
     * a database in this case, it is required to supply the Clob instance to
     * an updating or inserting setXXX method of a Prepared or Callable
     * Statement, or to supply the Blob instance to an updateXXX method of an
     * updateable ResultSet. <p>
     *
     * <b>Implementation Notes:</b> <p>
     *
     * HSQLDB throws an SQLException if the specified {@code len} is greater
     * than the value returned by {@link #length() length}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param len the length, in characters, to which the {@code CLOB} value
     *        should be truncated
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if len is less than 0
     *
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public void truncate(final long len) throws SQLException {

        checkReadonly();

        final String data = getData();
        final long   dlen = data.length();

        if (len == dlen) {
            return;
        }

        if (len < 0 || len > dlen) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }

        setData(data.substring(0, (int) len));
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * This method releases the resources that the {@code Clob} object
     * holds.  The object is invalid once the {@code free} method
     * is called.
     * <p>
     * After {@code free} has been called, any attempt to invoke a
     * method other than {@code free} will result in a {@code SQLException}
     * being thrown.  If {@code free} is called multiple times, the subsequent
     * calls to {@code free} are treated as a no-op.
     *
     * @throws SQLException if an error occurs releasing
     * the Clob's resources
     *
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     * does not support this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void free() throws SQLException {
        m_closed = true;
        m_data   = null;
    }

    /**
     * Returns a {@code Reader} object that contains
     * a partial {@code Clob} value, starting with the character
     * specified by pos, which is length characters in length.
     *
     * @param pos the offset to the first character of the partial value to
     * be retrieved.  The first character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be retrieved.
     * @return {@code Reader} through which
     *         the partial {@code Clob} value can be read.
     * @throws SQLException if pos is less than 1;
     *         or if pos is greater than the number of characters
     *         in the {@code Clob};
     *         or if pos + length is greater than the number of
     * characters in the {@code Clob}
     *
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver
     *         does not support this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Reader getCharacterStream(
            long pos,
            long length)
            throws SQLException {

        if (length > Integer.MAX_VALUE) {
            throw JDBCUtil.outOfRangeArgument("length: " + length);
        }

        final String data = getData();
        final int    dlen = data.length();

        if (pos == MIN_POS && length == dlen) {
            return new StringReader(data);
        }

        if (pos < MIN_POS || pos > dlen) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        final long startIndex = pos - 1;

        if (length < 0 || length > dlen - startIndex) {
            throw JDBCUtil.outOfRangeArgument("length: " + length);
        }

        final int    endIndex = (int) (startIndex + length);    // exclusive
        final char[] chars    = new char[(int) length];

        data.getChars((int) startIndex, endIndex, chars, 0);

        return new CharArrayReader(chars);
    }

    // ---------------------- internal implementation --------------------------
    private static final long MIN_POS = 1L;
    private static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
    private boolean           m_closed;
    private String            m_data;
    private final boolean     m_createdByConnection;

    /**
     * Constructs a new, read-only JDBCClob object wrapping the given character
     * sequence. <p>
     *
     * This constructor is used internally to retrieve result set values as
     * Clob objects, yet it must be public to allow access from other packages.
     * As such (in the interest of efficiency) this object maintains a reference
     * to the given String object rather than making a copy and so it is
     * gently suggested (in the interest of effective memory management) that
     * external clients using this constructor either take pause to consider
     * the implications or at least take care to provide a String object whose
     * internal character buffer is not much larger than required to represent
     * the value.
     *
     * @param data the character sequence representing the Clob value
     * @throws SQLException if the argument is null
     */
    public JDBCClob(final String data) throws SQLException {

        if (data == null) {
            throw JDBCUtil.nullArgument();
        }

        m_data                = data;
        m_createdByConnection = false;
    }

    /**
     * Constructs a new, empty (zero-length), read/write JDBCClob object.
     */
    protected JDBCClob() {
        m_data                = "";
        m_createdByConnection = true;
    }

    protected void checkReadonly() throws SQLException {
        if (!m_createdByConnection) {
            throw JDBCUtil.sqlException(ErrorCode.X_25006, "Clob is read-only");
        }
    }

    protected synchronized void checkClosed() throws SQLException {
        if (m_closed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }
    }

    synchronized String getData() throws SQLException {
        checkClosed();

        return m_data;
    }

    private synchronized void setData(String data) throws SQLException {
        checkClosed();

        m_data = data;
    }

    /**
     * Behavior is identical to {@link #setString(long, java.lang.String, int, int)}.
     *
     * @param pos the position at which to start writing to this
     *        {@code CLOB} object; The first position  is 1
     * @param sb the buffer to be written to the {@code CLOB}
     *        value that this {@code Clob} object represents
     * @param offset the offset into {@code sb} to start reading
     *        the characters to be written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *            {@code CLOB} value or if pos is less than 1
     */
    public int setStringBuffer(
            final long pos,
            final StringBuffer sb,
            final int offset,
            final int len)
            throws SQLException {

        checkReadonly();

        String data = getData();

        if (sb == null) {
            throw JDBCUtil.nullArgument("sb");
        }

        final int strlen = sb.length();
        final int dlen   = data.length();
        final int ipos   = (int) (pos - 1);

        if (offset == 0 && len == strlen && ipos == 0 && len >= dlen) {
            setData(sb.toString());

            return len;
        }

        if (offset < 0 || offset > strlen) {
            throw JDBCUtil.outOfRangeArgument("offset: " + offset);
        }

        if (len > strlen - offset) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }

        if (pos < MIN_POS || (pos - MIN_POS) > (Integer.MAX_VALUE - len)) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        final long endPos = (pos + len);
        char[]     chars;

        if (pos > dlen) {

            // 1.)  'datachars' + '\32\32\32...' + substring
            chars = new char[(int) endPos - 1];

            data.getChars(0, dlen, chars, 0);

            for (int i = dlen; i < ipos; i++) {
                chars[i] = ' ';
            }

            sb.getChars(offset, offset + len, chars, ipos);
        } else if (endPos > dlen) {

            // 2.)  'datach...' + substring
            chars = new char[(int) endPos - 1];

            data.getChars(0, ipos, chars, 0);
            sb.getChars(offset, offset + len, chars, ipos);
        } else {

            // 3.)  'dat' + substring + 'rs'
            chars = new char[dlen];

            data.getChars(0, ipos, chars, 0);
            sb.getChars(offset, offset + len, chars, ipos);

            final int dataOffset = ipos + len;

            data.getChars(dataOffset, dlen, chars, dataOffset);
        }

        setData(new String(chars));

        return len;
    }
}
