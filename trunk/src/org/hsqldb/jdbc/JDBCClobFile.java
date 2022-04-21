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


package org.hsqldb.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCBlobFile.OutputStreamAdapter;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.InOutUtil;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.List;

/**
 * A client-side file-based implementation of Clob.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h1>HSQLDB-Specific Information:</h1> <p>
 *
 * Starting with 2.1, in addition to HSQLDB driver support for both client-side
 * in-memory and remote SQL CLOB data implementations, this class is provided
 * to expose efficient, relatively high-performance CLOB operations over client
 * accessible files.<p>
 *
 * <b>Design Notes</b><p>
 *
 * Although it is possible to implement a transactional version of this class,
 * the present implementation directly propagates changes to the underlying
 * file such that changes become visible as soon as they are either
 * implicitly or explicitly flushed to disk.
 *
 * </div>
 * <!-- end release-specific documentation -->
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.5.0
 * @since HSQLDB 2.1
 */
public class JDBCClobFile implements java.sql.Clob {

    private static final FrameworkLogger LOG =
        FrameworkLogger.getLog(JDBCClobFile.class);

    /**
     * Retrieves the number of characters
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     *
     * @return length of the <code>CLOB</code> in characters
     * @exception SQLException if there is an error accessing the
     *            length of the <code>CLOB</code> value
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public long length() throws SQLException {

        checkClosed();

        if (m_fixedWidthCharset) {
            return m_file.length() / m_maxCharWidth;
        }

        ReaderAdapter adapter = null;

        try {
            adapter = new ReaderAdapter(m_file, 0, Long.MAX_VALUE);

            final long length = adapter.skip(Long.MAX_VALUE);

            return length;
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            closeSafely(adapter);
        }
    }

    /**
     * Retrieves a copy of the specified substring
     * in the <code>CLOB</code> value
     * designated by this <code>Clob</code> object.
     * The substring begins at position
     * <code>pos</code> and has up to <code>length</code> consecutive
     * characters.
     *
     * @param pos the first character of the substring to be extracted.
     *            The first character is at position 1.
     * @param length the number of consecutive characters to be copied;
     * the value for length must be 0 or greater
     * @return a <code>String</code> that is the specified substring in
     *         the <code>CLOB</code> value designated by this <code>Clob</code> object
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value; if pos is less than 1 or length is
     * less than 0
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public String getSubString(final long pos,
                               final int length) throws SQLException {

        Reader          reader = null;
        CharArrayWriter writer;
        final int initialCapacity =
            Math.min(InOutUtil.DEFAULT_COPY_BUFFER_SIZE, length);

        try {
            reader = getCharacterStream(pos, length);
            writer = new CharArrayWriter(initialCapacity);

            InOutUtil.copy(reader, writer, length);

            return writer.toString();
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            closeSafely(reader);
        }
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as a <code>java.io.Reader</code> object (or as a stream of
     * characters).
     *
     * @return a <code>java.io.Reader</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setCharacterStream
     * @since JDK 1.2
     */
    public Reader getCharacterStream() throws SQLException {
        return getCharacterStream(1, Long.MAX_VALUE);
    }

    /**
     * Retrieves the <code>CLOB</code> value designated by this <code>Clob</code>
     * object as an ASCII stream.
     *
     * @return a <code>java.io.InputStream</code> object containing the
     *         <code>CLOB</code> data
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setAsciiStream
     * @since JDK 1.2
     */
    public InputStream getAsciiStream() throws SQLException {

        InputStream stream;

        try {
            stream = new JDBCBlobFile.InputStreamAdapter(m_file, 0,
                    Long.MAX_VALUE) {

                private boolean closed;

                public synchronized void close() throws IOException {

                    if (closed) {
                        return;
                    }

                    closed = true;

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
        } catch (FileNotFoundException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (SecurityException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (NullPointerException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IllegalArgumentException ex) {
            throw JDBCUtil.sqlException(ex);
        }

        m_streams.add(stream);

        return stream;
    }

    /**
     * Retrieves the character position at which the specified char[]
     * <code>pattern</code> appears in the <code>CLOB</code> value
     * represented by this <code>Clob</code> object.  The search
     * begins at position <code>start</code>.
     *
     * @param pattern the substring for which to search
     * @param start the position at which to begin searching; the first position
     *              is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     */
    public long position(final char[] pattern,
                         final long start) throws SQLException {

        if (start < 1) {
            throw JDBCUtil.outOfRangeArgument("start: " + start);
        } else if (pattern == null || pattern.length == 0) {
            return -1L;
        }

        long length = this.length();

        if (start > length || pattern.length > length
                || start > length - pattern.length) {
            return -1;
        }

        return position0(new String(pattern), start);
    }

    /**
     * Retrieves the character position at which the specified
     * <code>pattern</code> appears in the SQL <code>CLOB</code> value
     * represented by this <code>Clob</code> object. The search begins at
     * position <code>start</code>.
     *
     * @param pattern for which to search
     * @param start   position at which to begin searching; the first
     *                position is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if start is less than 1
     */
    private long position0(final String pattern,
                           final long start) throws SQLException {

        Pattern literal;

        try {
            literal = Pattern.compile(Pattern.quote(pattern));
        } catch (PatternSyntaxException ex) {
            throw JDBCUtil.sqlException(ex);
        }

        Reader  reader              = null;
        Scanner scanner             = null;
        boolean gotReaderAndScanner = false;

        try {
            reader = new ReaderAdapter(m_file, start - 1, m_file.length());
            scanner             = new Scanner(reader);
            gotReaderAndScanner = true;
        } catch (FileNotFoundException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (SecurityException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (NullPointerException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IllegalArgumentException ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            if (!gotReaderAndScanner) {
                closeSafely(scanner);
                closeSafely(reader);
            }
        }

        int position = -1;

        try {
            if (scanner.hasNext()) {
                final boolean found = (null
                                       != scanner.findWithinHorizon(literal,
                                           0));

                if (found) {
                    MatchResult match = scanner.match();

                    position = match.start() + 1;
                }
            }
        } finally {
            closeSafely(scanner);
            closeSafely(reader);
        }

        return position;
    }

    /**
     * Retrieves the character position at which the specified substring
     * <code>searchstr</code> appears in the SQL <code>CLOB</code> value
     * represented by this <code>Clob</code> object.  The search
     * begins at position <code>start</code>.
     *
     * @param searchstr the substring for which to search
     * @param start the position at which to begin searching; the first position
     *              is 1
     * @return the position at which the substring appears or -1 if it is not
     *         present; the first position is 1
     * @exception  SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if pos is less than 1
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public long position(String searchstr, long start) throws SQLException {

        if (start < 1) {
            throw JDBCUtil.outOfRangeArgument("start: " + start);
        } else if (searchstr == null || searchstr.isEmpty()) {
            return -1L;
        }

        final long length          = this.length();
        final int  searchstrLength = searchstr.length();

        if (start > length || searchstrLength > length
                || start > length - searchstrLength) {
            return -1;
        }

        return position0(searchstr, start);
    }

    /**
     * Retrieves the character position at which the specified
     * <code>Clob</code> object <code>searchstr</code> appears in this
     * <code>Clob</code> object.  The search begins at position
     * <code>start</code>.
     *
     * @param pattern the <code>Clob</code> object for which to search
     * @param start the position at which to begin searching; the first
     *              position is 1
     * @return the position at which the <code>Clob</code> object appears
     *              or -1 if it is not present; the first position is 1
     * @exception SQLException if there is an error accessing the
     *            <code>CLOB</code> value or if start is less than 1
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public long position(final Clob pattern,
                         final long start) throws SQLException {

        long patternLength;

        if (start < 1) {
            throw JDBCUtil.outOfRangeArgument("start: " + start);
        } else if ((patternLength = pattern == null ? 0
                                                    : pattern.length()) == 0) {
            return -1L;
        } else if (patternLength > Integer.MAX_VALUE) {
            throw JDBCUtil.outOfRangeArgument("pattern.length(): "
                                              + patternLength);
        }

        long length = this.length();

        if (start > length || patternLength > length
                || start > length - patternLength) {
            return -1;
        }

        String stringPattern;

        if (pattern instanceof JDBCClob) {
            stringPattern = ((JDBCClob) pattern).getData();
        } else {
            Reader       reader = null;
            StringWriter writer = new StringWriter();

            try {
                reader = pattern.getCharacterStream();

                InOutUtil.copy(reader, writer, patternLength);
            } catch (IOException ex) {
                throw JDBCUtil.sqlException(ex);
            } catch (Throwable ex) {
                throw JDBCUtil.sqlException(ex);
            } finally {
                closeSafely(reader);
            }

            stringPattern = writer.toString();
        }

        return position0(stringPattern, start);
    }

    //---------------------------- jdbc 3.0 -----------------------------------

    /**
     * Writes the given Java <code>String</code> to the <code>CLOB</code>
     * value that this <code>Clob</code> object designates at the position
     * <code>pos</code>. The string will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing the given string, then the length of the <code>Clob</code>
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos the position at which to start writing to the <code>CLOB</code>
     *         value that this <code>Clob</code> object represents;
     * The first position is 1
     * @param str the string to be written to the <code>CLOB</code>
     *        value that this <code>Clob</code> designates
     * @return the number of characters written
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if pos is less than 1
     *
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4
     */
    public int setString(final long pos,
                         final String str) throws SQLException {
        return setString(pos, str, 0, str == null ? 0
                                                  : str.length());
    }

    /**
     * Writes <code>len</code> characters of <code>str</code>, starting
     * at character <code>offset</code>, to the <code>CLOB</code> value
     * that this <code>Clob</code> represents.  The string will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>. If the end of the <code>Clob</code> value is reached
     * while writing the given string, then the length of the <code>Clob</code>
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos    the position at which to start writing to this
     *               <code>CLOB</code> object; The first position is 1
     * @param str the string to be written to the <code>CLOB</code>
     *        value that this <code>Clob</code> object represents
     * @param offset the offset into <code>str</code> to start reading
     *        the characters to be written
     * @param len    the number of characters to be written
     * @return the number of characters written
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if pos is less than 1
     *
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4
     */
    public int setString(final long pos, final String str, final int offset,
                         final int len) throws SQLException {

        checkClosed();

        if (str == null) {
            throw JDBCUtil.nullArgument("str");
        }

        final int strlen = str.length();

        if (offset < 0 || offset > strlen) {
            throw JDBCUtil.outOfRangeArgument("offset: " + offset);
        }

        if (len < 0 || len > strlen - offset) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }

        if (pos < 1L) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        long oldLength = this.length();

        if (pos > oldLength + 1) {
            fillSpace(oldLength + 1, pos);
        }

        Writer writer = null;

        try {
            writer = setCharacterStream(pos);

            writer.write(str, offset, len);
        } catch (SQLException ex) {
            throw ex;
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            closeSafely(writer);
        }

        return len;
    }

    /**
     *
     * @param startPos inclusive
     * @param endPos exclusive
     * @throws SQLException exception
     */
    private void fillSpace(final long startPos,
                           long endPos) throws SQLException {

        Writer writer = null;

        try {
            writer = setCharacterStream(startPos);

            for (long i = endPos - startPos; i >= 0; i--) {
                writer.append(' ');
            }
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            closeSafely(writer);
        }
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * <code>CLOB</code> value that this <code>Clob</code> object represents,
     * starting at position <code>pos</code>. Characters written to the stream
     * will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing characters to the stream, then the length of the <code>Clob</code>
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos the position at which to start writing to this
     *            <code>CLOB</code> object; The first position is 1
     * @return the stream to which ASCII encoded characters can be written
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if pos is less than 1
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getAsciiStream
     *
     * @since JDK 1.4
     */
    public OutputStream setAsciiStream(long pos) throws SQLException {

        if (pos < 1) {
            throw JDBCUtil.invalidArgument("pos: " + pos);
        }

        checkClosed();
        createFile();

        long thisLength = this.length();

        if (pos > thisLength + 1) {
            this.fillSpace(thisLength + 1, pos);
        }

        OutputStream stream;

        try {
            stream = new JDBCBlobFile.OutputStreamAdapter(m_file, pos - 1) {

                public void close() throws IOException {

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
        } catch (FileNotFoundException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (IllegalArgumentException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (NullPointerException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (SecurityException ex) {
            throw JDBCUtil.sqlException(ex);
        }

        m_streams.add(stream);

        return stream;
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters
     * to the <code>CLOB</code> value that this <code>Clob</code> object
     * represents, at position <code>pos</code>. Characters written to the stream
     * will overwrite the existing characters
     * in the <code>Clob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Clob</code> value is reached
     * while writing characters to the stream, then the length of the <code>Clob</code>
     * value will be increased to accommodate the extra characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h1>HSQLDB-Specific Information:</h1> <p>
     *
     * When the value specified for <code>pos</code> is greater then the
     * length+1, an  <code>SQLException</code> is thrown.
     * </div>
     *
     * @param pos the position at which to start writing to the
     *            <code>CLOB</code> value; The first position is 1
     *
     * @return a stream to which Unicode encoded characters can be written
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if pos is less than 1
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getCharacterStream
     *
     * @since JDK 1.4
     */
    public Writer setCharacterStream(final long pos) throws SQLException {

        if (pos < 1) {
            throw JDBCUtil.invalidArgument("pos: " + pos);
        }

        checkClosed();
        createFile();

        long thisLength = this.length();

        if (pos > thisLength + 1) {
            this.fillSpace(thisLength + 1, pos);
        }

        Writer        writer;
        WriterAdapter adapter;

        try {
            adapter = new WriterAdapter(m_file, pos - 1) {

                public void close() throws IOException {

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
            writer = new BufferedWriter(adapter);
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        }

        m_streams.add(adapter);

        return writer;
    }

    /**
     * Truncates the <code>CLOB</code> value that this <code>Clob</code>
     * designates to have a length of <code>len</code>
     * characters.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater than the length+1 of the <code>CLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param len the length, in characters, to which the <code>CLOB</code> value
     *        should be truncated
     * @exception SQLException if there is an error accessing the
     * <code>CLOB</code> value or if len is less than 0
     *
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4
     */
    public void truncate(long len) throws SQLException {

        if (len < 0) {
            throw JDBCUtil.invalidArgument("len: " + len);
        }

        checkClosed();

        ReaderAdapter    adapter          = null;
        RandomAccessFile randomAccessFile = null;
        long             filePointer;

        try {
            adapter     = new ReaderAdapter(m_file, len, Long.MAX_VALUE);
            filePointer = adapter.getFilePointer();

            adapter.close();

            randomAccessFile = new RandomAccessFile(m_file, "rw");

            randomAccessFile.setLength(filePointer);
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        } finally {
            closeSafely(adapter);
            closeSafely(randomAccessFile);
        }
    }

    /**
     * This method frees the <code>Clob</code> object and releases the resources
     * that it holds.  The object is invalid once the <code>free</code> method
     * is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * <p>
     * @throws SQLException if an error occurs releasing
     * the Clob's resources
     *
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4
     */
    public synchronized void free() throws SQLException {

        if (m_closed) {
            return;
        }

        m_closed = true;

        final List streams = m_streams;

        m_streams = null;

        for (Iterator itr = streams.iterator(); itr.hasNext(); ) {
            final Object stream = itr.next();

            closeSafely(stream);
        }

        if (m_deleteOnFree) {
            try {
                m_file.delete();
            } catch (SecurityException e) {}
        }
    }

    /**
     * Returns a <code>Reader</code> object that contains a partial <code>Clob</code> value, starting
     * with the character specified by pos, which is length characters in length.
     *
     * @param pos    the offset to the first character of the partial value to
     * be retrieved.  The first character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be retrieved.
     * @return <code>Reader</code> through which the partial <code>Clob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of
     * characters in the <code>Clob</code> or if pos + length is greater than the number of
     * characters in the <code>Clob</code>
     *
     * @exception java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
    public Reader getCharacterStream(long pos,
                                     long length) throws SQLException {

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (length < 0) {
            throw JDBCUtil.outOfRangeArgument("length: " + length);
        }

        Reader reader;

        try {
            reader = new ReaderAdapter(m_file, pos - 1, length) {

                public void close() throws IOException {

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        }

        m_streams.add(reader);

        return reader;
    }

    /**
     * Retrieves the canonical <code>File</code> object denoting the file that
     * backs this CLOB.
     *
     * @return the file that backs this CLOB.
     */
    public File getFile() {
        return m_file;
    }

    /**
     *
     * @return the name of the character encoding used to read and write
     *         character data in the underlying files, as well as to determine
     *         the character length and character offsets into the underlying
     *         file
     */
    public String getEncoding() {
        return m_encoding;
    }

    /**
     * Retrieves whether an attempt to delete the backing file
     * is made in response to invocation of {@link #free()}.
     *
     * @return true if backing file deletion is attempted; otherwise false.
     */
    public boolean isDeleteOnFree() {
        return m_deleteOnFree;
    }

    /**
     * Assigns whether an attempt to delete the backing file
     * is made in response to invocation of {@link #free()}.
     *
     * @param deleteOnFree the new value to assign
     */
    public void setDeleteOnFree(boolean deleteOnFree) {
        m_deleteOnFree = deleteOnFree;
    }

    //--------------------------------------------------------------------------
    // Internal Implementation
    //--------------------------------------------------------------------------
    public static final String TEMP_FILE_PREFIX = "hsql_jdbc_clob_file_";
    public static final String TEMP_FILE_SUFFIX = ".tmp";

    //
    private final File m_file;

    //
    private boolean        m_closed;
    private boolean        m_deleteOnFree;
    private String         m_encoding;
    private Charset        m_charset;
    private CharsetEncoder m_encoder;
    private boolean        m_fixedWidthCharset;
    private int            m_maxCharWidth;
    private List           m_streams = new HsqlArrayList();

    /**
     * Convenience constructor for {@link
     * #JDBCClobFile(java.lang.String)
     * JDBCClobFile((String)null)}. <p>
     *
     * @throws SQLException if the platform encoding is unsupported,
     *         the temp file cannot be created or some other
     *         error occurs that prevents the construction of a
     *         valid instance of this class.
     */
    public JDBCClobFile() throws SQLException {
        this((String) null);
    }

    /**
     * Constructs a new JDBCClobFile instance backed by a File object
     * created by File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX),
     * using the given encoding to read and write file content.
     *
     * @param encoding the name of the character encoding used to read and write
     *         character data in the underlying file, as well as to determine
     *         the character length of and character offsets into the underlying
     *         file. Specify null to denote the platform encoding.
     *
     * @throws SQLException if the given encoding is unsupported,
     *         the backing temp file could not be created or if a security
     *         manager exists and its <code>{@link
     *         java.lang.SecurityManager#checkWrite(java.lang.String)}</code>
     *         method does not allow a file to be created.
     */
    public JDBCClobFile(String encoding) throws SQLException {

        try {
            setEncoding(encoding);

            m_file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            m_deleteOnFree = true;
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (SecurityException se) {
            throw JDBCUtil.sqlException(se);
        }
    }

    /**
     * Convenience constructor for {@link
     * #JDBCClobFile(java.io.File, java.lang.String)
     * JDBCClobFile(file,null)}.
     *
     * @param file that is to back the new CLOB instance.
     *
     * @throws SQLException if an I/O error occurs, which is possible because the
     *         construction of the canonical pathname may require
     *         file-system queries; a required system property value
     *         cannot be accessed; a security manager exists and its
     *         <code>{@link java.lang.SecurityManager#checkRead}</code>
     *         method denies read access to the file
     */
    public JDBCClobFile(File file) throws SQLException {
        this(file, null);
    }

    /**
     * Constructs a new JDBCClobFile instance backed by the given File object
     * using the given encoding to read and write file content.
     *
     * @param file     that is to back the new CLOB instance.
     * @param encoding the name of the character encoding used to read and write
     *         character data in the underlying file, as well as to determine
     *         the character length of and character offsets into the underlying
     *         file. Specify null to denote the platform encoding.
     *
     * @throws SQLException if the given encoding is unsupported;
     *         an I/O error occurs, which is possible because the
     *         construction of the canonical pathname may require
     *         file-system queries; a required system property value
     *         cannot be accessed; a security manager exists and its
     *         <code>{@link java.lang.SecurityManager#checkRead}</code>
     *         method denies read access to the file
     */
    public JDBCClobFile(File file, String encoding) throws SQLException {

        if (file == null) {
            throw JDBCUtil.nullArgument("file");
        }

        try {
            setEncoding(encoding);

            m_file = file.getCanonicalFile();

            checkIsFile( /*checkExists*/false);

            m_deleteOnFree = false;
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (SQLException ex) {
            throw JDBCUtil.sqlException(ex);
        }
    }

    protected final void setEncoding(final String encoding)
    throws UnsupportedEncodingException {

        final Charset charSet = charsetForName(encoding);
        final CharsetEncoder encoder = charSet.newEncoder().onMalformedInput(
            CodingErrorAction.REPLACE).onUnmappableCharacter(
            CodingErrorAction.REPLACE);
        final float maxBytesPerChar     = encoder.maxBytesPerChar();
        final float averageBytesPerChar = encoder.averageBytesPerChar();
        final boolean fixedWidthCharset =
            (maxBytesPerChar == Math.round(maxBytesPerChar))
            && (maxBytesPerChar == averageBytesPerChar);

        //
        m_fixedWidthCharset = fixedWidthCharset;
        m_maxCharWidth      = Math.round(maxBytesPerChar);
        m_charset           = charSet;
        m_encoder           = encoder;
        m_encoding          = m_charset.name();
    }

    protected static Charset charsetForName(final String charsetName)
    throws UnsupportedEncodingException {

        String csn = charsetName;

        if (csn == null) {
            csn = Charset.defaultCharset().name();
        }

        try {
            if (Charset.isSupported(csn)) {
                return Charset.forName(csn);
            }
        } catch (IllegalCharsetNameException x) {
            LOG.warning(x.getMessage(), x);
        }

        throw new UnsupportedEncodingException(csn);
    }

    protected final void checkIsFile(final boolean checkExists)
    throws SQLException {

        boolean exists = false;
        boolean isFile = false;

        try {
            exists = m_file.exists();
        } catch (Exception ex) {
            throw JDBCUtil.sqlException(ex);
        }

        if (exists) {
            try {
                isFile = m_file.isFile();
            } catch (Exception ex) {
                throw JDBCUtil.sqlException(ex);
            }
        }

        if (exists) {
            if (!isFile) {
                throw JDBCUtil.invalidArgument("Is not a file: " + m_file);
            }
        } else if (checkExists) {
            throw JDBCUtil.invalidArgument("Does not exist: " + m_file);
        }
    }

    protected void checkClosed() throws SQLException {

        if (m_closed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }
    }

    protected void createFile() throws SQLException {

        try {
            if (!m_file.exists()) {
                FileUtil.getFileUtil().makeParentDirectories(m_file);
                m_file.createNewFile();
            }
        } catch (IOException ex) {
            throw JDBCUtil.sqlException(ex);
        } catch (Throwable ex) {
            throw JDBCUtil.sqlException(ex);
        }

        checkIsFile( /*checkExists*/true);
    }

    //<editor-fold defaultstate="collapsed" desc="JAVA 1.2 compliant closeSafely(...)">
    private static void closeSafely(final RandomAccessFile target) {

        if (target != null) {
            try {
                target.close();
            } catch (IOException ignoredIoe) {
                LOG.info(ignoredIoe.getMessage(), ignoredIoe);
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private static void closeSafely(final InputStream target) {

        if (target != null) {
            try {
                target.close();
            } catch (IOException ignoredIoe) {
                LOG.info(ignoredIoe.getMessage(), ignoredIoe);
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private static void closeSafely(final OutputStream target) {

        if (target != null) {
            try {
                target.close();
            } catch (IOException ignoredIoe) {
                LOG.info(ignoredIoe.getMessage(), ignoredIoe);
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private static void closeSafely(final Reader target) {

        if (target != null) {
            try {
                target.close();
            } catch (IOException ignoredIoe) {
                LOG.info(ignoredIoe.getMessage(), ignoredIoe);
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private static void closeSafely(final Writer target) {

        if (target != null) {
            try {
                target.close();
            } catch (IOException ignoredIoe) {
                LOG.info(ignoredIoe.getMessage(), ignoredIoe);
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private static void closeSafely(final Scanner target) {

        if (target != null) {
            try {
                target.close();
            } catch (Throwable ignoredRex) {
                LOG.info(ignoredRex.getMessage(), ignoredRex);
            }
        }
    }

    private void closeSafely(final Object target) {

        if (target instanceof RandomAccessFile) {
            closeSafely((RandomAccessFile) target);
        } else if (target instanceof InputStream) {
            closeSafely((InputStream) target);
        } else if (target instanceof OutputStream) {
            closeSafely((OutputStream) target);
        } else if (target instanceof Reader) {
            closeSafely((Reader) target);
        } else if (target instanceof Writer) {
            closeSafely((Writer) target);
        } else if (target instanceof Scanner) {
            closeSafely((Scanner) target);
        }
    }

    //</editor-fold>
    protected class WriterAdapter extends Writer {

        protected final Writer m_writer;

        protected WriterAdapter(final File file,
                                final long pos)
                                throws FileNotFoundException, IOException,
                                       SecurityException,
                                       NullPointerException,
                                       IllegalArgumentException {

            if (file == null) {
                throw new NullPointerException("file");
            }

            if (pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            ReaderAdapter reader = null;
            long          filePointer;

            try {
                reader      = new ReaderAdapter(file, pos, Long.MAX_VALUE);
                filePointer = reader.getFilePointer();
            } finally {
                closeSafely(reader);
            }

            RandomAccessFile raf     = null;
            boolean          success = false;

            try {
                raf = new RandomAccessFile(file, "rw");

                raf.seek(filePointer);

                success = true;
            } finally {
                if (!success) {
                    closeSafely(raf);
                }
            }

            final OutputStreamAdapter osa = new OutputStreamAdapter(raf);

            m_writer = m_encoding == null ? new OutputStreamWriter(osa)
                                          : new OutputStreamWriter(osa,
                                          m_charset);
        }

        public void flush() throws IOException {
            m_writer.flush();
        }

        public void close() throws IOException {
            m_writer.close();
        }

        public void write(char[] cbuf, int off, int len) throws IOException {
            m_writer.write(cbuf, off, len);
        }
    }

    protected class ReaderAdapter extends Reader {

        //
        private static final int CHARBUFFER_CAPACTIY = 128;

        //
        private final Reader m_reader;
        private long         m_remaining = Long.MAX_VALUE;
        private long         m_filePointer;
        private ByteBuffer   m_byteBuffer;
        private CharBuffer   m_charBuffer;

        protected ReaderAdapter(final File file, final long pos,
                                final long length)
                                throws FileNotFoundException, IOException,
                                       NullPointerException,
                                       IllegalArgumentException,
                                       SecurityException {

            if (file == null) {
                throw new NullPointerException("file");
            }

            if (pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            if (length < 0) {
                throw new IllegalArgumentException("length: " + length);
            }

            long fileLength = file.length();
            long maxCharsLength = m_fixedWidthCharset
                                  ? Math.round(fileLength
                                               / (double) m_maxCharWidth)
                                  : fileLength;

            if (maxCharsLength - pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            //
            if (!m_fixedWidthCharset) {
                final int charCapacity = CHARBUFFER_CAPACTIY;
                final int byteCapacity = charCapacity * m_maxCharWidth;

                m_charBuffer = CharBuffer.allocate(charCapacity);
                m_byteBuffer = ByteBuffer.allocate(byteCapacity);
            }

            FileInputStream fis = null;

            try {
                fis = new FileInputStream(file);

                final BufferedInputStream bis = new BufferedInputStream(fis);
                final InputStreamReader isr = new InputStreamReader(bis,
                    m_charset);

                m_reader = isr;
            } catch (FileNotFoundException ex) {
                closeSafely(fis);

                throw ex;
            } catch (SecurityException ex) {
                closeSafely(fis);

                throw ex;
            } catch (NullPointerException ex) {
                closeSafely(fis);

                throw ex;
            } catch (RuntimeException ex) {
                closeSafely(fis);

                throw ex;
            }

            long skipped = this.skip(pos);

            if (skipped < pos) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            // important - do not assign until *after* seek above.
            m_remaining = length;
        }

        public int read(final char[] cbuf, final int off,
                        int len) throws IOException {

            final long l_remaining = m_remaining;

            if (l_remaining <= 0) {
                return -1;
            } else if (l_remaining < len) {
                len = (int) l_remaining;
            }

            int charsRead = m_reader.read(cbuf, off, len);

            if (charsRead == -1) {
                return -1;
            } else if (charsRead > l_remaining) {
                charsRead   = (int) l_remaining;
                m_remaining = 0;
            } else {
                m_remaining -= charsRead;
            }

            int bytesRead;

            if (m_fixedWidthCharset) {
                bytesRead = (m_maxCharWidth * charsRead);
            } else {
                final boolean reallocate = (charsRead
                                            > m_charBuffer.capacity());
                final CharBuffer cb = reallocate
                                      ? CharBuffer.allocate(charsRead)
                                      : m_charBuffer;
                final ByteBuffer bb = reallocate
                                      ? ByteBuffer.allocate(charsRead
                                          * m_maxCharWidth)
                                      : m_byteBuffer;

                //
                cb.clear();
                bb.clear();
                cb.put(cbuf, off, charsRead);
                cb.flip();
                m_encoder.encode(cb, bb, /*endOfinput*/ true);
                bb.flip();

                bytesRead = bb.limit();

                if (reallocate) {
                    m_byteBuffer = bb;
                    m_charBuffer = cb;
                }
            }

            m_filePointer += bytesRead;

            return charsRead;
        }

        public void close() throws IOException {
            m_reader.close();
        }

        public long getFilePointer() {
            return m_filePointer;
        }
    }
}
