/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.hsqldb.SessionInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobInputStream;

/**
 * A wrapper for HSQLDB ClobData objects.
 *
 * Instances of this class are returned by calls to ResultSet methods.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since HSQLDB 1.9.0
 */
public class JDBCClobClient implements Clob {

    /**
     * Retrieves the {@code CLOB} value designated by this
     * {@code Clob} object as an ascii stream.
     *
     * The ascii stream consists of the low ordre bytes of UTF-16 characters
     * in the clob. The question mark character is returnd for UTF-16 characters
     * beyond the range of 8-bit ASCII.
     *
     * @return a {@code java.io.InputStream} object containing the
     *   {@code CLOB} data
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized InputStream getAsciiStream() throws SQLException {

        checkClosed();

        return new InputStream() {

            private Reader reader = clob.getCharacterStream(session);
            public int read() throws IOException {

                int c = reader.read();

                if (c < 0) {
                    return -1;
                }

                if (c >= 256) {
                    if (Character.isHighSurrogate((char) c)) {
                        reader.read();
                    }

                    c = '?';
                }

                return c;
            }
            public int read(byte[] b, int off, int len) throws IOException {

                if (b == null) {
                    throw new NullPointerException();
                }

                if (off < 0 || len < 0 || len > b.length - off) {
                    throw new IndexOutOfBoundsException();
                }

                if (len == 0) {
                    return 0;
                }

                int bytesRead = 0;

                for (int i = 0; i < len; i++) {
                    int c = reader.read();

                    if (c < 0) {
                        break;
                    }

                    if (c >= 256) {
                        if (Character.isHighSurrogate((char) c)) {
                            reader.read();
                        }

                        c = '?';
                    }

                    b[off + i] = (byte) c;

                    bytesRead++;
                }

                return bytesRead == 0
                       ? -1
                       : bytesRead;
            }
            public void close() throws IOException {
                try {
                    reader.close();
                } catch (Exception ex) {}
            }
        };
    }

    /**
     * Retrieves the {@code CLOB} value designated by this
     * {@code Clob} object as a {@code java.io.Reader} object (or
     * as a stream of characters).
     *
     * @return a {@code java.io.Reader} object containing the
     *   {@code CLOB} data
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized Reader getCharacterStream() throws SQLException {
        checkClosed();

        return new ClobInputStream(session, clob, 0, length());
    }

    /**
     * Retrieves a copy of the specified substring in the {@code CLOB}
     * value designated by this {@code Clob} object.
     *
     * @param pos the first character of the substring to be extracted. The
     *   first character is at position 1.
     * @param length the number of consecutive characters to be copied
     * @return a {@code String} that is the specified substring in the
     *   {@code CLOB} value designated by this {@code Clob} object
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized String getSubString(
            long pos,
            int length)
            throws SQLException {

        checkClosed();

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        try {
            return clob.getSubString(session, pos - 1, length);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the number of characters in the {@code CLOB} value
     * designated by this {@code Clob} object.
     *
     * @return length of the {@code CLOB} in characters
     * @throws SQLException if there is an error accessing the length of the
     *   {@code CLOB} value
     */
    public synchronized long length() throws SQLException {

        checkClosed();

        try {
            return clob.length(session);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the character position at which the specified substring
     * {@code searchstr} appears in the SQL {@code CLOB} value
     * represented by this {@code Clob} object.
     *
     * @param searchstr the substring for which to search
     * @param start the position at which to begin searching; the first
     *   position is 1
     * @return the position at which the substring appears or -1 if it is
     *   not present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized long position(
            String searchstr,
            long start)
            throws SQLException {

        checkClosed();

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        try {
            return clob.position(session, searchstr, start - 1);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the character position at which the specified
     * {@code Clob} object {@code searchstr} appears in this
     * {@code Clob} object.
     *
     * @param searchstr the {@code Clob} object for which to search
     * @param start the position at which to begin searching; the first
     *   position is 1
     * @return the position at which the {@code Clob} object appears or
     *   -1 if it is not present; the first position is 1
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized long position(
            Clob searchstr,
            long start)
            throws SQLException {

        checkClosed();

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (searchstr instanceof JDBCClobClient) {
            ClobDataID searchClob = ((JDBCClobClient) searchstr).clob;

            try {
                return clob.position(session, searchClob, start - 1);
            } catch (HsqlException e) {
                throw JDBCUtil.sqlException(e);
            }
        }

        if (!isInLimits(Integer.MAX_VALUE, 0, searchstr.length())) {
            throw JDBCUtil.outOfRangeArgument();
        }

        return position(
            searchstr.getSubString(1, (int) searchstr.length()),
            start);
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the
     * {@code CLOB} value that this {@code Clob} object represents,
     * starting at position {@code pos}.
     *
     * The bytes written to the OutputStream are stored verbatim in the clob as
     * the low order bytes of UTF-16 characters.
     *
     * @param pos the position at which to start writing to this
     *   {@code CLOB} object
     * @return the stream to which ASCII encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized OutputStream setAsciiStream(
            final long pos)
            throws SQLException {

        return new OutputStream() {

            Writer writer = setCharacterStream(pos);
            public void write(int b) throws IOException {
                writer.write(b & 0xff);
            }
            public void write(byte[] b, int off, int len) throws IOException {

                if (b == null) {
                    throw new NullPointerException();
                }

                if (off < 0 || len < 0 || len > b.length - off) {
                    throw new IndexOutOfBoundsException();
                }

                if (len == 0) {
                    return;
                }

                char[] charArray = new char[len];

                for (int i = 0; i < len; i++) {
                    charArray[i] = (char) b[off + i];
                }

                writer.write(charArray, 0, len);
            }
            public void close() throws IOException {
                writer.close();
            }
        };
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters
     * to the {@code CLOB} value that this {@code Clob} object
     * represents, at position {@code pos}.
     *
     * @param pos the position at which to start writing to the
     *   {@code CLOB} value
     * @return a stream to which Unicode encoded characters can be written
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized Writer setCharacterStream(
            final long pos)
            throws SQLException {

        checkClosed();

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }

        startUpdate();

        return new Writer() {

            private long    m_clobPosition = pos - 1;
            private boolean m_closed;
            public void write(
                    char[] cbuf,
                    int off,
                    int len)
                    throws IOException {

                checkClosed();
                clob.setChars(session, m_clobPosition, cbuf, off, len);

                m_clobPosition += len;
            }
            public void flush() throws IOException {

                // no-op
            }
            @Override
            public void close() throws IOException {
                m_closed = true;
            }
            private void checkClosed() throws IOException {
                if (m_closed || JDBCClobClient.this.isClosed()) {
                    throw new IOException("The stream is closed");
                }
            }
        };
    }

    /**
     * Writes the given Java {@code String} to the {@code CLOB}
     * value that this {@code Clob} object designates at the position
     * {@code pos}.
     *
     * @param pos the position at which to start writing to the
     *   {@code CLOB} value that this {@code Clob} object
     *   represents
     * @param str the string to be written to the {@code CLOB} value
     *   that this {@code Clob} designates
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized int setString(
            long pos,
            String str)
            throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    /**
     * Writes {@code len} characters of {@code str}, starting at
     * character {@code offset}, to the {@code CLOB} value that
     * this {@code Clob} represents.
     *
     * @param pos the position at which to start writing to this
     *   {@code CLOB} object
     * @param str the string to be written to the {@code CLOB} value
     *   that this {@code Clob} object represents
     * @param offset the offset into {@code str} to start reading the
     *   characters to be written
     * @param len the number of characters to be written
     * @return the number of characters written
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized int setString(
            long pos,
            String str,
            int offset,
            int len)
            throws SQLException {

        checkClosed();

        if (!isInLimits(str.length(), offset, len)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        if (pos < 1) {
            throw JDBCUtil.outOfRangeArgument("pos: " + pos);
        }

        if (!isWritable) {
            throw JDBCUtil.notUpdatableColumn();
        }

        try {
            startUpdate();

            str = str.substring(offset, offset + len);

            clob.setString(session, pos - 1, str);

            return len;
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Truncates the {@code CLOB} value that this {@code Clob}
     * designates to have a length of {@code len} characters.
     *
     * @param len the length, in bytes, to which the {@code CLOB} value
     *   should be truncated
     * @throws SQLException if there is an error accessing the
     *   {@code CLOB} value
     */
    public synchronized void truncate(long len) throws SQLException {

        checkClosed();

        if (len < 0) {
            throw JDBCUtil.outOfRangeArgument("len: " + len);
        }

        try {
            clob.truncate(session, len);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * This method frees the {@code Clob} object and releases the resources
     * that it holds.  The object is invalid once the {@code free} method
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
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void free() throws SQLException {
        isClosed = true;
        clob     = null;
        session  = null;
    }

    /**
     * Returns a {@code Reader} object that contains a partial {@code Clob} value, starting
     * with the character specified by pos, which is length characters in length.
     *
     * @param pos the offset to the first character of the partial value to
     * be retrieved.  The first character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be retrieved.
     * @return {@code Reader} through which the partial {@code Clob} value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of
     * characters in the {@code Clob} or if pos + length is greater than the number of
     * characters in the {@code Clob}
     *
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized Reader getCharacterStream(
            long pos,
            long length)
            throws SQLException {

        checkClosed();

        if (!isInLimits(this.length(), pos - 1, length)) {
            throw JDBCUtil.outOfRangeArgument();
        }

        return new ClobInputStream(session, clob, pos - 1, length);
    }

    char[] getChars(long position, int length) throws SQLException {

        try {
            return clob.getChars(session, position - 1, length);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    //
    ClobDataID       originalClob;
    ClobDataID       clob;
    SessionInterface session;
    int              colIndex;
    private boolean  isClosed;
    private boolean  isWritable;
    JDBCResultSet    resultSet;

    public JDBCClobClient(SessionInterface session, ClobDataID clob) {
        this.session = session;
        this.clob    = clob;
    }

    public ClobDataID getClob() {
        return clob;
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    public synchronized void setWritable(JDBCResultSet result, int index) {
        isWritable = true;
        resultSet  = result;
        colIndex   = index;
    }

    public synchronized void clearUpdates() {

        if (originalClob != null) {
            clob         = originalClob;
            originalClob = null;
        }
    }

    private void startUpdate() throws SQLException {

        if (originalClob != null) {
            return;
        }

        originalClob = clob;
        clob         = (ClobDataID) clob.duplicate(session);

        resultSet.startUpdate(colIndex + 1);

        resultSet.preparedStatement.parameterValues[colIndex] = clob;
        resultSet.preparedStatement.parameterSet[colIndex]    = true;
        resultSet.isRowUpdated                                = true;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw JDBCUtil.sqlException(ErrorCode.X_0F502);
        }
    }

    static boolean isInLimits(long fullLength, long pos, long len) {

        return fullLength >= 0
               && pos >= 0
               && len >= 0
               && pos <= fullLength - len;
    }
}
