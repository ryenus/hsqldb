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


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import java.util.Objects;

/**
 * An input stream that reads US-ASCII values from a Reader, in compliance
 * with the Java US_ASCII Charset encoder, including
 * <a href="https://unicodebook.readthedocs.io/unicode_encodings.html#utf-16-surrogate-pairs">
 * utf-16-surrogate-pairs</a>.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.1
 * @since 2.7.1
 */
public class AsciiInputStream extends InputStream {

    /**
     * is 7;
     */
    public static final int ASCII_BITS = 7;

    /**
     * is 0b01111111
     */
    public static final int ASCII_MASK = 0b01111111;

    /**
     * is 2^7 - 1 (127)
     */
    public static final int ASCII_MAX = 127;

    /**
     * is 2^7 (128)
     */
    public static final int NON_ASCII_MIN = 128;

    /**
     * is '?'
     */
    public static final int NON_ASCII_REPLACEMENT = '?';
    private boolean         hasNextChar           = false;
    private int             nextChar              = 0;
    private final Reader    reader;

    /**
     * Constructs a new instance for the given reader.
     *
     * @param reader from which to read
     */
    public AsciiInputStream(final Reader reader) {
        this.reader = Objects.requireNonNull(reader, "reader must not be null");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an {@code int} in the range {@code 0} to
     * {@link #ASCII_MAX}. If no byte is available because the end of the stream
     * has been reached, the value {@code -1} is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * <p>
     * UTF-16 characters above {@link #ASCII_MAX} produce a
     * {@link #NON_ASCII_REPLACEMENT} value when read.
     * <p>
     * Surrogate pairs are correctly counted as a single character and produce a
     * single {@link #NON_ASCII_REPLACEMENT} value when read.
     *
     * @return the next byte of data, or {@code -1} if the end of the
     *         stream is reached.
     * @exception IOException if an I/O error occurs.
     */
    public synchronized int read() throws IOException {

        if (hasNextChar) {
            hasNextChar = false;

            return nextChar;
        }

        final int c = reader.read();

        if (c < 0) {
            return -1;
        }

        if (Character.isHighSurrogate((char) c)) {
            final int nc = reader.read();

            hasNextChar = !Character.isLowSurrogate((char) nc);

            if (hasNextChar) {
                nextChar = nc < NON_ASCII_MIN
                           ? nc & ASCII_MASK
                           : NON_ASCII_REPLACEMENT;
            }
        }

        return c < NON_ASCII_MIN
               ? c & ASCII_MASK
               : NON_ASCII_REPLACEMENT;
    }
}
