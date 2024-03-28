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
import java.io.OutputStream;
import java.io.Writer;

import java.util.Objects;

/**
 * An OutputStream that writes 7-bit US-ASCII values to a Writer, in compliance
 * with the Java US_ASCII Charset decoder.
 * <p>
 * In particular, values greater than {@link #ASCII_MAX} are written as
 * {@link #NON_ASCII_REPLACEMENT}.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.1
 * @since  2.7.1
 */
public class AsciiOutputStream extends OutputStream {

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
     * is '\uFFFD' (65533), the Unicode replacement character.
     * see https://www.fileformat.info/info/unicode/char/fffd/index.htm
     */
    public static final int NON_ASCII_REPLACEMENT = '\uFFFD';
    private final Writer    writer;

    public AsciiOutputStream(Writer writer) {
        this.writer = Objects.requireNonNull(
            writer,
            "writer must not be null.");
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void write(int b) throws IOException {
        writer.write(b < NON_ASCII_MIN
                     ? b & ASCII_MASK
                     : NON_ASCII_REPLACEMENT);
    }
}
