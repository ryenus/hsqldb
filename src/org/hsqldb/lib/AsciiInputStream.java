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
    public static final int ASCII_MAX =  127;
    /**
     * is 2^7  (128) 
     */
    public static final int NON_ASCII_MIN = 128;
    /**
     * is '?'
     */
    public static final int NON_ASCII_REPLACEMENT = '?';
    //
    private final Reader reader;
    private boolean hasNextChar = false;
    private int nextChar = 0;

    /**
     * Constructs a new instance for the given reader.
     *
     * @param reader from which to read
     */
    public AsciiInputStream(final Reader reader) {
        this.reader = Objects.requireNonNull(reader, "reader must not be null");
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * {@link #ASCII_MAX}. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * <p>
     * UTF-16 characters above {@link #ASCII_MAX} produce a
     * {@link #NON_ASCII_REPLACEMENT} value when read.
     * <p>
     * Surrogate pairs are correctly counted as a single character and produce a
     * single {@link #NON_ASCII_REPLACEMENT} value when read.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
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

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
