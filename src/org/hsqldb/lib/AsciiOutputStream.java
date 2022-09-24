package org.hsqldb.lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Objects;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
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
    public static final int ASCII_MAX =  127;
    /**
     * is 2^7  (128) 
     */
    public static final int NON_ASCII_MIN = 128;
    /**
     * is '\uFFFD' (65533), the Unicode replacement character.
     * 
     * @see https://www.fileformat.info/info/unicode/char/fffd/index.htm
     */
    private static final int NON_ASCII_REPLACEMENT = '\uFFFD';
    private final Writer writer;

    public AsciiOutputStream(Writer writer) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null.");
    }

    @Override
    public void write(int b) throws IOException {
        writer.write(b < NON_ASCII_MIN ? b & ASCII_MASK : NON_ASCII_REPLACEMENT);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

}
