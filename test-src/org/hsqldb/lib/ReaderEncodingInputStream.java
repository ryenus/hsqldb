package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Objects;

/**
 * Encodes the given reader to an input stream.
 *
 * {@code null} {@code encoding} and {@code charset} constructor parameters are
 * interpreted as{@l ink Charset#defaultCharset()}.
 */
@SuppressWarnings("ClassWithoutLogger")
public class ReaderEncodingInputStream extends InputStream {

    private static final int END_OF_STREAM = -1;

    private final Reader reader;
    private final Charset charset;
    private final CharsetEncoder encoder;
    private final CharBuffer singleCharCharBuffer;
    private final ByteBuffer singleCharByteBuffer;

    /**
     * Constructs a new instance wrapping the given reader.
     * <p>
     * The encoding is the {@link Charset#defaultCharset()}.
     * </p>
     *
     * @param reader that is the source of characters (REQUIRED).
     * @throws IOException          if an I/O error occurs.
     * @throws NullPointerException if reader is {@code null}.
     */
    public ReaderEncodingInputStream(final Reader reader) throws IOException {
        this(reader, Charset.defaultCharset());
    }

    /**
     * Constructs a new instance wrapping the given reader, using the given
     * encoding.
     *
     * @param reader   that is the source of characters (REQUIRED).
     * @param encoding to apply (Optional: null is interpreted as
     *                 {@link Charset#defaultCharset()}.
     * @throws IOException                 if an I/O error occurs.
     * @throws NullPointerException        if reader is {@code null}.
     * @throws IllegalCharsetNameException If the given encoding name is
     *                                     illegal.
     * @throws UnsupportedCharsetException If no support for the named encoding
     *                                     is available in this instance of the
     *                                     Java virtual machine.
     */
    public ReaderEncodingInputStream(final Reader reader, final String encoding)
            throws IOException, IllegalCharsetNameException,
            UnsupportedCharsetException {
        this(reader, encoding == null
                ? Charset.defaultCharset()
                : Charset.forName(encoding));
    }

    /**
     * Constructs a new instance wrapping the given reader, using the given
     * Charset.
     *
     * @param reader  that is the source of characters (REQUIRED).
     * @param charset to apply (Optional: null is interpreted as
     *                {@link Charset#defaultCharset()}.
     * @throws IOException          if an I/O error occurs.
     * @throws NullPointerException if reader is {@code null}.
     */
    public ReaderEncodingInputStream(final Reader reader, final Charset charset)
            throws IOException {
        this.reader = Objects.requireNonNull(reader,
                "reader must not be null.");
        this.charset = charset == null
                ? Charset.defaultCharset()
                : charset;
        this.encoder = this.charset.newEncoder()
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .onMalformedInput(CodingErrorAction.REPLACE);
        this.singleCharCharBuffer = CharBuffer.allocate(1);
        this.singleCharByteBuffer = ByteBuffer.allocate(
                (int) Math.ceil(this.encoder.maxBytesPerChar()));
        // set "empty" : avoid phantom iniitial read of (char)0;
        this.singleCharByteBuffer.limit(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(final byte[] buff, final int offset, final int length)
            throws IOException {
        if (buff == null) {
            throw new NullPointerException("buff must not be null.");
        } else if (offset < 0 || length < 0 || length > buff.length - offset) {
            throw new IndexOutOfBoundsException(String.format(
                    "buff.length: %s, offset: %s, length: %s",
                    buff.length,
                    offset,
                    length));
        } else if (length == 0) {
            return 0;
        }
        // left over bytes from any previous read();
        final ByteBuffer scbb = this.singleCharByteBuffer;
        final int remaining = scbb.remaining();
        int total;
        if (remaining > 0) {
            total = Math.min(remaining, length);
            scbb.put(buff, offset, total);
            return total;
        }
        final CharsetEncoder enc = this.encoder;
        float maxBytesPerChar = enc.maxBytesPerChar();
        // length <= maxBytesPerChar => avoid under /overflow
        if (length <= maxBytesPerChar) {
            int b = read();
            if (b == END_OF_STREAM) {
                total = END_OF_STREAM;
            } else {
                buff[offset] = (byte) b;
                total = 1;
            }
            return total;
        }
        //  avoid under /overflow
        float absMaxChars = length / maxBytesPerChar;
        final int maxChars = (int) Math.ceil(absMaxChars - 1);
        final char[] cbuf = new char[maxChars];
        final int charsRead = this.reader.read(cbuf);
        if (charsRead <= 0) {
            total = charsRead;
        } else {
            final ByteBuffer wbb = ByteBuffer.wrap(buff, offset, length);
            final CharBuffer wcb = CharBuffer.wrap(cbuf, 0, charsRead);
            // we can do this safely, based on how we set up maxChars.
            boolean endOfInput = true;
            final CoderResult cr = enc.encode(wcb, wbb, endOfInput);
            if (cr.isError()) {
                cr.throwException();
            }
            total = wbb.position();
        }
        return total;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
        int result;
        final ByteBuffer scbb = this.singleCharByteBuffer;
        if (scbb.hasRemaining()) {
            result = scbb.get();
        } else {
            final int ch = reader.read();
            if (ch == END_OF_STREAM) {
                result = END_OF_STREAM;
            } else {
                final CharBuffer sccb = this.singleCharCharBuffer;
                sccb.clear();
                scbb.clear();
                sccb.put((char) ch);
                sccb.flip();
                final CoderResult cr = this.encoder.encode(sccb, scbb, true);
                if (cr.isError()) {
                    cr.throwException();
                }
                scbb.flip();
                result = scbb.get();
            }
        }
        return result;
    }
}
