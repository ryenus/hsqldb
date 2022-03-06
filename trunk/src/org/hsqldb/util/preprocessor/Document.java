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
package org.hsqldb.util.preprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/*
 * $Id$
 */
/**
 * Simple line-oriented text document ADT.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.6.2+
 * @since 1.8.1
 */
@SuppressWarnings("ClassWithoutLogger")
public class Document {

    /**
     * for the given character set name.
     *
     * @param charsetName to test; {@code null} returns {@code false}.
     * @return {@code true} if supported, else {@code false}.
     */
    public static boolean isSupportedCharset(final String charsetName) {
        return charsetName != null
                && !charsetName.trim().isEmpty()
                && Charset.isSupported(charsetName);
    }

    private final List<String> lines;

    /**
     * constructs a new, empty instance.
     */
    public Document() {
        this.lines = new ArrayList<>(16);
    }

    /**
     * Constructs a new instance that is effectively a copy of {@code source}.
     *
     * @param source to copy; may be {@code null}, which is treated like an
     *               {@code empty} instance.
     */
    public Document(final Document source) {
        this();
        if (source != null) {
            final Document target = this;
            target.appendDocument(source);
        }
    }

    /**
     * to this instance.
     *
     * @param line to add; must not be {@code null}.
     * @return this instance.
     * @throws IllegalArgumentException if {@code line} is {@code null}.
     */
    public Document addSouceLine(final String line) {
        if (line == null) {
            throw new IllegalArgumentException("line: null");
        }
        this.lines.add(line);
        return this;
    }

    /**
     * to this instance.
     *
     * @param source to append; may be {@code null}, which is treated like an
     *               {@code empty} instance.
     * @return this instance
     */
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public Document appendDocument(final Document source) {
        if (source != null && source.size() > 0) {
            this.lines.addAll(source.lines);
        }
        return this;
    }

    /**
     * this instance, making it {@link #isEmpty() empty}.
     *
     * @return this instance.
     */
    public Document clear() {
        this.lines.clear();
        return this;
    }

    /**
     * if at least one line in this instance contains the given sequence.
     * <p>
     * <b>Note</b>: each line is tested separately; no test is performed to see
     * if the sequence spans more than one line.
     * </p>
     *
     * @param sequence to test.
     * @return {@code true} if the condition holds, else {@code false}.
     */
    public boolean contains(final CharSequence sequence) {
        if (sequence == null) {
            return false;
        }
        final String toFind = sequence.toString();
        final List<String> list = this.lines;
        final int size = list.size();
        for (int i = 0; i < size; i++) {
            if (-1 < list.get(i).indexOf(toFind, 0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * from this instance at the given index.
     *
     * @param index at which to remove a line.
     * @return this instance.
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   ({@code index < 0 || index >= size())}
     */
    public Document deleteSourceLine(final int index) {
        this.lines.remove(index);
        return this;
    }

    /**
     * if the given document has effectively the same content as this instance.
     *
     * @param document to test; may be {@code null}, which is treated as
     *                 {@link #isEmpty() empty}.
     * @return {@code true} if the content of the given {@code document} is
     *         equal to the content of this instance, as per
     *         {@link List#equals(Object)}.
     */
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public boolean contentEquals(final Document document) {
        return (document == this)
                ? true
                : document == null
                        ? this.isEmpty()
                        : this.lines.equals(document.lines);
    }

    /**
     * at the given index.
     *
     * @param index at which to retrieve the line.
     * @return the line at the given index.
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   ({@code index < 0 || index >= size()})
     */
    public String getSourceLine(final int index) {
        return this.lines.get(index);
    }

    /**
     * if this instance contains no lines.
     *
     * @return {@code true} if {@link #size()} {@code == 0}, else {@code false}.
     */
    public boolean isEmpty() {
        return this.lines.isEmpty();
    }

    /**
     * at the specified index.
     *
     * @param index at which the specified line is to be inserted
     * @param line  to be inserted
     * @return this instance
     * @throws NullPointerException      if the specified line is {@code null}.
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   ({@code index < 0 || index >= size()})
     */
    public Document insertSourceLine(final int index, final String line) {
        if (line == null) {
            throw new IllegalArgumentException("line: null");
        }
        this.lines.add(index, line);
        return this;
    }

    /**
     * as if by invoking {@code this.clear().appendDocument(source)}.
     *
     * @param source content used to replace the existing content of this
     *               instance; may be {@code null}, which is treated as
     *               {@link #isEmpty() empty}.
     * @return this instance.
     */
    public Document replaceWith(final Document source) {
        return this.clear().appendDocument(source);
    }

    /**
     * at the specified index in this instance.
     *
     * @param index index of the line to replace
     * @param line  to be stored at the specified index
     * @return this instance.
     * @throws NullPointerException      if the specified line is {@code null}.
     * @throws IndexOutOfBoundsException if the index is out of range
     *                                   ({@code index < 0 || index >= size()})
     */
    public Document setSourceLine(final int index, final String line) {
        if (line == null) {
            throw new IllegalArgumentException("null");
        }
        this.lines.set(index, line);
        return this;
    }

    /**
     * of this instance, as the number of lines.
     *
     * @return the number of lines in this instance.
     */
    public int size() {
        return this.lines.size();
    }

// ------------------------ I/O convenience methods ----------------------------
    /**
     * this instance from the given input stream using the given character
     * encoding.
     * <p>
     * <b>Note</b>: it is the responsibility of the caller to close the input
     * stream.
     * </p>
     *
     * @param inputStream from which to load; must not be {@code null}.
     * @param encoding    {@code null} for the system default.
     * @return this object.
     * @throws IOException              if an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     * @throws NullPointerException     if the inputStream is {@code null}.
     */
    public Document load(final InputStream inputStream, final String encoding) throws IOException,
            IllegalArgumentException, NullPointerException {
        return load((Object) inputStream, encoding);
    }

    /**
     * this instance from the given reader.
     * <p>
     * <b>Note</b>: it is the responsibility of the caller to close the reader.
     * </p>
     *
     * @param reader from which to load; must not be {@code null}.
     * @return this object.
     * @throws IOException          in an I/O error occurs.
     * @throws NullPointerException if the reader is {@code null}.
     */
    public Document load(final Reader reader) throws IOException,
            NullPointerException {
        return load(reader, null);
    }

    /**
     * this instance from the given file using the given character encoding.
     * <p>
     * <b>Note</b>: the underlying file input stream is closed automatically.
     * </p>
     *
     * @param file     from which to load; {@code null} is treated the empty
     *                 path.
     * @param encoding {@code null} for the system default.
     * @return this object.
     * @throws IOException              in an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     */
    public Document load(final File file, final String encoding) throws IOException,
            IllegalArgumentException {
        return load((Object) (file == null ? new File("") : file), encoding);
    }

    /**
     * this instance from the given abstract file path using the given character
     * encoding.
     * <p>
     * <b>Note</b>: the underlying file input stream is closed automatically.
     * </p>
     *
     * @param path     from which to load; {@code null} is treated the empty
     *                 path.
     * @param encoding {@code null} for the system default.
     * @return this object.
     * @throws IOException              in an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     */
    public Document load(final String path, final String encoding) throws IOException,
            IllegalArgumentException {
        return load((Object) (path == null ? "" : path), encoding);
    }

    /**
     * this instance from the given source.
     * <p>
     * <b>Note</b>: if the source is an InputStream or Reader, it is the
     * responsibility of the caller to close; otherwise, the underlying file
     * input stream is closed automatically.
     * </p>
     *
     * @param source   required; by default, must be an instance of an
     *                 InputStream, Reader, File, or String denoting an
     *                 abstract file path.
     * @param encoding optional; ignored if source is a Writer. {@code null} is
     *                 taken to be {@link Charset#defaultCharset()}.
     * @return this instance.
     * @throws IOException              if an I/O error occurs.
     * @throws NullPointerException     if source is {@code null}.
     * @throws IllegalArgumentException if the encoding is not supported.
     */
    @SuppressWarnings("NestedAssignment")
    protected Document load(final Object source, final String encoding) throws IOException,
            NullPointerException,
            IllegalArgumentException {
        if (source == null) {
            throw new NullPointerException("source must not be null.");
        }
        Charset charset = (source instanceof Reader || encoding == null)
                ? Charset.defaultCharset() : null;
        if (charset == null && isSupportedCharset(encoding)) {
            charset = Charset.forName(encoding);
        } else {
            throw new IllegalArgumentException("encoding: " + encoding);
        }
        BufferedReader reader = null;
        boolean close = false;
        if (source instanceof InputStream) {
            final InputStream is = (InputStream) source;
            final InputStreamReader isr = new InputStreamReader(is, charset);
            reader = new BufferedReader(isr);
        } else if (source instanceof File) {
            final InputStream fis = new FileInputStream((File) source);
            final InputStreamReader isr = new InputStreamReader(fis, charset);
            close = true;
            reader = new BufferedReader(isr);
        } else if (source instanceof String) {
            final InputStream fis = new FileInputStream((String) source);
            final InputStreamReader isr = new InputStreamReader(fis, charset);
            close = true;
            reader = new BufferedReader(isr);
        } else if (source instanceof BufferedReader) {
            reader = (BufferedReader) source;
        } else if (source instanceof Reader) {
            reader = new BufferedReader((Reader) source);
        } else {
            throw new IOException("unhandled load source: " + source); // NOI18N
        }
        clear();
        String line;
        final List<String> list = this.lines;
        try {
            while (null != (line = reader.readLine())) {
                list.add(line);
            }
        } finally {
            if (close) {
                try {
                    reader.close();
                } catch (IOException ex) {
                }
            }
        }
        return this;
    }

    /**
     * to the given output stream using the given character encoding.
     * <p>
     * <b>Note</b>: it is the responsibility of the caller to close the output
     * stream.
     * </p>
     *
     * @param outputStream to which to save; must not be {@code null}.
     * @param encoding     {@code null} for the system default.
     * @return this object.
     * @throws IOException              if an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     * @throws NullPointerException     if the output stream is {
     * @null}.
     */
    public Document save(final OutputStream outputStream, final String encoding) throws IOException {
        return save((Object) outputStream, encoding);
    }

    /**
     * to the given file using the given character encoding.
     *
     * @param file     to which to save; {@code null} is treated as having an
     *                 empty path
     * @param encoding {@code null} for the system default.
     * @return this object.
     * @throws IOException              if an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     */
    public Document save(final File file, final String encoding) throws IOException,
            IllegalArgumentException {
        return save((Object) (file == null ? new File("") : file), encoding);
    }

    /**
     * to the given abstract file path using the given character encoding.
     *
     * @param path     to which to save; {@code null} is treated as the empty
     *                 path;
     * @param encoding {@code null} for the system default.
     * @return this object.
     * @throws IOException              if an I/O error occurs.
     * @throws IllegalArgumentException if the encoding is not supported.
     */
    public Document save(final String path, final String encoding) throws IOException,
            IllegalArgumentException {
        return save((Object) (path == null ? "" : path), encoding);
    }

    /**
     * to the given writer.
     * <p>
     * <b>Note</b>: it is the responsibility of the caller to close the writer.
     * </p>
     *
     * @param writer to which to save; must not be {@code null}.
     * @return this object.
     * @throws IOException          if an I/O error occurs.
     * @throws NullPointerException if the writer is {
     * @null}.
     */
    public Document save(final Writer writer) throws IOException {
        return save(writer, null);
    }

    /**
     * this instance to the given target.
     * <p>
     * <b>Note</b>: if the target is an OutputStream or Writer, it is the
     * responsibility of the caller to close; otherwise, the underlying file
     * output stream is closed automatically.
     * </p>
     *
     * @param target   required; by default, must be an instance of an
     *                 OutputStream, Writer, File, or String denoting an
     *                 abstract file path.
     * @param encoding optional; does not apply to Writer. {@code null} is taken
     *                 to be {@link Charset#defaultCharset()}.
     * @return this instance.
     * @throws IOException          if an I/O error occurs.
     * @throws NullPointerException if target is {@code null}.
     */
    protected Document save(final Object target, final String encoding) throws IOException {
        if (target == null) {
            throw new NullPointerException("target must not be null.");
        }
        Charset charset = (target instanceof Writer || encoding == null)
                ? Charset.defaultCharset() : null;
        if (charset == null && isSupportedCharset(encoding)) {
            charset = Charset.forName(encoding);
        } else {
            throw new IllegalArgumentException("encoding: " + encoding);
        }
        BufferedWriter writer = null;
        boolean close = false;
        if (target instanceof OutputStream) {
            final OutputStream os = (OutputStream) target;
            final OutputStreamWriter osw = new OutputStreamWriter(os, charset);
            writer = new BufferedWriter(osw);
        } else if (target instanceof File) {
            final OutputStream fos = new FileOutputStream((File) target);
            final OutputStreamWriter osw = new OutputStreamWriter(fos, charset);
            close = true;
            writer = new BufferedWriter(osw);
        } else if (target instanceof String) {
            OutputStream fos = new FileOutputStream((String) target);
            final OutputStreamWriter osw = new OutputStreamWriter(fos, charset);
            close = true;
            writer = new BufferedWriter(osw);
        } else if (target instanceof BufferedWriter) {
            writer = (BufferedWriter) target;
        } else if (target instanceof Writer) {
            writer = new BufferedWriter(writer);
        } else {
            throw new IOException("unhandled save target: " + target);
        }
        final List<String> list = this.lines;
        final int count = list.size();
        try {
            for (int i = 0; i < count; i++) {
                writer.write(list.get(i));
                writer.newLine();
            }
            writer.flush();
        } finally {
            if (close) {
                try {
                    writer.close();
                } catch (IOException ignored) {
                }
            }
        }
        return this;
    }
}
