/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb.testbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.NoSuchElementException;
import org.hsqldb.lib.Iterator;

/**
 * Retrieves line-oriented, semicolon terminated character sequence segments
 * from a BufferedReader or URL. <p>
 *
 * Ignores lines starting with '//' and '--', as well as lines consisting only
 * of whitespace.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class ScriptIterator implements Iterator {
    private static final String SLASH_COMMENT = "//";
    private static final String DASH_COMMENT = "--";
    private static final String SEMI    = ";";

    private String         segment;
    private BufferedReader reader;

    /**
     * Constructs a new ScriptIterator.
     *
     * @param reader from which to read SQL statements
     */
    public ScriptIterator(BufferedReader reader) {
        this.reader = reader;
    }

    /**
     * Constructs a new ScriptIterator.
     *
     * @param url from which to read SQL statements
     * @throws IOException
     */
    public ScriptIterator(URL url) throws IOException {
        this(new BufferedReader(new InputStreamReader(url.openStream())));
    }

    /**
     * Silent cleanup.
     */
    private void closeReader() {
        if (this.reader != null){
            try{
                this.reader.close();
            } catch(Exception e){}
        }

        this.reader = null;
    }

    /**
     * Retrieves whether there is an SQL segment available.
     *
     * @return true if there is an SQL segment available
     * @throws java.lang.RuntimeException if an internal IOException occurs
     */
    @SuppressWarnings("StringBufferWithoutInitialCapacity")
    public boolean hasNext() throws RuntimeException {
        String       line;
        StringBuilder sb;

        if (this.reader == null) {
            return false;
        } else if (this.segment == null) {
            sb   = null;
            line = null;

            while(true) {
                try {
                    line = this.reader.readLine();
                } catch (IOException ioe) {
                    closeReader();

                    throw new RuntimeException(ioe);
                }

                if (line == null) {
                    closeReader();

                    break;
                }

                String trimmed = line.trim();

                if ( (trimmed.length() == 0)
                   || trimmed.startsWith(SLASH_COMMENT)
                   || trimmed.startsWith(DASH_COMMENT)) {
                    continue;
                }

                if (sb == null) {
                    sb = new StringBuilder();
                }

                sb.append(line);

                if (trimmed.endsWith(SEMI)) {
                    this.segment = sb.toString();

                    break;
                } else {
                    sb.append('\n');
                }
            }
        }

        return (this.segment != null);
    }

    /**
     * Retrieves the next available SQL segment as a String.
     *
     * @return the next available SQL segment
     * @throws java.util.NoSuchElementException if there is
     * 	    no available SQL segment
     */
    public Object next() throws NoSuchElementException {
        String out = null;

        if (this.hasNext()) {
            out          = this.segment;
            this.segment = null;
        }

        if (out == null) {
            throw new NoSuchElementException();
        }

        return out;
    }

    /**
     * Unsupported.
     *
     * @return nothing
     * @throws NoSuchElementException never
     * @throws java.lang.UnsupportedOperationException always
     */
    public int nextInt() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported.
     *
     * @return nothing
     * @throws NoSuchElementException never
     * @throws java.lang.UnsupportedOperationException always
     */
    public long nextLong() throws NoSuchElementException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported.
     *
     * @throws java.lang.UnsupportedOperationException always
     */
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported.
     *
     * @param object ignored
     * @throws java.lang.UnsupportedOperationException always
     */
    public void setValue(Object object) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
}
