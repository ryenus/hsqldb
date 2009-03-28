/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.types;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.lib.ArrayUtil;

/**
 * Implementation of CLOB for memory character data.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ClobDataMemory implements ClobData {

    long   id;
    char[] data;

    public ClobDataMemory(char[] data, boolean clone) {

        if (clone) {
            data = (char[]) ArrayUtil.duplicateArray(data);
        }

        this.data = data;
    }

    public ClobDataMemory(String data) {
        this.data = data.toCharArray();
    }

    public ClobDataMemory(long length, Reader reader) throws HsqlException {

        int offset = 0;
        int count  = (int) length;

        data = new char[(int) length];

        try {
            while (true) {
                int read = reader.read(data, offset, count);

                if (read == -1) {
                    throw new IOException();
                }

                count  -= read;
                offset += read;

                if (count == 0) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new HsqlException(e, null, 0);
        }
    }

    public ClobDataMemory(long length, DataInput input) throws HsqlException {

        try {
            data = new char[(int) length];

            for (int i = 0; i < data.length; i++) {
                data[i] = input.readChar();
            }
        } catch (IOException e) {
            throw new HsqlException(e, null, 0);
        }
    }

    public char[] getChars(SessionInterface sessionInterface,
                           final long position, int length) {

        if (!isInLimits(data.length, position, length)) {
            throw new IndexOutOfBoundsException();
        }

        char[] c = new char[length];

        System.arraycopy(data, (int) position, c, 0, length);

        return c;
    }

    public long length(SessionInterface session) {
        return data.length;
    }

    public String getSubString(SessionInterface session, long offset,
                               int length) {
        return new String(data, (int) offset, length);
    }

    public ClobData getSubString(SessionInterface session, long offset,
                               long length) {
        return null;
    }

    public String toString() {
        return new String(data);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void truncate(SessionInterface session, long len) {}

    public Reader getCharacterStream(SessionInterface session) {
        return null;
    }

    public Reader getCharacterStream(SessionInterface session, long pos,
                                     long length) {
        return null;
    }

    public long setCharacterStream(SessionInterface session, long pos,
                                   Reader in) {
        return 0;
    }

    public int setString(SessionInterface session, long pos, String str) {
        return setString(session, 0, str, 0, str.length());
    }

    public int setChars(SessionInterface session, long pos, char[] chars,
                        int offset, int length) {

        if (!isInLimits(data.length, pos, 0)) {
            throw new IndexOutOfBoundsException();
        }

        if (!isInLimits(data.length, pos, length)) {
            data = (char[]) ArrayUtil.resizeArray(data, (int) pos + length);
        }

        System.arraycopy(chars, offset, data, (int) pos, length);

        return length;
    }

    public int setString(SessionInterface session, long pos, String str,
                         int offset, int length) {

        char[] chars = str.toCharArray();

        return setChars(session, pos, chars, offset, length);
    }

    public long position(SessionInterface session, String searchstr,
                         long start) {
        return 0L;
    }

    public long position(SessionInterface session, ClobData searchstr,
                         long start) {
        return 0L;
    }

    // temp
    public long nonSpaceLength(SessionInterface session) {
        return data.length;
    }

    /** @todo - implement */
    public long getRightTrimSize(SessionInterface session) {
        return 0;
    }

    public byte getClobType() {
        return 2;
    }

    //---
    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
