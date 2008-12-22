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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;

/**
 * Interface for Character Large Object implementations.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public interface ClobData {

    char[] getChars(final long position, int length) throws HsqlException;

    char[] getClonedChars();

    long length();

    String getSubString(final long pos, final int length) throws HsqlException;

    void truncate(long len) throws HsqlException;

    InputStream getAsciiStream() throws HsqlException;

    OutputStream setAsciiStream(long pos);

    Reader getCharacterStream() throws HsqlException;

    Writer setCharacterStream(long pos) throws HsqlException;

    int setString(long pos, String str) throws HsqlException;

    int setString(long pos, String str, int offset,
                  int len) throws HsqlException;

    int setChars(long pos, char[] chars, int offset,
                 int len) throws HsqlException;

    long position(String searchstr, long start) throws HsqlException;

    long position(ClobData searchstr, long start) throws HsqlException;

    long nonSpaceLength();

    Reader getCharacterStream(long pos, long length);

    long getId();

    void setId(long id);

    void free();

    boolean isClosed();

    void setSession(SessionInterface session);

    int getStreamBlockSize();

    long getRightTrimSize();
}
