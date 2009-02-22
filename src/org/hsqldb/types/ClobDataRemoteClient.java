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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.result.ResultLob;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Implementation of CLOB for client side.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ClobDataRemoteClient implements ClobData {

    long             id;
    final long       length;
    SessionInterface session;
    boolean          hasWriter;

    public ClobDataRemoteClient(long id, long length) {
        this.id     = id;
        this.length = length;
    }

    public void free() {}

    public InputStream getAsciiStream() {
        return null;
    }

    public Reader getCharacterStream(long pos, long length) {
        return null;
    }

    public Reader getCharacterStream() {
        return null;
    }

    public char[] getChars(final long position,
                           int length) throws HsqlException {

        if (!isInLimits(this.length, position, length)) {
            throw new IndexOutOfBoundsException();
        }

        ResultLob resultOut = ResultLob.newLobGetCharsRequest(id, position,
            length);
        ResultLob resultIn = (ResultLob) session.execute(resultOut);

        return resultIn.getCharArray();
    }

    public char[] getClonedChars() {
        return null;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSubString(long position,
                               int length) throws HsqlException {
        return new String(getChars(position, length));
    }

    public long length() {
        return length;
    }

    public long position(String searchstr, long start) throws HsqlException {
        return 0L;
    }

    public long position(ClobData searchstr, long start) {
        return 0L;
    }

    public long nonSpaceLength() {
        throw Error.runtimeError(ErrorCode.U_S0500, "ClobDataClient");
    }

    public OutputStream setAsciiStream(long pos) {
        return null;
    }

    public Writer setCharacterStream(long pos) {
        return null;
    }

    public int setString(long pos, String str) throws HsqlException {
        throw Error.runtimeError(ErrorCode.U_S0500, "ClobDataClient");
    }

    public int setString(long pos, String str, int offset,
                         int len) throws HsqlException {
        throw Error.runtimeError(ErrorCode.U_S0500, "ClobDataClient");
    }

    public int setChars(long pos, char[] chars, int offset,
                        int len) throws HsqlException {
        throw Error.runtimeError(ErrorCode.U_S0500, "ClobDataClient");
    }

    public void truncate(long len) throws HsqlException {
        throw Error.runtimeError(ErrorCode.U_S0500, "ClobDataClient");
    }

    public int getStreamBlockSize() {
        return 256 * 1024;
    }

    public long getRightTrimSize() {
        return 0;
    }

    public byte getClobType() {
        return 0;
    }

    public void setSession(SessionInterface session) {
        this.session = session;
    }

    public static ClobDataRemoteClient readClobDataClient(RowInputInterface in)
    throws HsqlException {

        try {
            long id     = in.readLong();
            long length = in.readLong();

            return new ClobDataRemoteClient(id, length);
        } catch (IOException e) {
            throw Error.error(ErrorCode.SERVER_TRANSFER_CORRUPTED);
        }
    }

    public void write(RowOutputInterface out)
    throws IOException, HsqlException {
        out.writeLong(id);
        out.writeLong(length);
    }

    //---
    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }

    public boolean isClosed() {
        return false;
    }

    void checkClosed() throws HsqlException {

        if (isClosed()) {
            throw Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID);
        }
    }
}
