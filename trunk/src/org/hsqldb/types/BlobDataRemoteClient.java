/* Copyright (c) 2001-2007, The HSQL Development Group
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

import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.Trace;
import org.hsqldb.result.ResultLob;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Implementation of BlobData for the client. No binary data is contained
 * here.<p>
 *
 * Instances of this class are contained in a Result rows returned to the
 * clinet.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class BlobDataRemoteClient implements BlobData {

    long       id;
    final long       blobLength;
    SessionInterface session;
    boolean          hasWriter;

    public BlobDataRemoteClient(long id, long length) {
        this.id         = id;
        this.blobLength = length;
    }

    public byte[] getBytes() {
        return null;
    }

    public byte[] getClonedBytes() {
        return null;
    }

    public long length() {
        return blobLength;
    }

    public byte[] getBytes(long pos, int length) throws HsqlException {

        if (!isInLimits(blobLength, pos, length)) {
            throw new IndexOutOfBoundsException();
        }

        ResultLob resultOut =
            ResultLob.newLobGetBytesRequest(id, pos, length);
        ResultLob resultIn =
            (ResultLob) session.execute(resultOut);

        return resultIn.getByteArray();
    }

    public InputStream getBinaryStream() throws HsqlException {
        return new BlobInputStream(this, 0, length());
    }

    public InputStream getBinaryStream(long pos, long length) throws HsqlException {
        return new BlobInputStream(this, pos, length);
    }

    public int setBytes(long pos, byte[] bytes, int offset,
                        int len) throws HsqlException {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BlobDataClient");
    }

    public int setBytes(long pos, byte[] bytes) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BlobDataClient");
    }

    public OutputStream setBinaryStream(long pos) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BlobDataClient");
    }

    public BlobData duplicate() {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BlobDataClient");
    }

    public void truncate(long len) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "BlobDataClient");
    }

    public long position(byte[] pattern, long start) throws HsqlException {

        ResultLob resultOut =
            ResultLob.newLobGetPatternPositionRequest(id, pattern,
                start);
        ResultLob resultIn =
            (ResultLob) session.execute(resultOut);

        return resultIn.getOffset();
    }

    public long position(BlobData pattern, long start) throws HsqlException {

        byte[] bytePattern = pattern.getBytes();

        return position(bytePattern, start);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStreamBlockSize() {
        return 256 * 1024;
    }

    public static BlobDataRemoteClient readBlobDataClient(RowInputInterface in)
    throws HsqlException {

        try {
            long id     = in.readLong();
            long length = in.readLong();

            return new BlobDataRemoteClient(id, length);
        } catch (IOException e) {
            throw Trace.error(Trace.TRANSFER_CORRUPTED);
        }
    }

    public void write(RowOutputInterface out) throws IOException, HsqlException {
        out.writeLong(id);
        out.writeLong(blobLength);
    }

    public boolean isClosed() {
        return false;
    }

    public void free() {}

    public void setSession(SessionInterface session) {
        this.session = session;
    }

    //---
    void checkClosed() throws HsqlException {

        if (isClosed()) {
            throw Trace.error(Trace.BLOB_IS_NO_LONGER_VALID);
        }
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
