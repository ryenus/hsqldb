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

import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;

public class BlobDataID implements BlobData {

    long       id;
    long length;

    public BlobDataID() {    }

    public BlobData duplicate() throws HsqlException {
        return null;
    }

    public void free() {}

    public InputStream getBinaryStream() throws HsqlException {
        return null;
    }

    public InputStream getBinaryStream(long pos,
                                       long length) throws HsqlException {
        return null;
    }

    public byte[] getBytes() {
        return null;
    }

    public byte[] getBytes(long pos, int length) throws HsqlException {
        return null;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStreamBlockSize() {
        return 0;
    }

    public boolean isClosed() {
        return false;
    }

    public long length() {
        return length;
    }

    public long bitLength() {
        return length * 8;
    }

    public boolean isBits() {
        return false;
    }

    public long position(BlobData pattern, long start) throws HsqlException {
        return 0L;
    }

    public long position(byte[] pattern, long start) throws HsqlException {
        return 0L;
    }

    public long nonZeroLength() {
        return length;
    }

    public OutputStream setBinaryStream(long pos) throws HsqlException {
        return null;
    }

    public int setBytes(long pos, byte[] bytes, int offset,
                        int len) throws HsqlException {
        return 0;
    }

    public int setBytes(long pos, byte[] bytes) throws HsqlException {
        return 0;
    }

    public long setBinaryStream(long pos, InputStream in) {
        return 0;
    }

    public void setSession(SessionInterface session) {}

    public void truncate(long len) throws HsqlException {}

    public byte getBlobType() {
        return 0;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
