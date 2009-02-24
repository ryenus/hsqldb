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


package org.hsqldb;

import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.ClobDataMemory;

public class LobManager {

    Database database;

    public LobManager(Database database) {
        this.database = database;
    }

    void initialise() {}

    long           lobIdSequence = 1;
    LongKeyHashMap lobs          = new LongKeyHashMap();

    ClobData getClob(long id) {
        return (ClobData) lobs.get(id);
    }

    BlobData getBlob(long id) {
        return (BlobData) lobs.get(id);
    }

    public Object getLob(long id) {
        return lobs.get(id);
    }

    public void addBlob(BlobData blob) {
        lobs.put(blob.getId(), blob);
    }

    public void addClob(ClobData clob) {
        lobs.put(clob.getId(), clob);
    }

    public BlobData createBlob() {

        BlobData blob = new BinaryData(new byte[0], 0);

        blob.setId(getNewLobId());

        return blob;
    }

    public ClobData createClob() {

        ClobData clob = new ClobDataMemory("");

        clob.setId(getNewLobId());

        return clob;
    }

    public long getNewLobId() {
        return lobIdSequence++;
    }
}
