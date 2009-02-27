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
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobDataMemory;

public class LobManager {

    Database  database;

    public LobManager(Database database) {
        this.database = database;
    }

    void initialise() {
    }

    long           lobIdSequence = 1;
    LongKeyHashMap lobs          = new LongKeyHashMap();
    LongKeyHashMap dataLobs      = new LongKeyHashMap();

    ClobData getClob(long id) {
        return (ClobData) lobs.get(id);
    }

    BlobData getBlob(long id) {
        return (BlobData) lobs.get(id);
    }

    // temp stuff
    ClobData getClobData(long id) {
        return (ClobData) dataLobs.get(id);
    }

    BlobData getBlobData(long id) {
        return (BlobData) dataLobs.get(id);
    }

    public BlobData createBlob() {

        BlobData blob = new BlobDataID(getNewLobId(), 0);

        BlobData blobData = new BinaryData(new byte[0], false);

        blobData.setId(blob.getId());
        dataLobs.put(blob.getId(), blobData);
        lobs.put(blob.getId(), blob);

        return blob;
    }

    public ClobData createClob() {

        ClobData clob = new ClobDataID(getNewLobId(), 0);

        ClobData clobData = new ClobDataMemory("");

        clobData.setId(clob.getId());
        dataLobs.put(clob.getId(), clobData);
        lobs.put(clob.getId(), clob);

        return clob;
    }

    public long getNewLobId() {
        return lobIdSequence++;
    }

    Result performLOBOperation(ResultLob cmd) {

        long id        = cmd.getLobID();
        int  operation = cmd.getSubType();

        switch (operation) {

            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES :
                return ResultLob.newLobCreateBlobResponse(id);

            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS :
                return ResultLob.newLobCreateClobResponse(id);
        }

        // temp code using data lob
        Object lob = dataLobs.get(id);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        switch (operation) {

            case ResultLob.LobResultTypes.REQUEST_OPEN :
            case ResultLob.LobResultTypes.REQUEST_CLOSE : {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTES : {
                try {
                    byte[] bytes = ((BlobData) lob).getBytes(cmd.getOffset(),
                        (int) cmd.getBlockLength());

                    return ResultLob.newLobGetBytesResponse(id,
                            cmd.getOffset(), bytes);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {
                try {
                    ((BlobData) lob).setBytes(cmd.getOffset(),
                                              cmd.getByteArray());

                    return ResultLob.newLobSetBytesResponse(id);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_GET_CHARS : {
                try {
                    char[] chars = ((ClobData) lob).getChars(cmd.getOffset(),
                        (int) cmd.getBlockLength());

                    return ResultLob.newLobGetCharsResponse(id,
                            cmd.getOffset(), chars);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {
                try {
                    char[] chars = cmd.getCharArray();

                    ((ClobData) lob).setChars(cmd.getOffset(), chars, 0,
                                              chars.length);

                    return ResultLob.newLobSetCharsResponse(id);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
            case ResultLob.LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION : {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }
    }
}
