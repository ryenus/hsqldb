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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.LineGroupReader;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobDataMemory;

public class LobManager {

    Database database;

    //
    byte[] byteStore = new byte[1024 * 2];

    //
    String   fileName = "/org/hsqldb/resources/lob-schema.sql";
    String[] starters = new String[]{ "/*" };

    //
    int lobBlockSize = 16;

    //
    Table     lobTable;
    Statement getLob;
    Statement getLobPart;
    Statement deleteLobPart;
    Statement createLobPart;

    //
    private final static int BLOCK_ADDR   = 0;
    private final static int BLOCK_COUNT  = 1;
    private final static int BLOCK_OFFSET = 2;
    private final static int LOB_ID       = 3;
    private final static int LOB_LENGTH   = 4;

    //BLOCK_ADDR INT, BLOCK_COUNT INT, TX_ID BIGINT
    private static String initialiseBlocksQuery =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(0, 128, 0)";
    private static String getLobQuery =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ?";
    private static String getLobPartStatement =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET >= ? ORDER BY BLOCK_OFFSET";
    private static String deleteLobPartStatement =
        "DELETE FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET >= ? AND BLOCK_OFFSET < ?";
    private static String insertLobPartStatement =
        "INSERT INTO SYSTEM_LOBS.LOBS (LOB_ID, BLOCK_OFFSET, BLOCK_ADDR, BLOCK_COUNT) VALUES (?, ?, ?, ?)";
    private static String createLobPartStatement =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ? , ?, ?)";

    //    (OUT L_ADDR INT, IN B_COUNT INT, IN B_OFFSET INT, IN L_ID BIGINT, IN L_LENGTH BIGINT)
    public LobManager(Database database) {
        this.database = database;
    }

    void initialise() {

        Session session = database.sessionManager.getSysSession();

        try {
            InputStream fis = getClass().getResourceAsStream(fileName);
            InputStreamReader reader = new InputStreamReader(fis,
                "ISO-8859-1");
            LineNumberReader lineReader = new LineNumberReader(reader);
            LineGroupReader  lg = new LineGroupReader(lineReader, starters);
            HashMappedList   map        = lg.getAsMap();
            String sql = (String) map.get("/*lob_schema_definition*/");
            Result           result     = session.executeDirectStatement(sql);

            result = session.executeDirectStatement(initialiseBlocksQuery);

            session.commit(false);
        } catch (Exception e) {

            //
        }

        try {
            getLob        = session.compileStatement(getLobQuery);
            getLobPart    = session.compileStatement(getLobPartStatement);
            deleteLobPart = session.compileStatement(deleteLobPartStatement);
            createLobPart = session.compileStatement(createLobPartStatement);
        } catch (HsqlException e) {
            String s = e.getMessage();
        }
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

    public BlobData createBlob() {

        BlobData blob     = new BlobDataID(getNewLobId(), 0);
        BlobData blobData = new BinaryData(new byte[0], false);

        blobData.setId(blob.getId());
        dataLobs.put(blob.getId(), blobData);
        lobs.put(blob.getId(), blob);

        return blob;
    }

    public ClobData createClob() {

        ClobData clob     = new ClobDataID(getNewLobId(), 0);
        ClobData clobData = new ClobDataMemory("");

        clobData.setId(clob.getId());
        dataLobs.put(clob.getId(), clobData);
        lobs.put(clob.getId(), clob);

        return clob;
    }

    public long getNewLobId() {
        return lobIdSequence++;
    }

    public Result getChars(Session session, long lobID, long offset,
                           int length) {

        int      blockOffset     = (int) (offset / lobBlockSize);
        int      byteBlockOffset = (int) (offset % lobBlockSize);
        int limit = (int) ((offset + length) / lobBlockSize) - blockOffset;
        ClobData lob             = (ClobData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {
            char[] chars = lob.getChars(offset, length);

            return ResultLob.newLobGetCharsResponse(lobID, offset, chars);
        } catch (HsqlException e) {
            return ResultLob.newErrorResult(e);
        }
    }

// OK but reads possibly large byte[] from store
    public Result getBytes(Session session, long lobID, long offset,
                           int length) {

        int blockOffset     = (int) (offset / lobBlockSize);
        int byteBlockOffset = (int) (offset % lobBlockSize);
        int blockLimit      = (int) ((offset + length) / lobBlockSize);
        int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        int    dataBytesPosition = 0;
        byte[] dataBytes         = new byte[length];
        int[][] blockAddresses = getBlockAddresses(session, lobID,
            blockOffset);

        if (blockAddresses.length == 0) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        //
        int i = 0;
        byte[] bytes = getBlockBytes(lobID, blockAddresses[i][0],
                                     blockAddresses[i][1]);
        int subLength = lobBlockSize - byteBlockOffset;

        if (subLength > length) {
            subLength = length;
        }

        System.arraycopy(bytes, byteBlockOffset, dataBytes, dataBytesPosition,
                         subLength);

        dataBytesPosition += subLength;

        i++;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            bytes = getBlockBytes(lobID, blockAddresses[i][0],
                                  blockAddresses[i][1]);
            subLength = lobBlockSize * blockAddresses[i][1];

            if (subLength > length - dataBytesPosition) {
                subLength = length - dataBytesPosition;
            }

            System.arraycopy(bytes, 0, dataBytes, dataBytesPosition,
                             subLength);

            dataBytesPosition += subLength;
        }

        return ResultLob.newLobGetBytesResponse(lobID, offset, dataBytes);
    }

// todo - needs work - dividing blocks
    public Result setBytes(Session session, long lobID, byte[] dataBytes,
                           long offset, int length) {

        int blockOffset     = (int) (offset / lobBlockSize);
        int byteBlockOffset = (int) (offset % lobBlockSize);
        int blockLimit      = (int) ((offset + length) / lobBlockSize);
        int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        deleteBlockAddresses(session, lobID, blockOffset, blockLimit);
        createBlockAddresses(session, lobID, blockOffset,
                             blockLimit - blockOffset, length);

        int dataBytesPosition = 0;
        int[][] blockAddresses = getBlockAddresses(session, lobID,
            blockOffset);

        //
        int i         = 0;
        int subLength = lobBlockSize - byteBlockOffset;

        if (subLength > length) {
            subLength = length;
        }

        setBlockBytes(lobID, dataBytes, dataBytesPosition, subLength,
                      blockAddresses[i][0]);

        dataBytesPosition += subLength;

        i++;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            subLength = lobBlockSize * blockAddresses[i][1];

            if (subLength > length - dataBytesPosition) {
                subLength = length - dataBytesPosition;
            }

            setBlockBytes(lobID, dataBytes, dataBytesPosition, subLength,
                          blockAddresses[i][0]);
        }

        BlobData lob = (BlobData) lobs.get(lobID);

        ((BlobDataID) lob).setLength(length);

        return ResultLob.newLobSetBytesResponse(lobID);
    }

    public Result setBytes(Session session, long lobID, DataInput dataInput,
                           long length) {

        int blockLimit      = (int) (length / lobBlockSize);
        int byteLimitOffset = (int) (length % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        createBlockAddresses(session, lobID, 0, blockLimit, length);

        long    dataBytesPosition = 0;
        int[][] blockAddresses    = getBlockAddresses(session, lobID, 0);
        int     i                 = 0;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            for (int j = 0; j < blockAddresses[i][1]; j++) {
                int subLength = lobBlockSize;

                if (subLength > length - dataBytesPosition) {
                    subLength = (int) (length - dataBytesPosition);
                }

                byte[] dataBytes = new byte[(int) subLength];

                try {
                    dataInput.readFully(dataBytes, 0, subLength);
                } catch (IOException e) {

                    // deallocate
                    return Result.newErrorResult(e);
                }

                setBlockBytes(lobID, dataBytes, 0, subLength,
                              blockAddresses[i][0] + j);
            }
        }

        BlobData lob = (BlobData) lobs.get(lobID);

        ((BlobDataID) lob).setLength(length);

        return ResultLob.newLobSetBytesResponse(lobID);
    }

    byte[] getBlockBytes(long lobID, int blockAddress, int blockCount) {

        byte[] dataBytes = new byte[blockCount * lobBlockSize];

        System.arraycopy(byteStore, blockAddress * lobBlockSize, dataBytes, 0,
                         blockCount * lobBlockSize);

        return dataBytes;
    }

    void setBlockBytes(long lobID, byte[] dataBytes, int byteOffset,
                       int byteCount, int blockAddress) {
        System.arraycopy(dataBytes, byteOffset, byteStore,
                         blockAddress * lobBlockSize, byteCount);
    }

    int[][] getBlockAddresses(Session session, long lobID, int offset) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);

        Result result = session.executeCompiledStatement(getLobPart, params);
        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[size][3];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = (Object[]) navigator.getCurrent();

            blocks[i][0] = ((Integer) data[BLOCK_ADDR]).intValue();
            blocks[i][1] = ((Integer) data[BLOCK_COUNT]).intValue();
            blocks[i][2] = ((Integer) data[BLOCK_OFFSET]).intValue();
        }

        navigator.close();

        return blocks;
    }

    void deleteBlockAddresses(Session session, long lobID, int offset,
                              int count) {

        ResultMetaData meta     = deleteLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);
        params[2] = Integer.valueOf(count);

        Result result = session.executeCompiledStatement(deleteLobPart,
            params);
    }

    void createBlockAddresses(Session session, long lobID, int offset,
                              int count, long length) {

        ResultMetaData meta     = createLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[BLOCK_ADDR]   = null;
        params[BLOCK_COUNT]  = Integer.valueOf(count);
        params[BLOCK_OFFSET] = Integer.valueOf(offset);
        params[LOB_ID]       = Long.valueOf(lobID);
        params[LOB_LENGTH]   = Long.valueOf(length);

        Result result = session.executeCompiledStatement(createLobPart,
            params);
    }

    public Result setChars(Session session, long lobID, long offset,
                           char[] chars) {

        ClobData lob = (ClobData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {
            lob.setChars(offset, chars, 0, chars.length);

            return ResultLob.newLobSetCharsResponse(lobID);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    public Result setChars(Session session, long lobID, long offset,
                           DataInput dataInput, long length) {

        ClobData lob     = (ClobData) lobs.get(lobID);
        ClobData dataLob = (ClobData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {

            // temp code makes memory lob
            ClobData clobData = new ClobDataMemory(length, dataInput);

            ((ClobDataID) lob).setLength(length);
            dataLob.setChars(offset, clobData.getChars(0, (int) length), 0,
                             (int) length);

            return ResultLob.newLobSetCharsResponse(lobID);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    public Result truncate(Session session, long lobID, long offset) {

        Object lob = dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {
            if (lob instanceof BlobData) {
                ((BlobData) lob).truncate(offset);
            } else {
                ((ClobData) lob).truncate(offset);
            }

            return ResultLob.newLobTruncateResponse(lobID);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }
}
