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
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.navigator.RowSetNavigator;

import java.io.InputStream;
import java.io.Reader;
import java.io.LineNumberReader;
import java.io.InputStreamReader;

import org.hsqldb.lib.LineGroupReader;

import java.io.IOException;

import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HashMappedList;

import java.io.DataInput;

public class LobManager {

    Database database;

    //
    String   fileName = "/org/hsqldb/resources/lob-schema.sql";
    String[] starters = new String[]{ "/*" };

    //
    int diskBlockSize = 16 * 1024;

    //
    Table     lobTable;
    Statement getLob;
    Statement getLobPart;
    Statement deleteLobPart;
    Statement insertLobPart;

    //
    private final static int BLOCK_ADDR  = 0;
    private final static int BLOCK_COUNT = 1;
    private final static int BLOCK_ORDER = 2;
    private final static int LOB_ID      = 3;
    private final static int LOB_LENGTH  = 4;
    private final static int LOB_TYPE    = 5;

    //
    private static String getLobQuery = "SELECT * FROM LOBS WHERE LOB_ID = ?";
    private static String getLobPartStatement =
        "SELECT * FROM LOBS WHERE LOB_ID = ? ORDER BY BLOCK_ORDER OFFSET ? FETCH ? ROWS ONLY";
    private static String deleteLobPartStatement =
        "DELETE FROM LOBS WHERE LOB_ID = ? AND BLOCK_ORDER >= ?";
    private static String insertLobPartStatement =
        "INSERT INTO FROM LOBS (LOB_ID, BLOCK_ORDER, BLOCK_ADDR, BLOCK_COUNT) VALUES (?, ?, ?, ?)";

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
        } catch (IOException e) {}

        try {
            getLob     = session.compileStatement(getLobQuery);
            getLobPart = session.compileStatement(getLobPartStatement);
            getLobPart = session.compileStatement(deleteLobPartStatement);
        } catch (HsqlException e) {}
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

        int      blockOffset     = (int) (offset / diskBlockSize);
        int      byteBlockOffset = (int) (offset % diskBlockSize);
        int limit = (int) ((offset + length) / diskBlockSize) - blockOffset;
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

    public Result getBytes(Session session, long lobID, long offset,
                           int length) {

        BlobData lob = (BlobData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {
            byte[] bytes = lob.getBytes(offset, length);

            return ResultLob.newLobGetBytesResponse(lobID, offset, bytes);
        } catch (HsqlException e) {
            return ResultLob.newErrorResult(e);
        }
    }

    int[][] getBlockAddresses(Session session, long lobID, int offset,
                              int count) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);
        params[2] = Integer.valueOf(count);

        Result result = session.executeCompiledStatement(getLobPart, params);
        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[3][size];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = (Object[]) navigator.getCurrent();

            blocks[0][i] = ((Integer) data[BLOCK_ADDR]).intValue();
            blocks[1][i] = ((Integer) data[BLOCK_COUNT]).intValue();
            blocks[2][i] = ((Integer) data[BLOCK_ORDER]).intValue();
        }

        navigator.close();

        return blocks;
    }

    void deleteBlockAddresses(Session session, long lobID, int offset) {

        ResultMetaData meta     = deleteLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);

        Result result = session.executeCompiledStatement(deleteLobPart,
            params);
    }

    int getBlockOrder(Session session, long lobID, int offset) {

        int[][] blocks = getBlockAddresses(session, lobID, 0,
                                           Integer.MAX_VALUE);
        int order = 0;
        int count = 0;

        for (int i = 0; i < blocks[0].length; i++) {
            count += blocks[2][i];
            order = blocks[2][i];

            if (count > offset) {
                return order;
            }
        }

        return order + 1;
    }

    // not done yet
    int[][] newBlockAddresses(Session session, long lobID, int offset,
                              int count, boolean truncate) {

        // get blocks after offset
        // copy rows for blocks to deleted_blocks table
        // delete blocks after offset;
        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);
        params[2] = Integer.valueOf(count);

        Result result = session.executeCompiledStatement(getLobPart, params);
        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[2][size];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = (Object[]) navigator.getCurrent();

            blocks[0][i] = BLOCK_ADDR;
            blocks[1][i] = BLOCK_COUNT;
        }

        navigator.close();

        return blocks;
    }

/*
    private final static int LOB_ID  = 0;
    private final static int BLOCK_ORDER = 1;
    private final static int BLOCK_ADDR  = 2;
    private final static int BLOCK_COUNT = 3;
    private final static int LOB_LENGTH = 4;
    private final static int LOB_TYPE = 5;
*/
    void newBlockAddress(Session session, long lobID, int address, int order,
                         int count, long length) {

        ResultMetaData meta     = insertLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[LOB_ID]      = Long.valueOf(lobID);
        params[BLOCK_ORDER] = Integer.valueOf(order);
        params[BLOCK_ADDR]  = Integer.valueOf(address);
        params[BLOCK_COUNT] = Integer.valueOf(count);
        params[LOB_LENGTH]  = Long.valueOf(length);
        params[LOB_TYPE]    = null;

        Result result = session.executeCompiledStatement(insertLobPart,
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

    public Result setBytes(Session session, long lobID, long offset,
                           byte[] bytes) {

        BlobData lob = (BlobData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {
            lob.setBytes(offset, bytes);

            return ResultLob.newLobSetBytesResponse(lobID);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    public Result setBytes(Session session, long lobID, long offset,
                           DataInput dataInput, long length) {

        BlobData   lob     = (BlobData) lobs.get(lobID);
        BinaryData dataLob = (BinaryData) dataLobs.get(lobID);

        if (lob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        try {

            // temp code makes memory lob
            BlobData blobData = new BinaryData(length, dataInput);

            ((BlobDataID) lob).setLength(length);
            dataLob.setBytes(0, blobData.getBytes());

            return ResultLob.newLobSetBytesResponse(lobID);
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
