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
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlByteArrayInputStream;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.LineGroupReader;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class LobManager {

    Database database;

    //
    int lobBlockSize         = 2;
    int blocksInLargeBlock   = 2;
    int largeBlockSize       = lobBlockSize * blocksInLargeBlock;
    int totalBlockLimitCount = 1024 * 1024;

    //
    HsqlArrayList byteStoreList = new HsqlArrayList();

    //
    String   fileName = "/org/hsqldb/resources/lob-schema.sql";
    String[] starters = new String[]{ "/*" };

    //
    Statement getLob;
    Statement getLobPart;
    Statement deleteLobPart;
    Statement divideLobPart;
    Statement createLob;
    Statement createLobPart;
    Statement setLobLength;

    //
    private final static int BLOCK_ADDR   = 0;
    private final static int BLOCK_COUNT  = 1;
    private final static int BLOCK_OFFSET = 2;
    private final static int LOB_ID       = 3;
    private final static int LOB_LENGTH   = 4;

    //BLOCK_ADDR INT, BLOCK_COUNT INT, TX_ID BIGINT
    private static String initialiseBlocksSQL =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(?,?,?)";
    private static String getLobSQL =
        "SELECT * FROM SYSTEM_LOBS.LOB_IDS WHERE LOB_ID = ?";
    private static String getLobPartSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET >= ? ORDER BY BLOCK_OFFSET";

    // DELETE_BLOCKS(L_ID BIGINT, B_OFFSET INT, B_COUNT INT, TX_ID BIGINT)
    private static String deleteLobPartSQL =
        "CALL SYSTEM_LOBS.DELETE_BLOCKS(?,?,?,?)";
    private static String createLobSQL =
        "INSERT INTO SYSTEM_LOBS.LOB_IDS VALUES(?, ?, ?)";
    private static String updateLobLengthSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_LENGTH = ? WHERE LOB_ID = ?";
    private static String createLobPartSQL =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ? , ?, ?)";
    private static String divideLobPartSQL =
        "CALL SYSTEM_LOBS.DIVIDE_BLOCK(?, ?)";
    private static String getSpanningBlockSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND ? > BLOCK_OFFSET AND ? < BLOCK_OFFSET + BLOCK_COUNT";

    //    (OUT L_ADDR INT, IN B_COUNT INT, IN B_OFFSET INT, IN L_ID BIGINT, IN L_LENGTH BIGINT)
    public LobManager(Database database) {
        this.database = database;
    }

    void initialise() throws HsqlException {

        Session session = database.sessionManager.getSysSession();

        if (database.schemaManager.findSchemaHsqlName("SYSTEM_LOBS") == null) {
            InputStream       fis = getClass().getResourceAsStream(fileName);
            InputStreamReader reader = null;

            try {
                reader = new InputStreamReader(fis, "ISO-8859-1");
            } catch (Exception e) {}

            LineNumberReader lineReader = new LineNumberReader(reader);
            LineGroupReader  lg = new LineGroupReader(lineReader, starters);
            HashMappedList   map        = lg.getAsMap();
            String sql = (String) map.get("/*lob_schema_definition*/");
            Statement        statement  = session.compileStatement(sql);
            Result           result     = statement.execute(session, null);
            Table table = database.schemaManager.getTable(session, "BLOCKS",
                "SYSTEM_LOBS");

            table.isTransactional = false;
            statement = session.compileStatement(initialiseBlocksSQL);

            Object[] args = new Object[3];

            args[0] = Integer.valueOf(0);
            args[1] = Integer.valueOf(totalBlockLimitCount);
            args[2] = Long.valueOf(0);

            statement.execute(session, args);
            session.commit(false);
        }

        getLob        = session.compileStatement(getLobSQL);
        getLobPart    = session.compileStatement(getLobPartSQL);
        createLob     = session.compileStatement(createLobSQL);
        createLobPart = session.compileStatement(createLobPartSQL);
        divideLobPart = session.compileStatement(divideLobPartSQL);
        deleteLobPart = session.compileStatement(deleteLobPartSQL);
        setLobLength  = session.compileStatement(updateLobLengthSQL);
    }

    long lobIdSequence = 1;

    Object[] getLobHeader(Session session, long lobID) {

        ResultMetaData meta     = getLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);

        Result result = getLob.execute(session, params);

        if (result.isError()) {
            return null;
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.close();

            return null;
        }

        Object[] data = (Object[]) navigator.getCurrent();

        return data;
    }

    BlobData getBlob(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return null;
        }

        long     length = ((Long) data[1]).longValue();
        BlobData blob   = new BlobDataID(lobID, length);

        return blob;
    }

    ClobData getClob(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return null;
        }

        long     length = ((Long) data[1]).longValue();
        ClobData clob   = new ClobDataID(lobID, length);

        return clob;
    }

    public long createBlob(Session session) {

        long           lobID    = getNewLobId();
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(0);
        params[2] = Integer.valueOf(Types.SQL_BLOB);

        Result result = session.executeCompiledStatement(createLob, params);

        return lobID;
    }

    public long createClob(Session session) {

        long           lobID    = getNewLobId();
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(0);
        params[2] = Integer.valueOf(Types.SQL_CLOB);

        Result result = session.executeCompiledStatement(createLob, params);

        return lobID;
    }

    public long getNewLobId() {
        return lobIdSequence++;
    }

    public Result getChars(Session session, long lobID, long offset,
                           int length) {

        Result result = getBytes(session, lobID, offset * 2, length * 2);

        if (result.isError()) {
            return result;
        }

        byte[]                   bytes = ((ResultLob) result).getByteArray();
        HsqlByteArrayInputStream be    = new HsqlByteArrayInputStream(bytes);
        char[]                   chars = new char[bytes.length / 2];

        try {
            for (int i = 0; i < chars.length; i++) {
                chars[i] = be.readChar();
            }
        } catch (Exception e) {
            return Result.newErrorResult(e);
        }

        return ResultLob.newLobGetCharsResponse(lobID, offset, chars);
    }

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
        int blockCount = blockAddresses[i][1]
                         - (blockAddresses[i][2] - blockOffset);

        if (blockAddresses[i][1] + blockAddresses[i][2] > blockLimit) {
            blockCount -= (blockAddresses[i][1] + blockAddresses[i][2]
                           - blockLimit);
        }

        byte[] bytes = getBlockBytes(lobID,
                                     blockAddresses[i][0] + blockOffset,
                                     blockCount);
        int subLength = lobBlockSize * blockCount - byteBlockOffset;

        if (subLength > length) {
            subLength = length;
        }

        System.arraycopy(bytes, byteBlockOffset, dataBytes, dataBytesPosition,
                         subLength);

        dataBytesPosition += subLength;

        i++;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            blockCount = blockAddresses[i][1];

            if (blockAddresses[i][1] + blockAddresses[i][2] > blockLimit) {
                blockCount -= (blockAddresses[i][1] + blockAddresses[i][2]
                               - blockLimit);
            }

            bytes     = getBlockBytes(lobID, blockAddresses[i][0], blockCount);
            subLength = lobBlockSize * blockCount;

            if (subLength > length - dataBytesPosition) {
                subLength = length - dataBytesPosition;
            }

            System.arraycopy(bytes, 0, dataBytes, dataBytesPosition,
                             subLength);

            dataBytesPosition += subLength;
        }

        return ResultLob.newLobGetBytesResponse(lobID, offset, dataBytes);
    }

    public Result setBytesBA(Session session, long lobID, byte[] dataBytes,
                             long offset, int length) {

        BlobData blob = getBlob(session, lobID);

        if (blob == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        int blockOffset     = (int) (offset / lobBlockSize);
        int byteBlockOffset = (int) (offset % lobBlockSize);
        int blockLimit      = (int) ((offset + length) / lobBlockSize);
        int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        // should turn into SP -
        divideBlockAddresses(session, lobID, blockOffset);
        divideBlockAddresses(session, lobID, blockLimit);
        deleteBlockAddresses(session, lobID, blockOffset, blockLimit);
        createBlockAddresses(session, lobID, blockOffset,
                             blockLimit - blockOffset, length);

        // merge with the existing
        byte[] newBytes = getBlockBytes(lobID, blockOffset,
                                        blockLimit - blockOffset);

        System.arraycopy(dataBytes, 0, newBytes, byteBlockOffset, length);

        int[][] blockAddresses = getBlockAddresses(session, lobID,
            blockOffset);

        //
        for (int i = 0; i < blockAddresses.length; i++) {
            setBlockBytes(lobID, newBytes, blockAddresses[i][0],
                          blockAddresses[i][1]);
        }

        long newLength = blob.length();

        if (offset + length > newLength) {
            newLength = offset + length;

            setLength(session, lobID, newLength);
        }

        return ResultLob.newLobSetResponse(lobID, newLength);
    }

    private Result setBytesDI(Session session, long lobID,
                              DataInput dataInput, long length) {

        int blockLimit      = (int) (length / lobBlockSize);
        int byteLimitOffset = (int) (length % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        createBlockAddresses(session, lobID, 0, blockLimit, length);

        int[][] blockAddresses = getBlockAddresses(session, lobID, 0);
        byte[]  dataBytes      = new byte[lobBlockSize];

        for (int i = 0; i < blockAddresses.length; i++) {
            for (int j = 0; j < blockAddresses[i][1]; j++) {
                int localLength = lobBlockSize;

                if (i == blockAddresses.length - 1
                        && j == blockAddresses[i][1] - 1) {
                    localLength = byteLimitOffset;

                    for (int k = localLength; k < lobBlockSize; k++) {
                        dataBytes[k] = 0;
                    }
                }

                try {
                    dataInput.readFully(dataBytes, 0, localLength);
                } catch (IOException e) {

                    // deallocate
                    return Result.newErrorResult(e);
                }

                setBlockBytes(lobID, dataBytes, blockAddresses[i][0] + j, 1);
            }
        }

        return ResultLob.newLobSetResponse(lobID, length);
    }

    public Result setBytes(Session session, long lobID, byte[] dataBytes,
                           long offset) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        long length = ((Long) data[1]).longValue();
        Result result = setBytesBA(session, lobID, dataBytes, offset,
                                   dataBytes.length);

        if (offset + dataBytes.length > length) {
            setLength(session, lobID, offset + dataBytes.length);
        }

        return result;
    }

    public Result setBytes(Session session, long lobID, DataInput dataInput,
                           long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, length);
        }

        Result result = setBytesDI(session, lobID, dataInput, length);

        setLength(session, lobID, length);

        return result;
    }

    public Result setChars(Session session, long lobID, long offset,
                           char[] chars) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        long length = ((Long) data[1]).longValue();
        HsqlByteArrayOutputStream os =
            new HsqlByteArrayOutputStream(chars.length * 2);

        os.write(chars, 0, chars.length);

        Result result = setBytesBA(session, lobID, os.getBuffer(), offset * 2,
                                   os.getBuffer().length);

        if (result.isError()) {
            return result;
        }

        if (offset + chars.length > length) {
            setLength(session, lobID, offset + chars.length);
        }

        return ResultLob.newLobSetResponse(lobID, length);
    }

    public Result setChars(Session session, long lobID, long offset,
                           DataInput dataInput, long length) {

        Result result = setBytes(session, lobID, dataInput, length * 2);

        if (result.isError()) {
            return result;
        }

        setLength(session, lobID, length);

        return ResultLob.newLobSetResponse(lobID, length);
    }

    public Result truncate(Session session, long lobID, long offset) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(
                Error.error(ErrorCode.BLOB_IS_NO_LONGER_VALID));
        }

        // todo - scale offset for clob
        long length          = ((Long) data[1]).longValue();
        int  blockOffset     = (int) (offset / lobBlockSize);
        int  blockLimit      = (int) ((offset + length) / lobBlockSize);
        int  byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset != 0) {
            blockLimit++;
        }

        ResultMetaData meta     = deleteLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(blockOffset);
        params[2] = Integer.valueOf(blockLimit);
        params[3] = Long.valueOf(session.transactionTimestamp);

        Result result = session.executeCompiledStatement(deleteLobPart,
            params);

        this.setLength(session, lobID, offset);

        return ResultLob.newLobTruncateResponse(lobID);
    }

    public void setLength(Session session, long lobID, long length) {

        ResultMetaData meta     = setLobLength.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(length);
        params[1] = Long.valueOf(lobID);

        Result result = session.executeCompiledStatement(setLobLength, params);
    }

    byte[] getBlockBytes(long lobID, int blockAddress, int blockCount) {

        byte[] dataBytes       = new byte[blockCount * lobBlockSize];
        int    dataBlockOffset = 0;

        while (blockCount > 0) {
            int    largeBlockIndex   = blockAddress / blocksInLargeBlock;
            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;

            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }

            System.arraycopy(largeBlock, blockOffset * lobBlockSize,
                             dataBytes, dataBlockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);

            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }

        return dataBytes;
    }

    void setBlockBytes(long lobID, byte[] dataBytes, int blockAddress,
                       int blockCount) {

        int dataBlockOffset = 0;

        while (blockCount > 0) {
            int largeBlockIndex = blockAddress / blocksInLargeBlock;
            int largeBlockLimit = (blockAddress + blockCount)
                                  / blocksInLargeBlock;

            if ((blockAddress + blockCount) % blocksInLargeBlock != 0) {
                largeBlockLimit++;
            }

            if (largeBlockIndex >= byteStoreList.size()) {
                byteStoreList.add(new byte[largeBlockSize]);
            }

            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;

            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }

            System.arraycopy(dataBytes, dataBlockOffset * lobBlockSize,
                             largeBlock, blockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);

            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }
    }

    int[][] getBlockAddresses(Session session, long lobID, int offset) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);

        Result          result    = getLobPart.execute(session, params);
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

    void divideBlockAddresses(Session session, long lobID, int offset) {

        ResultMetaData meta     = divideLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);

        Result result = session.executeCompiledStatement(divideLobPart,
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
}
