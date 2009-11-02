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


package org.hsqldb.persist;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Statement;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMappedList;
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
import org.hsqldb.types.Types;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class LobManager {

    static final String resourceFileName =
        "/org/hsqldb/resources/lob-schema.sql";
    static final String[] starters = new String[]{ "/*" };

    //
    Database database;
    LobStore lobStore;
    Session  sysLobSession;

    //
    //
    int lobBlockSize         = 1024 * 32;
    int totalBlockLimitCount = Integer.MAX_VALUE;

    //
    Statement getLob;
    Statement getLobPart;
    Statement deleteLobCall;
    Statement deleteLobPartCall;
    Statement divideLobPartCall;
    Statement createLob;
    Statement createLobPartCall;
    Statement updateLobLength;
    Statement updateLobUsage;
    Statement getNextLobId;

    // LOBS columns
    private interface LOBS {

        int BLOCK_ADDR   = 0;
        int BLOCK_COUNT  = 1;
        int BLOCK_OFFSET = 2;
        int LOB_ID       = 3;
    }

    private interface LOB_IDS {

        int LOB_ID          = 0;
        int LOB_LENGTH      = 1;
        int LOB_USAGE_COUNT = 2;
        int LOB_TYPE        = 3;
    }

    private interface GET_LOB_PART {

        int LOB_ID       = 0;
        int BLOCK_OFFSET = 1;
        int BLOCK_LIMIT  = 2;
    }

    private interface DIVIDE_BLOCK {
        int BLOCK_OFFSET = 0;
        int LOB_ID       = 1;
    }

    private interface DELETE_BLOCKS {

        int LOB_ID       = 0;
        int BLOCK_OFFSET = 1;
        int BLOCK_LIMIT  = 2;
        int TX_ID        = 3;
    }

    private interface ALLOC_BLOCKS {

        int BLOCK_COUNT  = 0;
        int BLOCK_OFFSET = 1;
        int LOB_ID       = 2;
    }

    private interface UPDATE_USAGE {
        int BLOCK_COUNT = 0;
        int LOB_ID      = 1;
    }

    private interface UPDATE_LENGTH {
        int LOB_LENGTH = 0;
        int LOB_ID     = 1;
    }

    //BLOCK_ADDR INT, BLOCK_COUNT INT, TX_ID BIGINT
    private static String initialiseBlocksSQL =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(?,?,?)";
    private static String getLobSQL =
        "SELECT * FROM SYSTEM_LOBS.LOB_IDS WHERE LOB_ID = ?";
    private static String getLobPartSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET + BLOCK_COUNT > ? AND BLOCK_OFFSET < ? ORDER BY BLOCK_OFFSET";

    // DELETE_BLOCKS(L_ID BIGINT, B_OFFSET INT, B_COUNT INT, TX_ID BIGINT)
    private static String deleteLobPartCallSQL =
        "CALL SYSTEM_LOBS.DELETE_BLOCKS(?,?,?,?)";
    private static String createLobSQL =
        "INSERT INTO SYSTEM_LOBS.LOB_IDS VALUES(?, ?, ?, ?)";
    private static String updateLobLengthSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_LENGTH = ? WHERE LOB_ID = ?";
    private static String createLobPartCallSQL =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ?)";
    private static String divideLobPartCallSQL =
        "CALL SYSTEM_LOBS.DIVIDE_BLOCK(?, ?)";
    private static String getSpanningBlockSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND ? > BLOCK_OFFSET AND ? < BLOCK_OFFSET + BLOCK_COUNT";
    private static String updateLobUsageSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_USAGE_COUNT = ? WHERE LOB_ID = ?";
    private static String getNextLobIdSQL =
        "VALUES NEXT VALUE FOR SYSTEM_LOBS.LOB_ID";
    private static String deleteLobCallSQL =
        "CALL SYSTEM_LOBS.DELETE_LOB(?, ?)";

    public LobManager(Database database) {
        this.database = database;
    }

    public void createSchema() {

        sysLobSession = database.sessionManager.getSysLobSession();

        InputStream fis = getClass().getResourceAsStream(resourceFileName);
        InputStreamReader reader = null;

        try {
            reader = new InputStreamReader(fis, "ISO-8859-1");
        } catch (Exception e) {}

        LineNumberReader lineReader = new LineNumberReader(reader);
        LineGroupReader  lg = new LineGroupReader(lineReader, starters);
        HashMappedList   map        = lg.getAsMap();

        lg.close();

        String    sql       = (String) map.get("/*lob_schema_definition*/");
        Statement statement = sysLobSession.compileStatement(sql);
        Result    result    = statement.execute(sysLobSession);
        HsqlName name =
            database.schemaManager.getSchemaHsqlName("SYSTEM_LOBS");

        name.owner = SqlInvariants.LOBS_SCHEMA_HSQLNAME.owner;

        Table table = database.schemaManager.getTable(sysLobSession, "BLOCKS",
            "SYSTEM_LOBS");

        getLob     = sysLobSession.compileStatement(getLobSQL);
        getLobPart = sysLobSession.compileStatement(getLobPartSQL);
        createLob  = sysLobSession.compileStatement(createLobSQL);
        createLobPartCall =
            sysLobSession.compileStatement(createLobPartCallSQL);
        divideLobPartCall =
            sysLobSession.compileStatement(divideLobPartCallSQL);
        deleteLobCall = sysLobSession.compileStatement(deleteLobCallSQL);
        deleteLobPartCall =
            sysLobSession.compileStatement(deleteLobPartCallSQL);
        updateLobLength = sysLobSession.compileStatement(updateLobLengthSQL);
        updateLobUsage  = sysLobSession.compileStatement(updateLobUsageSQL);
        getNextLobId    = sysLobSession.compileStatement(getNextLobIdSQL);
    }

    public void initialiseLobSpace() {

        Statement statement =
            sysLobSession.compileStatement(initialiseBlocksSQL);
        Object[] args = new Object[3];

        args[0] = Integer.valueOf(0);
        args[1] = Integer.valueOf(totalBlockLimitCount);
        args[2] = Long.valueOf(0);

        sysLobSession.executeCompiledStatement(statement, args);
    }

    public void open() {

        if (database.getType() == DatabaseURL.S_RES) {
            lobStore = new LobStoreInJar(database, lobBlockSize);
        } else if (database.getType() == DatabaseURL.S_FILE) {
            lobStore = new LobStoreRAFile(database, lobBlockSize);
        } else {
            lobStore = new LobStoreMem(lobBlockSize);
        }
    }

    public void close() {}

    //
    private long getNewLobID() {

        Result result = getNextLobId.execute(sysLobSession);

        if (result.isError()) {
            return 0;
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.close();

            return 0;
        }

        Object[] data = navigator.getCurrent();

        return ((Long) data[0]).longValue();
    }

    private Object[] getLobHeader(long lobID) {

        ResultMetaData meta     = getLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getLob.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        if (result.isError()) {
            return null;
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.close();

            return null;
        }

        Object[] data = navigator.getCurrent();

        return data;
    }

    public BlobData getBlob(long lobID) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return null;
        }

        BlobData blob = new BlobDataID(lobID);

        return blob;
    }

    public ClobData getClob(long lobID) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return null;
        }

        ClobData clob = new ClobDataID(lobID);

        return clob;
    }

    public long createBlob(long length) {

        long           lobID    = getNewLobID();
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[LOB_IDS.LOB_ID]          = Long.valueOf(lobID);
        params[LOB_IDS.LOB_LENGTH]      = Long.valueOf(length);
        params[LOB_IDS.LOB_USAGE_COUNT] = Integer.valueOf(0);
        params[LOB_IDS.LOB_TYPE]        = Integer.valueOf(Types.SQL_BLOB);

        Result result = sysLobSession.executeCompiledStatement(createLob,
            params);

        return lobID;
    }

    public long createClob(long length) {

        long           lobID    = getNewLobID();
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[LOB_IDS.LOB_ID]          = Long.valueOf(lobID);
        params[LOB_IDS.LOB_LENGTH]      = Long.valueOf(length);
        params[LOB_IDS.LOB_USAGE_COUNT] = Integer.valueOf(0);
        params[LOB_IDS.LOB_TYPE]        = Integer.valueOf(Types.SQL_CLOB);

        Result result = sysLobSession.executeCompiledStatement(createLob,
            params);

        return lobID;
    }

    public Result deleteLob(long lobID) {

        ResultMetaData meta     = deleteLobCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(0);

        Result result = sysLobSession.executeCompiledStatement(deleteLobCall,
            params);

        return result;
    }

    public Result getLength(long lobID) {

        try {
            long length = getLengthValue(lobID);

            return ResultLob.newLobSetResponse(lobID, length);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    public long getLengthValue(long lobID) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            throw Error.error(ErrorCode.X_0F502);
        }

        long length = ((Long) data[1]).longValue();

        return length;
    }

    /**
     * Used for SUBSTRING
     */
    public Result getLob(long lobID, long offset, long length) {
        throw Error.runtimeError(ErrorCode.U_S0500, "LobManager");
    }

    public Result createDuplicateLob(long lobID) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long   newLobID = getNewLobID();
        Object params[] = new Object[data.length];

        params[LOB_IDS.LOB_ID] = Long.valueOf(newLobID);
        params[1]              = data[1];
        params[2]              = data[2];
        params[3]              = data[3];

        Result result = sysLobSession.executeCompiledStatement(createLob,
            params);

        if (result.isError()) {
            return result;
        }

        long length     = ((Long) data[1]).longValue();
        long byteLength = length;
        int  lobType    = ((Integer) data[1]).intValue();

        if (lobType == Types.SQL_CLOB) {
            byteLength *= 2;
        }

        int newBlockCount = (int) byteLength / lobBlockSize;

        if (byteLength % lobBlockSize != 0) {
            newBlockCount++;
        }

        createBlockAddresses(newLobID, 0, newBlockCount);

        // copy the contents
        int[][] sourceBlocks = getBlockAddresses(lobID, 0, Integer.MAX_VALUE);
        int[][] targetBlocks = getBlockAddresses(newLobID, 0,
            Integer.MAX_VALUE);

        try {
            copyBlockSet(sourceBlocks, targetBlocks);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        return ResultLob.newLobSetResponse(newLobID, length);
    }

    private void copyBlockSet(int[][] source, int[][] target) {

        int sourceIndex = 0;
        int targetIndex = 0;

        while (true) {
            int sourceOffset = source[sourceIndex][LOBS.BLOCK_OFFSET]
                               + sourceIndex;
            int targetOffset = target[targetIndex][LOBS.BLOCK_OFFSET]
                               + targetIndex;
            byte[] bytes = lobStore.getBlockBytes(sourceOffset, 1);

            lobStore.setBlockBytes(bytes, targetOffset, 1);

            sourceOffset++;
            targetOffset++;

            if (sourceOffset == source[sourceIndex][LOBS.BLOCK_COUNT]) {
                sourceOffset = 0;

                sourceIndex++;
            }

            if (targetOffset == target[sourceIndex][LOBS.BLOCK_COUNT]) {
                targetOffset = 0;

                targetIndex++;
            }

            if (sourceIndex == source.length) {
                break;
            }
        }
    }

    public Result getChars(long lobID, long offset, int length) {

        Result result = getBytes(lobID, offset * 2, length * 2);

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

    public Result getBytes(long lobID, long offset, int length) {

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
        int[][] blockAddresses = getBlockAddresses(lobID, blockOffset,
            blockLimit);

        if (blockAddresses.length == 0) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        //
        int i = 0;
        int blockCount = blockAddresses[i][LOBS.BLOCK_COUNT]
                         + blockAddresses[i][LOBS.BLOCK_OFFSET] - blockOffset;

        if (blockAddresses[i][LOBS.BLOCK_COUNT]
                + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
            blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                           + blockAddresses[i][LOBS.BLOCK_OFFSET]
                           - blockLimit);
        }

        byte[] bytes;

        try {
            bytes = lobStore.getBlockBytes(blockAddresses[i][LOBS.BLOCK_ADDR]
                                           + blockOffset, blockCount);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        int subLength = lobBlockSize * blockCount - byteBlockOffset;

        if (subLength > length) {
            subLength = length;
        }

        System.arraycopy(bytes, byteBlockOffset, dataBytes, dataBytesPosition,
                         subLength);

        dataBytesPosition += subLength;

        i++;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            blockCount = blockAddresses[i][LOBS.BLOCK_COUNT];

            if (blockAddresses[i][LOBS.BLOCK_COUNT]
                    + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
                blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                               + blockAddresses[i][LOBS.BLOCK_OFFSET]
                               - blockLimit);
            }

            try {
                bytes =
                    lobStore.getBlockBytes(blockAddresses[i][LOBS.BLOCK_ADDR],
                                           blockCount);
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

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

    public Result setBytesBA(long lobID, byte[] dataBytes, long offset,
                             int length) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long oldLength       = ((Long) data[1]).longValue();
        int  blockOffset     = (int) (offset / lobBlockSize);
        int  byteBlockOffset = (int) (offset % lobBlockSize);
        int  blockLimit      = (int) ((offset + length) / lobBlockSize);
        int  byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        int[][] blockAddresses = getBlockAddresses(lobID, blockOffset,
            blockLimit);
        byte[] newBytes = new byte[(blockLimit - blockOffset) * lobBlockSize];

        if (blockAddresses.length > 0) {
            int blockAddress = blockAddresses[0][LOBS.BLOCK_ADDR]
                               + (blockOffset
                                  - blockAddresses[0][LOBS.BLOCK_OFFSET]);

            try {
                byte[] block = lobStore.getBlockBytes(blockAddress, 1);

                System.arraycopy(block, 0, newBytes, 0, lobBlockSize);

                if (blockAddresses.length > 1) {
                    blockAddress =
                        blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_ADDR]
                        + (blockLimit
                           - blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_OFFSET]
                           - 1);
                    block = lobStore.getBlockBytes(blockAddress, 1);

                    System.arraycopy(block, 0, newBytes,
                                     blockLimit - blockOffset - 1,
                                     lobBlockSize);
                } else if (blockLimit - blockOffset > 1) {
                    blockAddress = blockAddresses[0][LOBS.BLOCK_ADDR]
                                   + (blockLimit
                                      - blockAddresses[0][LOBS.BLOCK_OFFSET]
                                      - 1);
                    block = lobStore.getBlockBytes(blockAddress, 1);

                    System.arraycopy(block, 0, newBytes,
                                     (blockLimit - blockOffset - 1)
                                     * lobBlockSize, lobBlockSize);
                }
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            // should turn into SP
            divideBlockAddresses(lobID, blockOffset);
            divideBlockAddresses(lobID, blockLimit);
            deleteBlockAddresses(lobID, blockOffset, blockLimit);
        }

        createBlockAddresses(lobID, blockOffset, blockLimit - blockOffset);
        System.arraycopy(dataBytes, 0, newBytes, byteBlockOffset, length);

        blockAddresses = getBlockAddresses(lobID, blockOffset, blockLimit);

        //
        try {
            for (int i = 0; i < blockAddresses.length; i++) {
                lobStore.setBlockBytes(newBytes, blockAddresses[i][0],
                                       blockAddresses[i][1]);
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        if (offset + length > oldLength) {
            oldLength = offset + length;

            setLength(lobID, oldLength);
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    private Result setBytesIS(long lobID, InputStream inputStream,
                              long length) {

        int blockLimit      = (int) (length / lobBlockSize);
        int byteLimitOffset = (int) (length % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        createBlockAddresses(lobID, 0, blockLimit);

        int[][] blockAddresses = getBlockAddresses(lobID, 0, blockLimit);
        byte[]  dataBytes      = new byte[lobBlockSize];

        for (int i = 0; i < blockAddresses.length; i++) {
            for (int j = 0; j < blockAddresses[i][LOBS.BLOCK_COUNT]; j++) {
                int localLength = lobBlockSize;

                if (i == blockAddresses.length - 1
                        && j == blockAddresses[i][LOBS.BLOCK_COUNT] - 1) {
                    localLength = byteLimitOffset;

// todo -- use block op
                    for (int k = localLength; k < lobBlockSize; k++) {
                        dataBytes[k] = 0;
                    }
                }

                try {
                    int count = 0;

                    while (localLength > 0) {
                        int read = inputStream.read(dataBytes, count,
                                                    localLength);

                        if (read == -1) {
                            return Result.newErrorResult(new EOFException());
                        }

                        localLength -= read;
                        count       += read;
                    }

                    // read more
                } catch (IOException e) {

                    // deallocate
                    return Result.newErrorResult(e);
                }

                try {
                    lobStore.setBlockBytes(dataBytes,
                                           blockAddresses[i][LOBS.BLOCK_ADDR]
                                           + j, 1);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result setBytes(long lobID, byte[] dataBytes, long offset) {

        if (dataBytes.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long   length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        Result result = setBytesBA(lobID, dataBytes, offset, dataBytes.length);

        if (offset + dataBytes.length > length) {
            setLength(lobID, offset + dataBytes.length);
        }

        return result;
    }

    public Result setBytesForNewBlob(long lobID, InputStream inputStream,
                                     long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Result result = setBytesIS(lobID, inputStream, length);

        return result;
    }

    public Result setChars(long lobID, long offset, char[] chars) {

        if (chars.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        HsqlByteArrayOutputStream os =
            new HsqlByteArrayOutputStream(chars.length * 2);

        os.write(chars, 0, chars.length);

        Result result = setBytesBA(lobID, os.getBuffer(), offset * 2,
                                   os.getBuffer().length);

        if (result.isError()) {
            return result;
        }

        if (offset + chars.length > length) {
            result = setLength(lobID, offset + chars.length);

            if (result.isError()) {
                return result;
            }
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result setCharsForNewClob(long lobID, InputStream inputStream,
                                     long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Result result = setBytesIS(lobID, inputStream, length * 2);

        if (result.isError()) {
            return result;
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result truncate(long lobID, long offset) {

        Object[] data = getLobHeader(lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        /** @todo 1.9.0 - double offset for clob */
        long           length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        int            blockOffset = (int) (offset / lobBlockSize);
        ResultMetaData meta        = deleteLobPartCall.getParametersMetaData();
        Object         params[]    = new Object[meta.getColumnCount()];

        params[DELETE_BLOCKS.LOB_ID]       = Long.valueOf(lobID);
        params[DELETE_BLOCKS.BLOCK_OFFSET] = Integer.valueOf(blockOffset);
        params[DELETE_BLOCKS.BLOCK_LIMIT]  = Integer.MAX_VALUE;
        params[DELETE_BLOCKS.TX_ID] =
            Long.valueOf(sysLobSession.getTransactionTimestamp());

        Result result =
            sysLobSession.executeCompiledStatement(deleteLobPartCall, params);

        setLength(lobID, offset);

        return ResultLob.newLobTruncateResponse(lobID);
    }

    Result setLength(long lobID, long length) {

        ResultMetaData meta     = updateLobLength.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_LENGTH.LOB_LENGTH] = Long.valueOf(length);
        params[UPDATE_LENGTH.LOB_ID]     = Long.valueOf(lobID);

        Result result = sysLobSession.executeCompiledStatement(updateLobLength,
            params);

        return result;
    }

    public Result adjustUsageCount(long lobID, int delta) {

        Object[] data  = getLobHeader(lobID);
        int      count = ((Number) data[LOB_IDS.LOB_USAGE_COUNT]).intValue();

        if (count + delta == 0) {

//            return deleteLob(lobID);
        }

        ResultMetaData meta     = updateLobUsage.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_USAGE.BLOCK_COUNT] = Long.valueOf(count + delta);
        params[UPDATE_USAGE.LOB_ID]      = Long.valueOf(lobID);

        Result result = sysLobSession.executeCompiledStatement(updateLobUsage,
            params);

        return result;
    }

    int[][] getBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[GET_LOB_PART.LOB_ID]       = Long.valueOf(lobID);
        params[GET_LOB_PART.BLOCK_OFFSET] = Integer.valueOf(offset);
        params[GET_LOB_PART.BLOCK_LIMIT]  = Integer.valueOf(limit);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getLobPart.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[size][3];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = navigator.getCurrent();

            blocks[i][LOBS.BLOCK_ADDR] =
                ((Integer) data[LOBS.BLOCK_ADDR]).intValue();
            blocks[i][LOBS.BLOCK_COUNT] =
                ((Integer) data[LOBS.BLOCK_COUNT]).intValue();
            blocks[i][LOBS.BLOCK_OFFSET] =
                ((Integer) data[LOBS.BLOCK_OFFSET]).intValue();
        }

        navigator.close();

        return blocks;
    }

    void deleteBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = deleteLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DELETE_BLOCKS.LOB_ID]       = Long.valueOf(lobID);
        params[DELETE_BLOCKS.BLOCK_OFFSET] = Integer.valueOf(offset);
        params[DELETE_BLOCKS.BLOCK_LIMIT]  = Integer.valueOf(limit);
        params[DELETE_BLOCKS.TX_ID] =
            Long.valueOf(sysLobSession.getTransactionTimestamp());

        Result result =
            sysLobSession.executeCompiledStatement(deleteLobPartCall, params);
    }

    void divideBlockAddresses(long lobID, int offset) {

        ResultMetaData meta     = divideLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DIVIDE_BLOCK.BLOCK_OFFSET] = Integer.valueOf(offset);
        params[DIVIDE_BLOCK.LOB_ID]       = Long.valueOf(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(divideLobPartCall, params);
    }

    void createBlockAddresses(long lobID, int offset, int count) {

        ResultMetaData meta     = createLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_BLOCKS.BLOCK_COUNT]  = Integer.valueOf(count);
        params[ALLOC_BLOCKS.BLOCK_OFFSET] = Integer.valueOf(offset);
        params[ALLOC_BLOCKS.LOB_ID]       = Long.valueOf(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(createLobPartCall, params);
    }
}
