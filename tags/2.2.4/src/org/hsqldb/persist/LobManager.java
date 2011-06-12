/* Copyright (c) 2001-2011, The HSQL Development Group
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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Session;
import org.hsqldb.Statement;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.LineGroupReader;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.Types;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class LobManager {

    static final String resourceFileName =
        "/org/hsqldb/resources/lob-schema.sql";
    static final String[] starters = new String[]{ "/*" };

    //
    Database         database;
    LobStore         lobStore;
    Session          sysLobSession;
    volatile boolean storeModified;

    //
    //
    int lobBlockSize;
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
    Statement deleteUnusedLobs;
    Statement getLobCount;

    //
    boolean usageCountChanged;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          writeLock = lock.writeLock();

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
    private static final String initialiseBlocksSQL =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(?,?,?)";
    private static final String getLobSQL =
        "SELECT * FROM SYSTEM_LOBS.LOB_IDS WHERE LOB_ID = ?";
    private static final String getLobPartSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET + BLOCK_COUNT > ? AND BLOCK_OFFSET < ? ORDER BY BLOCK_OFFSET";
    private static final String deleteLobPartCallSQL =
        "CALL SYSTEM_LOBS.DELETE_BLOCKS(?,?,?,?)";
    private static final String createLobSQL =
        "INSERT INTO SYSTEM_LOBS.LOB_IDS VALUES(?, ?, ?, ?)";
    private static final String updateLobLengthSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_LENGTH = ? WHERE LOB_ID = ?";
    private static final String createLobPartCallSQL =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ?)";
    private static final String divideLobPartCallSQL =
        "CALL SYSTEM_LOBS.DIVIDE_BLOCK(?, ?)";
    private static final String getSpanningBlockSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND ? > BLOCK_OFFSET AND ? < BLOCK_OFFSET + BLOCK_COUNT";
    private static final String updateLobUsageSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_USAGE_COUNT = (CASE LOB_USAGE_COUNT WHEN 2147483647 THEN 0 ELSE LOB_USAGE_COUNT END) + ? WHERE LOB_ID = ?";
    private static final String getNextLobIdSQL =
        "VALUES NEXT VALUE FOR SYSTEM_LOBS.LOB_ID";
    private static final String deleteLobCallSQL =
        "CALL SYSTEM_LOBS.DELETE_LOB(?, ?)";
    private static final String deleteUnusedCallSQL =
        "CALL SYSTEM_LOBS.DELETE_UNUSED()";
    private static final String getLobCountSQL =
        "SELECT COUNT(*) FROM SYSTEM_LOBS.LOB_IDS";

    public LobManager(Database database) {
        this.database = database;
    }

    public void lock() {
        writeLock.lock();
    }

    public void unlock() {
        writeLock.unlock();
    }

    public void createSchema() {

        sysLobSession = database.sessionManager.getSysLobSession();

        InputStream fis = (InputStream) AccessController.doPrivileged(
            new PrivilegedAction() {

            public InputStream run() {
                return getClass().getResourceAsStream(resourceFileName);
            }
        });
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

        if (result.isError()) {
            throw result.getException();
        }

        HsqlName name =
            database.schemaManager.getSchemaHsqlName("SYSTEM_LOBS");
        Table table = database.schemaManager.getTable(sysLobSession, "BLOCKS",
            "SYSTEM_LOBS");

        compileStatements();
    }

    public void compileStatements() {

        writeLock.lock();

        try {
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
            updateLobLength =
                sysLobSession.compileStatement(updateLobLengthSQL);
            updateLobUsage = sysLobSession.compileStatement(updateLobUsageSQL);
            getNextLobId   = sysLobSession.compileStatement(getNextLobIdSQL);
            deleteUnusedLobs =
                sysLobSession.compileStatement(deleteUnusedCallSQL);
            getLobCount = sysLobSession.compileStatement(getLobCountSQL);
        } finally {
            writeLock.unlock();
        }
    }

    public void initialiseLobSpace() {

        Statement statement =
            sysLobSession.compileStatement(initialiseBlocksSQL);
        Object[] params = new Object[3];

        params[0] = ValuePool.INTEGER_0;
        params[1] = ValuePool.getInt(totalBlockLimitCount);
        params[2] = ValuePool.getLong(0);

        sysLobSession.executeCompiledStatement(statement, params);
    }

    public void open() {

        lobBlockSize = database.logger.getLobBlockSize();

        if (database.getType() == DatabaseURL.S_RES) {
            lobStore = new LobStoreInJar(database, lobBlockSize);
        } else if (database.getType() == DatabaseURL.S_FILE) {
            lobStore = new LobStoreRAFile(database, lobBlockSize);
        } else {
            lobStore = new LobStoreMem(lobBlockSize);
        }
    }

    public void close() {
        lobStore.close();
    }

    public LobStore getLobStore() {

        if (lobStore == null) {
            open();
        }

        return lobStore;
    }

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

        params[0] = ValuePool.getLong(lobID);

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

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return null;
            }

            BlobData blob = new BlobDataID(lobID);

            return blob;
        } finally {
            writeLock.unlock();
        }
    }

    public ClobData getClob(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return null;
            }

            ClobData clob = new ClobDataID(lobID);

            return clob;
        } finally {
            writeLock.unlock();
        }
    }

    public long createBlob(long length) {

        writeLock.lock();

        try {
            long           lobID    = getNewLobID();
            ResultMetaData meta     = createLob.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[LOB_IDS.LOB_ID]          = ValuePool.getLong(lobID);
            params[LOB_IDS.LOB_LENGTH]      = ValuePool.getLong(length);
            params[LOB_IDS.LOB_USAGE_COUNT] = ValuePool.INTEGER_MAX;
            params[LOB_IDS.LOB_TYPE]        = ValuePool.getInt(Types.SQL_BLOB);

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params);

            usageCountChanged = true;

            return lobID;
        } finally {
            writeLock.unlock();
        }
    }

    public long createClob(long length) {

        writeLock.lock();

        try {
            long           lobID    = getNewLobID();
            ResultMetaData meta     = createLob.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[LOB_IDS.LOB_ID]          = ValuePool.getLong(lobID);
            params[LOB_IDS.LOB_LENGTH]      = ValuePool.getLong(length);
            params[LOB_IDS.LOB_USAGE_COUNT] = ValuePool.INTEGER_MAX;
            params[LOB_IDS.LOB_TYPE]        = ValuePool.getInt(Types.SQL_CLOB);

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params);

            usageCountChanged = true;

            return lobID;
        } finally {
            writeLock.unlock();
        }
    }

    public Result deleteLob(long lobID) {

        writeLock.lock();

        try {
            ResultMetaData meta     = deleteLobCall.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[0] = ValuePool.getLong(lobID);
            params[1] = ValuePool.getLong(0);

            Result result =
                sysLobSession.executeCompiledStatement(deleteLobCall, params);

            usageCountChanged = true;

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result deleteUnusedLobs() {

        writeLock.lock();

        try {
            if (!usageCountChanged) {
                return Result.updateZeroResult;
            }

            Result result =
                sysLobSession.executeCompiledStatement(deleteUnusedLobs,
                    ValuePool.emptyObjectArray);

            usageCountChanged = false;

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result getLength(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                throw Error.error(ErrorCode.X_0F502);
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int  type   = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            return ResultLob.newLobSetResponse(lobID, length);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(BlobData a, byte[] b) {

        writeLock.lock();

        try {
            Object[] data    = getLobHeader(a.getId());
            long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int[][] aAddresses = getBlockAddresses(a.getId(), 0,
                                                   Integer.MAX_VALUE);
            int aIndex  = 0;
            int bOffset = 0;
            int aOffset = 0;

            while (true) {
                int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR]
                                   + aOffset;
                byte[] aBytes = getLobStore().getBlockBytes(aBlockOffset, 1);

                for (int i = 0; i < aBytes.length; i++) {
                    if (bOffset + i >= b.length) {
                        if (aLength == b.length) {
                            return 0;
                        }

                        return 1;
                    }

                    if (aBytes[i] == b[bOffset + i]) {
                        continue;
                    }

                    return (((int) aBytes[i]) & 0xff)
                           > (((int) b[bOffset + i]) & 0xff) ? 1
                                                             : -1;
                }

                aOffset++;

                bOffset += lobBlockSize;

                if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                    aOffset = 0;

                    aIndex++;
                }

                if (aIndex == aAddresses.length) {
                    break;
                }
            }

            return -1;
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(BlobData a, BlobData b) {

        if (a.getId() == b.getId()) {
            return 0;
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(a.getId());

            // abnormal case
            if (data == null) {
                return 1;
            }

            long lengthA = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

            data = getLobHeader(b.getId());

            // abnormal case
            if (data == null) {
                return -1;
            }

            long lengthB = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

            if (lengthA > lengthB) {
                return 1;
            }

            if (lengthA < lengthB) {
                return -1;
            }

            return compareBytes(a.getId(), b.getId());
        } finally {
            writeLock.unlock();
        }
    }

    // todo - implement as compareText()
    public int compare(ClobData a, String b) {

        writeLock.lock();

        try {
            Object[] data    = getLobHeader(a.getId());
            long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int[][] aAddresses = getBlockAddresses(a.getId(), 0,
                                                   Integer.MAX_VALUE);
            int aIndex  = 0;
            int bOffset = 0;
            int aOffset = 0;

            while (true) {
                int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR]
                                   + aOffset;
                byte[] aBytes = getLobStore().getBlockBytes(aBlockOffset, 1);
                long aLimit =
                    aLength
                    - (aAddresses[aIndex][LOBS.BLOCK_OFFSET] + aOffset)
                      * lobBlockSize / 2;

                if (aLimit > lobBlockSize / 2) {
                    aLimit = lobBlockSize / 2;
                }

                String aString = new String(ArrayUtil.byteArrayToChars(aBytes),
                                            0, (int) aLimit);
                int bLimit = b.length() - bOffset;

                if (bLimit > lobBlockSize / 2) {
                    bLimit = lobBlockSize / 2;
                }

                String bString = b.substring(bOffset, bOffset + bLimit);
                int    diff    = database.collation.compare(aString, bString);

                if (diff != 0) {
                    return diff;
                }

                aOffset++;

                bOffset += lobBlockSize / 2;

                if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                    aOffset = 0;

                    aIndex++;
                }

                if (aIndex == aAddresses.length) {
                    break;
                }
            }

            return 0;
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(ClobData a, ClobData b) {

        if (a.getId() == b.getId()) {
            return 0;
        }

        return compareText(a.getId(), b.getId());
    }

    private int compareBytes(long aID, long bID) {

        int[][] aAddresses = getBlockAddresses(aID, 0, Integer.MAX_VALUE);
        int[][] bAddresses = getBlockAddresses(bID, 0, Integer.MAX_VALUE);
        int     aIndex     = 0;
        int     bIndex     = 0;
        int     aOffset    = 0;
        int     bOffset    = 0;

        while (true) {
            int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR] + aOffset;
            int bBlockOffset = bAddresses[bIndex][LOBS.BLOCK_ADDR] + bOffset;
            byte[] aBytes    = getLobStore().getBlockBytes(aBlockOffset, 1);
            byte[] bBytes    = getLobStore().getBlockBytes(bBlockOffset, 1);

            for (int i = 0; i < aBytes.length; i++) {
                if (aBytes[i] == bBytes[i]) {
                    continue;
                }

                return (((int) aBytes[i]) & 0xff) > (((int) bBytes[i]) & 0xff)
                       ? 1
                       : -1;
            }

            aOffset++;
            bOffset++;

            if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                aOffset = 0;

                aIndex++;
            }

            if (bOffset == bAddresses[bIndex][LOBS.BLOCK_COUNT]) {
                bOffset = 0;

                bIndex++;
            }

            if (aIndex == aAddresses.length) {
                break;
            }
        }

        return 0;
    }

    /** @todo - word-separator and end block zero issues */
    private int compareText(long aID, long bID) {

        Object[] data    = getLobHeader(aID);
        long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

        data = getLobHeader(bID);

        long    bLength    = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        int[][] aAddresses = getBlockAddresses(aID, 0, Integer.MAX_VALUE);
        int[][] bAddresses = getBlockAddresses(bID, 0, Integer.MAX_VALUE);
        int     aIndex     = 0;
        int     bIndex     = 0;
        int     aOffset    = 0;
        int     bOffset    = 0;

        while (true) {
            int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR] + aOffset;
            int bBlockOffset = bAddresses[bIndex][LOBS.BLOCK_ADDR] + bOffset;
            byte[] aBytes    = getLobStore().getBlockBytes(aBlockOffset, 1);
            byte[] bBytes    = getLobStore().getBlockBytes(bBlockOffset, 1);
            long aLimit = aLength
                          - (aAddresses[aIndex][LOBS.BLOCK_OFFSET] + aOffset)
                            * lobBlockSize / 2;

            if (aLimit > lobBlockSize / 2) {
                aLimit = lobBlockSize / 2;
            }

            long bLimit = bLength
                          - (bAddresses[bIndex][LOBS.BLOCK_OFFSET] + bOffset)
                            * lobBlockSize / 2;

            if (bLimit > lobBlockSize / 2) {
                bLimit = lobBlockSize / 2;
            }

            String aString = new String(ArrayUtil.byteArrayToChars(aBytes), 0,
                                        (int) aLimit);
            String bString = new String(ArrayUtil.byteArrayToChars(bBytes), 0,
                                        (int) bLimit);
            int diff = database.collation.compare(aString, bString);

            if (diff != 0) {
                return diff;
            }

            aOffset++;
            bOffset++;

            if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                aOffset = 0;

                aIndex++;
            }

            if (bOffset == bAddresses[bIndex][LOBS.BLOCK_COUNT]) {
                bOffset = 0;

                bIndex++;
            }

            if (aIndex == aAddresses.length) {
                break;
            }
        }

        return 0;
    }

    /**
     * Used for SUBSTRING
     */
    public Result getLob(long lobID, long offset, long length) {

        if (offset == 0) {
            return createDuplicateLob(lobID, length);
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "LobManager");
    }

    public Result createDuplicateLob(long lobID, long newLength) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

            if (length <= newLength) {
                return ResultLob.newLobCreateBlobResponse(lobID);
            }

            long   newLobID = getNewLobID();
            Object params[] = new Object[data.length];

            params[LOB_IDS.LOB_ID] = ValuePool.getLong(newLobID);
            params[1]              = data[1];
            params[2]              = data[2];
            params[3]              = data[3];

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params);

            if (result.isError()) {
                return result;
            }

            long byteLength = newLength;
            int  lobType    = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            if (lobType == Types.SQL_CLOB) {
                byteLength *= 2;
            }

            int newBlockCount = (int) (byteLength / lobBlockSize);

            if (byteLength % lobBlockSize != 0) {
                newBlockCount++;
            }

            createBlockAddresses(newLobID, 0, newBlockCount);

            // copy the contents
            int[][] sourceBlocks = getBlockAddresses(lobID, 0,
                Integer.MAX_VALUE);
            int[][] targetBlocks = getBlockAddresses(newLobID, 0,
                Integer.MAX_VALUE);

            try {
                copyBlockSet(sourceBlocks, targetBlocks);
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            // clear the end block unused space
            int endOffset = (int) (byteLength % lobBlockSize);

            if (endOffset != 0) {
                int[] block = targetBlocks[targetBlocks.length - 1];
                int blockOffset = block[LOBS.BLOCK_ADDR]
                                  + block[LOBS.BLOCK_COUNT] - 1;
                byte[] bytes = getLobStore().getBlockBytes(blockOffset, 1);

                ArrayUtil.fillArray(bytes, endOffset, (byte) 0);
                getLobStore().setBlockBytes(bytes, blockOffset, 1);
            }

            usageCountChanged = true;

            return ResultLob.newLobSetResponse(newLobID, newLength);
        } finally {
            writeLock.unlock();
        }
    }

    // todo - currently returns whole length
    public Result getTruncateLength(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                throw Error.error(ErrorCode.X_0F502);
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int  type   = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            return ResultLob.newLobSetResponse(lobID, length);
        } finally {
            writeLock.unlock();
        }
    }

    private void copyBlockSet(int[][] source, int[][] target) {

        int sourceIndex = 0;
        int targetIndex = 0;

        while (true) {
            int sourceOffset = source[sourceIndex][LOBS.BLOCK_OFFSET]
                               + sourceIndex;
            int targetOffset = target[targetIndex][LOBS.BLOCK_OFFSET]
                               + targetIndex;
            byte[] bytes = getLobStore().getBlockBytes(sourceOffset, 1);

            getLobStore().setBlockBytes(bytes, targetOffset, 1);

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

            if (targetIndex == target.length) {
                break;
            }
        }

        storeModified = true;
    }

    public Result getChars(long lobID, long offset, int length) {

        Result result = getBytes(lobID, offset * 2, length * 2);

        if (result.isError()) {
            return result;
        }

        byte[] bytes = ((ResultLob) result).getByteArray();
        char[] chars = ArrayUtil.byteArrayToChars(bytes);

        return ResultLob.newLobGetCharsResponse(lobID, offset, chars);
    }

    public Result getBytes(long lobID, long offset, int length) {

        writeLock.lock();

        try {
            int blockOffset     = (int) (offset / lobBlockSize);
            int byteBlockOffset = (int) (offset % lobBlockSize);
            int blockLimit      = (int) ((offset + length) / lobBlockSize);
            int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

            if (byteLimitOffset == 0) {
                byteLimitOffset = lobBlockSize;
            } else {
                blockLimit++;
            }

            if (length == 0) {
                return ResultLob.newLobGetBytesResponse(lobID, offset,
                        BinaryData.zeroLengthBytes);
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
                             + blockAddresses[i][LOBS.BLOCK_OFFSET]
                             - blockOffset;

            if (blockAddresses[i][LOBS.BLOCK_COUNT]
                    + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
                blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                               + blockAddresses[i][LOBS.BLOCK_OFFSET]
                               - blockLimit);
            }

            byte[] bytes;

            try {
                bytes = getLobStore().getBlockBytes(
                    blockAddresses[i][LOBS.BLOCK_ADDR]
                    - blockAddresses[i][LOBS.BLOCK_OFFSET]
                    + blockOffset, blockCount);
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            int subLength = lobBlockSize * blockCount - byteBlockOffset;

            if (subLength > length) {
                subLength = length;
            }

            System.arraycopy(bytes, byteBlockOffset, dataBytes,
                             dataBytesPosition, subLength);

            dataBytesPosition += subLength;

            i++;

            for (; i < blockAddresses.length && dataBytesPosition < length;
                    i++) {
                blockCount = blockAddresses[i][LOBS.BLOCK_COUNT];

                if (blockAddresses[i][LOBS.BLOCK_COUNT]
                        + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
                    blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                                   + blockAddresses[i][LOBS.BLOCK_OFFSET]
                                   - blockLimit);
                }

                try {
                    bytes = getLobStore().getBlockBytes(
                        blockAddresses[i][LOBS.BLOCK_ADDR], blockCount);
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
        } finally {
            writeLock.unlock();
        }
    }

    public Result setBytesBA(long lobID, byte[] dataBytes, long offset,
                             int length) {

        writeLock.lock();

        try {
            int blockOffset     = (int) (offset / lobBlockSize);
            int byteBlockOffset = (int) (offset % lobBlockSize);
            int blockLimit      = (int) ((offset + length) / lobBlockSize);
            int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

            if (byteLimitOffset == 0) {
                byteLimitOffset = lobBlockSize;
            } else {
                blockLimit++;
            }

            int[][] blockAddresses = getBlockAddresses(lobID, blockOffset,
                blockLimit);
            byte[] newBytes =
                new byte[(blockLimit - blockOffset) * lobBlockSize];

            if (blockAddresses.length > 0) {
                int blockAddress = blockAddresses[0][LOBS.BLOCK_ADDR]
                                   + (blockOffset
                                      - blockAddresses[0][LOBS.BLOCK_OFFSET]);

                try {
                    byte[] block = getLobStore().getBlockBytes(blockAddress,
                        1);

                    System.arraycopy(block, 0, newBytes, 0, lobBlockSize);

                    if (blockAddresses.length > 1) {
                        blockAddress =
                            blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_ADDR]
                            + (blockLimit
                               - blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_OFFSET]
                               - 1);
                        block = getLobStore().getBlockBytes(blockAddress, 1);

                        System.arraycopy(block, 0, newBytes,
                                         blockLimit - blockOffset - 1,
                                         lobBlockSize);
                    } else if (blockLimit - blockOffset > 1) {
                        blockAddress =
                            blockAddresses[0][LOBS.BLOCK_ADDR]
                            + (blockLimit
                               - blockAddresses[0][LOBS.BLOCK_OFFSET] - 1);
                        block = getLobStore().getBlockBytes(blockAddress, 1);

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
                    getLobStore().setBlockBytes(newBytes,
                                                blockAddresses[i][0],
                                                blockAddresses[i][1]);
                }
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            storeModified = true;

            return ResultLob.newLobSetResponse(lobID, 0);
        } finally {
            writeLock.unlock();
        }
    }

    private Result setBytesIS(long lobID, InputStream inputStream,
                              long length, boolean adjustLength) {

        long writeLength     = 0;
        int  blockLimit      = (int) (length / lobBlockSize);
        int  byteLimitOffset = (int) (length % lobBlockSize);

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

                    java.util.Arrays.fill(dataBytes, (byte) 0);
                }

                try {
                    int count = 0;

                    while (localLength > 0) {
                        int read = inputStream.read(dataBytes, count,
                                                    localLength);

                        if (read == -1) {
                            if (adjustLength) {
                                read = localLength;
                            } else {
                                return Result.newErrorResult(
                                    new EOFException());
                            }
                        } else {
                            writeLength += read;
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
                    getLobStore().setBlockBytes(
                        dataBytes, blockAddresses[i][LOBS.BLOCK_ADDR] + j, 1);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
        }

        storeModified = true;

        return ResultLob.newLobSetResponse(lobID, writeLength);
    }

    public Result setBytes(long lobID, long offset, byte[] dataBytes) {

        if (dataBytes.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            Result result = setBytesBA(lobID, dataBytes, offset,
                                       dataBytes.length);

            if (offset + dataBytes.length > length) {
                setLength(lobID, offset + dataBytes.length);
            }

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result setBytesForNewBlob(long lobID, InputStream inputStream,
                                     long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        writeLock.lock();

        try {
            Result result = setBytesIS(lobID, inputStream, length, false);

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result setChars(long lobID, long offset, char[] chars) {

        if (chars.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long   length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            byte[] bytes  = ArrayUtil.charArrayToBytes(chars);
            Result result = setBytesBA(lobID, bytes, offset * 2,
                                       chars.length * 2);

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
        } finally {
            writeLock.unlock();
        }
    }

    public Result setCharsForNewClob(long lobID, InputStream inputStream,
                                     long length, boolean adjustLength) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        writeLock.lock();

        try {
            Result result = setBytesIS(lobID, inputStream, length * 2,
                                       adjustLength);

            if (result.isError()) {
                return result;
            }

            long newLength = ((ResultLob) result).getBlockLength();

            if (newLength < length) {
                Result trunc = truncate(lobID, newLength);
            }

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result truncate(long lobID, long offset) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length     = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            long byteLength = offset;

            if (((Integer) data[LOB_IDS.LOB_TYPE]).intValue()
                    == Types.SQL_CLOB) {
                byteLength *= 2;
            }

            int blockOffset = (int) ((byteLength + lobBlockSize - 1)
                                     / lobBlockSize);
            ResultMetaData meta = deleteLobPartCall.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[DELETE_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);
            params[DELETE_BLOCKS.BLOCK_OFFSET] = new Integer(blockOffset);
            params[DELETE_BLOCKS.BLOCK_LIMIT] = new Integer(Integer.MAX_VALUE);
            params[DELETE_BLOCKS.TX_ID] =
                ValuePool.getLong(sysLobSession.getTransactionTimestamp());

            Result result =
                sysLobSession.executeCompiledStatement(deleteLobPartCall,
                    params);

            setLength(lobID, offset);

            return ResultLob.newLobTruncateResponse(lobID);
        } finally {
            writeLock.unlock();
        }
    }

    private Result setLength(long lobID, long length) {

        ResultMetaData meta     = updateLobLength.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_LENGTH.LOB_LENGTH] = ValuePool.getLong(length);
        params[UPDATE_LENGTH.LOB_ID]     = ValuePool.getLong(lobID);

        Result result = sysLobSession.executeCompiledStatement(updateLobLength,
            params);

        return result;
    }

    /**
     * Executes in user session. No synchronization
     */
    public Result adjustUsageCount(Session session, long lobID, int delta) {

        ResultMetaData meta     = updateLobUsage.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_USAGE.BLOCK_COUNT] = ValuePool.getInt(delta);
        params[UPDATE_USAGE.LOB_ID]      = ValuePool.getLong(lobID);

        session.sessionContext.pushDynamicArguments(params);

        Result result = updateLobUsage.execute(session);

        session.sessionContext.pop();

        return result;
    }

    private int[][] getBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[GET_LOB_PART.LOB_ID]       = ValuePool.getLong(lobID);
        params[GET_LOB_PART.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[GET_LOB_PART.BLOCK_LIMIT]  = ValuePool.getInt(limit);

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

    private void deleteBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = deleteLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DELETE_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);
        params[DELETE_BLOCKS.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[DELETE_BLOCKS.BLOCK_LIMIT]  = ValuePool.getInt(limit);
        params[DELETE_BLOCKS.TX_ID] =
            ValuePool.getLong(sysLobSession.getTransactionTimestamp());

        Result result =
            sysLobSession.executeCompiledStatement(deleteLobPartCall, params);
    }

    private void divideBlockAddresses(long lobID, int offset) {

        ResultMetaData meta     = divideLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DIVIDE_BLOCK.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[DIVIDE_BLOCK.LOB_ID]       = ValuePool.getLong(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(divideLobPartCall, params);
    }

    private void createBlockAddresses(long lobID, int offset, int count) {

        ResultMetaData meta     = createLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_BLOCKS.BLOCK_COUNT]  = ValuePool.getInt(count);
        params[ALLOC_BLOCKS.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[ALLOC_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(createLobPartCall, params);
    }

    public int getLobCount() {

        writeLock.lock();

        try {
            sysLobSession.sessionContext.pushDynamicArguments(new Object[]{});

            Result result = getLobCount.execute(sysLobSession);

            sysLobSession.sessionContext.pop();

            RowSetNavigator navigator = result.getNavigator();
            boolean         next      = navigator.next();

            if (!next) {
                navigator.close();

                return 0;
            }

            Object[] data = navigator.getCurrent();

            return ((Number) data[0]).intValue();
        } finally {
            writeLock.unlock();
        }
    }

    public void synch() {

        if (storeModified) {
            if (lobStore != null) {
                writeLock.lock();

                try {
                    lobStore.synch();

                    storeModified = false;
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }
}
