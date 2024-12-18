/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.CharArrayWriter;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.LongKeyLongValueHashMap;
import org.hsqldb.lib.ReaderInputStream;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.persist.PersistentStoreCollectionSession;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.LobData;

/**
 * Session semi-persistent data structures.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class SessionData {

    private final Database                  database;
    private final Session                   session;
    public PersistentStoreCollectionSession persistentStoreCollection;

    // large results
    LongKeyHashMap<Result> resultMap;

    // VALUE
    Object currentValue;

    // SEQUENCE
    HashMap<HsqlName, Number>       sequenceMap;
    HashMap<NumberSequence, Number> sequenceUpdateMap;

    // lobs in results
    LongKeyLongValueHashMap resultLobs = new LongKeyLongValueHashMap();

    // new lob tracking
    static final long noLobFloor  = -1;
    long              newLobFloor = noLobFloor;

    public SessionData(Database database, Session session) {

        this.database = database;
        this.session  = session;
        persistentStoreCollection = new PersistentStoreCollectionSession(
            session);
    }

    public PersistentStore getSubqueryRowStore(TableBase table) {

        PersistentStore store = persistentStoreCollection.getStore(table);

        store.removeAll();

        return store;
    }

    public PersistentStore getNewResultRowStore(
            TableBase table,
            boolean isCached) {

        try {
            PersistentStore store = persistentStoreCollection.getStore(table);

            if (!isCached) {
                store.setMemory(true);
            }

            return store;
        } catch (HsqlException e) {}

        throw Error.runtimeError(ErrorCode.U_S0500, "SessionData");
    }

    // result
    void setResultSetProperties(Result command, Result result) {

        int required = command.rsProperties;
        int returned = result.getStatement().getResultProperties();

        if (required != returned) {
            if (ResultProperties.isUpdatable(required)) {
                if (ResultProperties.isReadOnly(returned)) {
                    session.addWarning(Error.error(ErrorCode.W_36502));
                }
            }

            if (ResultProperties.isSensitive(required)) {
                session.addWarning(Error.error(ErrorCode.W_36501));
            }

            returned = ResultProperties.addScrollable(
                returned,
                ResultProperties.isScrollable(required));
            returned = ResultProperties.addHoldable(
                returned,
                ResultProperties.isHoldable(required));
            result.rsProperties = returned;
        }
    }

    Result getDataResultHead(Result command, Result result, boolean isNetwork) {

        int fetchSize = command.getFetchSize();

        result.setResultId(session.actionSCN);

        int required = command.rsProperties;
        int returned = result.rsProperties;

        if (required != returned) {
            if (ResultProperties.isReadOnly(required)) {
                returned = ResultProperties.addHoldable(
                    returned,
                    ResultProperties.isHoldable(required));
            } else {
                if (ResultProperties.isReadOnly(returned)) {
                    returned = ResultProperties.addHoldable(
                        returned,
                        ResultProperties.isHoldable(required));

                    // add warning for concurrency conflict
                } else {
                    if (session.isAutoCommit()) {
                        returned = ResultProperties.addHoldable(
                            returned,
                            ResultProperties.isHoldable(required));
                    } else {
                        returned = ResultProperties.addHoldable(
                            returned,
                            false);
                    }
                }
            }

            returned = ResultProperties.addScrollable(
                returned,
                ResultProperties.isScrollable(required));
            result.rsProperties = returned;
        }

        boolean hold = false;
        boolean copy = false;

        if (ResultProperties.isUpdatable(result.rsProperties)) {
            hold = true;
        }

        if (isNetwork) {
            if (fetchSize != 0 && result.getNavigator().getSize() > fetchSize) {
                copy = true;
                hold = true;
            }
        } else {
            if (!result.getNavigator().isMemory()) {
                hold = true;
            }
        }

        if (hold) {
            if (resultMap == null) {
                resultMap = new LongKeyHashMap<>();
            }

            resultMap.put(result.getResultId(), result);

            result.rsProperties = ResultProperties.addIsHeld(
                result.rsProperties,
                true);
        }

        if (copy) {
            result = Result.newDataHeadResult(session, result, 0, fetchSize);
        }

        return result;
    }

    Result getDataResultSlice(long id, int offset, int count) {

        Result          result = resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return Result.newDataRowsResult(result, offset, count);
    }

    Result getDataResult(long id) {
        Result result = resultMap.get(id);

        return result;
    }

    RowSetNavigatorClient getRowSetSlice(long id, int offset, int count) {

        Result          result = resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return new RowSetNavigatorClient(source, offset, count);
    }

    public void closeNavigator(long id) {

        Result result = resultMap.remove(id);

        if (result != null) {
            result.getNavigator().release();
        }
    }

    public void closeAllNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator<Result> it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = it.next();

            result.getNavigator().release();
        }

        resultMap.clear();
    }

    public void closeAllTransactionNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator<Result> it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = it.next();

            if (!ResultProperties.isHoldable(result.rsProperties)) {
                result.getNavigator().release();
                it.remove();
            }
        }
    }

    // lobs in transaction
    public void adjustLobUsageCount(LobData value, int adjust) {

        if (session.isProcessingLog() || session.isProcessingScript()) {
            return;
        }

        if (value == null) {
            return;
        }

        database.lobManager.adjustUsageCount(session, value.getId(), adjust);
    }

    public void adjustLobUsageCount(
            TableBase table,
            Object[] data,
            int adjust) {

        if (!table.hasLobColumn) {
            return;
        }

        if (table.isTemp) {
            return;
        }

        if (session.isProcessingLog() || session.isProcessingScript()) {
            return;
        }

        for (int j = 0; j < table.columnCount; j++) {
            if (table.colTypes[j].isLobType()) {
                Object value = data[j];

                if (value == null) {
                    continue;
                }

                database.lobManager.adjustUsageCount(
                    session,
                    ((LobData) value).getId(),
                    adjust);
            }
        }
    }

    /**
     * allocate storage for a new LOB
     */
    public Result allocateLobForResult(ResultLob result) {

        Result      actionResult = Result.updateZeroResult;
        InputStream inputStream  = result.getInputStream();
        long        lobId        = result.getLobID();
        long        dataLength   = result.getBlockLength();

        try {
            switch (result.getSubType()) {

                case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES : {
                    dataLength = result.getBlockLength();

                    if (lobId >= 0) {
                        if (dataLength < 0) {

                            // embedded session + unknown lob length
                            actionResult = allocateBlobSegments(
                                result,
                                inputStream);
                        } else {

                            // embedded session + known lob length
                            actionResult =
                                database.lobManager.setBytesForNewBlob(
                                    lobId,
                                    inputStream,
                                    dataLength);
                        }
                    } else {

                        // server session + known or unknown lob length
                        BlobData blob = session.createBlob(dataLength);

                        resultLobs.put(lobId, blob.getId());

                        lobId = blob.getId();
                        actionResult = database.lobManager.setBytesForNewBlob(
                            lobId,
                            inputStream,
                            dataLength);
                    }

                    break;
                }

                case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS : {
                    if (lobId >= 0) {
                        if (dataLength < 0) {

                            // embedded session + unknown lob length
                            actionResult = allocateClobSegments(
                                result,
                                result.getReader());
                        } else {

                            // embedded session + known lob length
                            if (result.getReader() != null) {
                                inputStream = new ReaderInputStream(
                                    result.getReader());
                            }

                            actionResult =
                                database.lobManager.setCharsForNewClob(
                                    lobId,
                                    inputStream,
                                    result.getBlockLength());
                        }
                    } else {

                        // server session + known or unknown lob length
                        ClobData clob = session.createClob(dataLength);

                        resultLobs.put(lobId, clob.getId());

                        lobId = clob.getId();
                        actionResult = database.lobManager.setCharsForNewClob(
                            lobId,
                            inputStream,
                            result.getBlockLength());
                    }

                    break;
                }

                case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {

                    // server session + unknown lob length
                    if (lobId < 0) {
                        lobId = resultLobs.get(lobId);
                    }

                    byte[] byteArray = result.getByteArray();

                    actionResult = database.lobManager.setBytes(
                        lobId,
                        result.getOffset(),
                        byteArray,
                        (int) dataLength);
                    break;
                }

                case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {

                    // server session + unknown lob length
                    if (lobId < 0) {
                        lobId = resultLobs.get(lobId);
                    }

                    char[] charArray = result.getCharArray();

                    actionResult = database.lobManager.setChars(
                        lobId,
                        result.getOffset(),
                        charArray,
                        (int) dataLength);
                    break;
                }
            }
        } catch (Throwable e) {
            resultLobs.clear();

            throw Error.error(ErrorCode.GENERAL_ERROR, e);
        }

        return actionResult;
    }

    Result allocateBlobSegments(
            ResultLob result,
            InputStream stream)
            throws IOException {

        long   currentOffset = result.getOffset();
        int    bufferLength  = session.getStreamBlockSize();
        HsqlByteArrayOutputStream byteArrayOS = new HsqlByteArrayOutputStream(
            bufferLength);
        Result actionResult  = null;
        long   totalLength   = 0;

        while (true) {
            byteArrayOS.reset();
            byteArrayOS.write(stream, bufferLength);

            if (byteArrayOS.size() == 0) {
                break;
            }

            byte[] byteArray = byteArrayOS.getBuffer();

            actionResult = database.lobManager.setBytes(
                result.getLobID(),
                currentOffset,
                byteArray,
                byteArrayOS.size());

            if (actionResult.isError()) {
                break;
            }

            currentOffset += byteArrayOS.size();
            totalLength   += byteArrayOS.size();

            if (byteArrayOS.size() < bufferLength) {
                break;
            }
        }

        if (actionResult == null) {
            actionResult = ResultLob.newLobSetResponse(
                result.getLobID(),
                totalLength);
        }

        return actionResult;
    }

    private Result allocateClobSegments(
            ResultLob result,
            Reader reader)
            throws IOException {

        return allocateClobSegments(
            result.getLobID(),
            result.getOffset(),
            reader);
    }

    private Result allocateClobSegments(
            long lobID,
            long offset,
            Reader reader)
            throws IOException {

        int             bufferLength  = session.getStreamBlockSize();
        CharArrayWriter charWriter    = new CharArrayWriter(bufferLength);
        long            currentOffset = offset;

        while (true) {
            charWriter.reset();
            charWriter.write(reader, bufferLength);

            char[] charArray = charWriter.getBuffer();

            if (charWriter.size() == 0) {
                return Result.updateZeroResult;
            }

            Result actionResult = database.lobManager.setChars(
                lobID,
                currentOffset,
                charArray,
                charWriter.size());

            if (actionResult.isError()) {
                return actionResult;
            }

            currentOffset += charWriter.size();

            if (charWriter.size() < bufferLength) {
                return Result.updateZeroResult;
            }
        }
    }

    public void registerLobForResult(Result result) {

        RowSetNavigator navigator = result.getNavigator();

        if (navigator == null) {
            registerLobsForRow((Object[]) result.valueData);
        } else {
            while (navigator.next()) {
                Object[] data = navigator.getCurrent();

                registerLobsForRow(data);
            }

            navigator.reset();
        }

        resultLobs.clear();
    }

    private void registerLobsForRow(Object[] data) {

        for (int i = 0; i < data.length; i++) {
            if (data[i] instanceof BlobDataID) {
                BlobData blob = (BlobDataID) data[i];
                long     id   = blob.getId();

                if (id < 0) {
                    id = resultLobs.get(id);
                }

                data[i] = database.lobManager.getBlob(id);

                // handle invalid id;
            } else if (data[i] instanceof ClobDataID) {
                ClobData clob = (ClobDataID) data[i];
                long     id   = clob.getId();

                if (id < 0) {
                    id = resultLobs.get(id);
                }

                data[i] = database.lobManager.getClob(id);

                // handle invalid id;
            }
        }
    }

    ClobData createClobFromFile(String filename, String encoding) {

        File        file       = getFile(filename);
        long        fileLength = file.length();
        InputStream is         = null;

        try {
            ClobData clob = session.createClob(fileLength);

            is = new FileInputStream(file);

            Reader reader = new InputStreamReader(is, encoding);

            allocateClobSegments(clob.getId(), 0, reader);

            return clob;
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR, e.toString());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {}
        }
    }

    BlobData createBlobFromFile(String filename) {

        File        file       = getFile(filename);
        long        fileLength = file.length();
        InputStream is         = null;

        try {
            BlobData blob = session.createBlob(fileLength);

            is = new FileInputStream(file);

            database.lobManager.setBytesForNewBlob(
                blob.getId(),
                is,
                fileLength);

            return blob;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {}
        }
    }

    private File getFile(String name) {

        session.checkAdmin();

        String fileName = database.logger.getSecurePath(name, false, false);

        if (fileName == null) {
            throw Error.error(ErrorCode.ACCESS_IS_DENIED, name);
        }

        File    file   = new File(fileName);
        boolean exists = file.exists();

        if (!exists) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }

        return file;
    }

    // sequences
    public void startRowProcessing() {
        if (sequenceMap != null) {
            sequenceMap.clear();
        }
    }

    public Number getSequenceValue(NumberSequence sequence) {

        if (sequenceMap == null) {
            sequenceMap       = new HashMap<>();
            sequenceUpdateMap = new HashMap<>();
        }

        HsqlName key   = sequence.getName();
        Number   value = sequenceMap.get(key);

        if (value == null) {
            value = sequence.getValueObject();

            sequenceMap.put(key, value);
            sequenceUpdateMap.put(sequence, value);
        }

        return value;
    }

    public Number getSequenceCurrent(NumberSequence sequence) {
        return sequenceUpdateMap == null
               ? null
               : sequenceUpdateMap.get(sequence);
    }
}
