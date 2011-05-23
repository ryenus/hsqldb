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
import org.hsqldb.lib.CharArrayWriter;
import org.hsqldb.lib.CountdownInputStream;
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

/*
 * Session semi-persistent data structures
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class SessionData {

    private final Database           database;
    private final Session            session;
    PersistentStoreCollectionSession persistentStoreCollection;

    // large results
    LongKeyHashMap resultMap;

    // VALUE
    Object currentValue;

    // SEQUENCE
    HashMap sequenceMap;
    HashMap sequenceUpdateMap;

    public SessionData(Database database, Session session) {

        this.database = database;
        this.session  = session;
        persistentStoreCollection =
            new PersistentStoreCollectionSession(session);
    }

    public PersistentStore getSubqueryRowStore(TableBase table) {

        PersistentStore store = persistentStoreCollection.getStore(table);

        store.removeAll();

        return store;
    }

    public PersistentStore getNewResultRowStore(TableBase table,
            boolean isCached) {

        try {
            PersistentStore store = session.database.logger.newStore(session,
                persistentStoreCollection, table);

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
        int returned = result.rsProperties;

        if (required != returned) {
            if (ResultProperties.isReadOnly(required)) {
                returned = ResultProperties.addHoldable(returned,
                        ResultProperties.isHoldable(required));
            } else {
                if (ResultProperties.isUpdatable(returned)) {
                    if (ResultProperties.isHoldable(required)) {
                        session.addWarning(Error.error(ErrorCode.W_36503));
                    }
                } else {
                    returned = ResultProperties.addHoldable(returned,
                            ResultProperties.isHoldable(required));

                    session.addWarning(Error.error(ErrorCode.W_36502));
                }
            }

            if (ResultProperties.isSensitive(required)) {
                session.addWarning(Error.error(ErrorCode.W_36501));
            }

            returned = ResultProperties.addScrollable(returned,
                    ResultProperties.isScrollable(required));
            result.rsProperties = returned;
        }
    }

    Result getDataResultHead(Result command, Result result,
                             boolean isNetwork) {

        int fetchSize = command.getFetchSize();

        result.setResultId(session.actionTimestamp);

        int required = command.rsProperties;
        int returned = result.rsProperties;

        if (required != returned) {
            if (ResultProperties.isReadOnly(required)) {
                returned = ResultProperties.addHoldable(returned,
                        ResultProperties.isHoldable(required));
            } else {
                if (ResultProperties.isReadOnly(returned)) {
                    returned = ResultProperties.addHoldable(returned,
                            ResultProperties.isHoldable(required));

                    // add warning for concurrency conflict
                } else {
                    if (session.isAutoCommit()) {
                        returned = ResultProperties.addHoldable(returned,
                                ResultProperties.isHoldable(required));
                    } else {
                        returned = ResultProperties.addHoldable(returned,
                                false);
                    }
                }
            }

            returned = ResultProperties.addScrollable(returned,
                    ResultProperties.isScrollable(required));
            result.rsProperties = returned;
        }

        boolean hold = false;
        boolean copy = false;

        if (ResultProperties.isUpdatable(result.rsProperties)) {
            hold = true;
        }

        if (isNetwork) {
            if (fetchSize != 0
                    && result.getNavigator().getSize() > fetchSize) {
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
                resultMap = new LongKeyHashMap();
            }

            resultMap.put(result.getResultId(), result);
        }

        if (copy) {
            result = Result.newDataHeadResult(session, result, 0, fetchSize);
        }

        return result;
    }

    Result getDataResultSlice(long id, int offset, int count) {

        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return Result.newDataRowsResult(result, offset, count);
    }

    Result getDataResult(long id) {

        Result result = (Result) resultMap.get(id);

        return result;
    }

    RowSetNavigatorClient getRowSetSlice(long id, int offset, int count) {

        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return new RowSetNavigatorClient(source, offset, count);
    }

    public void closeNavigator(long id) {

        Result result = (Result) resultMap.remove(id);

        result.getNavigator().close();
    }

    public void closeAllNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = (Result) it.next();

            result.getNavigator().close();
        }

        resultMap.clear();
    }

    public void closeAllTransactionNavigators() {

        if (resultMap == null) {
            return;
        }

        Iterator it = resultMap.values().iterator();

        while (it.hasNext()) {
            Result result = (Result) it.next();

            if (!ResultProperties.isHoldable(result.rsProperties)) {
                result.getNavigator().close();
                it.remove();
            }
        }

        resultMap.clear();
    }

    // lobs in results
    LongKeyLongValueHashMap resultLobs = new LongKeyLongValueHashMap();

    // lobs in transaction
    boolean hasLobOps;

    public void addToCreatedLobs(long lobID) {
        hasLobOps = true;
    }

    public void adjustLobUsageCount(Object value, int adjust) {

        if (session.isProcessingLog || session.isProcessingScript) {
            return;
        }

        if (value == null) {
            return;
        }

        database.lobManager.adjustUsageCount(session,
                                             ((LobData) value).getId(),
                                             adjust);

        hasLobOps = true;
    }

    public void adjustLobUsageCount(TableBase table, Object[] data,
                                    int adjust) {

        if (!table.hasLobColumn) {
            return;
        }

        if (table.isTemp) {
            return;
        }

        if (session.isProcessingLog || session.isProcessingScript) {
            return;
        }

        for (int j = 0; j < table.columnCount; j++) {
            if (table.colTypes[j].isLobType()) {
                Object value = data[j];

                if (value == null) {
                    continue;
                }

                database.lobManager.adjustUsageCount(session,
                                                     ((LobData) value).getId(),
                                                     adjust);

                hasLobOps = true;
            }
        }
    }

    /**
     * allocate storage for a new LOB
     */
    public void allocateLobForResult(ResultLob result,
                                     InputStream inputStream) {

        try {
            CountdownInputStream countStream;

            switch (result.getSubType()) {

                case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES : {
                    long blobId;
                    long blobLength = result.getBlockLength();

                    if (blobLength < 0) {

                        // embedded session + unknown lob length
                        allocateBlobSegments(result, result.getInputStream());

                        break;
                    }

                    if (inputStream == null) {

                        // embedded session + known lob length
                        blobId      = result.getLobID();
                        inputStream = result.getInputStream();
                    } else {

                        // server session + known or unknown lob length
                        BlobData blob = session.createBlob(blobLength);

                        blobId = blob.getId();

                        resultLobs.put(result.getLobID(), blobId);
                    }

                    countStream = new CountdownInputStream(inputStream);

                    countStream.setCount(blobLength);
                    database.lobManager.setBytesForNewBlob(
                        blobId, countStream, result.getBlockLength());

                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS : {
                    long clobId;
                    long clobLength = result.getBlockLength();

                    if (clobLength < 0) {

                        // embedded session + unknown lob length
                        allocateClobSegments(result, result.getReader());

                        break;
                    }

                    if (inputStream == null) {
                        clobId = result.getLobID();

                        // embedded session + known lob length
                        if (result.getReader() != null) {
                            inputStream =
                                new ReaderInputStream(result.getReader());
                        } else {
                            inputStream = result.getInputStream();
                        }
                    } else {

                        // server session + known or unknown lob length
                        ClobData clob = session.createClob(clobLength);

                        clobId = clob.getId();

                        resultLobs.put(result.getLobID(), clobId);
                    }

                    countStream = new CountdownInputStream(inputStream);

                    countStream.setCount(clobLength * 2);
                    database.lobManager.setCharsForNewClob(
                        clobId, countStream, result.getBlockLength(), false);

                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {

                    // server session + unknown lob length
                    long   blobId     = resultLobs.get(result.getLobID());
                    long   blobLength = result.getBlockLength();
                    byte[] byteArray  = result.getByteArray();
                    Result actionResult = database.lobManager.setBytes(blobId,
                        result.getOffset(), byteArray);

                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {

                    // server session + unknown lob length
                    long   clobId     = resultLobs.get(result.getLobID());
                    long   clobLength = result.getBlockLength();
                    char[] charArray  = result.getCharArray();
                    Result actionResult = database.lobManager.setChars(clobId,
                        result.getOffset(), charArray);

                    break;
                }
            }
        } catch (Throwable e) {
            resultLobs.clear();

            throw Error.error(ErrorCode.GENERAL_ERROR, e);
        }
    }

    private void allocateBlobSegments(ResultLob result,
                                      InputStream stream) throws IOException {

        //
        long currentOffset = result.getOffset();
        int  bufferLength  = session.getStreamBlockSize();
        HsqlByteArrayOutputStream byteArrayOS =
            new HsqlByteArrayOutputStream(bufferLength);

        while (true) {
            byteArrayOS.reset();
            byteArrayOS.write(stream, bufferLength);

            byte[] byteArray = byteArrayOS.getBuffer();

            if (byteArrayOS.size() < bufferLength) {
                byteArray = byteArrayOS.toByteArray();
            }

            Result actionResult =
                database.lobManager.setBytes(result.getLobID(), currentOffset,
                                             byteArray);

            currentOffset += byteArrayOS.size();

            if (byteArrayOS.size() < bufferLength) {
                return;
            }
        }
    }

    private void allocateClobSegments(ResultLob result,
                                      Reader reader) throws IOException {

        long            currentOffset = result.getOffset();
        int             bufferLength  = session.getStreamBlockSize();
        CharArrayWriter charWriter    = new CharArrayWriter(bufferLength);

        while (true) {
            charWriter.reset();
            charWriter.write(reader, bufferLength);

            char[] charArray = charWriter.getBuffer();

            if (charWriter.size() < bufferLength) {
                charArray = charWriter.toCharArray();
            }

            Result actionResult =
                database.lobManager.setChars(result.getLobID(), currentOffset,
                                             charArray);

            currentOffset += charWriter.size();

            if (charWriter.size() < bufferLength) {
                return;
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

        File    file   = new File(filename);
        boolean exists = file.exists();

        if (!exists) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }

        long        fileLength = file.length();
        InputStream is         = null;

        try {
            ClobData clob = session.createClob(fileLength);

            is = new FileInputStream(file);

            Reader reader = new InputStreamReader(is, encoding);

            is = new ReaderInputStream(reader);

            database.lobManager.setCharsForNewClob(clob.getId(), is,
                                                   fileLength, true);

            return clob;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, e.toString());
        } finally {
            try {
                is.close();
            } catch (Exception e) {}
        }
    }

    BlobData createBlobFromFile(String filename) {

        File    file   = new File(filename);
        boolean exists = file.exists();

        if (!exists) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }

        long        fileLength = file.length();
        InputStream is         = null;

        try {
            BlobData blob = session.createBlob(fileLength);

            is = new FileInputStream(file);

            database.lobManager.setBytesForNewBlob(blob.getId(), is,
                                                   fileLength);

            return blob;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        } finally {
            try {
                is.close();
            } catch (Exception e) {}
        }
    }

    // sequences
    public void startRowProcessing() {

        if (sequenceMap != null) {
            sequenceMap.clear();
        }
    }

    public Object getSequenceValue(NumberSequence sequence) {

        if (sequenceMap == null) {
            sequenceMap       = new HashMap();
            sequenceUpdateMap = new HashMap();
        }

        HsqlName key   = sequence.getName();
        Object   value = sequenceMap.get(key);

        if (value == null) {
            value = sequence.getValueObject();

            sequenceMap.put(key, value);
            sequenceUpdateMap.put(sequence, value);
        }

        return value;
    }

    public Object getSequenceCurrent(NumberSequence sequence) {
        return sequenceUpdateMap == null ? null
                                         : sequenceUpdateMap.get(sequence);
    }
}
