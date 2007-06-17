/* Copyright (c) 2001-2007, The HSQL Development Group
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
import java.io.DataInputStream;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.index.Node;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.LongKeyLongValueHashMap;
import org.hsqldb.navigator.ClientRowSetNavigator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.persist.TempDataFileCache;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.BlobDataMemory;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobDataMemory;

/*
 * Session data structures
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class SessionData {

    /**
     * SessionData
     *
     * @param database Database
     * @param session Session
     */
    public SessionData(Database database, Session session) {
        this.database = database;
        this.session  = session;
    }

    private Database database;
    private Session  session;

    // two types of temp tables
    private IntKeyHashMap indexArrayMap;
    private IntKeyHashMap indexArrayKeepMap;

    // large results
    LongKeyHashMap    resultMap;
    TempDataFileCache resultCache;

    // lobs
    LongKeyLongValueHashMap lobs = new LongKeyLongValueHashMap();

    // VALUE
    Object currentValue;

    public Object getCurrentValue() {
        return currentValue;
    }
    /**
     * get the root for a temp table index
     */
    public Node getIndexRoot(HsqlName index, boolean preserve) {

        if (preserve) {
            if (indexArrayKeepMap == null) {
                return null;
            }

            return (Node) indexArrayKeepMap.get(index.hashCode());
        } else {
            if (indexArrayMap == null) {
                return null;
            }

            return (Node) indexArrayMap.get(index.hashCode());
        }
    }

    /**
     * set the root for a temp table index
     */
    public void setIndexRoot(HsqlName index, boolean preserve, Node root) {

        if (preserve) {
            if (indexArrayKeepMap == null) {
                if (root == null) {
                    return;
                }

                indexArrayKeepMap = new IntKeyHashMap();
            }

            indexArrayKeepMap.put(index.hashCode(), root);
        } else {
            if (indexArrayMap == null) {
                if (root == null) {
                    return;
                }

                indexArrayMap = new IntKeyHashMap();
            }

            indexArrayMap.put(index.hashCode(), root);
        }
    }

    void dropIndex(HsqlName index, boolean preserve) {

        if (preserve) {
            if (indexArrayKeepMap != null) {
                indexArrayKeepMap.remove(index.hashCode());
            }
        } else {
            if (indexArrayMap != null) {
                indexArrayMap.remove(index.hashCode());
            }
        }
    }

    /**
     * clear default temp table contents for this session
     */
    void clearIndexRoots() {

        if (indexArrayMap != null) {
            indexArrayMap.clear();
        }
    }

    /**
     * clear ON COMMIT PRESERVE temp table contents for this session
     */
    void clearIndexRootsKeep() {

        if (indexArrayKeepMap != null) {
            indexArrayKeepMap.clear();
        }
    }

    // result
    Result getDataResultHead(Result command, Result result,
                             boolean isNetwork) {

        int fetchSize = command.getFetchSize();

        result.setResultId(session.actionTimestamp);

        boolean hold = false;
        boolean copy = false;

        if (isNetwork) {
            if (fetchSize != 0
                    && result.getNavigator().getSize() > fetchSize) {
                copy = true;
                hold = true;
            }
        } else {
            if (result.getNavigator().isDiskBased()) {
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

        ClientRowSetNavigator navigator = getRowSetSlice(id, offset, count);
        return Result.newDataRowsResult(navigator);
    }

    ClientRowSetNavigator getRowSetSlice(long id, int offset, int count) {

        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();

        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }

        return new ClientRowSetNavigator(source, offset, count);
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

    TempDataFileCache getResultCache() throws HsqlException {

        if (resultCache == null) {
            String path = database.getTempDirectoryPath();

            if (path == null) {
                return null;
            }

            resultCache =
                new TempDataFileCache(database,
                                      path + "/session."
                                      + Long.toString(session.getId()));

            resultCache.open(false);
        }

        return resultCache;
    }

    synchronized void closeResultCache() {

        if (resultCache != null) {
            try {
                resultCache.close(false);
            } catch (HsqlException e) {}

            resultCache = null;
        }
    }

    // LOBs

    /**
     * allocate storage for a new LOB
     */
    public void allocateLobForResult(ResultLob result,
                                     DataInput dataInput)
                                     throws HsqlException {

        switch (result.getSubType()) {

            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES : {
                if (dataInput == null) {
                    dataInput = new DataInputStream(result.getInputStream());
                }

                BlobData blob = new BlobDataMemory(result.getBlockLength(),
                                                   dataInput);
                long resultLobId = result.getLobID();

                if (dataInput != null) {
                    blob.setId(database.lobManager.getNewLobId());
                    lobs.put(resultLobId, blob.getId());
                }

                database.lobManager.addBlob(blob);

                break;
            }
            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS : {
                ClobData clob;

                if (dataInput == null) {
                    dataInput = new DataInputStream(result.getInputStream());
                    clob = new ClobDataMemory(result.getBlockLength(),
                                              result.getReader());
                } else {
                    clob = new ClobDataMemory(result.getBlockLength(),
                                              dataInput);
                }

                long resultLobId = result.getLobID();

                if (dataInput != null) {
                    clob.setId(database.lobManager.getNewLobId());
                    lobs.put(resultLobId, clob.getId());
                }

                database.lobManager.addClob(clob);

                break;
            }
        }
    }

    public void registerLobForResult(Result result) throws HsqlException {

        RowSetNavigator navigator = result.getNavigator();

        while (navigator.next()) {
            Object[] data = (Object[]) navigator.getCurrent();

            for (int i = 0; i < data.length; i++) {
                if (data[i] instanceof BlobDataID) {
                    BlobData blob = (BlobDataID) data[i];
                    long     id   = blob.getId();

                    blob.setId(lobs.get(id));
                } else if (data[i] instanceof ClobDataID) {
                    ClobData clob = (ClobDataID) data[i];
                    long     id   = clob.getId();

                    clob.setId(lobs.get(id));
                }
            }
        }

        lobs.clear();
        navigator.reset();
    }
}
