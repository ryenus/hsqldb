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

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.index.Index;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.Error;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public abstract class SimpleStore implements PersistentStore {

    public DataFileCache        cache;
    protected TableSpaceManager spaceManager;
    long                        storageSize;

    public void set(CachedObject object) {}

    public CachedObject get(long i) {

        try {
            return cache.get(i, this, false);
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(CachedObject object, boolean keep) {

        try {
            return cache.get(object, this, keep);
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(long i, boolean keep) {

        try {
            return cache.get(i, this, keep);
        } catch (HsqlException e) {
            return null;
        }
    }

    public void remove(CachedObject object) {

        if (cache != null) {
            cache.remove(object, spaceManager);
        }
    }

    public void commitPersistence(CachedObject object) {}

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) throws HsqlException {
        throw Error.runtimeError(ErrorCode.U_S0500, "PersistentStore");
    }

    public void removeAll() {}

    public DataFileCache getCache() {
        return cache;
    }

    public TableSpaceManager getSpaceManager() {
        return spaceManager;
    }

    public void setSpaceManager(TableSpaceManager manager) {
        spaceManager = manager;
    }

    public boolean isMemory() {
        return cache == null;
    }

    public void reindex(Session session, Index index) {}

    public void setCache(DataFileCache cache) {
        this.cache = cache;
    }

    public void release() {}

    public PersistentStore getAccessorStore(Index index) {
        return null;
    }

    public CachedObject getAccessor(Index key) {
        return null;
    }

    public double searchCost(Session session, Index idx, int count,
                             int opType) {
        return 1;
    }

    public long elementCount() {
        return 0;
    }

    public long elementCount(Session session) {
        return 0;
    }

    public long elementCountUnique(Index index) {
        return 0;
    }

    public void setElementCount(Index key, long size, long uniqueSize) {}

    public void setAccessor(Index key, long accessor) {}

    public void setAccessor(Index key, CachedObject accessor) {}

    public boolean hasNull(int pos) {
        return false;
    }

    public void resetAccessorKeys(Index[] keys) throws HsqlException {}

    public void setMemory(boolean mode) {}

    public void delete(Session session, Row row) {}

    public CachedObject get(CachedObject object, RowInputInterface in) {
        return object;
    }

    public void indexRow(Session session, Row row) throws HsqlException {}

    public void indexRows(Session session) throws HsqlException {}

    public RowIterator rowIterator() {
        return null;
    }

    public int getAccessCount() {
        return 0;
    }

    public Index[] getAccessorKeys() {
        return null;
    }

    public void moveDataToSpace() {}

    public void moveData(Session session, PersistentStore other, int colindex,
                         int adjust) {}

    public void setReadOnly(boolean readonly) {}

    public void writeLock() {}

    public void writeUnlock() {}

    public TableBase getTable() {
        return null;
    }

    public long getTimestamp() {
        return 0;
    }

    public void setTimestamp(long timestamp) {}

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {}

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {}
}
