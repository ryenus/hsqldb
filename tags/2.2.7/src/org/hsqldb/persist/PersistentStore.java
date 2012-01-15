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

import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.index.Index;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;

/**
 * Interface for a store for CachedObject objects.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public interface PersistentStore {

    int               INT_STORE_SIZE  = 4;
    int               LONG_STORE_SIZE = 8;
    PersistentStore[] emptyArray      = new PersistentStore[]{};

    TableBase getTable();

    long getTimestamp();

    void setTimestamp(long timestamp);

    boolean isMemory();

    void setMemory(boolean mode);

    int getAccessCount();

    void set(CachedObject object);

    /** get object */
    CachedObject get(int key);

    /** get object with keep, ensuring future gets will return the same instance of the object */
    CachedObject get(int key, boolean keep);

    CachedObject get(CachedObject object, boolean keep);

    int getStorageSize(int key);

    /** add new object */
    void add(CachedObject object);

    CachedObject get(RowInputInterface in);

    CachedObject getNewInstance(int size);

    CachedObject getNewCachedObject(Session session, Object object,
                                    boolean tx);

    /** remove the persisted image but not the cached copy */
    void removePersistence(int i);

    void removeAll();

    /** remove both persisted and cached copies */
    void remove(int i);

    /** remove the cached copies */
    void release(int i);

    /** commit persisted image */
    void commitPersistence(CachedObject object);

    //
    void delete(Session session, Row row);

    void indexRow(Session session, Row row);

    void commitRow(Session session, Row row, int changeAction, int txModel);

    void rollbackRow(Session session, Row row, int changeAction, int txModel);

    //
    void indexRows(Session session);

    RowIterator rowIterator();

    //
    DataFileCache getCache();

    void setCache(DataFileCache cache);

    void release();

    PersistentStore getAccessorStore(Index index);

    CachedObject getAccessor(Index key);

    void setAccessor(Index key, CachedObject accessor);

    void setAccessor(Index key, int accessor);

    int elementCount();

    int elementCount(Session session);

    int elementCountUnique(Index index);

    void setElementCount(Index key, int size, int uniqueSize);

    void resetAccessorKeys(Index[] keys);

    Index[] getAccessorKeys();

    void moveData(Session session, PersistentStore other, int colindex,
                  int adjust);

    void reindex(Session session, Index index);

    void writeLock();

    void writeUnlock();
}
