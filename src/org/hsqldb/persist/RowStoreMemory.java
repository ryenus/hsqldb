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

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.index.Index;
import org.hsqldb.index.Node;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for MEMORY tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreMemory implements PersistentStore {

    TableBase                       table;
    PersistentStoreCollection       manager;
    private LongKeyHashMap          accessorMap   = new LongKeyHashMap();
    private IntKeyHashMapConcurrent rowIdMap = new IntKeyHashMapConcurrent();
    int                             rowIdSequence = 0;

    public RowStoreMemory(PersistentStoreCollection manager, TableBase table) {

        this.manager = manager;
        this.table   = table;

        manager.setStore(table, this);
    }

    public CachedObject get(int i) {
        return (CachedObject) rowIdMap.get(i);
    }

    public CachedObject getKeep(int i) {
        return (CachedObject) rowIdMap.get(i);
    }

    public int getStorageSize(int i) {
        return 0;
    }

    public void restore(CachedObject row) throws HsqlException {
        row.restore();
    }

    public CachedObject get(RowInputInterface in) {
        return null;
    }

    public CachedObject getNewCachedObject(Session session,
                                           Object object)
                                           throws HsqlException {

        Row row = new Row(table, (Object[]) object);

        if (session != null) {
            RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);
        }

        int id = rowIdSequence++;

        row.setPos(id);
        rowIdMap.put(id, row);

        return row;
    }

    public void removeAll() {
        rowIdMap.clear();
        accessorMap.clear();
    }

    public void remove(int i) {
        rowIdMap.remove(i);
    }

    public void removePersistence(int i) {}

    public void release(int i) {}

    public void commit(CachedObject row) {}

    public DataFileCache getCache() {
        return null;
    }

    public void setCache(DataFileCache cache) {}

    public void release() {
        accessorMap.clear();
    }

    public Object getAccessor(Object key) {

        Index index = (Index) key;

        return (Node) accessorMap.get(index.getPersistenceId());
    }

    public void setAccessor(Object key, Object accessor) {

        Index index = (Index) key;

        accessorMap.put(index.getPersistenceId(), accessor);
    }

    public void add(CachedObject object) throws HsqlException {
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }
}
