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


package org.hsqldb.persist;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAVLDiskData;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * Implementation of PersistentStore for TEXT tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.9.0
 */
public class RowStoreAVLDiskData extends RowStoreAVL {

    TextCache          cache;
    RowOutputInterface rowOut;
    AtomicInteger      accessCount;

    public RowStoreAVLDiskData(Table table) {

        this.database     = table.database;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        lock              = new ReentrantReadWriteLock();
        readLock          = lock.readLock();
        writeLock         = lock.writeLock();
    }

    public Object[] getData(RowAVLDiskData row) {

        cache.writeLock.lock();

        try {
            cache.get(row, this, false);

            return row.getData();
        } finally {
            cache.writeLock.unlock();
        }
    }

    public int getNextAccessCount() {
        return accessCount.incrementAndGet();
    }

    public CachedObject get(long key, boolean keep) {
        CachedObject object = cache.get(key, this, keep);

        return object;
    }

    public CachedObject get(CachedObject object, boolean keep) {
        object = cache.get(object, this, keep);

        return object;
    }

    public void add(Session session, CachedObject object, boolean tx) {

        cache.writeLock.lock();

        try {
            int size = object.getRealSize(rowOut);

            object.setStorageSize(size);

            long pos = tableSpace.getFilePosition(size);

            object.setPos(pos);

            if (tx) {
                RowAction.addInsertAction(session, table, this, (Row) object);
            }

            cache.add(object, false);
        } finally {
            cache.writeLock.unlock();
        }
    }

    public CachedObject get(RowInputInterface in) {

        RowAVLDiskData row = new RowAVLDiskData(this, table, in);

        cache.cache.put(row);

        return row;
    }

    public CachedObject get(CachedObject object, RowInputInterface in) {

        Object[] rowData = in.readData(table.getColumnTypes());

        ((RowAVLDiskData) object).setData(rowData);

        return object;
    }

    public CachedObject getNewCachedObject(
            Session session,
            Object object,
            boolean tx) {

        Row row = new RowAVLDiskData(this, table, (Object[]) object);

        add(session, row, tx);

        return row;
    }

    public void indexRow(Session session, Row row) {
        super.indexRow(session, row);
    }

    public boolean isMemory() {
        return false;
    }

    public void removeAll() {
        destroyIndexes();
        elementCount.set(0);
        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(CachedObject object) {
        cache.remove(object);
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        }

        return accessorList[position];
    }

    public void commitPersistence(CachedObject row) {
        try {
            cache.saveRow(row);
        } catch (HsqlException e1) {}
    }

    public void postCommitAction(Session session, RowAction action) {

        if (action.getType() == RowAction.ACTION_DELETE_FINAL
                && !action.isDeleteComplete()) {
            action.setDeleteComplete();

            Row row = action.getRow();

            if (row == null) {
                row = (Row) get(action.getPos(), false);
            }

            delete(session, row);
            remove(row);
        }
    }

    public void commitRow(
            Session session,
            Row row,
            int changeAction,
            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                cache.removePersistence(row);
                break;

            case RowAction.ACTION_INSERT :
                commitPersistence(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row);
                } else {
                    delete(session, row);
                    remove(row);
                }

                break;

            case RowAction.ACTION_DELETE_FINAL :
                throw Error.runtimeError(ErrorCode.U_S0500, "RowStore");
        }
    }

    public void rollbackRow(
            Session session,
            Row row,
            int changeAction,
            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                if (txModel == TransactionManager.LOCKS) {
                    ((RowAVL) row).setNewNodes(this);
                    indexRow(session, row);
                }

                break;

            case RowAction.ACTION_INSERT :
                if (txModel == TransactionManager.LOCKS) {
                    delete(session, row);
                    remove(row);
                }

                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row);
                } else {
                    delete(session, row);
                    remove(row);
                }

                break;
        }
    }

    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {

        this.cache  = (TextCache) cache;
        this.tableSpace = cache.spaceManager.getTableSpace(
            DataSpaceManager.tableIdDefault);
        accessCount = cache.getAccessCount();
        rowOut      = cache.rowOut;
    }

    /**
     * Does not adjust usage count
     */
    public void release() {

        destroyIndexes();
        table.database.logger.textTableManager.closeTextCache((Table) table);

        cache = null;

        elementCount.set(0);
        ArrayUtil.fillArray(accessorList, null);
    }

    public void readLock() {
        readLock.lock();
    }

    public void readUnlock() {
        readLock.unlock();
    }

    public void writeLock() {
        writeLock.lock();
    }

    public void writeUnlock() {
        writeLock.unlock();
    }
}
