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

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAVLDisk;
import org.hsqldb.RowAVLDiskLarge;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.index.NodeAVLDisk;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * Implementation of PersistentStore for CACHED tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class RowStoreAVLDisk extends RowStoreAVL implements PersistentStore {

    DataFileCache      cache;
    RowOutputInterface rowOut;
    boolean            largeData;

    public RowStoreAVLDisk(PersistentStoreCollection manager,
                           DataFileCache cache, Table table) {

        this(manager, table);

        this.cache = cache;
        rowOut     = cache.rowOut.duplicate();

        cache.adjustStoreCount(1);

        largeData  = database.logger.propLargeData;
        tableSpace = cache.spaceManager.getTableSpace(table.getSpaceID());
    }

    protected RowStoreAVLDisk(PersistentStoreCollection manager, Table table) {

        this.database     = table.database;
        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];

        manager.setStore(table, this);

        largeData = database.logger.getDataFileFactor() > 1;
    }

    public boolean isMemory() {
        return false;
    }

    public int getAccessCount() {
        return cache.getAccessCount();
    }

    public void set(CachedObject object) {
        database.txManager.setTransactionInfo(this, object);
    }

    public CachedObject get(long key) {

        CachedObject object = cache.get(key, this, false);

        return object;
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

        int size = object.getRealSize(rowOut);

        size += indexList.length * NodeAVLDisk.SIZE_IN_BYTE;
        size = rowOut.getStorageSize(size);

        object.setStorageSize(size);

        long pos = tableSpace.getFilePosition(size, false);

        object.setPos(pos);

        if (tx) {
            Row row = (Row) object;
            RowAction action = new RowAction(session, table,
                                             RowAction.ACTION_INSERT, row,
                                             null);

            row.rowAction = action;

            database.txManager.addTransactionInfo(object);
        }

        cache.add(object);

        storageSize += size;
    }

    public CachedObject get(RowInputInterface in) {

        try {
            if (largeData) {
                return new RowAVLDiskLarge(table, in);
            } else {
                return new RowAVLDisk(table, in);
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        Row row;

        if (largeData) {
            row = new RowAVLDiskLarge(table, (Object[]) object, this);
        } else {
            row = new RowAVLDisk(table, (Object[]) object, this);
        }

        add(session, row, tx);

        return row;
    }

    public void indexRow(Session session, Row row) {

        try {
            row = (Row) get(row, true);

            super.indexRow(session, row);
            row.keepInMemory(false);
        } catch (HsqlException e) {
            database.txManager.removeTransactionInfo(row);

            throw e;
        }
    }

    public void removeAll() {

        elementCount.set(0);

        ArrayUtil.fillArray(accessorList, null);
        cache.spaceManager.freeTableSpace(tableSpace.getSpaceID());
    }

    public void remove(CachedObject object) {

        cache.remove(object);

        tableSpace.release(object.getPos(), object.getStorageSize());

        storageSize -= object.getStorageSize();
    }

    public void commitPersistence(CachedObject row) {}

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        Object[] data = row.getData();

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                database.logger.writeDeleteStatement(session, (Table) table,
                                                     data);

                if (txModel == TransactionManager.LOCKS) {
                    remove(row);
                }
                break;

            case RowAction.ACTION_INSERT :
                database.logger.writeInsertStatement(session, row,
                                                     (Table) table);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row);
                }
                break;

            case RowAction.ACTION_DELETE_FINAL :
                delete(session, row);

                // remove info after delete but before removing persistence
                database.txManager.removeTransactionInfo(row);
                remove(row);
                break;
        }
    }

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                if (txModel == TransactionManager.LOCKS) {
                    row = (Row) get(row, true);

                    ((RowAVL) row).setNewNodes(this);
                    row.keepInMemory(false);
                    indexRow(session, row);
                }
                break;

            case RowAction.ACTION_INSERT :
                delete(session, row);

                // remove info after delete but before removing persistence
                database.txManager.removeTransactionInfo(row);
                remove(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :
                if (txModel != TransactionManager.LOCKS) {

                    // INSERT + DELETE
                    delete(session, row);

                    // remove info after delete but before removing persistence
                    database.txManager.removeTransactionInfo(row);
                }

                remove(row);
                break;
        }
    }

    public void postCommitAction(Session session, RowAction action) {
        database.txManager.removeTransactionInfo(action.getPos());
    }

    //
    public DataFileCache getCache() {
        return cache;
    }

    /**
     * Works only for TEXT TABLE as others need specific spaceManager
     */
    public void setCache(DataFileCache cache) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLDisk");
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        cache.adjustStoreCount(-1);

        cache        = null;
        elementCount.set(0);
    }

    public CachedObject getAccessor(Index key) {

        NodeAVL node = (NodeAVL) accessorList[key.getPosition()];

        if (node == null) {
            return null;
        }

        RowAVL row = (RowAVL) get(node.getRow(this), false);

        node                            = row.getNode(key.getPosition());
        accessorList[key.getPosition()] = node;

        return node;
    }

    public void setAccessor(Index key, long accessor) {

        CachedObject object = get(accessor, false);

        if (object != null) {
            NodeAVL node = ((RowAVL) object).getNode(key.getPosition());

            object = node;
        }

        setAccessor(key, object);
    }

    public void resetAccessorKeys(Session session, Index[] keys) {

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLDisk");
    }

    public void setReadOnly(boolean readOnly) {}

    public void moveDataToSpace() {

        Table table    = (Table) this.table;
        long  rowCount = elementCount();

        if (rowCount == 0) {
            return;
        }

        if (rowCount > Integer.MAX_VALUE) {

            // error too big
            return;
        }

        DoubleIntIndex pointerLookup = new DoubleIntIndex((int) rowCount,
            true);

        pointerLookup.setKeysSearchTarget();
        cache.writeLock.lock();

        try {
            moveDataToSpace(this, cache, tableSpace, pointerLookup);

            CachedObject[] newAccessorList =
                new CachedObject[accessorList.length];

            for (int i = 0; i < accessorList.length; i++) {
                long pos = pointerLookup.lookup(accessorList[i].getPos());

                if (pos == -1) {
                    throw Error.error(ErrorCode.DATA_FILE_ERROR);
                }

                newAccessorList[i] = cache.get(pos, this, false);
            }

            RowIterator it = rowIterator();

            // todo - check this - must remove from old space, not new one
            while (it.hasNext()) {
                Row row = it.getNextRow();

                cache.remove(row);
                tableSpace.release(row.getPos(), row.getStorageSize());
            }

            accessorList = newAccessorList;
        } finally {
            cache.writeLock.unlock();
        }

        database.logger.logDetailEvent("table written "
                                       + table.getName().name);
    }

    public static void moveDataToSpace(PersistentStore store,
                                       DataFileCache cache,
                                       TableSpaceManager tableSpace,
                                       LongLookup pointerLookup) {

        RowIterator it = store.rowIterator();

        while (it.hasNext()) {
            CachedObject row = it.getNextRow();
            long newPos = tableSpace.getFilePosition(row.getStorageSize(),
                false);

            pointerLookup.addUnsorted(row.getPos(), newPos);
        }

        it = store.rowIterator();

        while (it.hasNext()) {
            CachedObject row = it.getNextRow();

            cache.rowOut.reset();
            row.write(cache.rowOut, pointerLookup);

            long pos = pointerLookup.lookup(row.getPos());

            cache.saveRowOutput(pos);
        }
    }

    long getStorageSizeEstimate() {

        if (elementCount.get() == 0) {
            return 0;
        }

        CachedObject accessor = getAccessor(indexList[0]);
        CachedObject row      = get(accessor.getPos());

        return row.getStorageSize() * elementCount.get();
    }


    public void writeLock() {
        cache.writeLock.lock();
    }

    public void writeUnlock() {
        cache.writeLock.unlock();
    }
}
