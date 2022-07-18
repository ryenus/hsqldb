/* Copyright (c) 2001-2022, The HSQL Development Group
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAVLDisk;
import org.hsqldb.RowAVLDiskLarge;
import org.hsqldb.RowAction;
import org.hsqldb.RowActionBase;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.index.NodeAVLDisk;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DoubleLongIndex;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * Implementation of PersistentStore for CACHED tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.9.0
 */
public class RowStoreAVLDisk extends RowStoreAVL {

    DataFileCache      cache;
    RowOutputInterface rowOut;
    boolean            largeData;

    public RowStoreAVLDisk(DataFileCache cache, Table table) {

        this.table        = table;
        this.database     = table.database;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        this.cache        = cache;
        rowOut            = cache.rowOut.duplicate();
        largeData         = database.logger.getDataFileFactor() > 1;

        cache.adjustStoreCount(1);

        rowActionMap = new LongKeyHashMap(8);
        largeData    = database.logger.propLargeData;
        tableSpace   = cache.spaceManager.getTableSpace(table.getSpaceID());
        lock         = new ReentrantReadWriteLock(true);
        readLock     = lock.readLock();
        writeLock    = lock.writeLock();
    }

    public boolean isMemory() {
        return false;
    }

    private void set(CachedObject object) {

        if (database.txManager.isMVRows()) {
            RowAction action = (RowAction) rowActionMap.get(object.getPos());

            if (action != null) {
                ((Row) object).rowAction = action;
            }
        }
    }

    public CachedObject get(long key, boolean keep) {

        CachedObject object = cache.get(key, this, keep);

        return object;
    }

    public CachedObject get(long key) {

        CachedObject object = cache.get(key, this, false);

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

        long pos = tableSpace.getFilePosition(size);

        object.setPos(pos);

        if (tx) {
            RowAction action = RowAction.addInsertAction(session, table, this,
                (Row) object);

            if (database.txManager.isMVRows()) {
                rowActionMap.put(object.getPos(), action);
            }
        }

        cache.add(object, false);

        storageSize += size;
    }

    public boolean canRead(Session session, long pos, int mode, int[] colMap) {

        if (database.txManager.isMVRows()) {
            RowAction action = (RowAction) rowActionMap.get(pos);

            if (action == null) {
                return true;
            }

            return action.canRead(session, mode);
        }

        return true;
    }

    public boolean canRead(Session session, CachedObject object, int mode,
                           int[] colMap) {

        RowAction action = ((Row) object).rowAction;

        if (action == null) {
            return true;
        }

        return action.canRead(session, mode);
    }

    public CachedObject get(RowInputInterface in) {

        Row row;

        if (largeData) {
            row = new RowAVLDiskLarge(this, in);
        } else {
            row = new RowAVLDisk(this, in);
        }

        set(row);

        return row;
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

    public void delete(Session session, Row row) {

        try {
            row = (Row) get(row, true);

            super.delete(session, row);
        } finally {
            row.keepInMemory(false);
        }
    }

    public void indexRow(Session session, Row row) {

        try {
            row = (Row) get(row, true);

            super.indexRow(session, row);
        } finally {
            row.keepInMemory(false);
        }
    }

    public void removeAll() {

        if (cache.spaceManager.isMultiSpace()) {
            if (tableSpace.isDefaultSpace()) {
                LongLookup pointerLookup = getPointerList();

                removeDefaultSpaces(pointerLookup);
            } else {
                cache.spaceManager.freeTableSpace(tableSpace.getSpaceID());
            }
        }

        elementCount.set(0);
        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(CachedObject object) {

        cache.remove(object);
        tableSpace.release(object.getPos(), object.getStorageSize());

        storageSize -= object.getStorageSize();
    }

    public void commitPersistence(CachedObject row) {}

    public void postCommitAction(Session session, RowAction action) {

        if (action.getType() == RowAction.ACTION_NONE) {
            Lock mapLock = rowActionMap.getWriteLock();

            mapLock.lock();

            try {

                // remove only if not changed
                if (action.getType() == RowActionBase.ACTION_NONE) {
                    rowActionMap.remove(action.getPos());
                }
            } finally {
                mapLock.unlock();
            }
        } else if (action.getType() == RowAction.ACTION_DELETE_FINAL
                   && !action.isDeleteComplete()) {
            action.setDeleteComplete();

            Row row = action.getRow();

            delete(session, row);

            // remove info after delete but before removing persistence
            rowActionMap.remove(row.getPos());
            remove(row);
        }
    }

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
                if (database.txManager.isMVRows()) {
                    rowActionMap.remove(row.getPos());
                }

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
                } else {
                    RowAction ra = row.getAction();

                    if (ra.getType() == RowAction.ACTION_NONE) {
                        rowActionMap.remove(row.getPos());
                    }
                }
                break;

            case RowAction.ACTION_INSERT :
                delete(session, row);

                // remove info after delete but before removing persistence
                if (database.txManager.isMVRows()) {
                    rowActionMap.remove(row.getPos());
                }

                remove(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :
                if (txModel == TransactionManager.LOCKS) {
                    remove(row);
                } else {

                    // INSERT + DELETE
                    delete(session, row);

                    // remove info after delete but before removing persistence
                    rowActionMap.remove(row.getPos());
                    remove(row);
                }
                break;
        }
    }

    public RowAction addDeleteActionToRow(Session session, Row row,
                                          int[] colMap, boolean isMV) {

        RowAction action = null;

        if (!isMV) {
            synchronized (row) {
                return RowAction.addDeleteAction(session, table, this, row,
                                                 colMap);
            }
        }

        row = (Row) cache.get(row, this, true);

        Lock mapLock = rowActionMap.getWriteLock();

        mapLock.lock();

        try {
            action = RowAction.addDeleteAction(session, table, this, row,
                                               colMap);

            if (action != null) {
                rowActionMap.put(action.getPos(), action);
            }
        } finally {
            mapLock.unlock();
            row.keepInMemory(false);
        }

        return action;
    }

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

        cache.adjustStoreCount(-1);

        cache = null;

        elementCount.set(0);
        ArrayUtil.fillArray(accessorList, null);
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLDisk");
        }

        NodeAVL node = (NodeAVL) accessorList[position];

        if (node == null) {
            return null;
        }

        RowAVL row = (RowAVL) get(node.getRow(this), false);

        node                            = row.getNode(key.getPosition());
        accessorList[key.getPosition()] = node;

        return node;
    }

    /**
     *  Sets the index roots of a cached table to specified file pointers.
     *  If a file pointer is -1 then the particular index root is null. A null
     *  index root signifies an empty table. Accordingly, all index roots should
     *  be null or all should be a valid file pointer/reference.
     */
    public void setAccessors(long base, long[] accessors, long cardinality) {

        for (int index = 0; index < indexList.length; index++) {
            setAccessor(indexList[index], accessors[index]);
        }

        setElementCount(cardinality);
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

    public void setSpaceManager(Session session) {

        if (table.getSpaceID() == DataSpaceManager.tableIdDefault) {
            return;
        }

        TableSpaceManager tableSpace =
            cache.spaceManager.getTableSpace(table.getSpaceID());

        setSpaceManager(tableSpace);
        moveDataToSpace(session);
    }

    public void moveDataToSpace(Session session) {

        Table table    = (Table) this.table;
        long  rowCount = elementCount();

        if (rowCount == 0) {
            return;
        }

        if (rowCount > Integer.MAX_VALUE) {

            // error too big
            return;
        }

        writeLock();

        try {
            LongLookup pointerLookup = getPointerList();
            LongLookup removeList    = pointerLookup.duplicate();

            moveDataToNewSpace(cache, pointerLookup);

            CachedObject[] newAccessorList =
                new CachedObject[accessorList.length];

            for (int i = 0; i < accessorList.length; i++) {
                Index   key  = indexList[i];
                long    pos  = pointerLookup.lookup(accessorList[i].getPos());
                RowAVL  row  = (RowAVL) cache.get(pos, this, false);
                NodeAVL node = row.getNode(key.getPosition());

                newAccessorList[key.getPosition()] = node;
            }

            removeDefaultSpaces(removeList);

            accessorList = newAccessorList;
        } finally {
            writeUnlock();
        }

        database.logger.logDetailEvent("table written "
                                       + table.getName().name);
    }

    public void moveDataToSpace(DataFileCache targetCache,
                                LongLookup pointerLookup) {
        populatePointerList(pointerLookup);
        moveDataToNewSpace(targetCache, pointerLookup);
    }

    private DoubleLongIndex getPointerList() {

        DoubleLongIndex pointerLookup =
            new DoubleLongIndex((int) elementCount());

        populatePointerList(pointerLookup);

        return pointerLookup;
    }

    private void populatePointerList(LongLookup pointerLookup) {

        RowIterator it = indexList[0].firstRow(this);

        while (it.next()) {
            CachedObject row = it.getCurrentRow();

            pointerLookup.addUnsorted(row.getPos(), row.getStorageSize());
        }

        pointerLookup.sort();
    }

    private void moveDataToNewSpace(DataFileCache targetCache,
                                    LongLookup pointerLookup) {

        int spaceId = table.getSpaceID();
        TableSpaceManager targetSpace =
            targetCache.spaceManager.getTableSpace(spaceId);

        for (int i = 0; i < pointerLookup.size(); i++) {
            long newPos = targetSpace.getFilePosition(
                (int) pointerLookup.getLongValue(i));

            pointerLookup.setLongValue(i, newPos);
        }

        RowIterator it = indexList[0].firstRow(this);

        while (it.next()) {
            CachedObject row    = it.getCurrentRow();
            long         newPos = pointerLookup.lookup(row.getPos());

            // write
            targetCache.rowOut.reset();
            row.write(targetCache.rowOut, pointerLookup);
            targetCache.saveRowOutput(newPos);
        }
    }

    private void removeDefaultSpaces(LongLookup removeList) {

        DataSpaceManager manager = cache.spaceManager;
        int              scale   = cache.getDataFileScale();

        for (int i = 0; i < removeList.size(); i++) {
            long pos  = removeList.getLongKey(i);
            long size = removeList.getLongValue(i) / scale;

            removeList.setLongValue(i, size);
            cache.release(pos);
        }

        removeList.compactLookupAsIntervals();
        manager.freeTableSpace(DataSpaceManager.tableIdDefault, removeList, 0,
                               0);
    }

    long getStorageSizeEstimate() {

        if (elementCount.get() == 0) {
            return 0;
        }

        CachedObject accessor = getAccessor(indexList[0]);
        CachedObject row      = get(accessor.getPos(), false);

        return row.getStorageSize() * elementCount.get();
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
