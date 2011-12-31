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
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * Implementation of PersistentStore for CACHED tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class RowStoreAVLDisk extends RowStoreAVL {

    DataFileCache      cache;
    RowOutputInterface rowOut;

    public RowStoreAVLDisk(PersistentStoreCollection manager,
                           DataFileCache cache, Table table) {

        this.database     = table.database;
        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        this.cache        = cache;

        if (cache != null) {
            rowOut = cache.rowOut.duplicate();

            cache.adjustStoreCount(1);
        }

        manager.setStore(table, this);
    }

    public boolean isMemory() {
        return false;
    }

    public int getAccessCount() {
        return cache.getAccessCount();
    }

    public void set(CachedObject object) {

        Row row = ((Row) object);

        database.txManager.setTransactionInfo(row);
    }

    public CachedObject get(int key) {

        CachedObject object = cache.get(key, this, false);

        return object;
    }

    public CachedObject get(int key, boolean keep) {

        CachedObject object = cache.get(key, this, keep);

        return object;
    }

    public CachedObject get(CachedObject object, boolean keep) {

        object = cache.get(object, this, keep);

        return object;
    }

    public int getStorageSize(int i) {
        return cache.get(i, this, false).getStorageSize();
    }

    public void add(CachedObject object) {

        int size = object.getRealSize(rowOut);

        size += indexList.length * NodeAVLDisk.SIZE_IN_BYTE;
        size = rowOut.getStorageSize(size);

        object.setStorageSize(size);
        cache.add(object);
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new RowAVLDisk(table, in);
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        Row row = new RowAVLDisk(table, (Object[]) object, this);

        add(row);

        if (tx) {
            RowAction action = new RowAction(session, table,
                                             RowAction.ACTION_INSERT, row,
                                             null);

            row.rowAction = action;
        }

        return row;
    }

    public void indexRow(Session session, Row row) {

        try {
            super.indexRow(session, row);
        } catch (HsqlException e) {
            database.txManager.removeTransactionInfo(row);

            throw e;
        }
    }

    public void removeAll() {

        elementCount = 0;

        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {
        cache.remove(i, this);
    }

    public void removePersistence(int i) {}

    public void release(int i) {
        cache.release(i);
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
                    remove(row.getPos());
                }
                break;

            case RowAction.ACTION_INSERT :
                database.logger.writeInsertStatement(session, row,
                                                     (Table) table);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                }
                break;

            case RowAction.ACTION_DELETE_FINAL :
                delete(session, row);

                // remove info after delete
                database.txManager.removeTransactionInfo(row);
                remove(row.getPos());
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
                if (txModel == TransactionManager.LOCKS) {
                    delete(session, row);
                    remove(row.getPos());
                }
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                }
                break;
        }
    }

    //
    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        this.cache = cache;
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        cache.adjustStoreCount(-1);

        cache        = null;
        elementCount = 0;
    }

    public CachedObject getAccessor(Index key) {

        NodeAVL node = (NodeAVL) accessorList[key.getPosition()];

        if (node == null) {
            return null;
        }

        if (!node.isInMemory()) {
            RowAVL row = (RowAVL) get(node.getPos(), false);

            node                            = row.getNode(key.getPosition());
            accessorList[key.getPosition()] = node;
        }

        return node;
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, int accessor) {

        CachedObject object = get(accessor, false);

        if (object != null) {
            NodeAVL node = ((RowAVL) object).getNode(key.getPosition());

            object = node;
        }

        setAccessor(key, object);
    }

    public void resetAccessorKeys(Index[] keys) {

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLDisk");
    }

    public void writeLock() {
        cache.writeLock.lock();
    }

    public void writeUnlock() {
        cache.writeLock.unlock();
    }
}
