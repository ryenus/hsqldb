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
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.index.NodeAVLDisk;

/*
 * Implementation of PersistentStore for result set and temporary tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class RowStoreAVLHybrid extends RowStoreAVL implements PersistentStore {

    DataFileCacheSession cache;
    private int          maxMemoryRowCount;
    private boolean      useDisk;
    boolean              isCached;
    int                  rowIdSequence = 0;

    public RowStoreAVLHybrid(Session session,
                             PersistentStoreCollection manager,
                             TableBase table, boolean diskBased) {

        this.session           = session;
        this.manager           = manager;
        this.table             = table;
        this.maxMemoryRowCount = session.getResultMemoryRowCount();
        this.useDisk           = diskBased;

        if (maxMemoryRowCount == 0) {
            this.useDisk = false;
        }

        if (table.getTableType() == TableBase.RESULT_TABLE) {
            setTimestamp(session.getActionTimestamp());
        }

// test code to force use of cache
/*
        if (diskBased) {
            this.maxMemoryRowCount = 0;
            this.useDisk           = true;
        }
*/

//
        resetAccessorKeys(table.getIndexList());
        manager.setStore(table, this);
    }

    RowStoreAVLHybrid(Session session, TableBase table) {

        this.session = session;
        this.table   = table;
        useDisk      = true;
        isCached     = true;

        resetAccessorKeys(table.getIndexList());
    }

    public boolean isMemory() {
        return !isCached;
    }

    public void setMemory(boolean mode) {
        useDisk = !mode;
    }

    public synchronized int getAccessCount() {
        return isCached ? cache.getAccessCount()
                        : 0;
    }

    public void set(CachedObject object) {}

    public CachedObject get(int i) {

        try {
            if (isCached) {
                return cache.get(i, this, false);
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowStoreAVLHybrid");
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(int i, boolean keep) {

        try {
            if (isCached) {
                return cache.get(i, this, keep);
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowStoreAVLHybrid");
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(CachedObject object, boolean keep) {

        try {
            if (isCached) {
                return cache.get(object, this, keep);
            } else {
                return object;
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public int getStorageSize(int i) {

        try {
            if (isCached) {
                return cache.get(i, this, false).getStorageSize();
            } else {
                return 0;
            }
        } catch (HsqlException e) {
            return 0;
        }
    }

    public void add(CachedObject object) {

        if (isCached) {
            int size = object.getRealSize(cache.rowOut);

            size += indexList.length * NodeAVLDisk.SIZE_IN_BYTE;
            size = cache.rowOut.getStorageSize(size);

            object.setStorageSize(size);
            cache.add(object);
        }
    }

    public CachedObject get(RowInputInterface in) {

        try {
            if (isCached) {
                return new RowAVLDisk(table, in);
            }
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }

        return null;
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        if (!isCached) {
            if (useDisk && elementCount >= maxMemoryRowCount) {
                changeToDiskTable(session);
            }
        }

        if (isCached) {
            Row row = new RowAVLDisk(table, (Object[]) object, this);

            add(row);

            if (tx) {
                RowAction.addInsertAction(session, (Table) table, row);
            }

            return row;
        } else {
            int id  = rowIdSequence++;
            Row row = new RowAVL(table, (Object[]) object, id, this);

            if (tx) {
                RowAction action = new RowAction(session, table,
                                                 RowAction.ACTION_INSERT, row,
                                                 null);

                row.rowAction = action;
            }

            return row;
        }
    }

    public void removeAll() {

        elementCount = 0;

        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {

        if (isCached) {
            cache.remove(i, this);
        }
    }

    public void removePersistence(int i) {}

    public void release(int i) {

        if (isCached) {
            cache.release(i);
        }
    }

    public void commitPersistence(CachedObject row) {}

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                remove(row.getPos());
                break;

            case RowAction.ACTION_INSERT :
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row.getPos());
                break;

            case RowAction.ACTION_DELETE_FINAL :
                delete(session, row);
                break;
        }
    }

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                row = (Row) get(row, true);

                ((RowAVL) row).setNewNodes(this);
                row.keepInMemory(false);
                indexRow(session, row);
                break;

            case RowAction.ACTION_INSERT :
                delete(session, row);
                remove(row.getPos());
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row.getPos());
                break;
        }
    }

    //
    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLHybrid");
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);

        if (isCached) {
            cache.adjustStoreCount(-1);

            cache    = null;
            isCached = false;
        }

        manager.setStore(table, null);
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, int accessor) {}

    public synchronized void resetAccessorKeys(Index[] keys) {

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        if (isCached) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLHybrid");
        }

        super.resetAccessorKeys(keys);
    }

    public final void changeToDiskTable(Session session) {

        cache = ((PersistentStoreCollectionSession) manager).getResultCache();

        if (cache != null) {
            RowIterator iterator = table.rowIterator(this);

            ArrayUtil.fillArray(accessorList, null);

            elementCount = 0;
            isCached     = true;

            cache.adjustStoreCount(1);

            while (iterator.hasNext()) {
                Row row = iterator.getNextRow();
                Row newRow = (Row) getNewCachedObject(session, row.getData(),
                                                      false);

                indexRow(session, newRow);
                row.destroy();
            }
        }

        maxMemoryRowCount = Integer.MAX_VALUE;
    }
}
