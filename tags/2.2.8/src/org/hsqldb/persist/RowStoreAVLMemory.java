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

import org.hsqldb.Database;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for MEMORY tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class RowStoreAVLMemory extends RowStoreAVL implements PersistentStore {

    Database database;
    int      rowIdSequence = 0;

    public RowStoreAVLMemory(PersistentStoreCollection manager, Table table) {

        this.database     = table.database;
        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];

        manager.setStore(table, this);
    }

    public boolean isMemory() {
        return true;
    }

    public int getAccessCount() {
        return 0;
    }

    public void set(CachedObject object) {}

    public CachedObject get(int i) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVMemory");
    }

    public CachedObject get(int i, boolean keep) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVLMemory");
    }

    public CachedObject get(CachedObject object, boolean keep) {
        return object;
    }

    public int getStorageSize(int i) {
        return 0;
    }

    public void add(CachedObject object) {}

    public CachedObject get(RowInputInterface in) {
        return null;
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        int id;

        synchronized (this) {
            id = rowIdSequence++;
        }

        Row row = new RowAVL(table, (Object[]) object, id, this);

        if (tx) {
            RowAction action = new RowAction(session, table,
                                             RowAction.ACTION_INSERT, row,
                                             null);

            row.rowAction = action;
        }

        return row;
    }

    public void removeAll() {

        elementCount = 0;

        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {}

    public void removePersistence(int i) {}

    public void release(int i) {}

    public void commitPersistence(CachedObject row) {}

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        Object[] data = row.getData();

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                database.logger.writeDeleteStatement(session, (Table) table,
                                                     data);
                break;

            case RowAction.ACTION_INSERT :
                database.logger.writeInsertStatement(session, row,
                                                     (Table) table);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
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
                if (txModel == TransactionManager.LOCKS) {
                    ((RowAVL) row).setNewNodes(this);
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
        return null;
    }

    public void setCache(DataFileCache cache) {}

    public void release() {

        setTimestamp(0);
        ArrayUtil.fillArray(accessorList, null);

        elementCount = 0;
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, int accessor) {}
}
