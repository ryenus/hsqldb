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

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAVLDiskData;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for TEXT tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreAVLDiskData extends RowStoreAVLDisk {

    IntKeyHashMap rowActionMap = new IntKeyHashMap();

    public RowStoreAVLDiskData(PersistentStoreCollection manager,
                               Table table) {
        super(manager, null, table);
    }

    public void add(CachedObject object) {

        int size = object.getRealSize(cache.rowOut);

        object.setStorageSize(size);

        if (cache != null) {
            cache.add(object);
        }
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new RowAVLDiskData(table, in);
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public CachedObject getNewCachedObject(Session session, Object object) {

        Row row = new RowAVLDiskData(table, (Object[]) object);

        add(row);

        if (session != null) {
            RowAction.addInsertAction(session, table, row);

            RowAction action = row.rowAction;

            if (database.txManager.getTransactionControl()
                    != TransactionManager.LOCKS) {
                rowActionMap.put(action.getPos(), action);
            }
        }

        return row;
    }

    public void indexRow(Session session, Row row) {

        int i = 0;

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }
        } catch (HsqlException e) {

            // unique index violation - rollback insert
            for (--i; i >= 0; i--) {
                indexList[i].delete(this, row);
            }

            remove(row.getPos());

            throw e;
        }
    }

    public void set(CachedObject object) {

        if (database.txManager.getTransactionControl()
                == TransactionManager.LOCKS) {
            return;
        }

        Row       row    = (Row) object;
        RowAction rowact = (RowAction) rowActionMap.get(row.getPos());

        if (rowact == null) {
            return;
        }

        if (rowact.getType() == RowAction.ACTION_NONE) {
            rowActionMap.remove(row.getPos());

            return;
        }

        row.rowAction = rowact;
    }

    public void removeAll() {
        ArrayUtil.fillArray(accessorList, null);
        rowActionMap.clear();
    }

    public void remove(int i) {

        if (cache != null) {
            cache.remove(i, this);
        }
    }

    public void removePersistence(int i) {

        if (cache != null) {
            cache.removePersistence(i, this);
        }
    }

    public void release(int i) {

        if (cache != null) {
            cache.release(i);
        }
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
            if (cache != null) {
                cache.saveRow(row);
            }
        } catch (HsqlException e1) {}
    }

    public void delete(Row row) {

        for (int j = indexList.length - 1; j >= 0; j--) {
            indexList[j].delete(this, row);
        }

        row.delete(this);
    }

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                removePersistence(row.getPos());
                break;

            case RowAction.ACTION_INSERT :
                commitPersistence(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                } else {
                    delete(row);
                    remove(row.getPos());
                    rowActionMap.remove(row.getPos());
                }
                break;

            case RowAction.ACTION_DELETE_FINAL :
                if (txModel != TransactionManager.LOCKS) {
                    delete(row);
                    remove(row.getPos());
                    rowActionMap.remove(row.getPos());
                }
                break;
        }
    }

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                if (txModel == TransactionManager.LOCKS) {
                    row = (Row) get(row, true);

                    row.delete(this);
                    row.keepInMemory(false);
                    indexRow(session, row);
                }
                break;

            case RowAction.ACTION_INSERT :
                if (txModel == TransactionManager.LOCKS) {
                    delete(row);
                    remove(row.getPos());
                } else {}
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                } else {
                    delete(row);
                    remove(row.getPos());
                    rowActionMap.remove(row.getPos());
                }
                break;
        }
    }

    //
    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        table.database.logger.closeTextCache((Table) table);

        cache = null;
    }
}
