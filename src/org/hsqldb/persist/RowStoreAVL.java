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
import org.hsqldb.RowAVL;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.types.Type;
import org.hsqldb.ColumnSchema;

public abstract class RowStoreAVL implements PersistentStore {

    PersistentStoreCollection manager;
    Index[]                   indexList    = Index.emptyArray;
    CachedObject[]            accessorList = CachedObject.emptyArray;
    TableBase                 table;

    // for result tables
    long timestamp;

    public boolean isMemory() {
        return false;
    }

    public abstract int getAccessCount();

    public abstract void set(CachedObject object);

    public abstract CachedObject get(int key, boolean keep);

    public abstract CachedObject get(CachedObject object, boolean keep);

    public abstract int getStorageSize(int key);

    public abstract void add(CachedObject object);

    public abstract CachedObject get(RowInputInterface in);

    public abstract CachedObject getNewInstance(int size);

    public abstract CachedObject getNewCachedObject(Session session,
            Object object);

    public abstract void removePersistence(int i);

    public abstract void removeAll();

    public abstract void remove(int i);

    public abstract void release(int i);

    public abstract void commitPersistence(CachedObject object);

    public abstract DataFileCache getCache();

    public abstract void setCache(DataFileCache cache);

    public abstract void release();

    public PersistentStore getAccessorStore(Index index) {
        return null;
    }

    public CachedObject getAccessor(Index key) {

        int position = key.getPosition();

        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        }

        return accessorList[position];
    }

    /**
     * Basic delete with no logging or referential checks.
     */
    public final void delete(Row row) {

        for (int i = indexList.length - 1; i >= 0; i--) {
            indexList[i].delete(this, row);
        }
    }

    public final void indexRow(Session session, Row row) {

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

    public final void indexRows() {

        RowIterator it = rowIterator();

        for (int i = 1; i < indexList.length; i++) {
            setAccessor(indexList[i], null);
        }

        while (it.hasNext()) {
            Row row = it.getNextRow();

            if (row instanceof RowAVL) {
                ((RowAVL) row).clearNonPrimaryNodes();
            }

            for (int i = 1; i < indexList.length; i++) {
                indexList[i].insert(null, this, row);
            }
        }
    }

    public final RowIterator rowIterator() {

        if (indexList.length == 0 || indexList[0] == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        }

        return indexList[0].firstRow(this);
    }

    public abstract void setAccessor(Index key, CachedObject accessor);

    public abstract void setAccessor(Index key, int accessor);

    public abstract void resetAccessorKeys(Index[] keys);

    /**
     * Moves the data from an old store to new after changes to table
     * The colindex argument is the index of the column that was
     * added or removed. The adjust argument is {-1 | 0 | +1}
     */
    public void moveData(Session session, PersistentStore other,
        int colindex, int adjust
        ) {

        Object       colvalue = null;
        Type         oldtype  = null;
        Type         newtype  = null;

        if (adjust >= 0 && colindex != -1) {
            ColumnSchema column   = ((Table) table).getColumn(colindex);
            colvalue = column.getDefaultValue(session);

            if (adjust == 0) {
                oldtype = ((Table) ((RowStoreAVL) other).table).getColumnTypes()[colindex];
                newtype = ((Table) table).getColumnTypes()[colindex];
            }
        }


        RowIterator it    = other.rowIterator();
        Table       table = (Table) this.table;

        try {
            while (it.hasNext()) {
                Row row = it.getNextRow();

                Object[] o    = row.getData();
                Object[] data = table.getEmptyRowData();


                if (adjust == 0 && colindex != -1) {
                    colvalue = newtype.convertToType(session, o[colindex],
                                                     oldtype);
                }

                ArrayUtil.copyAdjustArray(o, data, colvalue, colindex, adjust);


                table.systemSetIdentityColumn(session, data);
                table.enforceRowConstraints(session, data);

                // get object without RowAction
                Row newrow = (Row) getNewCachedObject(null, data);

                if (row.rowAction != null) {
                    newrow.rowAction =
                        row.rowAction.duplicate(newrow.getPos());
                }

                indexRow(null, newrow);
            }
        } catch (java.lang.OutOfMemoryError e) {
            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        }
    }

    public void lock() {}

    public void unlock() {}

    void dropIndexFromRows(Index primaryIndex, Index oldIndex) {

        RowIterator it       = primaryIndex.firstRow(this);
        int         position = oldIndex.getPosition() - 1;

        while (it.hasNext()) {
            Row     row      = it.getNextRow();
            int     i        = position - 1;
            NodeAVL backnode = ((RowAVL) row).getNode(0);

            while (i-- > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }
    }

    boolean insertIndexNodes(Index primaryIndex, Index newIndex) {

        int           position = newIndex.getPosition();
        RowIterator   it       = primaryIndex.firstRow(this);
        int           rowCount = 0;
        HsqlException error    = null;

        try {
            while (it.hasNext()) {
                Row row = it.getNextRow();

                ((RowAVL) row).insertNode(position);

                // count before inserting
                rowCount++;

                newIndex.insert(null, this, row);
            }

            return true;
        } catch (java.lang.OutOfMemoryError e) {
            error = Error.error(ErrorCode.OUT_OF_MEMORY);
        } catch (HsqlException e) {
            error = e;
        }

        // backtrack on error
        // rowCount rows have been modified
        it = primaryIndex.firstRow(this);

        for (int i = 0; i < rowCount; i++) {
            Row     row      = it.getNextRow();
            NodeAVL backnode = ((RowAVL) row).getNode(0);
            int     j        = position;

            while (--j > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }

        throw error;
    }

    /**
     * for result tables
     */
    void reindex(Session session, Index index) {

        setAccessor(index, null);

        RowIterator it = table.rowIterator(session);

        while (it.hasNext()) {
            Row row = it.getNextRow();

            // may need to clear the node before insert
            index.insert(session, this, row);
        }
    }
}
