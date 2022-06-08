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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.hsqldb.ColumnSchema;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.OpTypes;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.IndexAVL;
import org.hsqldb.index.IndexAVLCheck.IndexAVLProbe;
import org.hsqldb.index.IndexStats;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.types.LobData;
import org.hsqldb.types.Type;

/*
 * Base implementation of PersistentStore for different table types.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
public abstract class RowStoreAVL implements PersistentStore {

    Database              database;
    TableSpaceManager     tableSpace;
    Index[]               indexList    = Index.emptyArray;
    CachedObject[]        accessorList = CachedObject.emptyArray;
    TableBase             table;
    long                  baseElementCount;
    AtomicLong            elementCount = new AtomicLong();
    long                  storageSize;
    boolean[]             nullsList;
    double[][]            searchCost;
    boolean               isSchemaStore;
    public LongKeyHashMap rowActionMap;

    //
    ReadWriteLock lock;
    Lock          readLock;
    Lock          writeLock;

    // for result tables
    // for INFORMATION SCHEMA tables
    private long timestamp;

    //
    PersistentStore[] subStores = PersistentStore.emptyArray;

    public boolean isRowStore() {
        return true;
    }

    public boolean isRowSet() {
        return false;
    }

    public TableBase getTable() {
        return table;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public abstract boolean isMemory();

    public void setMemory(boolean mode) {}

    public abstract CachedObject get(long key, boolean keep);

    public abstract CachedObject get(CachedObject object, boolean keep);

    public int compare(Session session, long key) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public abstract void add(Session session, CachedObject object, boolean tx);

    public final void add(CachedObject object, boolean keep) {}

    public boolean canRead(Session session, long pos, int mode, int[] colMap) {
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

    public abstract CachedObject get(RowInputInterface in);

    public CachedObject get(CachedObject object, RowInputInterface in) {
        return object;
    }

    public CachedObject getNewInstance(int size) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public int getDefaultObjectSize() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public abstract CachedObject getNewCachedObject(Session session,
            Object object, boolean tx);

    public abstract void removeAll();

    public abstract void remove(CachedObject object);

    public abstract void commitPersistence(CachedObject object);

    public RowAction addDeleteActionToRow(Session session, Row row,
                                          int[] colMap, boolean isMV) {

        synchronized (row) {
            return RowAction.addDeleteAction(session, table, this, row,
                                             colMap);
        }
    }

    public abstract DataFileCache getCache();

    public TableSpaceManager getSpaceManager() {
        return tableSpace;
    }

    public void setSpaceManager(TableSpaceManager manager) {
        tableSpace = manager;
    }

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
    public void delete(Session session, Row row) {

        writeLock();

        try {
            for (int i = 0; i < indexList.length; i++) {
                indexList[i].delete(session, this, row);
            }

            for (int i = 0; i < subStores.length; i++) {
                subStores[i].delete(session, row);
            }

            row.delete(this);

            long count = elementCount.decrementAndGet();

            if (count > 16 * 1024 && count < baseElementCount / 2) {
                baseElementCount = count;
                searchCost       = null;
            }
        } finally {
            writeUnlock();
        }
    }

    public void indexRow(Session session, Row row) {

        int i = 0;

        writeLock();

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }

            int j = 0;

            try {
                for (j = 0; j < subStores.length; j++) {
                    subStores[j].indexRow(session, row);
                }
            } catch (HsqlException e) {

                // unique index violation - rollback insert
                int count = j;

                j = 0;

                for (; j < count; j++) {
                    subStores[j].delete(session, row);
                }

                throw e;
            }

            long count = elementCount.incrementAndGet();

            if (count > 16 * 1024 && count > baseElementCount * 2) {
                baseElementCount = count;
                searchCost       = null;
            }
        } catch (HsqlException e) {
            int count = i;

            i = 0;

            // unique index violation - rollback insert
            for (; i < count; i++) {
                indexList[i].delete(session, this, row);
            }

            if (rowActionMap != null) {
                rowActionMap.remove(row.getPos());
            }

            remove(row);

            throw e;
        } catch (Throwable t) {
            int count = i;

            i = 0;

            // unique index violation - rollback insert
            for (; i < count; i++) {
                indexList[i].delete(session, this, row);
            }

            if (rowActionMap != null) {
                rowActionMap.remove(row.getPos());
            }

            // do not remove as there may be still be reference
            throw Error.error(ErrorCode.GENERAL_ERROR, t);
        } finally {
            writeUnlock();
        }
    }

    public abstract void commitRow(Session session, Row row, int changeAction,
                                   int txModel);

    public abstract void rollbackRow(Session session, Row row,
                                     int changeAction, int txModel);

    public abstract void postCommitAction(Session session, RowAction action);

    //
    public final void indexRows(Session session) {

        writeLock();

        try {
            for (int i = 1; i < indexList.length; i++) {
                setAccessor(indexList[i], null);
            }

            RowIterator it = rowIterator();

            while (it.next()) {
                Row row = it.getCurrentRow();

                ((RowAVL) row).clearNonPrimaryNodes();

                for (int i = 1; i < indexList.length; i++) {
                    indexList[i].insert(session, this, row);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public RowIterator rowIterator() {

        Index index = indexList[0];

        for (int i = 0; i < indexList.length; i++) {
            if (indexList[i].isClustered()) {
                index = indexList[i];

                break;
            }
        }

        return index.firstRow(this);
    }

    public void setAccessors(long base, long[] accessors, long cardinality) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
    }

    public void setAccessor(Index key, CachedObject accessor) {
        accessorList[key.getPosition()] = accessor;
    }

    public void setAccessor(Index key, long accessor) {}

    public void resetAccessorKeys(Session session, Index[] keys) {

        searchCost = null;

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        // method might be called twice
        if (indexList == keys) {
            return;
        }

        Index[]        oldIndexList = indexList;
        CachedObject[] oldAccessors = accessorList;
        int            limit        = indexList.length;
        int            diff         = keys.length - indexList.length;
        int            position     = 0;

        if (diff < -1) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        } else if (diff == -1) {
            limit = keys.length;
        } else if (diff == 0) {
            return;
        } else if (diff == 1) {

            //
        } else {
            for (; position < limit; position++) {
                if (indexList[position] != keys[position]) {
                    break;
                }
            }

            Index[] tempKeys = (Index[]) ArrayUtil.toAdjustedArray(indexList,
                null, position, 1);

            tempKeys[position] = keys[position];

            resetAccessorKeys(session, tempKeys);
            resetAccessorKeys(session, keys);

            return;
        }

        for (; position < limit; position++) {
            if (indexList[position] != keys[position]) {
                break;
            }
        }

        accessorList = (CachedObject[]) ArrayUtil.toAdjustedArray(accessorList,
                null, position, diff);
        indexList = keys;

        try {
            if (diff > 0) {
                insertIndexNodes(session, indexList[0], indexList[position]);
            } else {
                dropIndexFromRows(indexList[0], oldIndexList[position]);
            }
        } catch (HsqlException e) {
            accessorList = oldAccessors;
            indexList    = oldIndexList;

            throw e;
        }
    }

    public Index[] getAccessorKeys() {
        return indexList;
    }

    public double searchCost(Session session, Index index, int count,
                             int opType) {

        if (count == 0) {
            return elementCount.get();
        }

        if (opType != OpTypes.EQUAL) {
            return ((double) elementCount.get()) / 2;
        }

        if (index.isUnique() && count == index.getColumnCount()) {
            return 1;
        }

        int position = index.getPosition();

        synchronized (this) {
            if (searchCost == null || searchCost.length != indexList.length) {
                searchCost = new double[indexList.length][];
            }

            if (searchCost[position] == null) {
                searchCost[position] = indexList[position].searchCost(session,
                        this);
            }

            return searchCost[index.getPosition()][count - 1];
        }
    }

    public long elementCount() {

        Index index = this.indexList[0];

        if (elementCount.get() < 0) {
            readLock();

            try {
                long count = index.size(null, this);

                elementCount.set(count);
            } finally {
                readUnlock();
            }
        }

        return elementCount.get();
    }

    public long elementCount(Session session) {

        if (session == null) {
            return elementCount();
        }

        Index index = this.indexList[0];

        if (session.database.txManager.isMVRows()) {
            switch (table.getTableType()) {

                case TableBase.MEMORY_TABLE :
                case TableBase.CACHED_TABLE :
                case TableBase.TEXT_TABLE :
                    readLock();

                    try {
                        return index.size(session, this);
                    } finally {
                        readUnlock();
                    }
                default :
            }
        }

        return elementCount();
    }

    public long elementCountUnique(Index index) {
        return 0;
    }

    final void setElementCount(long size) {
        elementCount.set(size);
    }

    public boolean hasNull(int pos) {
        return false;
    }

    /**
     * Moves the data from an old store to new after changes to table
     * The colIndex argument is the indexes of the columns that were
     * removed, type modified or added. The adjust argument is {-1 | 0 | +1}.
     */
    public final void moveData(Session session, PersistentStore other,
                               int[] colIndex, int adjust) {

        try {
            Table       table = (Table) this.table;
            RowIterator it    = other.rowIterator();

            while (it.next()) {
                Row      row     = it.getCurrentRow();
                Object[] olddata = row.getData();
                Object[] data    = table.getEmptyRowData();

                ArrayUtil.copyAdjustArray(olddata, data, colIndex, adjust);

                for (int i = 0; i < colIndex.length; i++) {
                    if (adjust == 0) {
                        Type oldtype =
                            other.getTable().getColumnTypes()[colIndex[i]];
                        Type newtype =
                            this.table.getColumnTypes()[colIndex[i]];
                        Object oldvalue = olddata[colIndex[i]];
                        Object value = newtype.convertToType(session,
                                                             oldvalue,
                                                             oldtype);

                        data[colIndex[i]] = value;

                        table.systemSetIdentityColumn(session, data);
                    } else if (adjust > 0) {
                        ColumnSchema column = table.getColumn(colIndex[i]);
                        Object value =
                            table.getColumnDefaultOrGeneratedValue(session,
                                column, data);

                        data[colIndex[i]] = value;

                        table.systemSetIdentityColumn(session, data);
                    } else {}
                }

                table.enforceRowConstraints(session, data);

                // get object without RowAction
                Row newrow = (Row) getNewCachedObject(session, data, false);

                indexRow(session, newrow);
            }

            if (table.isTemp()) {
                return;
            }

            if (colIndex.length == 0) {
                return;
            }

            if (adjust <= 0) {
                Type type = other.getTable().getColumnTypes()[colIndex[0]];

                if (type != null && type.isLobType()) {
                    it = other.rowIterator();

                    while (it.next()) {
                        Row      row      = it.getCurrentRow();
                        Object[] olddata  = row.getData();
                        LobData  oldvalue = (LobData) olddata[colIndex[0]];

                        if (oldvalue != null) {
                            session.sessionData.adjustLobUsageCount(oldvalue,
                                    -1);
                        }
                    }
                }
            }

            if (adjust >= 0) {
                Type type = this.table.getColumnTypes()[colIndex[0]];

                if (type != null && type.isLobType()) {
                    it = rowIterator();

                    while (it.next()) {
                        Row      row   = it.getCurrentRow();
                        Object[] data  = row.getData();
                        LobData  value = (LobData) data[colIndex[0]];

                        if (value != null) {
                            session.sessionData.adjustLobUsageCount(value, +1);
                        }
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            throw Error.error(ErrorCode.OUT_OF_MEMORY, e);
        }
    }

    public void reindex(Session session, Index index, Index useIndex) {

        writeLock();

        try {

            // get the iterator first in case the index set null
            RowIterator it;

            if (useIndex == null) {
                it = table.rowIterator(this);
            } else {
                it = useIndex.firstRow(this);
            }

            setAccessor(index, null);

            while (it.next()) {
                RowAVL row = (RowAVL) it.getCurrentRow();

                row.getNode(index.getPosition()).delete();
                index.insert(session, this, row);
            }
        } finally {
            writeUnlock();
        }
    }

    public IndexStats[] checkIndexes(Session session, int mode) {

        IndexStats[] indexStats = new IndexStats[accessorList.length];

        for (int i = 0; i < indexList.length; i++) {
            IndexAVL index = (IndexAVL) indexList[i];
            NodeAVL  node  = (NodeAVL) accessorList[i];
            IndexAVLProbe probe = new IndexAVLProbe(session, this, index,
                node);

            probe.probe();

            indexStats[i] = probe.getStats();
        }

        return indexStats;
    }

    public void setReadOnly(boolean readOnly) {}

    public void readLock() {}

    public void readUnlock() {}

    public void writeLock() {}

    public void writeUnlock() {}

    void dropIndexFromRows(Index primaryIndex, Index oldIndex) {

        RowIterator it       = primaryIndex.firstRow(this);
        int         position = oldIndex.getPosition() - 1;

        while (it.next()) {
            Row     row      = it.getCurrentRow();
            int     i        = position;
            NodeAVL backnode = ((RowAVL) row).getNode(0);

            while (i-- > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }

        it.release();
    }

    boolean insertIndexNodes(Session session, Index primaryIndex,
                             Index newIndex) {

        writeLock();

        try {
            int           position = newIndex.getPosition();
            RowIterator   it       = primaryIndex.firstRow(this);
            int           rowCount = 0;
            HsqlException error    = null;

            try {
                while (it.next()) {
                    Row row = it.getCurrentRow();

                    ((RowAVL) row).insertNode(position);

                    // count before inserting
                    rowCount++;

                    newIndex.insert(session, this, row);
                }

                it.release();

                return true;
            } catch (OutOfMemoryError e) {
                error = Error.error(ErrorCode.OUT_OF_MEMORY, e);
            } catch (HsqlException e) {
                error = e;
            }

            // backtrack on error
            // rowCount rows have been modified
            it = primaryIndex.firstRow(this);

            while (it.next()) {
                Row     row      = it.getCurrentRow();
                NodeAVL backnode = ((RowAVL) row).getNode(0);
                int     j        = 0;

                while (backnode != null) {
                    backnode = backnode.nNext;

                    j++;
                }

                if (j < indexList.length) {
                    break;
                }

                backnode       = ((RowAVL) row).getNode(position - 1);
                backnode.nNext = backnode.nNext.nNext;
            }

            it.release();

            throw error;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Used with memory indexes
     */
    void destroyIndexes() {

        if (indexList.length == 0) {
            return;
        }

        IndexAVL idx  = (IndexAVL) indexList[0];
        NodeAVL  root = (NodeAVL) accessorList[0];

        idx.unlinkNodes(this, root);
    }
}
