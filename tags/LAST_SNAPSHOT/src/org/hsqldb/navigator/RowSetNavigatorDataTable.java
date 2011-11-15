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


package org.hsqldb.navigator;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.OpTypes;
import org.hsqldb.QueryExpression;
import org.hsqldb.QuerySpecification;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SortAndSlice;
import org.hsqldb.TableBase;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Implementation of RowSetNavigator using a table as the data store.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class RowSetNavigatorDataTable extends RowSetNavigatorData {

    final Session          session;
    public TableBase       table;
    public PersistentStore store;
    RowIterator            iterator;
    Row                    currentRow;
    int                    maxMemoryRowCount;
    boolean                isClosed;
    int                    visibleColumnCount;
    boolean                isAggregate;
    boolean                isSimpleAggregate;
    Object[]               simpleAggregateData;
    Object[]               tempRowData;

    //
    boolean reindexTable;

    //
    private Index mainIndex;
    private Index fullIndex;
    private Index orderIndex;
    private Index groupIndex;
    private Index idIndex;

    public RowSetNavigatorDataTable(Session session,
                                    QuerySpecification select) {

        super(session);

        this.session       = session;
        this.rangePosition = select.resultRangePosition;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        visibleColumnCount = select.indexLimitVisible;
        table              = select.resultTable.duplicate();
        table.store = store = session.sessionData.getNewResultRowStore(table,
                !select.isAggregated);
        isAggregate       = select.isAggregated;
        isSimpleAggregate = select.isAggregated && !select.isGrouped;
        reindexTable      = select.isGrouped;
        mainIndex         = select.mainIndex;
        fullIndex         = select.fullIndex;
        orderIndex        = select.orderIndex;
        groupIndex        = select.groupIndex;
        idIndex           = select.idIndex;
        tempRowData       = new Object[1];
    }

    public RowSetNavigatorDataTable(Session session,
                                    QuerySpecification select,
                                    RowSetNavigatorData navigator) {

        this(session, (QuerySpecification) select);

        navigator.reset();

        while (navigator.hasNext()) {
            add(navigator.getNext());
        }
    }

    public RowSetNavigatorDataTable(Session session,
                                    QueryExpression queryExpression) {

        super(session);

        this.session       = session;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        table              = queryExpression.resultTable.duplicate();
        visibleColumnCount = table.getColumnCount();
        table.store = store = session.sessionData.getNewResultRowStore(table,
                true);
        mainIndex = queryExpression.mainIndex;
        fullIndex = queryExpression.fullIndex;
    }

    public RowSetNavigatorDataTable(Session session, TableBase table) {

        super(session);

        this.session       = session;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        this.table         = table;
        visibleColumnCount = table.getColumnCount();
        store              = table.getRowStore(session);
        mainIndex          = table.getPrimaryIndex();
        fullIndex          = table.getFullIndex();
        this.size          = mainIndex.size(session, store);

        reset();
    }

    public void sortFull(Session session) {

        if (reindexTable) {
            store.indexRows(session);
        }

        mainIndex = fullIndex;

        reset();
    }

    public void sortOrder(Session session) {

        if (orderIndex != null) {
            if (reindexTable) {
                store.indexRows(session);
            }

            mainIndex = orderIndex;

            reset();
        }
    }

    public void sortOrderUnion(Session session, SortAndSlice sortAndSlice) {

        if (sortAndSlice.index != null) {
            mainIndex = sortAndSlice.index;

            reset();
        }
    }

    public void add(Object[] data) {

        try {
            Row row = (Row) store.getNewCachedObject(session, data, false);

            store.indexRow(null, row);

            size++;
        } catch (HsqlException e) {}
    }

    void addAdjusted(Object[] data, int[] columnMap) {

        try {
            if (columnMap == null) {
                data = (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                        visibleColumnCount);
            } else {
                Object[] newData = new Object[visibleColumnCount];

                ArrayUtil.projectRow(data, columnMap, newData);

                data = newData;
            }

            add(data);
        } catch (HsqlException e) {}
    }

    public void update(Object[] oldData, Object[] newData) {

        if (isSimpleAggregate) {
            return;
        }

        RowIterator it = groupIndex.findFirstRow(session, store, oldData);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            it.remove();
            it.release();

            size--;

            add(newData);
        }
    }

    public void clear() {

        table.clearAllData(store);

        size = 0;

        reset();
    }

    public boolean absolute(int position) {
        return super.absolute(position);
    }

    public Object[] getCurrent() {
        return currentRow.getData();
    }

    public Row getCurrentRow() {
        return currentRow;
    }

    public boolean next() {

        boolean result = super.next();

        currentRow = iterator.getNextRow();

        return result;
    }

    public void remove() {

        if (currentRow != null) {
            iterator.remove();

            currentRow = null;

            currentPos--;
            size--;
        }
    }

    public void reset() {

        super.reset();

        iterator = mainIndex.firstRow(store);
    }

    public void close() {

        if (isClosed) {
            return;
        }

        iterator.release();
        store.release();

        isClosed = true;
    }

    public boolean isMemory() {
        return store.isMemory();
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {}

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException {

        reset();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (hasNext()) {
            Object[] data = getNext();

            out.writeData(meta.getExtendedColumnCount(), meta.columnTypes,
                          data, null, null);
        }

        reset();
    }

    public Object[] getData(Long rowId) {

        tempRowData[0] = rowId;

        RowIterator it = idIndex.findFirstRow(session, store, tempRowData,
                                              idIndex.getDefaultColumnMap());

        return it.getNext();
    }

    public void copy(RowSetNavigatorData other, int[] rightColumnIndexes) {

        while (other.hasNext()) {
            Object[] currentData = other.getNext();

            addAdjusted(currentData, rightColumnIndexes);
        }

        other.close();
    }

    public void union(Session session, RowSetNavigatorData other) {

        Object[] currentData;

        removeDuplicates(session);
        reset();

        while (other.hasNext()) {
            currentData = other.getNext();

            RowIterator it = findFirstRow(currentData);

            if (!it.hasNext()) {
                currentData =
                    (Object[]) ArrayUtil.resizeArrayIfDifferent(currentData,
                        table.getColumnCount());

                add(currentData);
            }
        }

        other.close();
    }

    public void intersect(Session session, RowSetNavigatorData other) {

        removeDuplicates(session);
        reset();
        other.sortFull(session);

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean  hasRow      = other.containsRow(currentData);

            if (!hasRow) {
                remove();
            }
        }

        other.close();
    }

    public void intersectAll(Session session, RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Row         otherRow  = null;
        Object[]    otherData = null;

        sortFull(session);
        reset();
        other.sortFull(session);

        it = fullIndex.emptyIterator();

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean newGroup =
                compareData == null
                || fullIndex.compareRowNonUnique(
                    session, currentData, compareData,
                    fullIndex.getColumnCount()) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            otherRow  = it.getNextRow();
            otherData = otherRow == null ? null
                                         : otherRow.getData();

            if (otherData != null
                    && fullIndex.compareRowNonUnique(
                        session, currentData, otherData,
                        fullIndex.getColumnCount()) == 0) {
                continue;
            }

            remove();
        }

        other.close();
    }

    public void except(Session session, RowSetNavigatorData other) {

        removeDuplicates(session);
        reset();
        other.sortFull(session);

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean  hasRow      = other.containsRow(currentData);

            if (hasRow) {
                remove();
            }
        }

        other.close();
    }

    public void exceptAll(Session session, RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Row         otherRow  = null;
        Object[]    otherData = null;

        sortFull(session);
        reset();
        other.sortFull(session);

        it = fullIndex.emptyIterator();

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean newGroup =
                compareData == null
                || fullIndex.compareRowNonUnique(
                    session, currentData, compareData,
                    fullIndex.getColumnCount()) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            otherRow  = it.getNextRow();
            otherData = otherRow == null ? null
                                         : otherRow.getData();

            if (otherData != null
                    && fullIndex.compareRowNonUnique(
                        session, currentData, otherData,
                        fullIndex.getColumnCount()) == 0) {
                remove();
            }
        }

        other.close();
    }

    public boolean hasUniqueNotNullRows(Session session) {

        sortFull(session);
        reset();

        Object[] lastRowData = null;

        while (hasNext()) {
            Object[] currentData = getNext();

            if (hasNull(currentData)) {
                continue;
            }

            if (lastRowData != null
                    && fullIndex.compareRow(session, lastRowData, currentData)
                       == 0) {
                return false;
            } else {
                lastRowData = currentData;
            }
        }

        return true;
    }

    public void removeDuplicates(Session session) {

        sortFull(session);
        reset();

        Object[] lastRowData = null;

        while (next()) {

            Object[] currentData = getCurrent();

            if (lastRowData != null
                    && fullIndex.compareRow(session, lastRowData, currentData)
                       == 0) {
                remove();
            } else {
                lastRowData = currentData;
            }
        }
    }

    public void trim(int limitstart, int limitcount) {

        if (size == 0) {
            return;
        }

        if (limitstart >= size) {
            clear();

            return;
        }

        if (limitstart != 0) {
            reset();

            for (int i = 0; i < limitstart; i++) {
                next();
                remove();
            }
        }

        if (limitcount == 0 || limitcount >= size) {
            return;
        }

        reset();

        for (int i = 0; i < limitcount; i++) {
            next();
        }

        while (hasNext()) {
            next();
            remove();
        }
    }

    boolean hasNull(Object[] data) {

        for (int i = 0; i < visibleColumnCount; i++) {
            if (data[i] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Special case for isSimpleAggregate cannot use index lookup.
     */
    public Object[] getGroupData(Object[] data) {

        if (isSimpleAggregate) {
            if (simpleAggregateData == null) {
                simpleAggregateData = data;

                return null;
            }

            return simpleAggregateData;
        }

        RowIterator it = groupIndex.findFirstRow(session, store, data);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            if (isAggregate) {
                row.setChanged(true);
            }

            return row.getData();
        }

        return null;
    }

    boolean containsRow(Object[] data) {

        RowIterator it     = mainIndex.findFirstRow(session, store, data);
        boolean     result = it.hasNext();

        it.release();

        return result;
    }

    RowIterator findFirstRow(Object[] data) {
        return mainIndex.findFirstRow(session, store, data);
    }
}
