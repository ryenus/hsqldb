/* Copyright (c) 2001-2024, The HSQL Development Group
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

import java.util.Comparator;
import java.util.TreeMap;

import org.hsqldb.QueryExpression;
import org.hsqldb.QuerySpecification;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SortAndSlice;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.List;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Implementation of RowSetNavigator for result sets.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public class RowSetNavigatorData extends RowSetNavigator
        implements Comparator<Object[]> {

    public static final Object[][] emptyTable = new Object[0][];

    //
    private Object[][] dataTable = emptyTable;

    //
    int      visibleColumnCount;
    boolean  isAggregate;
    boolean  isSimpleAggregate;
    Object[] simpleAggregateData;

    //
    boolean reindexTable;

    //
    Index mainIndex;
    Index fullIndex;
    Index orderIndex;
    Index groupIndex;
    Index idIndex;
    Index rowNumIndex;

    //
    final SortAndSlice sortAndSlice;

    //
    TreeMap<Object[], Integer> groupMap;
    LongKeyHashMap<Object[]>   idMap;

    RowSetNavigatorData(Session session, SortAndSlice sortAndSlice) {
        this.session      = session;
        this.sortAndSlice = sortAndSlice;
    }

    public RowSetNavigatorData(Session session, QuerySpecification select) {

        this.session       = session;
        this.rangePosition = select.resultRangePosition;
        visibleColumnCount = select.getColumnCount();
        isSimpleAggregate  = select.isAggregated && !select.isGrouped;
        mainIndex          = select.mainIndex;
        fullIndex          = select.fullIndex;
        orderIndex         = select.orderIndex;
        sortAndSlice       = select.sortAndSlice;
        idIndex            = select.idIndex;

        if (select.isGrouped) {
            mainIndex = select.groupIndex;
            groupMap  = new TreeMap<>(this);
        }

        if (idIndex != null) {
            idMap = new LongKeyHashMap<>();
        }
    }

    public RowSetNavigatorData(
            Session session,
            QueryExpression queryExpression) {

        this.session       = session;
        mainIndex          = queryExpression.mainIndex;
        fullIndex          = queryExpression.fullIndex;
        orderIndex         = queryExpression.orderIndex;
        visibleColumnCount = queryExpression.getColumnCount();
        sortAndSlice       = queryExpression.sortAndSlice;
    }

    public RowSetNavigatorData(Session session, RowSetNavigator navigator) {

        this.session = session;
        sortAndSlice = SortAndSlice.noSort;

        setCapacity(navigator.size);

        while (navigator.next()) {
            add(navigator.getCurrent());
        }
    }

    public RowSetNavigatorData(Session session, List<Object[]> list) {

        this.session = session;
        sortAndSlice = SortAndSlice.noSort;

        setCapacity(list.size());

        for (int i = 0; i < list.size(); i++) {
            add(list.get(i));
        }
    }

    public void sortFull() {

        mainIndex = fullIndex;

        ArraySort.sort(dataTable, size, this);
        reset();
    }

    public void sortOrder() {

        if (orderIndex != null) {
            mainIndex = orderIndex;

            ArraySort.sort(dataTable, size, this);
        }

        reset();
    }

    public void sortOrderUnion(SortAndSlice sortAndSlice) {

        if (sortAndSlice.index != null) {
            mainIndex = sortAndSlice.index;

            ArraySort.sort(dataTable, size, this);
            reset();
        }
    }

    public void add(Object[] data) {

        ensureCapacity();

        dataTable[size] = data;

        if (groupMap != null) {
            groupMap.put(data, size);
        }

        if (idMap != null) {
            Long id =
                (Long) data[visibleColumnCount + ResultMetaData.SysOffsets.rowId];

            idMap.put(id.longValue(), data);
        }

        size++;
    }

    public void setPosition(Object[] data) {

        Integer mapPos = groupMap.get(data);

        if (mapPos == null) {
            return;
        }

        int pos = mapPos.intValue();

        currentPos = pos;
    }

    public void updateData(long oldRowId, Object[] newData) {

        if (idMap == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorData");
        }

        Object[] oldData = idMap.get(oldRowId);
        long newId =
            (Long) newData[visibleColumnCount + ResultMetaData.SysOffsets.rowId];

        idMap.remove(oldRowId);
        idMap.put(newId, oldData);
        ArrayUtil.copyArray(newData, oldData, newData.length);
    }

    public void updateData(Object[] oldData, Object[] newData) {

        // no op
    }

    void addAdjusted(Object[] data, int[] columnMap) {
        data = projectData(data, columnMap);

        add(data);
    }

    Object[] projectData(Object[] data, int[] columnMap) {

        if (columnMap == null) {
            data = (Object[]) ArrayUtil.resizeArrayIfDifferent(
                data,
                visibleColumnCount);
        } else {
            Object[] newData = new Object[visibleColumnCount];

            ArrayUtil.projectRow(data, columnMap, newData);

            data = newData;
        }

        return data;
    }

    /**
     * for union only
     */
    void insert(Object[] data) {

        ensureCapacity();
        System.arraycopy(
            dataTable,
            currentPos,
            dataTable,
            currentPos + 1,
            size - currentPos);

        dataTable[currentPos] = data;

        size++;
    }

    public Object[][] removeDataTable() {

        Object[][] table = dataTable;

        dataTable = emptyTable;
        size      = 0;

        reset();

        return table;
    }

    public void release() {

        this.dataTable = emptyTable;
        this.size      = 0;

        reset();

        isClosed = true;
    }

    public void clear() {

        this.dataTable = emptyTable;
        this.size      = 0;

        reset();
    }

    public void resetRowMap() {
        groupMap = new TreeMap<>(this);
    }

    public boolean absolute(int position) {
        return super.absolute(position);
    }

    public Object[] getCurrent() {

        if (currentPos < 0 || currentPos >= size) {
            return null;
        }

        return dataTable[currentPos];
    }

    public Row getCurrentRow() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorData");
    }

    public boolean next() {
        return super.next();
    }

    public void removeCurrent() {

        System.arraycopy(
            dataTable,
            currentPos + 1,
            dataTable,
            currentPos,
            size - currentPos - 1);

        dataTable[size - 1] = null;

        currentPos--;
        size--;
    }

    public void removeRange(int start, int rows) {

        ArrayUtil.adjustArray(
            ArrayUtil.CLASS_CODE_OBJECT,
            dataTable,
            size,
            start,
            -rows);

        if (currentPos >= start + rows) {
            currentPos -= rows;
        } else if (currentPos >= start) {
            currentPos = start;
        }

        size -= rows;
    }

    public void reset() {
        super.reset();
    }

    public boolean isMemory() {
        return true;
    }

    public void read(RowInputInterface in, ResultMetaData meta) {}

    public void write(RowOutputInterface out, ResultMetaData meta) {

        reset();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (next()) {
            Object[] data = getCurrent();

            out.writeData(
                meta.getExtendedColumnCount(),
                meta.columnTypes,
                data,
                null,
                null);
        }

        reset();
    }

    public Object[] getData(long rowId) {
        return idMap.get(rowId);
    }

    public void copy(RowIterator other, int[] rightColumnIndexes) {

        while (other.next()) {
            Object[] currentData = other.getCurrent();

            addAdjusted(currentData, rightColumnIndexes);
        }
    }

    public void union(RowSetNavigatorData other) {

        Object[] currentData;

        removeDuplicates();
        other.removeDuplicates();

        mainIndex = fullIndex;

        while (other.next()) {
            currentData = other.getCurrent();

            int position = ArraySort.searchFirst(
                dataTable,
                0,
                size,
                currentData,
                this);

            if (position < 0) {
                position   = -position - 1;
                currentPos = position;

                insert(currentData);
            }
        }

        reset();
    }

    public void unionAll(RowSetNavigatorData other) {

        mainIndex = fullIndex;

        other.reset();

        while (other.next()) {
            Object[] currentData = other.getCurrent();

            add(currentData);
        }

        reset();
    }

    public void intersect(RowSetNavigatorData other) {

        removeDuplicates();
        other.sortFull();

        while (next()) {
            Object[] currentData = getCurrent();
            boolean  hasRow      = other.containsRow(currentData);

            if (!hasRow) {
                removeCurrent();
            }
        }

        reset();
    }

    public void intersectAll(RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Object[]    otherData = null;

        sortFull();
        other.sortFull();

        it = RowIterator.emptyRowIterator;

        while (next()) {
            Object[] currentData = getCurrent();
            boolean newGroup = compareData == null
                               || fullIndex.compareRowNonUnique(
                                   (Session) session,
                                   currentData,
                                   compareData,
                                   visibleColumnCount) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            if (it.next()) {
                otherData = it.getCurrent();

                if (fullIndex.compareRowNonUnique((Session) session,
                                                  currentData,
                                                  otherData,
                                                  visibleColumnCount) == 0) {
                    continue;
                }
            }

            removeCurrent();
        }

        reset();
    }

    public void except(RowSetNavigatorData other) {

        removeDuplicates();
        other.sortFull();

        while (next()) {
            Object[] currentData = getCurrent();
            boolean  hasRow      = other.containsRow(currentData);

            if (hasRow) {
                removeCurrent();
            }
        }

        reset();
    }

    public void exceptNoDedup(RowSetNavigatorData other) {

        other.sortFull();
        reset();

        while (next()) {
            Object[] currentData = getCurrent();
            boolean  hasRow      = other.containsRow(currentData);

            if (hasRow) {
                removeCurrent();
            }
        }

        reset();
    }

    public void exceptAll(RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Object[]    otherData = null;

        sortFull();
        other.sortFull();

        it = RowIterator.emptyRowIterator;

        while (next()) {
            Object[] currentData = getCurrent();
            boolean newGroup = compareData == null
                               || fullIndex.compareRowNonUnique(
                                   (Session) session,
                                   currentData,
                                   compareData,
                                   fullIndex.getColumnCount()) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            if (it.next()) {
                otherData = it.getCurrent();

                if (fullIndex.compareRowNonUnique((Session) session,
                                                  currentData,
                                                  otherData,
                                                  fullIndex.getColumnCount()) == 0) {
                    removeCurrent();
                }
            }
        }

        reset();
    }

    public boolean hasUniqueNotNullRows() {

        sortFull();
        reset();

        Object[] lastRowData = null;

        while (next()) {
            Object[] currentData = getCurrent();

            if (hasNull(currentData)) {
                continue;
            }

            if (lastRowData != null
                    && fullIndex.compareRow((Session) session,
                                            lastRowData,
                                            currentData) == 0) {
                return false;
            } else {
                lastRowData = currentData;
            }
        }

        return true;
    }

    public void removeDuplicates() {

        sortFull();
        reset();

        int      lastRowPos  = -1;
        Object[] lastRowData = null;

        while (next()) {
            Object[] currentData = getCurrent();

            if (lastRowData == null) {
                lastRowPos  = currentPos;
                lastRowData = currentData;
                continue;
            }

            if (fullIndex.compareRow((Session) session,
                                     lastRowData,
                                     currentData) != 0) {
                lastRowPos++;

                lastRowData           = currentData;
                dataTable[lastRowPos] = currentData;
            }
        }

        for (int i = lastRowPos + 1; i < size; i++) {
            dataTable[i] = null;
        }

        super.size = lastRowPos + 1;

        reset();
    }

    public void trim(int limitstart, int limitcount) {

        if (size == 0) {
            return;
        }

        if (limitstart >= size) {
            clear();

            return;
        }

        if ((long) limitstart + limitcount < size) {
            reset();
            removeRange(
                limitstart + limitcount,
                size - limitstart - limitcount);
        }

        removeRange(0, limitstart);
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

        Integer position = groupMap.get(data);

        if (position == null) {
            return null;
        }

        int pos = position.intValue();

        return dataTable[pos];
    }

    /**
     * Special case for isSimpleAggregate cannot use index lookup.
     */
    public Object[] getGroupDataAndPosition(Object[] data) {

        Integer mapPos = groupMap.get(data);

        if (mapPos == null) {
            return null;
        }

        int pos = mapPos.intValue();

        currentPos = pos;

        return dataTable[pos];
    }

    boolean containsRow(Object[] data) {
        int position = ArraySort.searchFirst(dataTable, 0, size, data, this);

        return position >= 0;
    }

    RowIterator findFirstRow(Object[] data) {

        int position = ArraySort.searchFirst(dataTable, 0, size, data, this);

        if (position < 0) {
            position = size;
        } else {
            position--;
        }

        return new DataIterator(position);
    }

    /**
     * baseBlockSize remains unchanged.
     */
    void getBlock(int offset) {

        // no op for no blocks
    }

    private void setCapacity(int newSize) {
        if (size > dataTable.length) {
            dataTable = new Object[newSize][];
        }
    }

    private void ensureCapacity() {

        if (size == dataTable.length) {
            int        newSize  = size == 0
                                  ? 4
                                  : size * 2;
            Object[][] newTable = new Object[newSize][];

            System.arraycopy(dataTable, 0, newTable, 0, size);

            dataTable = newTable;
        }
    }

    class DataIterator implements RowIterator {

        int pos;

        DataIterator(int position) {
            pos = position;
        }

        public Object getField(int col) {

            if (pos < size) {
                return dataTable[pos][col];
            } else {
                return null;
            }
        }

        public boolean next() {

            if (pos < size - 1) {
                pos++;

                return true;
            }

            return false;
        }

        public Row getCurrentRow() {
            return null;
        }

        public Object[] getCurrent() {

            if (pos < size) {
                return dataTable[pos];
            } else {
                return null;
            }
        }

        public void removeCurrent() {}

        public void release() {}

        public long getRowId() {
            return 0L;
        }
    }

    public int compare(Object[] a, Object[] b) {
        return mainIndex.compareRow((Session) session, a, b);
    }
}
