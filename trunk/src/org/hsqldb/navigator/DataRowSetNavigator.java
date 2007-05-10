/* Copyright (c) 2001-2007, The HSQL Development Group
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

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Row;
import org.hsqldb.Select;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableUtil;
import org.hsqldb.TableWorks;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.persist.TempDataFileCache;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.SchemaObject;
import org.hsqldb.SchemaManager;

/**
 * Implementation or RowSetNavigator using a table as the data store.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class DataRowSetNavigator extends RowSetNavigator {

    Table          table;
    RowIterator    iterator;
    Index          mainIndex;
    Index          fullIndex;
    Index          orderIndex;
    final Session  session;
    Row            currentRow;
    final Select   select;
    final Database database;
    boolean        hasOrder;
    boolean        hasUnion;
    int            maxMemoryRowCount;
    boolean        isDiskBased;
    boolean        isClosed;

    public DataRowSetNavigator(Session session,
                               Select select) throws HsqlException {

        this.session      = session;
        this.select       = select;
        this.database     = session.getDatabase();
        maxMemoryRowCount = session.getResultMemoryRowCount();

        if (maxMemoryRowCount == 0) {
            maxMemoryRowCount = Integer.MAX_VALUE;
        }

        createTable(session, select);
    }

    void createTable(Session session, Select select) throws HsqlException {

        Database database = session.getDatabase();
        HsqlName tablename = database.nameManager.newHsqlName(
            database.schemaManager.SYSTEM_SCHEMA_HSQLNAME, "SYSTEM_RESULT",
            false, SchemaObject.TABLE);
        int tableType = Table.RESULT;

        table = new Table(session, database, tablename, tableType);

        ResultMetaData meta      = select.getMetaData();
        String[]       colLabels = meta.colLabels;
        boolean[]      colQuoted = meta.isLabelQuoted;

        meta.colLabels     = new String[colLabels.length];
        meta.isLabelQuoted = new boolean[colQuoted.length];

        for (int i = 0; i < meta.colLabels.length; i++) {
            meta.colLabels[i] = HsqlNameManager.getAutoColumnNameString(i);
        }

        TableUtil.addColumns(table, meta, meta.colLabels.length);
        table.createPrimaryKey();

        mainIndex          = table.getPrimaryIndex();
        meta.colLabels     = colLabels;
        meta.isLabelQuoted = colQuoted;

        if (select.orderByColumnCount != 0 && !select.isAggregateSorted) {
            sortOrder();

            hasOrder = true;
        }

        if (select.unionType != Select.NOUNION
                && select.unionType != Select.UNIONALL) {
            sortUnion();

            hasUnion = true;
        }
    }

    public void changeToDiskTable() throws HsqlException {

        TableWorks tw = new TableWorks(session, table);

        tw.setTableType(session, Table.CACHED_RESULT);

        table = tw.getTable();

        ((TempDataFileCache) table.cache).resultCount++;

        mainIndex = table.getPrimaryIndex();

        if (hasOrder) {
            orderIndex = mainIndex = table.getIndex(1);

            if (hasUnion) {
                fullIndex = mainIndex = table.getIndex(2);
            }
        } else {
            if (hasUnion) {
                table.getIndex(1);

                fullIndex = mainIndex = table.getIndex(1);
            }
        }

        reset();

        isDiskBased = true;
    }

    public void sortUnion() throws HsqlException {

        if (fullIndex != null) {
            mainIndex = fullIndex;

            return;
        }

        int[] fullCols = new int[select.visibleColumnCount];

        ArrayUtil.fillSequence(fullCols);

        HsqlName indexName = database.nameManager.newAutoName("IDX_T",
            table.getSchemaName(), table.getName(), SchemaObject.INDEX);

        fullIndex = table.createIndex(session, fullCols, null, indexName,
                                      false, false, false);
        mainIndex = fullIndex;
    }

    public void sortOrder() throws HsqlException {

        if (orderIndex != null) {
            mainIndex = orderIndex;

            return;
        }

        if (select.orderByColumnCount != 0) {
            boolean[] orderDesc = new boolean[select.orderByColumnCount];

            for (int i = 0; i < orderDesc.length; i++) {
                orderDesc[i] = select.sortDirection[i] == -1;
            }

            HsqlName indexName = database.nameManager.newAutoName("IDX_T",
                table.getSchemaName() , table.getName(), SchemaObject.INDEX);

            orderIndex = table.createIndex(session, select.sortOrder,
                                           orderDesc, indexName, false, false,
                                           false);
        } else {
            orderIndex = table.getPrimaryIndex();
        }

        mainIndex = orderIndex;
    }

    public void add(Object data) {

        try {
            if (size == maxMemoryRowCount) {
                changeToDiskTable();
            }

            Row row = table.newRow((Object[]) data);

            table.indexRow(session, row);

            size++;
        } catch (HsqlException e) {}
    }

    private void addAdjusted(Object data) {

        try {
            data = ArrayUtil.resizeArrayIfDifferent(data,
                    table.getColumnCount());

            Row row = table.newRow((Object[]) data);

            table.indexRow(session, row);

            size++;
        } catch (HsqlException e) {}
    }

    public void clear() {

        table.clearAllData(session);

        size = 0;

        reset();
    }

    public Object getCurrent() {
        return currentRow.getData();
    }

    public boolean next() {

        boolean result = super.next();

        currentRow = iterator.getNext();

        return result;
    }

    public void remove() throws HsqlException {

        if (currentRow != null) {
            iterator.remove();

            currentRow = null;

            currentPos--;
            size--;
        }
    }

    public void reset() {

        super.reset();

        try {
            iterator = mainIndex.firstRow(session);
        } catch (HsqlException e) {

            // todo
        }
    }

    public void close() {

        if (isClosed) {
            return;
        }

        iterator.release();

        if (table.cache != null) {
            TempDataFileCache cache = (TempDataFileCache) table.cache;

            cache.resultCount--;

            if (cache.resultCount == 0) {
                cache.clear();
            }
        }

        isClosed = true;
    }

    public boolean isDiskBased() {
        return isDiskBased;
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws HsqlException, IOException {}

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws HsqlException, IOException {

        reset();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (hasNext()) {
            Object[] data = (Object[]) getNext();

            out.writeData(meta.getColumnCount(), meta.colTypes, data, null,
                          null);
        }

        reset();
    }

    public void union(DataRowSetNavigator other) throws HsqlException {
        unionAll(other);
        removeDuplicates();
    }

    public void unionAll(DataRowSetNavigator other) throws HsqlException {

        other.reset();

        while (other.hasNext()) {
            addAdjusted(other.getNext());
        }

        other.close();
    }

    public void intersect(DataRowSetNavigator other) throws HsqlException {

        removeDuplicates();
        reset();
        other.sortUnion();

        while (hasNext()) {
            getNext();

            Object[] currentData = currentRow.getData();
            RowIterator it = other.fullIndex.findFirstRow(session,
                currentData);

            if (!it.hasNext()) {
                remove();
            }
        }

        other.close();
    }

    public void except(DataRowSetNavigator other) throws HsqlException {

        removeDuplicates();
        reset();
        other.sortUnion();

        while (hasNext()) {
            getNext();

            Object[] currentData = currentRow.getData();
            RowIterator it = other.fullIndex.findFirstRow(session,
                currentData);

            if (it.hasNext()) {
                remove();
            }
        }

        other.close();
    }

    public boolean hasUniqueNotNullRows() throws HsqlException {

        sortUnion();
        reset();

        Object[] lastRowData = null;

        while (hasNext()) {
            getNext();

            Object[] currentData = currentRow.getData();

            if (hasNull(currentData)) {
                continue;
            }

            if (lastRowData != null && equals(lastRowData, currentData)) {
                return false;
            } else {
                lastRowData = currentData;
            }
        }

        return true;
    }

    public void removeDuplicates() throws HsqlException {

        sortUnion();
        reset();

        Object[] lastRowData = null;

        while (hasNext()) {
            getNext();

            Object[] currentData = currentRow.getData();

            if (lastRowData != null && equals(lastRowData, currentData)) {
                remove();
            } else {
                lastRowData = currentData;
            }
        }
    }

    public void trim(int limitstart, int limitcount) throws HsqlException {

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

    private boolean hasNull(Object[] data) {

        for (int i = 0; i < select.visibleColumnCount; i++) {
            if (data[i] == null) {
                return true;
            }
        }

        return false;
    }

    private boolean equals(Object[] data1, Object[] data2) {

        for (int i = 0; i < select.visibleColumnCount; i++) {
            if (!equals(data1[i], data2[i])) {
                return false;
            }
        }

        return true;
    }

    private boolean equals(Object o1, Object o2) {
        return (o1 == null) ? o2 == null
                            : o1.equals(o2);
    }
}
