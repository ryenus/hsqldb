/* Copyright (c) 2001-2019, The HSQL Development Group
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

import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.index.Index;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 2.5.0
 */
public class RowInsertVersioning implements RowInsertInterface {

    final Session         session;
    final ErrorLogger     callback;
    final int             mode;
    RowSetNavigatorClient rowSet = new RowSetNavigatorClient(64);
    Table                 table  = null;
    PersistentStore       store;
    Index                 index = null;

    public RowInsertVersioning(Session session, ErrorLogger callback,
                               int mode) {

        this.session  = session;
        this.callback = callback;
        this.mode     = mode;
    }

    public void finishTable() {
        applyChangeSet();
    }

    public void close() {
        callback.close();
    }

    public long getErrorLineNumber() {
        return 0L;
    }

    public void insert(Table table, PersistentStore store, Object[] rowData) {

        if (this.table != table) {
            resetTable(table, store);
        }

        if (isSameRowSet(rowData)) {
            rowSet.add(rowData);
        } else {
            applyChangeSet();
            rowSet.add(rowData);
        }
    }

    public void setStartLineNumber(long number) {}

    boolean isSameRowSet(Object[] rowData) {

        if (rowSet.isEmpty()) {
            return true;
        }

        return index.compareRow(session, rowData, rowSet.getData(0)) == 0;
    }

    void applyChangeSet() {

        if (rowSet.getSize() == 0) {
            return;
        }

        int         startNewRow  = 0;
        RowIterator it = index.findFirstRow(session, store, rowSet.getData(0));
        boolean     reportFailed = false;
        Row         currentRow   = null;

        while (it.next()) {
            if (!isSameRowSet(it.getCurrent())) {
                break;
            }

            currentRow = it.getCurrentRow();

            TimestampData ts      = currentRow.getSystemStartVersion();
            Object[]      newData = rowSet.getData(startNewRow);
            int compare = compareColumn(ts, newData,
                                        table.getSystemPeriodStartIndex());

            if (compare < 0) {
                // both start and end row TS are before new data row
                ts = currentRow.getSystemEndVersion();
                compare = compareColumn(ts, newData,
                                        table.getSystemPeriodStartIndex());

                if (compare <= 0) {
                    continue;
                } else {
                    reportFailed = true;

                    break;
                }
            }

            if (compare > 0) {
                reportFailed = true;

                break;
            }

            ts = currentRow.getSystemEndVersion();
            compare = compareColumn(ts, newData,
                                    table.getSystemPeriodEndIndex());

            if (compare == 0) {
                startNewRow++;

                continue;
            }

            if (ts.getSeconds() == DateTimeType.epochLimitSeconds) {
                it.removeCurrent();

                break;
            }
        }

        if (reportFailed) {
            for (int i = startNewRow; i < rowSet.getSize(); i++) {
                Object[] newData = rowSet.getData(i);
                Row      newRow  = new Row(table, newData);

                callback.writeRow(0, newRow);
            }
        } else {
            for (int i = startNewRow; i < rowSet.getSize(); i++) {
                Object[] newData = rowSet.getData(i);

                table.insertFromScript(session, store, newData);
            }
        }

        rowSet.clear();
    }

    void resetTable(Table newTable, PersistentStore newStore) {

        table = newTable;
        store = newStore;
        index = newTable.getPrimaryIndex();
    }

    int compareColumn(TimestampData ts, Object[] data, int colIndex) {
        return Type.SQL_TIMESTAMP_WITH_TIME_ZONE.compare(session, ts,
                data[colIndex]);
    }
}
