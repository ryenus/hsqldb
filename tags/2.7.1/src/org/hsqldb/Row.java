/* Copyright (c) 2001-2021, The HSQL Development Group
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


package org.hsqldb;

import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.TimestampData;

/**
 * Base class for a database row object.
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.5.0
 */
public class Row implements CachedObject {

    long                      position;
    Object[]                  rowData;
    public volatile RowAction rowAction;
    protected TableBase       table;

    public RowAction getAction() {
        return rowAction;
    }

    /**
     *  Default constructor used only in subclasses.
     */
    public Row(TableBase table, Object[] data) {
        this.table   = table;
        this.rowData = data;
    }

    public Object getField(int col) {
        return rowData[col];
    }

    /**
     * Returns the array of fields in the database row.
     */
    public Object[] getData() {
        return rowData;
    }

    public Object[] getDataCopy() {

        Object[] newData = new Object[rowData.length];
        Object[] data    = getData();

        System.arraycopy(data, 0, newData, 0, data.length);

        return newData;
    }

    boolean isDeleted(Session session, PersistentStore store) {

        RowAction action;
        Row       row = (Row) store.get(this, false);

        if (row == null) {
            return true;
        }

        action = row.rowAction;

        if (action == null) {
            return false;
        }

        return !action.canRead(session, TransactionManager.ACTION_READ);
    }

    public TimestampData getSystemStartVersion() {

        if (table.isSystemVersioned) {
            TimestampData ts = (TimestampData) getField(
                ((Table) table).systemPeriodStartColumn);

            return ts;
        }

        return DateTimeType.epochTimestamp;
    }

    public TimestampData getSystemEndVersion() {

        if (table.isSystemVersioned) {
            TimestampData ts = (TimestampData) getField(
                ((Table) table).systemPeriodEndColumn);

            return ts;
        }

        return DateTimeType.epochLimitTimestamp;
    }

    public boolean isCurrentSystemVersion() {

        if (table.isSystemVersioned) {
            TimestampData ts = (TimestampData) getField(
                ((Table) table).systemPeriodEndColumn);

            return DateTimeType.epochLimitSeconds == ts.getSeconds();
        }

        return true;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    public final boolean isInvariable() {
        return false;
    }

    public final boolean isBlock() {
        return false;
    }

    public boolean isMemory() {
        return true;
    }

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public long getPos() {
        return position;
    }

    public long getId() {
        return ((long) table.getId() << 40) + position;
    }

    public void setPos(long pos) {
        position = pos;
    }

    public boolean isNew() {
        return false;
    }

    public boolean hasChanged() {
        return false;
    }

    public void setChanged(boolean flag) {}

    public boolean isKeepInMemory() {
        return true;
    }

    public boolean keepInMemory(boolean keep) {
        return true;
    }

    public boolean isInMemory() {
        return true;
    }

    public void setInMemory(boolean in) {}

    public void delete(PersistentStore store) {}

    public void restore() {}

    public void destroy() {}

    public int getRealSize(RowOutputInterface out) {
        return 0;
    }

    public TableBase getTable() {
        return table;
    }

    public int getDefaultCapacity() {
        return 0;
    }

    public void read(RowInputInterface in) {}

    public void write(RowOutputInterface out) {}

    public void write(RowOutputInterface out, LongLookup lookup) {}

    /**
     * Lifetime scope of this method is limited depends on the operations
     * performed. Rows deleted completely can equal rows produced later.
     * This can return invalid results if used with deleted rows.
     *
     * @param obj row to compare
     * @return boolean
     */
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof Row) {
            return ((Row) obj).table == table
                   && ((Row) obj).position == position;
        }

        return false;
    }

    /**
     * Hash code is always valid.
     *
     * @return file position of row
     */
    public int hashCode() {
        return (int) position;
    }
}
