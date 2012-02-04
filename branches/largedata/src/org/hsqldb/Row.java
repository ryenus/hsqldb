package org.hsqldb;

import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Base class for a database row object.
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.3.0
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

    /**
     * Returns the array of fields in the database row.
     */
    public Object[] getData() {
        return rowData;
    }

    boolean isDeleted(Session session, PersistentStore store) {

        Row       row    = (Row) store.get(this, false);
        RowAction action = row.rowAction;

        if (action == null) {
            return false;
        }

        return !action.canRead(session, TransactionManager.ACTION_READ);
    }

    public void setChanged(boolean changed) {}

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
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

    public boolean hasChanged() {
        return false;
    }

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
