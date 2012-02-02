package org.hsqldb.persist;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.RowAVLDiskData;
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
 * Implementation of PersistentStore for TEXT tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class RowStoreAVLDiskData extends RowStoreAVLDisk {

    RowAVLDiskData currentRow;

    public RowStoreAVLDiskData(PersistentStoreCollection manager,
                               Table table) {
        super(manager, null, table);
    }

    public CachedObject get(CachedObject object, boolean keep) {

        writeLock();

        try {
            currentRow = (RowAVLDiskData) object;
            object     = cache.get(object, this, keep);

            return object;
        } finally {
            currentRow = null;

            writeUnlock();
        }
    }

    public void add(CachedObject object) {

        writeLock();

        try {
            int size = object.getRealSize(cache.rowOut);

            object.setStorageSize(size);
            cache.add(object);
        } finally {
            writeUnlock();
        }
    }

    public CachedObject get(RowInputInterface in) {

        try {
            Object[] data = RowAVLDiskData.getRowData(table, in);

            if (currentRow == null) {
                RowAVLDiskData row = new RowAVLDiskData(this, table, data);

                row.setPos(in.getPos());
                row.setStorageSize(in.getSize());
                row.setChanged(false);
                ((TextCache) cache).addInit(row);

                return row;
            }

            currentRow.setData(data);

            return currentRow;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        Row row = new RowAVLDiskData(this, table, (Object[]) object);

        add(row);

        if (tx) {
            RowAction.addInsertAction(session, table, row);
        }

        return row;
    }

    public void indexRow(Session session, Row row) {
        super.indexRow(session, row);
    }

    public void set(CachedObject object) {}

    public void removeAll() {

        elementCount = 0;

        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(long i) {
        cache.remove(i, this);
    }

    public void removePersistence(Row row) {
        cache.removePersistence(row);
    }

    public void release(long i) {
        cache.release(i);
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
            cache.saveRow(row);
        } catch (HsqlException e1) {}
    }

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                removePersistence(row);
                break;

            case RowAction.ACTION_INSERT :
                commitPersistence(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                } else {
                    delete(session, row);
                    remove(row.getPos());
                }
                break;

            case RowAction.ACTION_DELETE_FINAL :
                if (txModel != TransactionManager.LOCKS) {
                    delete(session, row);
                    remove(row.getPos());
                }
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
                } else {}
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELETE
                if (txModel == TransactionManager.LOCKS) {
                    remove(row.getPos());
                } else {
                    delete(session, row);
                    remove(row.getPos());
                }
                break;
        }
    }

    /**
     * Does not adjust usage count
     */
    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        table.database.logger.closeTextCache((Table) table);

        cache = null;
    }
}
