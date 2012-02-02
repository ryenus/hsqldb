package org.hsqldb;

import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.0.0
 */
public interface TransactionManager {

    //
    public int LOCKS   = 0;
    public int MVLOCKS = 1;
    public int MVCC    = 2;

    //
    public int ACTION_READ = 0;
    public int ACTION_DUP  = 1;
    public int ACTION_REF  = 2;

    public long getGlobalChangeTimestamp();

    public RowAction addDeleteAction(Session session, Table table, Row row,
                                     int[] colMap);

    public void addInsertAction(Session session, Table table,
                                PersistentStore store, Row row,
                                int[] changedColumns);

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs);

    public void beginActionResume(Session session);

    public void beginTransaction(Session session);

    // functional unit - accessibility of rows
    public boolean canRead(Session session, Row row, int mode, int[] colMap);

    public boolean canRead(Session session, long id, int mode);

    public boolean commitTransaction(Session session);

    public void completeActions(Session session);

    public int getTransactionControl();

    public boolean isMVRows();

    public boolean isMVCC();

    public boolean prepareCommitActions(Session session);

    public void rollback(Session session);

    public void rollbackAction(Session session);

    public void rollbackSavepoint(Session session, int index);

    public void setTransactionControl(Session session, int mode);

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(CachedObject object);

    /**
     * remove the transaction info
     */
    public void removeTransactionInfo(CachedObject object);
}
