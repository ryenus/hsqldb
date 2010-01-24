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


package org.hsqldb;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class TransactionManagerMVCC implements TransactionManager {

    Database database;
    boolean  hasPersistence;

    //
    ReentrantReadWriteLock           lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // functional unit - sessions involved in live transactions

    /** live transactions keeping committed transactions from being merged */
    LongDeque  liveTransactionTimestamps = new LongDeque();
    AtomicLong globalChangeTimestamp     = new AtomicLong();

    // functional unit - merged committed transactions
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();

    // functional unit - cached table transactions

    /** Map : rowID -> RowAction */
    public IntKeyHashMapConcurrent rowActionMap =
        new IntKeyHashMapConcurrent(10000);

    // locks
    Session  catalogWriteSession;
    HsqlName catalogName;
    int      transactionCount = 0;

    //
    public TransactionManagerMVCC(Database db) {

        database       = db;
        hasPersistence = database.logger.isLogged();
        catalogName    = database.getCatalogName();
    }

    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }

    public boolean isMVRows() {
        return true;
    }

    public int getTransactionControl() {
        return Database.MVCC;
    }

    public void setTransactionControl(Session session, int mode) {

        writeLock.lock();

        try {

            // statement runs as transaction
            if (liveTransactionTimestamps.size() == 1) {
                switch (mode) {

                    case Database.MVCC :
                        break;

                    case Database.MVLOCKS : {
                        TransactionManagerMV2PL manager =
                            new TransactionManagerMV2PL(database);

                        manager.globalChangeTimestamp.set(
                            globalChangeTimestamp.get());
                        manager.liveTransactionTimestamps.addLast(
                            session.transactionTimestamp);

                        database.txManager = manager;

                        break;
                    }
                    case Database.LOCKS : {
                        TransactionManager2PL manager =
                            new TransactionManager2PL(database);

                        manager.globalChangeTimestamp.set(
                            globalChangeTimestamp.get());

                        database.txManager = manager;

                        break;
                    }
                }

                return;
            }
        } finally {
            writeLock.unlock();
        }

        throw Error.error(ErrorCode.X_25001);
    }

    int redoCount = 0;

    public void completeActions(Session session) {

        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();

        logActions(session, list, limit);
    }

    private void logActions(Session session, Object[] list,
                            int limit) throws RuntimeException {

        for (int i = session.actionIndex; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            if (!hasPersistence || action.table == null
                    || !action.table.isLogged) {
                continue;
            }

            Row row = action.memoryRow;

            if (row == null) {
                PersistentStore store =
                    session.sessionData.getRowStore(action.table);

                row = (Row) store.get(action.getPos(), false);
            }

            Object[] data = row.getData();

            try {
                int actionType = action.getActionType(session.actionTimestamp);

                switch (actionType) {

                    case RowActionBase.ACTION_INSERT :
                        database.logger.writeInsertStatement(
                            session, (Table) action.table, data);
                        break;

                    case RowActionBase.ACTION_DELETE :
                        database.logger.writeDeleteStatement(
                            session, (Table) action.table, data);
                        break;

                    case RowActionBase.ACTION_INSERT_DELETE :

                        // no logging
                        break;
                }
            } catch (HsqlException e) {

                // can put db in special state
            }
        }
    }

    public boolean prepareCommitActions(Session session) {

        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();

        if (session.abortTransaction) {

//            System.out.println("cascade fail " + session + " " + session.actionTimestamp);
            return false;
        }

        writeLock.lock();

        try {
            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];

                if (!rowact.canCommit(session, session.tempSet)) {

//                System.out.println("commit conflicts " + session + " " + session.actionTimestamp);
                    return false;
                }
            }

            session.actionTimestamp = nextChangeTimestamp();

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                action.prepareCommit(session);
            }

            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current = (Session) session.tempSet.get(i);

                current.abortTransaction = true;
            }

            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    public boolean commitTransaction(Session session) {

        if (session.abortTransaction) {
            return false;
        }

        int      limit = session.rowActionList.size();
        Object[] list  = session.rowActionList.getArray();

        writeLock.lock();

        try {
            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];

                if (!rowact.canCommit(session, session.tempSet)) {

//                System.out.println("commit conflicts " + session + " " + session.actionTimestamp);
                    return false;
                }
            }

            endTransaction(session);

            // new actionTimestamp used for commitTimestamp
            session.actionTimestamp = nextChangeTimestamp();

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                action.commit(session);
            }

            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current = (Session) session.tempSet.get(i);

                current.abortTransaction = true;
            }

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                if (action.table != null && action.table.hasLobColumn) {
                    int type = action.getCommitTypeOn(session.actionTimestamp);

                    switch (type) {

                        case RowActionBase.ACTION_INSERT :
                            Row row = action.memoryRow;

                            if (row == null) {
                                PersistentStore store =
                                    session.sessionData.getRowStore(
                                        action.table);

                                row = (Row) store.get(action.getPos(), false);
                            }

                            session.sessionData.addLobUsageCount(
                                action.table, row.getData());
                            break;

                        default :
                    }
                }

                if (action.table != null
                        && action.table.tableType == TableBase.TEXT_TABLE) {
                    PersistentStore store =
                        session.sessionData.getRowStore(action.table);
                    int type = action.getCommitTypeOn(session.actionTimestamp);

                    switch (type) {

                        case RowActionBase.ACTION_DELETE :
                            store.removePersistence(action.getPos());
                            break;

                        case RowActionBase.ACTION_INSERT :
                            Row row = (Row) store.get(action.getPos(), false);

                            store.commitPersistence(row);
                            break;

                        default :
                    }
                }
            }

            // session.actionTimestamp is the committed tx timestamp
            if (getFirstLiveTransactionTimestamp() > session.actionTimestamp) {
                mergeTransaction(session, list, 0, limit,
                                 session.actionTimestamp);
                rowActionMapRemoveTransaction(list, 0, limit, true);
            } else {
                list = session.rowActionList.toArray();

                addToCommittedQueue(session, list);
            }

            try {
                session.logSequences();
                database.logger.writeCommitStatement(session);
            } catch (HsqlException e) {}

            endTransactionTPL(session);

            //
            countDownLatches(session);

            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    private void countDownLatches(Session session) {

        if (!session.waitingSessions.isEmpty()) {
            for (int i = 0; i < session.waitingSessions.size(); i++) {
                Session current = (Session) session.waitingSessions.get(i);

                current.latch.countDown();
            }

            session.waitingSessions.clear();
        }
    }

    public void rollback(Session session) {

        writeLock.lock();

        try {
            session.abortTransaction = false;
            session.actionTimestamp  = nextChangeTimestamp();

            rollbackPartial(session, 0, session.transactionTimestamp);
            endTransaction(session);
            countDownLatches(session);
            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }
    }

    public void rollbackSavepoint(Session session, int index) {

        long timestamp = session.sessionContext.savepointTimestamps.get(index);
        Integer oi = (Integer) session.sessionContext.savepoints.get(index);
        int     start  = oi.intValue();

        while (session.sessionContext.savepoints.size() > index + 1) {
            session.sessionContext.savepoints.remove(
                session.sessionContext.savepoints.size() - 1);
            session.sessionContext.savepointTimestamps.removeLast();
        }

        rollbackPartial(session, start, timestamp);
    }

    public void rollbackAction(Session session) {
        rollbackPartial(session, session.actionIndex, session.actionTimestamp);
    }

    /**
     * rollback the row actions from start index in list and
     * the given timestamp
     */
    void rollbackPartial(Session session, int start, long timestamp) {

        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();

        if (start == limit) {
            return;
        }

        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            if (action != null) {
                action.rollback(session, timestamp);
            } else {
                System.out.println("null action in rollback " + start);
            }
        }

        // rolled back transactions can always be merged as they have never been
        // seen by other sessions
        mergeRolledBackTransaction(session, timestamp, list, start, limit);
        rowActionMapRemoveTransaction(list, start, limit, false);
        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table, Row row) {

        RowAction action;
        boolean   newAction;

        synchronized (row) {
            newAction = row.rowAction == null;
            action    = RowAction.addDeleteAction(session, table, row);
        }

        if (action == null) {
            writeLock.lock();

            try {
                Session current = (Session) session.tempSet.get(0);

                session.tempSet.clear();

                if (session.isolationLevel == SessionInterface
                        .TX_REPEATABLE_READ || session
                        .isolationLevel == SessionInterface.TX_SERIALIZABLE) {
                    session.abortTransaction = true;

                    throw Error.error(ErrorCode.X_40501);
                }

                rollbackAction(session);

                session.redoAction = true;

                if (row.rowAction.isDeleted()) {
                    redoCount++;

                    throw Error.error(ErrorCode.X_40501);
                }

                boolean canWait = checkDeadlock(session, session.tempSet);

                if (canWait) {
                    // assert current.isInMidTransaction();

                    session.latch.countUp();
                    current.waitingSessions.add(session);
                } else {
                    session.redoAction = false;
                }

                redoCount++;

                throw Error.error(ErrorCode.X_40501);
            } finally {
                writeLock.unlock();
            }
        }

        session.rowActionList.add(action);

        if (newAction) {
            rowActionMap.put(action.getPos(), action);
        }

        return action;
    }

    public void addInsertAction(Session session, Table table, Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
        }

        session.rowActionList.add(action);

        if (!row.isMemory()) {
            rowActionMap.put(action.getPos(), action);
        }
    }

// functional unit - accessibility of rows
    public boolean canRead(Session session, Row row, int mode) {

        RowAction action = row.rowAction;

        if (mode == TransactionManager.ACTION_READ) {
            if (action == null) {
                return true;
            }

            return action.canRead(session);
        }

        if (mode == ACTION_REF) {
            boolean result;

            if (action == null) {
                result = true;

                synchronized (row) {
                    action = RowAction.addRefAction(session, row);
                }
            } else {
                result = action.canRead(session, mode);
            }

            if (result) {
                session.rowActionList.add(action);
            }

            return result;
        } else {
            if (action == null) {
                return true;
            }

            return action.canRead(session, mode);
        }
    }

    public boolean canRead(Session session, int id, int mode) {

        RowAction action = (RowAction) rowActionMap.get(id);

        if (action == null) {
            return true;
        }

        if (mode == TransactionManager.ACTION_READ) {
            return action.canRead(session);
        }

        return action.canRead(session, mode);
    }

    /**
     *  todo - can remove a row that has previously inserted by the same transaction
     */
    void rowActionMapRemoveTransaction(Object[] list, int start, int limit,
                                       boolean commit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (!rowact.isMemory) {
                synchronized (rowact) {
                    if (rowact.type == RowActionBase.ACTION_NONE
                            || rowact.type
                               == RowActionBase.ACTION_DELETE_FINAL) {
                        int pos = rowact.getPos();

                        rowActionMap.remove(pos);
                    }
                }
            }
        }

        deleteRows(list, start, limit, commit);
    }

    void deleteRows(Object[] list, int start, int limit, boolean commit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (rowact.type == RowActionBase.ACTION_DELETE_FINAL
                    && !rowact.deleteComplete) {
                try {
                    rowact.deleteComplete = true;

                    PersistentStore store =
                        rowact.session.sessionData.getRowStore(rowact.table);
                    Row row = rowact.memoryRow;

                    if (row == null) {
                        row = (Row) store.get(rowact.getPos(), false);
                    }

                    if (commit && rowact.table.hasLobColumn) {
                        Object[] data = row.getData();

                        rowact.session.sessionData.removeLobUsageCount(
                            rowact.table, data);
                    }

                    store.delete(row);
                    store.remove(row.getPos());
                } catch (HsqlException e) {

//                    throw unexpectedException(e.getMessage());
                }
            }
        }
    }

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(CachedObject object) {

        Row row = (Row) object;

        if (row.rowAction != null) {
            return;
        }

        RowAction rowact = (RowAction) rowActionMap.get(row.position);

        row.rowAction = rowact;
    }

    /**
     * merge a given list of transaction rollback action with given timestamp
     */
    void mergeRolledBackTransaction(Session session, long timestamp,
                                    Object[] list, int start, int limit) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];
            Row       row    = rowact.memoryRow;

            if (row == null) {
                if (rowact.type == RowAction.ACTION_NONE) {
                    continue;
                }

                PersistentStore store =
                    rowact.session.sessionData.getRowStore(rowact.table);

                row = (Row) store.get(rowact.getPos(), false);
            }

            if (row == null) {

                // only if transaction has been merged
                // shouldn't normally happen
                continue;
            }

            synchronized (row) {
                rowact.mergeRollback(session, timestamp, row);
            }
        }
    }

    /**
     * add a list of actions to the end of queue
     */
    void addToCommittedQueue(Session session, Object[] list) {

        synchronized (committedTransactionTimestamps) {

            // add the txList according to commit timestamp
            committedTransactions.addLast(list);

            // get session commit timestamp
            committedTransactionTimestamps.addLast(session.actionTimestamp);
/* debug 190
            if (committedTransactions.size() > 64) {
                System.out.println("******* excessive transaction queue");
            }
// debug 190 */
        }
    }

    /**
     * expire all committed transactions that are no longer in scope
     */
    void mergeExpiredTransactions(Session session) {

        long timestamp = getFirstLiveTransactionTimestamp();

        while (true) {
            long     commitTimestamp = 0;
            Object[] actions         = null;

            synchronized (committedTransactionTimestamps) {
                if (committedTransactionTimestamps.isEmpty()) {
                    break;
                }

                commitTimestamp = committedTransactionTimestamps.getFirst();

                if (commitTimestamp < timestamp) {
                    committedTransactionTimestamps.removeFirst();

                    actions = (Object[]) committedTransactions.removeFirst();
                } else {
                    break;
                }
            }

            mergeTransaction(session, actions, 0, actions.length,
                             commitTimestamp);
            rowActionMapRemoveTransaction(actions, 0, actions.length, true);
        }
    }

    /**
     * merge a transaction committed at a given timestamp.
     */
    void mergeTransaction(Session session, Object[] list, int start,
                          int limit, long timestamp) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            if (rowact == null || rowact.type == RowActionBase.ACTION_NONE
                    || rowact.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = rowact.memoryRow;

            if (row == null) {
                PersistentStore store =
                    rowact.session.sessionData.getRowStore(rowact.table);

                row = (Row) store.get(rowact.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            synchronized (row) {
                rowact.mergeToTimestamp(row, timestamp);
            }
        }
    }

    /**
     * gets the next timestamp for an action
     */
    long nextChangeTimestamp() {
        return globalChangeTimestamp.incrementAndGet();
    }

    public void beginTransaction(Session session) {

        writeLock.lock();

        try {
            session.actionTimestamp      = nextChangeTimestamp();
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;

            liveTransactionTimestamps.addLast(session.transactionTimestamp);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs) {

        if (session.isTransaction) {
            return;
        }

        if (catalogWriteSession == null && !cs.isCatalogChange()) {
            return;
        }

        writeLock.lock();

        try {
            setWaitedSessionsTPL(session, cs);

            if (session.tempSet.isEmpty()) {
                lockTablesTPL(session, cs);

                // we dont set other sessions that would now be waiting for this one too
            } else {
                setWaitingSessionTPL(session);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginActionResume(Session session) {

        writeLock.lock();

        try {
            session.actionTimestamp = nextChangeTimestamp();

            if (!session.isTransaction) {
                session.transactionTimestamp = session.actionTimestamp;
                session.isTransaction        = true;

                liveTransactionTimestamps.addLast(session.actionTimestamp);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * remove session from queue when a transaction ends
     * and expire any committed transactions
     * that are no longer required. remove transactions ended before the first
     * timestamp in liveTransactionsSession queue
     */
    void endTransaction(Session session) {

        long timestamp = session.transactionTimestamp;

        session.isTransaction = false;

        int index = liveTransactionTimestamps.indexOf(timestamp);

        liveTransactionTimestamps.remove(index);
        mergeExpiredTransactions(session);
    }

    long getFirstLiveTransactionTimestamp() {

        if (liveTransactionTimestamps.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return liveTransactionTimestamps.get(0);
    }

    void getTransactionSessions(HashSet set) {

        Session[] sessions = database.sessionManager.getAllSessions();

        for (int i = 0; i < sessions.length; i++) {
            long timestamp = sessions[i].getTransactionTimestamp();

            if (liveTransactionTimestamps.contains(timestamp)) {
                set.add(sessions[i]);
            }
        }
    }

// functional unit - list actions and translate id's

    /**
     * Return an array of all row actions sorted by System Change No.
     */
    RowAction[] getRowActionList() {

        writeLock.lock();

        try {
            Session[]   sessions = database.sessionManager.getAllSessions();
            int[]       tIndex   = new int[sessions.length];
            RowAction[] rowActions;
            int         rowActionCount = 0;

            {
                int actioncount = 0;

                for (int i = 0; i < sessions.length; i++) {
                    actioncount += sessions[i].getTransactionSize();
                }

                rowActions = new RowAction[actioncount];
            }

            while (true) {
                boolean found        = false;
                long    minChangeNo  = Long.MAX_VALUE;
                int     sessionIndex = 0;

                // find the lowest available SCN across all sessions
                for (int i = 0; i < sessions.length; i++) {
                    int tSize = sessions[i].getTransactionSize();

                    if (tIndex[i] < tSize) {
                        RowAction current =
                            (RowAction) sessions[i].rowActionList.get(
                                tIndex[i]);

                        if (current.actionTimestamp < minChangeNo) {
                            minChangeNo  = current.actionTimestamp;
                            sessionIndex = i;
                        }

                        found = true;
                    }
                }

                if (!found) {
                    break;
                }

                HsqlArrayList currentList =
                    sessions[sessionIndex].rowActionList;

                for (; tIndex[sessionIndex] < currentList.size(); ) {
                    RowAction current =
                        (RowAction) currentList.get(tIndex[sessionIndex]);

                    // if the next change no is in this session, continue adding
                    if (current.actionTimestamp == minChangeNo + 1) {
                        minChangeNo++;
                    }

                    if (current.actionTimestamp == minChangeNo) {
                        rowActions[rowActionCount++] = current;

                        tIndex[sessionIndex]++;
                    } else {
                        break;
                    }
                }
            }

            return rowActions;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Return a lookup of all row ids for cached tables in transactions.
     * For auto-defrag, as currently there will be no RowAction entries
     * at the time of defrag.
     */
    public DoubleIntIndex getTransactionIDList() {

        writeLock.lock();

        try {
            int            size   = rowActionMap.size();
            DoubleIntIndex lookup = new DoubleIntIndex(size, false);

            lookup.setKeysSearchTarget();

            Iterator it = this.rowActionMap.keySet().iterator();

            for (; it.hasNext(); ) {
                lookup.addUnique(it.nextInt(), 0);
            }

            return lookup;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Convert row ID's for cached table rows in transactions
     */
    public void convertTransactionIDs(DoubleIntIndex lookup) {

        writeLock.lock();

        try {
            RowAction[] list = new RowAction[rowActionMap.size()];
            Iterator    it   = this.rowActionMap.values().iterator();

            for (int i = 0; it.hasNext(); i++) {
                list[i] = (RowAction) it.next();
            }

            rowActionMap.clear();

            for (int i = 0; i < list.length; i++) {
                int pos = lookup.lookupFirstEqual(list[i].getPos());

                list[i].setPos(pos);
                rowActionMap.put(pos, list[i]);
            }
        } finally {
            writeLock.unlock();
        }
    }

    void endTransactionTPL(Session session) {

        boolean isWriteSession = false;

        if (catalogWriteSession == session) {
            catalogWriteSession = null;
            isWriteSession      = true;
        }

        final int waitingCount = session.waitingSessions.size();

        if (waitingCount == 0) {
            return;
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                setWaitedSessionsTPL(current,
                                     current.sessionContext.currentStatement);

                if (current.tempSet.isEmpty()) {
                    lockTablesTPL(current,
                                  current.sessionContext.currentStatement);

                    current.tempUnlocked = true;
                }
            }
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (!current.tempUnlocked) {
                setWaitedSessionsTPL(current,
                                     current.sessionContext.currentStatement);
            }
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            setWaitingSessionTPL(current);
        }

        session.tempSet.clear();
        session.waitingSessions.clear();
    }

    boolean setWaitedSessionsTPL(Session session, Statement cs) {

        if (cs == null || session.abortTransaction) {
            return true;
        }

        session.tempSet.clear();

        boolean needsReadLock = cs.getTableNamesForRead().length > 0
                                || cs.getTableNamesForWrite().length > 0;
        boolean needsWriteLock = cs.isCatalogChange();

        if (needsReadLock || needsWriteLock) {
            if (catalogWriteSession != session
                    && catalogWriteSession != null) {
                session.tempSet.add(catalogWriteSession);
            }
        }

        if (needsWriteLock) {
            getTransactionSessions(session.tempSet);
            session.tempSet.remove(session);
        }

        return true;
    }

    boolean checkDeadlock(Session session, OrderedHashSet newWaits) {

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (newWaits.contains(current)) {
                return false;
            }

            if (!checkDeadlock(current, newWaits)) {
                return false;
            }
        }

        return true;
    }

    void setWaitingSessionTPL(Session session) {

        int count = session.tempSet.size();

        for (int i = 0; i < count; i++) {
            Session current = (Session) session.tempSet.get(i);

            current.waitingSessions.add(session);
        }

        session.tempSet.clear();
        session.latch.setCount(count);
    }

    void lockTablesTPL(Session session, Statement cs) {

        if (cs == null || session.abortTransaction) {
            return;
        }

        if (cs.isCatalogChange()) {
            catalogWriteSession = session;
        }
    }
}
