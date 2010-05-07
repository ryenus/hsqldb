/* Copyright (c) 2001-2010, The HSQL Development Group
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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.persist.CachedObject;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class TransactionManagerMVCC extends TransactionManagerCommon
implements TransactionManager {

    // functional unit - merged committed transactions
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();

    // locks
    boolean isLockedMode;
    Session catalogWriteSession;

    //
    int redoCount = 0;

    //
    public TransactionManagerMVCC(Database db) {

        database       = db;
        hasPersistence = database.logger.isLogged();
        lobSession     = database.sessionManager.getSysLobSession();
        rowActionMap   = new IntKeyHashMapConcurrent(10000);
        txModel        = MVCC;
    }

    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }

    public boolean isMVRows() {
        return true;
    }

    public int getTransactionControl() {
        return MVCC;
    }

    public void setTransactionControl(Session session, int mode) {

        writeLock.lock();

        try {

            // statement runs as transaction
            if (liveTransactionTimestamps.size() == 1) {
                switch (mode) {

                    case MVCC :
                        break;

                    case MVLOCKS : {
                        TransactionManagerMV2PL manager =
                            new TransactionManagerMV2PL(database);

                        manager.globalChangeTimestamp.set(
                            globalChangeTimestamp.get());
                        manager.liveTransactionTimestamps.addLast(
                            session.transactionTimestamp);

                        database.txManager = manager;

                        break;
                    }
                    case LOCKS : {
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

    public void completeActions(Session session) {}

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

            persistCommit(session, list, limit);

            // session.actionTimestamp is the committed tx timestamp
            if (getFirstLiveTransactionTimestamp() > session.actionTimestamp) {
                mergeTransaction(session, list, 0, limit,
                                 session.actionTimestamp);
                finaliseRows(session, list, 0, limit, true);
            } else {
                list = session.rowActionList.toArray();

                addToCommittedQueue(session, list);
            }

            endTransactionTPL(session);

            //
            countDownLatches(session);
        } finally {
            writeLock.unlock();
        }

        session.tempSet.clear();

        if (session != lobSession && lobSession.rowActionList.size() > 0) {
            lobSession.isTransaction = true;
            lobSession.actionIndex   = lobSession.rowActionList.size();

            lobSession.commit(false);
        }

        return true;
    }

    public void rollback(Session session) {

        writeLock.lock();

        try {
            session.abortTransaction = false;
            session.actionTimestamp  = nextChangeTimestamp();

            rollbackPartial(session, 0, session.transactionTimestamp);
            endTransaction(session);
            endTransactionTPL(session);
            countDownLatches(session);
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
        writeLock.lock();

        try {
            mergeRolledBackTransaction(session, timestamp, list, start, limit);
            finaliseRows(session, list, start, limit, false);
        } finally {
            writeLock.unlock();
        }

        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table, Row row,
                                     int[] colMap) {

        RowAction action = addDeleteActionToRow(session, table, row, colMap);

        if (action == null) {
            writeLock.lock();

            try {
                rollbackAction(session);

                if (session.isolationLevel == SessionInterface
                        .TX_REPEATABLE_READ || session
                        .isolationLevel == SessionInterface.TX_SERIALIZABLE) {
                    session.tempSet.clear();

                    session.abortTransaction = true;

                    throw Error.error(ErrorCode.X_40501);
                }

                // can redo when conflicting action is already committed
                if (row.rowAction != null && row.rowAction.isDeleted()) {
                    session.tempSet.clear();

                    session.redoAction = true;

                    redoCount++;

                    throw Error.error(ErrorCode.X_40501);
                }

                boolean canWait = checkDeadlock(session, session.tempSet);

                if (canWait) {
                    Session current = (Session) session.tempSet.get(0);

                    session.redoAction = true;

                    current.waitingSessions.add(session);
                    session.waitedSessions.add(current);
                    session.latch.countUp();
                } else {
                    session.redoAction       = false;
                    session.abortTransaction = true;
                }

                session.tempSet.clear();

                redoCount++;

                throw Error.error(ErrorCode.X_40501);
            } finally {
                writeLock.unlock();
            }
        }

        session.rowActionList.add(action);

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
    public boolean canRead(Session session, Row row, int mode, int[] colMap) {

        RowAction action = row.rowAction;

        if (mode == TransactionManager.ACTION_READ) {
            if (action == null) {
                return true;
            }

            return action.canRead(session, TransactionManager.ACTION_READ);
        }

        if (mode == ACTION_REF) {
            boolean result;

            if (action == null) {
                result = true;
            } else {
                result = action.canRead(session,
                                        TransactionManager.ACTION_READ);
            }

            return result;
/*
            if (result) {
                synchronized (row) {
                    if (row.isMemory()) {
                        result = RowAction.addRefAction(session, row, colMap);
                    } else {
                        ReentrantReadWriteLock.WriteLock mapLock =
                            rowActionMap.getWriteLock();

                        mapLock.lock();

                        try {
                            action = row.rowAction;

                            if (action == null) {
                                action =
                                    (RowAction) rowActionMap.get(row.getPos());
                                row.rowAction = action;
                            }

                            result = RowAction.addRefAction(session, row,
                                                            colMap);

                            if (result && action == null) {
                                rowActionMap.put(row.getPos(), action);
                            }
                        } finally {
                            mapLock.unlock();
                        }
                    }
                }

                if (result) {
                    session.rowActionList.add(row.rowAction);
                } else {
                    if (!session.tempSet.isEmpty()) {
                        Session current = (Session) session.tempSet.get(0);

                        session.redoAction = true;

                        session.latch.countUp();
                        current.waitingSessions.add(session);
                        session.waitedSessions.add(current);
                        session.tempSet.clear();

                        throw Error.error(ErrorCode.X_40501);
                    }
                }

                return true;
            }

            return false;
*/
        }

        if (action == null) {
            return true;
        }

        return action.canRead(session, mode);
    }

    public boolean canRead(Session session, int id, int mode) {

        RowAction action = (RowAction) rowActionMap.get(id);

        if (action == null) {
            return true;
        }

        return action.canRead(session, mode);
    }

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(CachedObject object) {

        Row       row    = (Row) object;
        RowAction rowact = (RowAction) rowActionMap.get(row.position);

        row.rowAction = rowact;
    }

    /**
     * remove the transaction info
     */
    public void removeTransactionInfo(CachedObject object) {
        rowActionMap.remove(object.getPos());
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
            long     commitTimestamp;
            Object[] actions;

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
            finaliseRows(session, actions, 0, actions.length, true);
        }
    }

    public void beginTransaction(Session session) {

        writeLock.lock();

        try {
            session.actionTimestamp      = nextChangeTimestamp();
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;

            liveTransactionTimestamps.addLast(session.transactionTimestamp);
            transactionCount++;
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

        if (cs == null) {
            return;
        }

        writeLock.lock();

        try {
            session.isPreTransaction = true;

            if (!isLockedMode && !cs.isCatalogChange()) {
                return;
            }

            beingActionTPL(session, cs);
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
                transactionCount++;
            }

            session.isPreTransaction = false;
        } finally {
            writeLock.unlock();
        }
    }

    RowAction addDeleteActionToRow(Session session, Table table, Row row,
                                   int[] colMap) {

        RowAction action = null;

        synchronized (row) {
            if (row.isMemory()) {
                action = RowAction.addDeleteAction(session, table, row,
                                                   colMap);
            } else {
                ReentrantReadWriteLock.WriteLock mapLock =
                    rowActionMap.getWriteLock();

                mapLock.lock();

                try {

                    /* using rowActionMap as source */
                    action = (RowAction) rowActionMap.get(row.getPos());

                    if (action == null) {
                        if (row.rowAction != null) {

                            // test code
                            action = row.rowAction;
                        }

                        action = RowAction.addDeleteAction(session, table,
                                                           row, colMap);

                        if (action != null) {
                            rowActionMap.put(row.getPos(), action);
                        }
                    } else {
                        if (row.rowAction != action) {

                            // test code
                            action = row.rowAction;
                        }

                        row.rowAction = action;
                        action = RowAction.addDeleteAction(session, table,
                                                           row, colMap);
                    }
/*

                    action = row.rowAction;

                    if (action == null) {
                        action = (RowAction) rowActionMap.get(row.getPos());
                    }

                    if (action == null) {
                        action = RowAction.addDeleteAction(session, table,
                                                           row, colMap);

                        if (action != null) {
                            rowActionMap.put(row.getPos(), action);

                            row.rowAction = action;
                        }
                    } else {

                        // possibly from rowActionMap
                        row.rowAction = action;
                        action = action.addDeleteAction(session, colMap);
                    }
*/
                } finally {
                    mapLock.unlock();
                }
            }
        }

        return action;
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

        if (index >= 0) {
            transactionCount--;
            liveTransactionTimestamps.remove(index);
            mergeExpiredTransactions(session);
        }

    }

// functional unit - list actions and translate id's

    /**
     * Return a lookup of all row ids for cached tables in transactions.
     * For auto-defrag, as currently there will be no RowAction entries
     * at the time of defrag.
     */
    public DoubleIntIndex getTransactionIDList() {
        return super.getTransactionIDList();
    }

    /**
     * Convert row ID's for cached table rows in transactions
     */
    public void convertTransactionIDs(DoubleIntIndex lookup) {
        super.convertTransactionIDs(lookup);
    }

    private void countDownLatches(Session session) {

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            current.waitedSessions.remove(session);
            current.latch.countDown();
        }

        session.waitingSessions.clear();
    }

    void getTransactionSessions(HashSet set) {

        Session[] sessions = database.sessionManager.getAllSessions();

        for (int i = 0; i < sessions.length; i++) {
            long timestamp = sessions[i].getTransactionTimestamp();

            if (liveTransactionTimestamps.contains(timestamp)) {
                set.add(sessions[i]);
            } else if (sessions[i].isPreTransaction) {
                set.add(sessions[i]);
            }
        }
    }

    void endTransactionTPL(Session session) {

        if (catalogWriteSession != session) {
            return;
        }

        catalogWriteSession = null;
        isLockedMode        = false;
    }

    boolean beingActionTPL(Session session, Statement cs) {

        if (cs == null) {
            return true;
        }

        if (session.abortTransaction) {
            return false;
        }

        session.tempSet.clear();

        if (cs.isCatalogChange()) {
            if (catalogWriteSession == null) {
                getTransactionSessions(session.tempSet);
                session.tempSet.remove(session);

                if (session.tempSet.isEmpty()) {
                    catalogWriteSession = session;
                    isLockedMode        = true;
                } else {
                    catalogWriteSession = session;
                    isLockedMode        = true;

                    setWaitingSessionTPL(session);
                }

                return true;
            } else {
                catalogWriteSession.waitingSessions.add(session);
                session.latch.countUp();

                return true;
            }
        }

        if (!isLockedMode) {
            return true;
        }

        boolean needsLock = cs.getTableNamesForRead().length > 0
                            || cs.getTableNamesForWrite().length > 0;

        if (!needsLock) {
            return true;
        }

        if (cs.getTableNamesForWrite().length > 0) {
            if (cs.getTableNamesForWrite()[0].schema
                    == SqlInvariants.LOBS_SCHEMA_HSQLNAME) {
                return true;
            }
        } else if (cs.getTableNamesForRead().length > 0) {
            if (cs.getTableNamesForRead()[0].schema
                    == SqlInvariants.LOBS_SCHEMA_HSQLNAME) {
                return true;
            }
        }

        catalogWriteSession.waitingSessions.add(session);
        session.latch.countUp();

        return true;
    }
}
