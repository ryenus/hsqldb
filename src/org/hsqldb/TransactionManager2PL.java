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
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.store.ValuePool;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class TransactionManager2PL implements TransactionManagerInterface {

    Database database;

    //
    ReentrantReadWriteLock           lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // functional unit - sessions involved in live transactions

    /** live transactions keeping committed transactions from being merged */
    AtomicLong globalChangeTimestamp = new AtomicLong();

    // functional unit - cached table transactions
    //
    //
    HashMap           tableWriteLocks = new HashMap();
    MultiValueHashMap tableReadLocks  = new MultiValueHashMap();

    public TransactionManager2PL(Database db) {
        database = db;
    }

    public boolean isMVRows() {
        return false;
    }

    public int getTransactionControl() {
        return Database.LOCKS;
    }

    public void setTransactionControl(Session session, int mode) {

        writeLock.lock();

        try {
            switch (mode) {

                case Database.MVCC :
                case Database.MVLOCKS :
                    TransactionManager manager =
                        new TransactionManager(database, mode);

                    manager.globalChangeTimestamp.set(
                        globalChangeTimestamp.get());
                    manager.liveTransactionTimestamps.addLast(
                        session.transactionTimestamp);

                    database.txManager = manager;
                    break;

                case Database.LOCKS :
                    break;
            }

            return;
        } finally {
            writeLock.unlock();
        }
    }

    public void completeActions(Session session) {

        Object[] list        = session.rowActionList.getArray();
        int      limit       = session.rowActionList.size();
        boolean  canComplete = true;

        for (int i = session.actionIndex; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            rowact.complete(session, session.tempSet);
        }
    }

    public boolean prepareCommitActions(Session session) {

        session.actionTimestamp = nextChangeTimestamp();

        return true;
    }

    public boolean commitTransaction(Session session) {

        if (session.abortTransaction) {
            return false;
        }

        int      limit = session.rowActionList.size();
        Object[] list  = limit == 0 ? ValuePool.emptyObjectArray
                                    : session.rowActionList.getArray();

        try {
            writeLock.lock();
            endTransaction(session);

            if (limit == 0) {
                endTransactionTPL(session);

                try {
                    session.logSequences();
                } catch (HsqlException e) {}

                return true;
            }

            // new actionTimestamp used for commitTimestamp
            // already set in prepareCommitActions();
            session.actionTimestamp = nextChangeTimestamp();

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                action.commit(session);
            }

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];

                if (action.type == RowActionBase.ACTION_NONE) {
                    continue;
                }

                int type = action.getCommitType(session.actionTimestamp);
                PersistentStore store =
                    session.sessionData.getRowStore(action.table);
                Row row = action.memoryRow;

                if (row == null) {
                    row = (Row) store.get(action.getPos(), false);
                }

                if (action.table.hasLobColumn) {
                    switch (type) {

                        case RowActionBase.ACTION_INSERT :
                            action.table.addLobUsageCount(session,
                                                          row.getData());
                            break;

                        default :
                    }
                }

                if (action.table.tableType == TableBase.TEXT_TABLE) {
                    switch (type) {

                        case RowActionBase.ACTION_DELETE :
                            store.removePersistence(action.getPos());
                            break;

                        case RowActionBase.ACTION_INSERT :
                            store.commitPersistence(row);
                            break;

                        default :
                    }
                }

                Object[] data = row.getData();

                try {
                    if (type == RowActionBase.ACTION_INSERT) {
                        database.logger.writeInsertStatement(session,
                                                             action.table,
                                                             data);
                    } else if (type == RowActionBase.ACTION_DELETE) {
                        database.logger.writeDeleteStatement(session,
                                                             action.table,
                                                             data);
                        store.remove(action.getPos());
                    } else if (type == RowActionBase.ACTION_NONE) {
                        store.remove(action.getPos());
                    }

                    action.setAsNoOp(row);
                } catch (HsqlException e) {}
            }

            try {
                session.logSequences();
                database.logger.writeCommitStatement(session);
            } catch (HsqlException e) {}

            endTransactionTPL(session);

            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
    }

    public void rollback(Session session) {

        session.abortTransaction = false;
        session.actionTimestamp  = nextChangeTimestamp();

        rollbackPartial(session, 0, session.transactionTimestamp);
        endTransaction(session);

        try {
            writeLock.lock();
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

        for (int i = limit - 1; i >= start; i--) {
            RowAction action = (RowAction) list[i];

            if (action == null || action.type == RowActionBase.ACTION_NONE
                    || action.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = action.memoryRow;
            PersistentStore store =
                session.sessionData.getRowStore(action.table);

            if (row == null) {
                row = (Row) store.get(action.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            action.rollback(session, timestamp);

            int type = action.getCommitType(session.actionTimestamp);

            action.mergeRollback(row);

            if (type == RowActionBase.ACTION_DELETE) {
                store.indexRow(session, row);
            } else if (type == RowActionBase.ACTION_INSERT) {
                store.delete(row);
                store.remove(action.getPos());
            }
        }

        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table, Row row) {

        RowAction action;

        synchronized (row) {
            action = RowAction.addAction(session, RowActionBase.ACTION_DELETE,
                                         table, row);
        }

        session.rowActionList.add(action);

        PersistentStore store = session.sessionData.getRowStore(table);

        store.delete(row);

        return action;
    }

    public void addInsertAction(Session session, Table table, Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
        }

        session.rowActionList.add(action);
    }

// functional unit - row comparison

    /**
     * Used for inserting rows into unique indexes
     */

    /** @todo test skips some equal rows because it relies on getPos() */
    int compareRowForInsert(Row newRow, Row existingRow) {

        // only allow new inserts over rows deleted by the same session
        if (newRow.rowAction != null
                && !canRead(newRow.rowAction.session, existingRow)) {
            return newRow.getPos() - existingRow.getPos();
        }

        return 0;
    }

// functional unit - accessibility of rows
    public boolean canRead(Session session, Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            return true;
        }

        return action.canRead(session);
    }

    public boolean isDeleted(Session session, Row row) {

        RowAction action = row.rowAction;

        if (action == null) {
            return false;
        }

        return !action.canRead(session);
    }

    public boolean canRead(Session session, int id) {
        return true;
    }

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(CachedObject object) {}

    /**
     * gets the next timestamp for an action
     */
    long nextChangeTimestamp() {
        return globalChangeTimestamp.incrementAndGet();
    }

    public void beginTransaction(Session session) {

        session.actionTimestamp      = nextChangeTimestamp();
        session.transactionTimestamp = session.actionTimestamp;
        session.isTransaction        = true;
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs) {

        session.actionTimestamp = nextChangeTimestamp();

        if (!session.isTransaction) {
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;
        }

        if (session.hasLocks()) {
            return;
        }

        try {
            writeLock.lock();

            boolean canProceed = beginActionTPL(session, cs);

            if (!canProceed) {
                session.abortTransaction = true;
            }
        } finally {
            writeLock.unlock();
        }
    }

    void endTransactionTPL(Session session) {

        int unlockedCount = 0;

        unlockTablesTPL(session);

        if (session.waitingSessions.isEmpty()) {
            return;
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                boolean canProceed = setWaitedSessionsTPL(current,
                    current.currentStatement);

                if (!canProceed) {
                    current.abortTransaction = true;
                }

                if (current.tempSet.isEmpty()) {
                    lockTablesTPL(current, current.currentStatement);

                    current.tempUnlocked = true;

                    unlockedCount++;
                }
            }
        }

        if (unlockedCount > 0) {
            for (int i = 0; i < session.waitingSessions.size(); i++) {
                Session current = (Session) session.waitingSessions.get(i);

                if (!current.tempUnlocked) {
                    boolean canProceed = setWaitedSessionsTPL(current,
                        current.currentStatement);

                    // this can introduce additional waits for the sessions
                    if (!canProceed) {
                        current.abortTransaction = true;
                    }
                }
            }
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            setWaitingSessionTPL(current);
        }

        session.tempSet.clear();
        session.waitingSessions.clear();
    }

    boolean beginActionTPL(Session session, Statement cs) {

        boolean canProceed = setWaitedSessionsTPL(session, cs);

        if (canProceed) {
            if (session.tempSet.isEmpty()) {
                lockTablesTPL(session, cs);

                // we dont set other sessions that would now be waiting for this one too
            } else {
                setWaitingSessionTPL(session);
            }

            return true;
        }

        return false;
    }

    boolean setWaitedSessionsTPL(Session session, Statement cs) {

        session.tempSet.clear();

        if (cs == null || session.abortTransaction) {
            return true;
        }

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = (Session) tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }

            Iterator it = tableReadLocks.get(name);

            while (it.hasNext()) {
                holder = (Session) it.next();

                if (holder != session) {
                    session.tempSet.add(holder);
                }
            }
        }

        nameList = cs.getTableNamesForRead();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = (Session) tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }
        }

        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (session.tempSet.contains(current)) {
                session.tempSet.clear();

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

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            tableWriteLocks.put(name, session);
        }

        nameList = cs.getTableNamesForRead();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            tableReadLocks.put(name, session);
        }
    }

    void unlockTablesTPL(Session session) {

        Iterator it = tableWriteLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.setValue(null);
            }
        }

        it = tableReadLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.remove();
            }
        }
    }

    /**
     * remove session from queue when a transaction ends
     * and expire any committed transactions
     * that are no longer required. remove transactions ended before the first
     * timestamp in liveTransactionsSession queue
     */
    void endTransaction(Session session) {
        session.isTransaction = false;
    }

// functional unit - list actions and translate id's

    /**
     * Return an array of all row actions sorted by System Change No.
     */
    RowAction[] getRowActionList() {

        try {
            writeLock.lock();

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
     */
    public DoubleIntIndex getTransactionIDList() {

        writeLock.lock();

        try {
            Session[]      sessions = database.sessionManager.getAllSessions();
            DoubleIntIndex lookup   = new DoubleIntIndex(10, false);

            lookup.setKeysSearchTarget();

            for (int i = 0; i < sessions.length; i++) {
                HsqlArrayList tlist = sessions[i].rowActionList;

                for (int j = 0, size = tlist.size(); j < size; j++) {
                    RowAction rowact = (RowAction) tlist.get(j);

                    if (rowact.table.getTableType()
                            == TableBase.CACHED_TABLE) {
                        lookup.addUnique(rowact.getPos(), 0);
                    }
                }
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
            Session[] sessions = database.sessionManager.getAllSessions();

            for (int i = 0; i < sessions.length; i++) {
                HsqlArrayList tlist = sessions[i].rowActionList;

                for (int j = 0, size = tlist.size(); j < size; j++) {
                    RowAction rowact = (RowAction) tlist.get(j);

                    if (rowact.memoryRow == null) {
                        int pos = lookup.lookupFirstEqual(rowact.getPos());

                        rowact.setPos(pos);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}
