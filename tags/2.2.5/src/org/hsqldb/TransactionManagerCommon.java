/* Copyright (c) 2001-2011, The HSQL Development Group
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Shared code for TransactionManager classes
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.0
 */
class TransactionManagerCommon {

    Database   database;
    Session    lobSession;
    int        txModel;
    HsqlName[] catalogNameList;

    //
    ReentrantReadWriteLock           lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // functional unit - sessions involved in live transactions

    /** live transactions keeping committed transactions from being merged */
    LongDeque liveTransactionTimestamps = new LongDeque();

    /** live transactions keeping committed transactions from being merged */
    AtomicLong globalChangeTimestamp = new AtomicLong(1);
    int        transactionCount      = 0;

    //
    HashMap           tableWriteLocks = new HashMap();
    MultiValueHashMap tableReadLocks  = new MultiValueHashMap();

    // functional unit - cached table transactions

    /** Map : rowID -> RowAction */
    public IntKeyHashMapConcurrent rowActionMap;

    void setTransactionControl(Session session, int mode) {

        TransactionManagerCommon manager = null;

        if (mode == txModel) {
            return;
        }

        // statement runs as transaction
        writeLock.lock();

        switch (txModel) {

            case TransactionManager.MVCC :
            case TransactionManager.MVLOCKS :
                if (liveTransactionTimestamps.size() != 1) {
                    throw Error.error(ErrorCode.X_25001);
                }
        }

        try {
            switch (mode) {

                case TransactionManager.MVCC : {
                    manager = new TransactionManagerMVCC(database);

                    manager.liveTransactionTimestamps.addLast(
                        session.transactionTimestamp);

                    break;
                }
                case TransactionManager.MVLOCKS : {
                    manager = new TransactionManagerMV2PL(database);

                    manager.liveTransactionTimestamps.addLast(
                        session.transactionTimestamp);

                    break;
                }
                case TransactionManager.LOCKS : {
                    manager = new TransactionManager2PL(database);

                    break;
                }
            }

            manager.globalChangeTimestamp.set(globalChangeTimestamp.get());

            manager.transactionCount = transactionCount;
            database.txManager       = (TransactionManager) manager;

            return;
        } finally {
            writeLock.unlock();
        }
    }

    void persistCommit(Session session, Object[] list, int limit) {

        for (int i = 0; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            if (action.type == RowActionBase.ACTION_NONE) {
                continue;
            }

            int type = action.getCommitTypeOn(session.actionTimestamp);
            Row row  = action.memoryRow;

            if (row == null) {
                row = (Row) action.store.get(action.getPos(), false);
            }

            if (action.table.hasLobColumn) {
                switch (type) {

                    case RowActionBase.ACTION_INSERT :
                        session.sessionData.adjustLobUsageCount(action.table,
                                row.getData(), 1);
                        break;

                    case RowActionBase.ACTION_DELETE :
                        session.sessionData.adjustLobUsageCount(action.table,
                                row.getData(), -1);
                        break;

                    case RowActionBase.ACTION_INSERT_DELETE :
                    default :
                }

                int newLimit = session.rowActionList.size();

                if (newLimit > limit) {
                    list = session.rowActionList.getArray();

                    for (int j = limit; j < newLimit; j++) {
                        RowAction lobAction = (RowAction) list[j];

                        lobAction.commit(session);
                    }

                    limit = newLimit;
                }
            }

            try {
                action.store.commitRow(session, row, type, txModel);

                if (txModel == TransactionManager.LOCKS) {
                    action.setAsNoOp();

                    row.rowAction = null;
                }
            } catch (HsqlException e) {
                database.logger.logWarningEvent("data commit failed", e);
            }
        }

        try {
            if (limit > 0) {
                database.logger.writeCommitStatement(session);
            }
        } catch (HsqlException e) {
            database.logger.logWarningEvent("data commit failed", e);
        }
    }

    void finaliseRows(Session session, Object[] list, int start, int limit,
                      boolean commit) {

        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            if (action.table.tableType == TableBase.CACHED_TABLE) {
                if (action.type == RowActionBase.ACTION_NONE) {
                    Lock mapLock = rowActionMap.getWriteLock();

                    mapLock.lock();

                    try {
                        synchronized (action) {

                            // remove only if not changed
                            if (action.type == RowActionBase.ACTION_NONE) {
                                rowActionMap.remove(action.getPos());
                            }
                        }
                    } finally {
                        mapLock.unlock();
                    }
                }
            }

            if (action.type == RowActionBase.ACTION_DELETE_FINAL
                    && !action.deleteComplete) {
                try {
                    action.deleteComplete = true;

                    if (action.table.getTableType() == TableBase.TEMP_TABLE) {
                        continue;
                    }

                    Row row = action.memoryRow;

                    if (row == null) {
                        row = (Row) action.store.get(action.getPos(), false);
                    }

                    action.store.commitRow(session, row, action.type, txModel);
                } catch (Exception e) {

//                    throw unexpectedException(e.getMessage());
                }
            }
        }
    }

    /**
     * merge a given list of transaction rollback action with given timestamp
     */
    void mergeRolledBackTransaction(Session session, long timestamp,
                                    Object[] list, int start, int limit) {

        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];
            Row       row    = action.memoryRow;

            if (row == null) {
                if (action.type == RowAction.ACTION_NONE) {
                    continue;
                }

                row = (Row) action.store.get(action.getPos(), false);
            }

            if (row == null) {

                // only if transaction has been merged
                // shouldn't normally happen
                continue;
            }

            synchronized (row) {
                action.mergeRollback(session, timestamp, row);
            }
        }
    }

    /**
     * merge a transaction committed at a given timestamp.
     */
    void mergeTransaction(Session session, Object[] list, int start,
                          int limit, long timestamp) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = (RowAction) list[i];

            rowact.mergeToTimestamp(timestamp);
        }
    }

    /**
     * gets the next timestamp for an action
     */
    long nextChangeTimestamp() {
        return globalChangeTimestamp.incrementAndGet();
    }

    boolean checkDeadlock(Session session, OrderedHashSet newWaits) {

        int size = session.waitingSessions.size();

        for (int i = 0; i < size; i++) {
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

    boolean checkDeadlock(Session session, Session other) {

        int size = session.waitingSessions.size();

        for (int i = 0; i < size; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (current == other) {
                return false;
            }

            if (!checkDeadlock(current, other)) {
                return false;
            }
        }

        return true;
    }

    void endActionTPL(Session session) {

        if (session.isolationLevel == SessionInterface.TX_REPEATABLE_READ
                || session.isolationLevel
                   == SessionInterface.TX_SERIALIZABLE) {
            return;
        }

        if (session.sessionContext.currentStatement == null) {

            // after java function / proc with db access
            return;
        }

        if (session.sessionContext.depth > 0) {

            // routine or trigger
            return;
        }

        HsqlName[] readLocks =
            session.sessionContext.currentStatement.getTableNamesForRead();

        if (readLocks.length == 0) {
            return;
        }

        writeLock.lock();

        try {
            unlockReadTablesTPL(session, readLocks);

            final int waitingCount = session.waitingSessions.size();

            if (waitingCount == 0) {
                return;
            }

            boolean canUnlock = false;

            // if write lock was used for read lock
            for (int i = 0; i < readLocks.length; i++) {
                if (tableWriteLocks.get(readLocks[i]) != session) {
                    canUnlock = true;

                    break;
                }
            }

            if (!canUnlock) {
                return;
            }

            canUnlock = false;

            for (int i = 0; i < waitingCount; i++) {
                Session current = (Session) session.waitingSessions.get(i);

                if (current.abortTransaction) {
                    canUnlock = true;

                    break;
                }

                Statement currentStatement =
                    current.sessionContext.currentStatement;

                if (currentStatement == null) {
                    canUnlock = true;

                    break;
                }

                if (ArrayUtil.containsAny(
                        readLocks, currentStatement.getTableNamesForWrite())) {
                    canUnlock = true;

                    break;
                }
            }

            if (!canUnlock) {
                return;
            }

            resetLocks(session);
            resetLatchesMidTransaction(session);
        } finally {
            writeLock.unlock();
        }
    }

    void endTransactionTPL(Session session) {

        unlockTablesTPL(session);

        final int waitingCount = session.waitingSessions.size();

        if (waitingCount == 0) {
            return;
        }

        resetLocks(session);
        resetLatches(session);
    }

    void resetLocks(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                boolean canProceed = setWaitedSessionsTPL(current,
                    current.sessionContext.currentStatement);

                if (canProceed) {
                    if (current.tempSet.isEmpty()) {
                        lockTablesTPL(current,
                                      current.sessionContext.currentStatement);

                        current.tempUnlocked = true;
                    }
                }
            }
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (current.tempUnlocked) {

                //
            } else if (current.abortTransaction) {

                //
            } else {

                // this can introduce additional waits for the sessions
                setWaitedSessionsTPL(current,
                                     current.sessionContext.currentStatement);
            }
        }
    }

    void resetLatches(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);

            if (!current.abortTransaction && current.tempSet.isEmpty()) {

                // valid for top level statements
//                boolean hasLocks = hasLocks(current, current.sessionContext.currentStatement);
//                if (!hasLocks) {
//                    System.out.println("trouble");
//                    hasLocks(current, current.sessionContext.currentStatement);
//                }
            }

            setWaitingSessionTPL(current);
        }

        session.waitingSessions.clear();
        session.latch.setCount(0);
    }

    void resetLatchesMidTransaction(Session session) {

        session.tempSet.clear();
        session.tempSet.addAll(session.waitingSessions);
        session.waitingSessions.clear();

        final int waitingCount = session.tempSet.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.tempSet.get(i);

            if (!current.abortTransaction && current.tempSet.isEmpty()) {

                // valid for top level statements
//                boolean hasLocks = hasLocks(current, current.sessionContext.currentStatement);
//                if (!hasLocks) {
//                    System.out.println("trouble");
//                    hasLocks(current, current.sessionContext.currentStatement);
//                }
            }

            setWaitingSessionTPL(current);
        }

        session.tempSet.clear();
    }

    boolean setWaitedSessionsTPL(Session session, Statement cs) {

        session.tempSet.clear();

        if (cs == null) {
            return true;
        }

        if (session.abortTransaction) {
            return false;
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

        if (txModel == TransactionManager.MVLOCKS && session.isReadOnly()) {
            nameList = catalogNameList;
        }

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

        if (session.tempSet.isEmpty()) {
            return true;
        }

        if (checkDeadlock(session, session.tempSet)) {
            return true;
        }

        session.tempSet.clear();

        session.abortTransaction = true;

        return false;
    }

    void setWaitingSessionTPL(Session session) {

        int count = session.tempSet.size();

        assert session.latch.getCount() <= count + 1;

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
                it.remove();
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

    void unlockReadTablesTPL(Session session, HsqlName[] locks) {

        for (int i = 0; i < locks.length; i++) {
            tableReadLocks.remove(locks[i], session);
        }
    }

    boolean hasLocks(Session session, Statement cs) {

        if (cs == null) {
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
                return false;
            }

            Iterator it = tableReadLocks.get(name);

            while (it.hasNext()) {
                holder = (Session) it.next();

                if (holder != session) {
                    return false;
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
                return false;
            }
        }

        return true;
    }

    long getFirstLiveTransactionTimestamp() {

        if (liveTransactionTimestamps.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return liveTransactionTimestamps.get(0);
    }

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
    DoubleIntIndex getTransactionIDList() {

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
    void convertTransactionIDs(DoubleIntIndex lookup) {

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
}
