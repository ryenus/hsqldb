/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Shared code for TransactionManager classes
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
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
    LongDeque liveTransactionSCNs = new LongDeque();

    /** global timestamp for database */
    AtomicLong systemChangeNumber = new AtomicLong(1);

    //
    AtomicInteger transactionCount = new AtomicInteger();

    //
    HashMap<HsqlName, Session> tableWriteLocks = new HashMap<>();
    MultiValueHashMap<HsqlName, Session> tableReadLocks =
        new MultiValueHashMap<>();

    /**
     * Normally false. When SET DATABASE TRANSACTION CONTROL is used to change
     * the transaction control model, the old instance of TransactionManager
     * is set as expired.
     */
    volatile boolean hasExpired;

    TransactionManagerCommon(Database database) {
        this.database   = database;
        catalogNameList = new HsqlName[]{ database.getCatalogName() };
    }

    void setTransactionControl(Session session, int mode) {

        TransactionManagerCommon manager = null;

        if (mode == txModel) {
            return;
        }

        // statement runs as transaction
        writeLock.lock();

        try {
            switch (txModel) {

                case TransactionManager.MVCC :
                case TransactionManager.MVLOCKS :
                    if (liveTransactionSCNs.size() != 1) {
                        throw Error.error(ErrorCode.X_25001);
                    }
            }

            switch (mode) {

                case TransactionManager.MVCC : {
                    TransactionManagerMVCC txMan = new TransactionManagerMVCC(
                        database);

                    txMan.liveTransactionSCNs.addLast(session.transactionSCN);

                    txMan.catalogWriteSession = session;
                    txMan.isLockedMode        = true;

                    OrderedHashSet<Session> set = session.waitingSessions;

                    for (int i = 0; i < set.size(); i++) {
                        Session current = set.get(i);

                        current.waitedSessions.add(session);
                    }

                    manager = txMan;
                    break;
                }

                case TransactionManager.MVLOCKS : {
                    manager = new TransactionManagerMV2PL(database);

                    manager.liveTransactionSCNs.addLast(session.transactionSCN);

                    OrderedHashSet<Session> set = session.waitingSessions;

                    for (int i = 0; i < set.size(); i++) {
                        Session current = set.get(i);

                        current.waitedSessions.clear();
                    }

                    break;
                }

                case TransactionManager.LOCKS : {
                    manager = new TransactionManager2PL(database);

                    OrderedHashSet<Session> set = session.waitingSessions;

                    for (int i = 0; i < set.size(); i++) {
                        Session current = set.get(i);

                        current.waitedSessions.clear();
                    }

                    break;
                }

                default :
                    throw Error.runtimeError(
                        ErrorCode.U_S0500,
                        "TransactionManagerCommon");
            }

            manager.systemChangeNumber.set(systemChangeNumber.get());

            manager.transactionCount = transactionCount;
            hasExpired               = true;
            database.txManager       = (TransactionManager) manager;
        } finally {
            writeLock.unlock();
        }
    }

    final void beginTransactionCommon(Session session) {

        session.actionSCN        = getNextSystemChangeNumber();
        session.actionStartSCN   = session.actionSCN;
        session.transactionSCN   = session.actionSCN;
        session.isPreTransaction = false;
        session.isTransaction    = true;

        transactionCount.incrementAndGet();
    }

    final void adjustLobUsage(Session session) {

        int  limit               = session.rowActionList.size();
        long lastActionTimestamp = session.actionSCN;

        for (int i = 0; i < limit; i++) {
            RowAction action = session.rowActionList.get(i);

            if (action.type == RowActionBase.ACTION_NONE) {
                continue;
            }

            if (action.table.hasLobColumn) {
                int type = action.getCommitTypeOn(lastActionTimestamp);
                Row row  = action.memoryRow;

                if (row == null) {
                    row = (Row) action.store.get(action.getPos(), false);
                }

                switch (type) {

                    case RowActionBase.ACTION_INSERT :
                        session.sessionData.adjustLobUsageCount(
                            action.table,
                            row.getData(),
                            1);
                        break;

                    case RowActionBase.ACTION_DELETE :
                        session.sessionData.adjustLobUsageCount(
                            action.table,
                            row.getData(),
                            -1);
                        break;

                    case RowActionBase.ACTION_INSERT_DELETE :
                    default :
                }
            }
        }

        int newLimit = session.rowActionList.size();

        if (newLimit > limit) {
            for (int i = limit; i < newLimit; i++) {
                RowAction lobAction = session.rowActionList.get(i);

                lobAction.commit(session);
            }
        }

        session.sessionData.newLobFloor = SessionData.noLobFloor;
    }

    final Statement updateCurrentStatement(Session session, Statement cs) {

        if (cs.getCompileTimestamp()
                < database.schemaManager.getSchemaChangeTimestamp()) {
            cs = session.statementManager.getStatement(cs);

            if (cs == null) {
                session.sessionContext.invalidStatement = true;
            } else {
                session.sessionContext.currentStatement = cs;
            }
        }

        return cs;
    }

    final void persistCommit(Session session) {

        int     limit       = session.rowActionList.size();
        boolean writeCommit = false;

        for (int i = 0; i < limit; i++) {
            RowAction action = session.rowActionList.get(i);

            if (action.type == RowActionBase.ACTION_NONE) {
                continue;
            }

            int type = action.getCommitTypeOn(session.actionSCN);
            Row row  = action.memoryRow;

            if (row == null) {
                row = (Row) action.store.get(action.getPos(), false);
            }

            if (!action.table.isTemp) {
                writeCommit = true;
            }

            try {
                action.store.commitRow(session, row, type, txModel);

                if (txModel == TransactionManager.LOCKS
                        || action.table.isTemp) {
                    action.setAsNoOp();

                    row.rowAction = null;
                }
            } catch (HsqlException e) {
                database.logger.logWarningEvent("data commit failed", e);
            }
        }

        try {
            session.logSequences();

            if (limit > 0 && writeCommit) {
                database.logger.writeCommitStatement(session);
            }
        } catch (HsqlException e) {
            database.logger.logWarningEvent("data commit logging failed", e);
        }
    }

    final void finaliseRows(
            Session session,
            Object[] list,
            int start,
            int limit) {

        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];

            action.store.postCommitAction(session, action);
        }
    }

    /**
     * merge a transaction committed at a given timestamp.
     */
    final void mergeTransaction(
            RowAction[] list,
            int start,
            int limit,
            long timestamp) {

        for (int i = start; i < limit; i++) {
            RowAction rowact = list[i];

            rowact.mergeToTimestamp(timestamp);
        }
    }

    /**
     * gets the next timestamp for an action
     */
    public final long getNextSystemChangeNumber() {
        return systemChangeNumber.incrementAndGet();
    }

    static boolean checkDeadlock(
            Session session,
            OrderedHashSet<Session> newWaits) {

        int size = session.waitingSessions.size();

        for (int i = 0; i < size; i++) {
            Session current = session.waitingSessions.get(i);

            if (newWaits.contains(current)) {
                return false;
            }

            if (!checkDeadlock(current, newWaits)) {
                return false;
            }
        }

        return true;
    }

    final boolean checkDeadlock(Session session, Session other) {

        int size = session.waitingSessions.size();

        for (int i = 0; i < size; i++) {
            Session current = session.waitingSessions.get(i);

            if (current == other) {
                return false;
            }

            if (!checkDeadlock(current, other)) {
                return false;
            }
        }

        return true;
    }

    final void getTransactionSessions(Session session) {

        OrderedHashSet<Session> set = session.tempSet;
        Session[] sessions          = database.sessionManager.getAllSessions();

        for (int i = 0; i < sessions.length; i++) {
            long timestamp = sessions[i].transactionSCN;

            if (session != sessions[i] && sessions[i].isTransaction) {
                set.add(sessions[i]);
            }
        }
    }

    final void getTransactionAndPreSessions(Session session) {

        OrderedHashSet<Session> set = session.tempSet;
        Session[] sessions          = database.sessionManager.getAllSessions();

        set.clear();

        for (int i = 0; i < sessions.length; i++) {
            if (session == sessions[i]) {
                continue;
            }

            if (sessions[i].isPreTransaction) {
                set.add(sessions[i]);
            } else if (sessions[i].isTransaction) {
                set.add(sessions[i]);
            }
        }
    }

    final void endActionTPL(Session session) {

        if (session.isolationLevel == SessionInterface.TX_REPEATABLE_READ
                || session.isolationLevel == SessionInterface.TX_SERIALIZABLE) {
            return;
        }

        if (session.sessionContext.invalidStatement) {
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
                Session current = session.waitingSessions.get(i);

                if (current.abortTransaction) {
                    canUnlock = true;
                    break;
                }

                Statement currentStatement =
                    current.sessionContext.currentStatement;

                if (current.sessionContext.invalidStatement) {
                    canUnlock = true;
                    break;
                }

                if (ArrayUtil.containsAny(readLocks,
                                          currentStatement.getTableNamesForWrite())) {
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

    final void resetLocks(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = session.waitingSessions.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                boolean canProceed = setWaitedSessionsTPL(
                    current,
                    current.sessionContext.currentStatement);

                if (canProceed) {
                    if (current.tempSet.isEmpty()) {
                        lockTablesTPL(
                            current,
                            current.sessionContext.currentStatement);

                        current.tempUnlocked = true;
                    }
                }
            }
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = session.waitingSessions.get(i);

            if (current.tempUnlocked) {

                //
            } else if (current.abortTransaction) {

                //
            } else {

                // this can introduce additional waits for the sessions
                setWaitedSessionsTPL(
                    current,
                    current.sessionContext.currentStatement);
            }
        }
    }

    final void resetLatches(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current     = session.waitingSessions.get(i);
            boolean monitorCode = false;

            if (monitorCode) {
                if (!current.abortTransaction && current.tempSet.isEmpty()) {

                    // test code valid only for top level statements
                    boolean hasLocks = hasLocks(
                        current,
                        current.sessionContext.currentStatement);

                    if (!hasLocks) {
                        System.out.println("tx graph");

                        hasLocks = hasLocks(
                            current,
                            current.sessionContext.currentStatement);
                    }
                }
            }

            setWaitingSessionTPL(current);
        }

        session.waitingSessions.clear();
        session.latch.setCount(0);
    }

    final void resetLatchesMidTransaction(Session session) {

        session.tempSet.clear();
        session.tempSet.addAll(session.waitingSessions);
        session.waitingSessions.clear();

        final int waitingCount = session.tempSet.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current     = session.tempSet.get(i);
            boolean monitorCode = false;

            if (monitorCode) {
                if (!current.abortTransaction && current.tempSet.isEmpty()) {

                    // test code valid for top level statements
                    boolean hasLocks = hasLocks(
                        current,
                        current.sessionContext.currentStatement);

                    if (!hasLocks) {
                        System.out.println("tx graph");

                        hasLocks = hasLocks(
                            current,
                            current.sessionContext.currentStatement);
                    }
                }
            }

            setWaitingSessionTPL(current);
        }

        session.tempSet.clear();
    }

    final boolean setWaitedSessionsTPL(Session session, Statement cs) {

        session.tempSet.clear();

        if (cs == null) {
            return true;
        }

        if (session.abortTransaction) {
            return false;
        }

        if (cs.isCatalogLock(txModel)) {
            getTransactionSessions(session);
        }

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }

            Iterator<Session> it = tableReadLocks.getValuesIterator(name);

            while (it.hasNext()) {
                holder = it.next();

                if (holder != session) {
                    session.tempSet.add(holder);
                }
            }
        }

        nameList = cs.getTableNamesForRead();

        if (txModel == TransactionManager.MVLOCKS && session.isReadOnly()) {
            if (nameList.length > 0) {
                nameList = catalogNameList;
            }
        }

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = tableWriteLocks.get(name);

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

    final void setWaitingSessionTPL(Session session) {

        int count      = session.tempSet.size();
        int latchCount = session.latch.getCount();

        if (latchCount > count + 1) {
            System.out.println("latch count " + latchCount);
        }

        for (int i = 0; i < count; i++) {
            Session current = session.tempSet.get(i);

            current.waitingSessions.add(session);
        }

        session.tempSet.clear();
        session.latch.setCount(count);
    }

    final void lockTablesTPL(Session session, Statement cs) {

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

        if (txModel == TransactionManager.MVLOCKS && session.isReadOnly()) {
            if (nameList.length > 0) {
                nameList = catalogNameList;
            }
        }

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            tableReadLocks.put(name, session);
        }
    }

    final void unlockTablesTPL(Session session) {

        Iterator<Session> it = tableWriteLocks.values().iterator();

        while (it.hasNext()) {
            Session s = it.next();

            if (s == session) {
                it.remove();
            }
        }

        it = tableReadLocks.values().iterator();

        while (it.hasNext()) {
            Session s = it.next();

            if (s == session) {
                it.remove();
            }
        }
    }

    final void unlockReadTablesTPL(Session session, HsqlName[] locks) {
        for (int i = 0; i < locks.length; i++) {
            tableReadLocks.remove(locks[i], session);
        }
    }

    final boolean hasLocks(Session session, Statement cs) {

        if (cs == null) {
            return true;
        }

        HsqlName[] nameList = cs.getTableNamesForWrite();

        for (int i = 0; i < nameList.length; i++) {
            HsqlName name = nameList[i];

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Session holder = tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                return false;
            }

            Iterator<Session> it = tableReadLocks.getValuesIterator(name);

            while (it.hasNext()) {
                holder = it.next();

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

            Session holder = tableWriteLocks.get(name);

            if (holder != null && holder != session) {
                return false;
            }
        }

        return true;
    }

    final long getFirstLiveTransactionTimestamp() {

        if (liveTransactionSCNs.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return liveTransactionSCNs.get(0);
    }

    /**
     * Return an array of all row actions sorted by System Change No.
     */
    final RowAction[] getRowActionList() {

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
                        RowAction current = sessions[i].rowActionList.get(
                            tIndex[i]);

                        if (current.actionSCN < minChangeNo) {
                            minChangeNo  = current.actionSCN;
                            sessionIndex = i;
                        }

                        found = true;
                    }
                }

                if (!found) {
                    break;
                }

                HsqlArrayList<RowAction> currentList =
                    sessions[sessionIndex].rowActionList;

                while (tIndex[sessionIndex] < currentList.size()) {
                    RowAction current = currentList.get(tIndex[sessionIndex]);

                    // if the next change not is in this session, continue adding
                    if (current.actionSCN == minChangeNo + 1) {
                        minChangeNo++;
                    }

                    if (current.actionSCN == minChangeNo) {
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

    void resetSession(
            Session session,
            Session targetSession,
            long statementTimestamp,
            int mode) {

        writeLock.lock();

        try {
            switch (mode) {

                case TransactionManager.resetSessionResults :
                    if (session != targetSession) {
                        break;
                    }

                    if (!targetSession.isInMidTransaction()) {
                        targetSession.sessionData.closeAllNavigators();
                    }

                    break;

                case TransactionManager.resetSessionTables :
                    if (session != targetSession) {
                        break;
                    }

                    if (!targetSession.isInMidTransaction()) {
                        targetSession.sessionData.persistentStoreCollection.clearAllTables();
                    }

                    break;

                case TransactionManager.resetSessionResetAll :
                    if (session != targetSession) {
                        break;
                    }

                    if (!targetSession.isInMidTransaction()) {
                        targetSession.resetSession();
                    }

                    break;

                case TransactionManager.resetSessionRollback :
                    if (targetSession.isInMidTransaction()) {
                        prepareReset(targetSession);

                        targetSession.abortTransaction = true;

                        if (targetSession.latch.getCount() > 0) {
                            targetSession.latch.setCount(0);
                        } else {
                            targetSession.rollbackNoCheck(true);
                        }
                    }

                    break;

                case TransactionManager.resetSessionStatement :
                    if (statementTimestamp != targetSession.statementStartSCN) {
                        break;
                    }

                    if (targetSession.isInMidTransaction()) {
                        prepareReset(targetSession);

                        targetSession.abortAction = true;

                        if (targetSession.latch.getCount() > 0) {
                            targetSession.latch.setCount(0);
                        }
                    }

                    break;

                case TransactionManager.resetSessionClose :
                    if (session == targetSession) {
                        break;
                    }

                    if (!targetSession.isInMidTransaction()) {
                        targetSession.rollbackNoCheck(true);
                        targetSession.close();
                    }

                    break;
            }
        } finally {
            writeLock.unlock();
        }
    }

    final void prepareReset(Session session) {

        OrderedHashSet<Session> waitedSessions = session.waitedSessions;

        for (int i = 0; i < waitedSessions.size(); i++) {
            Session current = waitedSessions.get(i);

            current.waitingSessions.remove(session);
        }

        waitedSessions.clear();
    }

    public void abortAction(Session session) {}
}
