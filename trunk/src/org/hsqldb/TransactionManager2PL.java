/* Copyright (c) 2001-2022, The HSQL Development Group
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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.PersistentStore;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 2.0.0
 */
public class TransactionManager2PL extends TransactionManagerCommon
implements TransactionManager {

    public TransactionManager2PL(Database db) {

        super(db);

        lobSession = database.sessionManager.getSysLobSession();
        txModel    = LOCKS;
    }

    public long getSystemChangeNumber() {
        return systemChangeNumber.get();
    }

    public void setSystemChangeNumber(long ts) {
        systemChangeNumber.set(ts);
    }

    public boolean isMVRows() {
        return false;
    }

    public boolean isMVCC() {
        return false;
    }

    public boolean is2PL() {
        return true;
    }

    public int getTransactionControl() {
        return LOCKS;
    }

    public void setTransactionControl(Session session, int mode) {
        super.setTransactionControl(session, mode);
    }

    public void completeActions(Session session) {
        endActionTPL(session);
    }

    public boolean prepareCommitActions(Session session) {

        session.actionSCN = getNextSystemChangeNumber();

        return true;
    }

    public boolean commitTransaction(Session session) {

        if (session.abortTransaction) {
            return false;
        }

        writeLock.lock();

        try {
            int limit = session.rowActionList.size();

            // new actionTimestamp used for commitTimestamp
            session.actionSCN         = getNextSystemChangeNumber();
            session.transactionEndSCN = session.actionSCN;

            endTransaction(session);

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) session.rowActionList.get(i);

                action.commit(session);
            }

            adjustLobUsage(session);
            persistCommit(session);

            session.isTransaction = false;

            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }

        session.tempSet.clear();

        return true;
    }

    public void rollback(Session session) {

        writeLock.lock();

        try {
            session.abortTransaction  = false;
            session.actionSCN         = getNextSystemChangeNumber();
            session.transactionEndSCN = session.actionSCN;

            rollbackPartial(session, 0, session.transactionSCN);
            endTransaction(session);
            session.logSequences();

            session.isTransaction = false;

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
            session.sessionContext.savepoints.removeEntry(
                session.sessionContext.savepoints.size() - 1);
            session.sessionContext.savepointTimestamps.removeLast();
        }

        rollbackPartial(session, start, timestamp);
    }

    public void rollbackAction(Session session) {
        rollbackPartial(session, session.actionIndex, session.actionStartSCN);
        endActionTPL(session);
    }

    /**
     * rollback the row actions from start index in list and
     * the given timestamp
     */
    public void rollbackPartial(Session session, int start, long timestamp) {

        int limit = session.rowActionList.size();

        if (start == limit) {
            return;
        }

        for (int i = limit - 1; i >= start; i--) {
            RowAction action = (RowAction) session.rowActionList.get(i);

            if (action == null || action.type == RowActionBase.ACTION_NONE
                    || action.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = action.memoryRow;

            if (row == null) {
                row = (Row) action.store.get(action.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            action.rollback(session, timestamp);

            int type = action.mergeRollback(session, timestamp);

            action.store.rollbackRow(session, row, type, txModel);
        }

        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table,
                                     PersistentStore store, Row row,
                                     int[] changedColumns) {

        RowAction action;

        synchronized (row) {
            action = RowAction.addDeleteAction(session, table, store, row,
                                               changedColumns);
        }

        store.delete(session, row);

        row.rowAction = null;

        if (table.persistenceScope == Table.SCOPE_ROUTINE) {
            return action;
        }

        session.rowActionList.add(action);

        return action;
    }

    public void addInsertAction(Session session, Table table,
                                PersistentStore store, Row row,
                                int[] changedColumns) {

        RowAction action = row.rowAction;

        if (action == null) {
/*
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
*/
            throw Error.runtimeError(ErrorCode.GENERAL_ERROR,
                                     "null insert action ");
        }

        store.indexRow(session, row);

        row.rowAction = null;

        if (table.persistenceScope == Table.SCOPE_ROUTINE) {
            return;
        }

        session.rowActionList.add(action);
    }

    public void beginTransaction(Session session) {

        if (!session.isTransaction) {
            beginTransactionCommon(session);
        }
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs) {

        writeLock.lock();

        try {
            if (hasExpired) {
                session.redoAction = true;

                return;
            }

            cs = updateCurrentStatement(session, cs);

            if (cs == null) {
                return;
            }

            boolean canProceed = setWaitedSessionsTPL(session, cs);

            if (canProceed) {
                session.isPreTransaction = true;

                if (session.tempSet.isEmpty()) {
                    lockTablesTPL(session, cs);

                    // we don't set other sessions that would now be waiting for this one too
                    // next lock release will do it
                } else {
                    setWaitingSessionTPL(session);
                }
            } else {

                // this is redundant as it has been set when canProceed == false
                session.abortTransaction = true;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void beginActionResume(Session session) {

        if (session.isTransaction) {
            session.actionSCN      = getNextSystemChangeNumber();
            session.actionStartSCN = session.actionSCN;
        } else {
            beginTransactionCommon(session);
        }
    }

    public void removeTransactionInfo(long id) {}

    public void resetSession(Session session, Session targetSession,
                             long statementTimestamp, int mode) {
        super.resetSession(session, targetSession, statementTimestamp, mode);
    }

    private void endTransaction(Session session) {

        if (session.isTransaction) {
            transactionCount.decrementAndGet();
        }
    }
}
