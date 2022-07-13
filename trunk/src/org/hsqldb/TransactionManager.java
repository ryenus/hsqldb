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

import org.hsqldb.persist.PersistentStore;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 2.0.0
 */
public interface TransactionManager {

    //
    int LOCKS   = 0;
    int MVLOCKS = 1;
    int MVCC    = 2;

    //
    int ACTION_READ = 0;
    int ACTION_DUP  = 1;
    int ACTION_REF  = 2;

    //
    int resetSessionResults   = 1;
    int resetSessionTables    = 2;
    int resetSessionResetAll  = 3;
    int resetSessionRollback  = 4;
    int resetSessionStatement = 5;
    int resetSessionClose     = 6;

    long getSystemChangeNumber();

    long getNextSystemChangeNumber();

    void setSystemChangeNumber(long ts);

    RowAction addDeleteAction(Session session, Table table,
                              PersistentStore store, Row row,
                              int[] changedColumns);

    void addInsertAction(Session session, Table table, PersistentStore store,
                         Row row, int[] changedColumns);

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    void beginAction(Session session, Statement cs);

    void beginActionResume(Session session);

    void beginTransaction(Session session);

    boolean commitTransaction(Session session);

    void completeActions(Session session);

    int getTransactionControl();

    boolean isMVRows();

    boolean isMVCC();

    boolean is2PL();

    boolean prepareCommitActions(Session session);

    void rollback(Session session);

    void rollbackAction(Session session);

    void rollbackSavepoint(Session session, int index);

    void rollbackPartial(Session session, int start, long timestamp);

    void setTransactionControl(Session session, int mode);

    void resetSession(Session session, Session targetSession,
                      long statementTimestamp, int mode);
}
