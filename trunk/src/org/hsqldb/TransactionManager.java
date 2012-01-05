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

import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
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

    public boolean canRead(Session session, int id, int mode);

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
