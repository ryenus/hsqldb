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

import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Represents the chain of insert / delete / rollback / commit actions on a row.
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class RowAction extends RowActionBase {

    //
    final TableBase table;
    Row             memoryRow;
    int             rowId;
    boolean         isMemory;

    public static RowAction addInsertAction(Session session, TableBase table,
            Row row) {

        RowAction action = new RowAction(session, table, ACTION_INSERT,
                                         row.isMemory(), row);

        row.rowAction = action;

        return action;
    }

    public static RowAction addDeleteAction(Session session, TableBase table,
            Row row, int[] colMap) {

        RowAction action = row.rowAction;

        if (action == null) {
            action = new RowAction(session, table, ACTION_DELETE,
                                   row.isMemory(), row);
            action.changeColumnMap = colMap;
            row.rowAction          = action;

            return action;
        }

        return action.addDeleteAction(session, colMap);
    }

    public static boolean addRefAction(Session session, Row row,
                                       int[] colMap) {

        RowAction action = row.rowAction;

        if (action == null) {
            action = new RowAction(session, row.getTable(), ACTION_REF,
                                   row.isMemory(), row);
            action.changeColumnMap = colMap;
            row.rowAction          = action;

            return true;
        }

        return action.addRefAction(session, colMap);
    }

    public RowAction(Session session, TableBase table, byte type,
                     boolean isMemory, Row row) {

        this.session    = session;
        this.type       = type;
        actionTimestamp = session.actionTimestamp;
        this.table      = table;
        this.isMemory   = isMemory;
        this.memoryRow  = row;
        rowId           = row.getPos();
    }

    synchronized RowAction addDeleteAction(Session session, int[] colMap) {

        if (type == ACTION_NONE) {
            setAsAction(session, ACTION_DELETE);

            changeColumnMap = colMap;
        } else {
            RowActionBase action = this;

            while (true) {
                if (action.type == ACTION_DELETE) {
                    if (session != action.session) {
                        session.tempSet.add(action.session);

                        return null;
                    }
                } else if (action.type == ACTION_REF) {
                    if (session != action.session
                            && action.commitTimestamp == 0) {
                        if (colMap == null
                                || ArrayUtil.haveCommonElement(
                                    colMap, action.changeColumnMap)) {
                            session.tempSet.add(action.session);

                            return null;
                        }
                    }
                }

                if (action.next == null) {
                    break;
                }

                action = action.next;
            }

            RowActionBase newAction = new RowActionBase(session,
                ACTION_DELETE);

            newAction.changeColumnMap = colMap;
            action.next               = newAction;
        }

        return this;
    }

    synchronized boolean addRefAction(Session session, int[] colMap) {

        if (type == ACTION_NONE) {
            setAsAction(session, ACTION_REF);

            changeColumnMap = colMap;

            return true;
        }

        RowActionBase action = this;

        while (true) {
            if (session == action.session) {
                if (action.type == ACTION_REF
                        && action.changeColumnMap == colMap
                        && action.commitTimestamp == 0) {
                    return false;
                }

                if (action.type == ACTION_INSERT) {
                    if (action.commitTimestamp == 0) {
                        return false;
                    }
                }
            } else {
                if (action.type == ACTION_DELETE
                        && action.commitTimestamp == 0) {
                    if (colMap == null
                            || ArrayUtil.haveCommonElement(
                                colMap, action.changeColumnMap)) {
                        session.tempSet.add(action.session);

                        return false;
                    }
                }
            }

            if (action.next == null) {
                break;
            }

            action = action.next;
        }

        RowActionBase newAction = new RowActionBase(session, ACTION_REF);

        newAction.changeColumnMap = colMap;
        action.next               = newAction;

        return true;
    }

    public boolean checkDeleteActions() {
        return false;
    }

    public synchronized RowAction duplicate(int newRowId) {

        RowAction action = duplicate();

        action.rowId = newRowId;

        return action;
    }

    synchronized RowAction duplicate() {

        RowAction action = new RowAction(session, table, type, isMemory,
                                         memoryRow);

        action.setAsAction(this);

        action.rowId = rowId;

        return action;
    }

    synchronized void setAsAction(Session session, byte type) {

        this.session    = session;
        this.type       = type;
        actionTimestamp = session.actionTimestamp;
        changeColumnMap = null;
    }

    synchronized void setAsAction(RowActionBase action) {
        super.setAsAction(action);
    }

    synchronized void setAsNoOp(Row row) {

        setAsNoOp();

        row.rowAction = null;
    }

    private void setAsNoOp() {

        memoryRow = null;
        session   = null;

//        actionTimestamp = 0;
        commitTimestamp = 0;

//        rolledback      = false;
        deleteComplete  = false;
        changeColumnMap = null;

//        prepared        = false;
        type = ACTION_NONE;
        next = null;
    }

    private void setAsDeleteFinal() {

//        actionTimestamp = 0;
//        commitTimestamp = 0;
        rolledback      = false;
        deleteComplete  = false;
        prepared        = false;
        changeColumnMap = null;
        type            = ACTION_DELETE_FINAL;
        next            = null;
    }

    /** for two-phased pre-commit */
    synchronized void prepareCommit(Session session) {

        RowActionBase action = this;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                action.prepared = true;
            }

            action = action.next;
        } while (action != null);
    }

    synchronized int commit(Session session) {

        RowActionBase action = this;
        int           type   = ACTION_NONE;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                action.commitTimestamp = session.actionTimestamp;
                action.prepared        = false;

                if (action.type == ACTION_INSERT) {
                    type = action.type;
                } else if (action.type == ACTION_DELETE) {
                    if (type == ACTION_INSERT) {

                        // ACTION_INSERT + ACTION_DELETE
                        type = ACTION_INSERT_DELETE;
                    } else {
                        type = action.type;
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return type;
    }

    /**
     * Rollback actions for a session including and after the given timestamp
     */
    synchronized int rollback(Session session, long timestamp) {

        RowActionBase action = this;
        int           type   = ACTION_NONE;

        do {
            if (action.session == session && action.commitTimestamp == 0) {
                if (action.actionTimestamp >= timestamp
                        || action.actionTimestamp == 0) {
                    action.commitTimestamp = session.actionTimestamp;
                    action.rolledback      = true;
                    action.prepared        = false;

                    if (action.type == ACTION_INSERT) {
                        type = action.type;
                    } else if (action.type == ACTION_DELETE) {
                        if (type == ACTION_INSERT) {

                            // ACTION_INSERT + ACTION_DELETE
                            type = ACTION_INSERT_DELETE;
                        } else {
                            type = action.type;
                        }
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return type;
    }

    public boolean isDeleted() {

        RowActionBase action = this;

        do {
            if (action.commitTimestamp != 0) {
                if (action.type == ACTION_DELETE
                        || action.type == ACTION_INSERT_DELETE
                        || action.type == ACTION_DELETE_FINAL) {
                    return true;
                }
            }

            action = action.next;
        } while (action != null);

        return false;
    }

    /**
     * returns type of commit performed on timestamp. ACTION_NONE if none.
     */
    synchronized int getCommitTypeOn(long timestamp) {

        RowActionBase action = this;
        int           type   = ACTION_NONE;

        do {
            if (action.commitTimestamp == timestamp) {
                if (action.type == ACTION_INSERT) {
                    type = action.type;
                } else if (action.type == ACTION_DELETE) {
                    if (type == ACTION_INSERT) {

                        // ACTION_INSERT + ACTION_DELETE
                        type = ACTION_INSERT_DELETE;
                    } else {
                        type = action.type;
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return type;
    }

    synchronized int getCommitTypeUpto(long timestamp) {

        RowActionBase action = this;
        int           type   = ACTION_NONE;

        do {
            if (action.commitTimestamp <= timestamp) {
                if (action.type == ACTION_INSERT) {
                    type = action.type;
                } else if (action.type == ACTION_DELETE) {
                    if (type == ACTION_INSERT) {

                        // ACTION_INSERT + ACTION_DELETE
                        type = ACTION_INSERT_DELETE;
                    } else {
                        type = action.type;
                    }
                }
            }

            action = action.next;
        } while (action != null);

        return type;
    }

    /**
     * returns false if another committed session has altered the same row
     */
    synchronized boolean canCommit(Session session, OrderedHashSet set) {

        RowActionBase action;
        long          timestamp       = session.transactionTimestamp;
        long          commitTimestamp = 0;
        final boolean readCommitted = session.isolationLevel
                                      == SessionInterface.TX_READ_COMMITTED;

        action = this;

        if (readCommitted) {
            do {
                if (action.session == session) {

                    // for READ_COMMITTED, use action timestamp for later conflicts
                    if (action.commitTimestamp == 0) {
                        timestamp = action.actionTimestamp;
                    }
                }

                action = action.next;
            } while (action != null);

            action = this;
        }

        do {
            if (action.rolledback || action.type == ACTION_NONE) {
                action = action.next;

                continue;
            }

            if (action.session != session) {
                if (action.prepared) {
                    return false;
                }

                if (action.commitTimestamp == 0
                        && action.actionTimestamp != 0) {
                    set.add(action.session);
                } else if (action.commitTimestamp > commitTimestamp) {
                    commitTimestamp = action.commitTimestamp;
                }
            }

            action = action.next;
        } while (action != null);

        return commitTimestamp < timestamp;
    }

    synchronized void complete(Session session) {

        RowActionBase action;

        action = this;

        do {
            if (action.session == session) {
                if (action.actionTimestamp == 0) {
                    action.actionTimestamp = session.actionTimestamp;
                }
            }

            action = action.next;
        } while (action != null);
    }

    /**
     * returns false if cannot complete
     * when READ COMMITTED, false result always means repeat action and adds
     * to set parameter the sessions to wait on (may be no wait)
     */
    synchronized boolean complete(Session session, OrderedHashSet set) {

        RowActionBase action;
        boolean readCommitted = session.isolationLevel
                                == SessionInterface.TX_READ_COMMITTED;
        boolean result = true;

        action = this;

        do {
            if (action.rolledback || action.type == ACTION_NONE) {
                action = action.next;

                continue;
            }

            if (action.session == session) {
                if (action.actionTimestamp == 0) {
                    action.actionTimestamp = session.actionTimestamp;
                }
            } else {
                if (action.prepared) {
                    set.add(action.session);

                    return false;
                }

                if (readCommitted) {
                    if (action.commitTimestamp > session.actionTimestamp) {

                        // 2.0 -- investigate
                        // can redo - if deletes
                        // can redo - if dup, but will likely fail at retry
                        // can redo - if ref, but will likely fail at retry
                        set.add(session);

                        result = false;
                    } else if (action.commitTimestamp == 0
                               && action.actionTimestamp != 0) {
                        set.add(action.session);

                        result = false;
                    }
                } else if (action.commitTimestamp
                           > session.transactionTimestamp) {
                    return false;
                }
            }

            action = action.next;
        } while (action != null);

        return result;
    }

    synchronized int getActionType(long timestamp) {

        int           actionType = ACTION_NONE;
        RowActionBase action     = this;

        do {
            if (action.actionTimestamp == timestamp) {
                if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    private int getRollbackActionType(Session session) {

        int           actionType = ACTION_NONE;
        RowActionBase action     = this;

        do {
            if (action.session == session && action.rolledback) {
                if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }
            }

            action = action.next;
        } while (action != null);

        return actionType;
    }

    synchronized int getPos() {
        return rowId;
    }

    synchronized void setPos(int pos) {
        rowId = pos;
    }

    /**
     * merge rolled back actions
     */
    synchronized void mergeRollback(Session session, long timestamp, Row row) {

        RowActionBase action         = this;
        RowActionBase head           = null;
        RowActionBase tail           = null;
        int           rollbackAction = getRollbackActionType(session);

        do {
            if (action.session == session && action.rolledback) {
                if (tail != null) {
                    tail.next = null;
                }
            } else {
                if (head == null) {
                    head = tail = action;
                } else {
                    tail.next = action;
                    tail      = action;
                }
            }

            action = action.next;
        } while (action != null);

        if (head == null) {
            switch (rollbackAction) {

                case ACTION_INSERT :
                case ACTION_INSERT_DELETE :
                    setAsDeleteFinal();
                    break;

                case ACTION_DELETE :
                default :
                    setAsNoOp(row);
                    break;
            }
        } else {
            if (head != this) {
                setAsAction(head);
            }
        }
    }

    /**
     * merge session actions committed on or before given timestamp.
     *
     * return false if row is to be deleted
     *
     */
    synchronized void mergeToTimestamp(Row row, long timestamp) {

        RowActionBase action     = this;
        RowActionBase head       = null;
        RowActionBase tail       = null;
        boolean       exists     = true;
        int           commitType = getCommitTypeUpto(timestamp);

/* debug 190
        if (row.rowActionB == null) {

//            row.rowActionB = this.duplicate(timestamp);
        } else {
            RowActionBase tailB = row.rowActionB;

            while (tailB.next != null) {
                tailB = tailB.next;
            }

//            tailB.next = this.duplicate(timestamp);
        }

// debug 190
*/
        if (type == ACTION_DELETE_FINAL || type == ACTION_NONE) {
            return;
        }

        do {
            if (action.commitTimestamp != 0
                    && action.commitTimestamp <= timestamp) {
                if (tail != null) {
                    tail.next = null;
                }

                exists = (action.type == ACTION_INSERT);
            } else {
                if (head == null) {
                    head = tail = action;
                } else {
                    tail.next = action;
                    tail      = action;
                }
            }

            action = action.next;
        } while (action != null);

        if (head == null) {
            switch (commitType) {

                case ACTION_DELETE :
                case ACTION_INSERT_DELETE :
                    setAsDeleteFinal();
                    break;

                case ACTION_INSERT :
                default :
                    setAsNoOp(row);
                    break;
            }
/*
            if (exists) {
                setAsNoOp(row);
            } else {
                setAsDeleteFinal();
            }
*/
        } else if (head != this) {
            setAsAction(head);
        }
    }

    synchronized boolean canRead(Session session) {

        long threshold;
        int  actionType = ACTION_NONE;

        if (type == ACTION_DELETE_FINAL) {
            return false;
        }

        RowActionBase action = this;

        if (session == null) {
            threshold = Long.MAX_VALUE;
        } else {
            switch (session.isolationLevel) {

                case SessionInterface.TX_READ_UNCOMMITTED :
                    threshold = Long.MAX_VALUE;
                    break;

                case SessionInterface.TX_READ_COMMITTED :
                    threshold = session.actionTimestamp;
                    break;

                case SessionInterface.TX_REPEATABLE_READ :
                case SessionInterface.TX_SERIALIZABLE :
                default :
                    threshold = session.transactionTimestamp;
                    break;
            }
        }

        do {
            if (action.type == ACTION_REF) {
                action = action.next;

                continue;
            }

            if (action.rolledback) {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }

                action = action.next;

                continue;
            }

            if (session == action.session ) {
                if (action.type == ACTION_DELETE) {
                    actionType = action.type;
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp == 0) {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp < threshold) {
                if (action.type == ACTION_DELETE) {
                    actionType = action.type;
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }
            } else {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }
            }

            action = action.next;

            continue;
        } while (action != null);

        return actionType == ACTION_NONE || actionType == ACTION_INSERT;
    }

    synchronized boolean canRead(Session session, int mode) {

        long threshold;
        int  actionType = ACTION_NONE;

        if (type == ACTION_DELETE_FINAL) {
            return false;
        }

        RowActionBase action = this;

        if (session == null) {
            threshold = Long.MAX_VALUE;
        } else {
            switch (session.isolationLevel) {

                case SessionInterface.TX_READ_UNCOMMITTED :
                    threshold = Long.MAX_VALUE;
                    break;

                case SessionInterface.TX_READ_COMMITTED :
                    threshold = session.actionTimestamp;
                    break;

                case SessionInterface.TX_REPEATABLE_READ :
                case SessionInterface.TX_SERIALIZABLE :
                default :
                    threshold = session.transactionTimestamp;
                    break;
            }
        }

        do {
            if (action.rolledback) {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }

                action = action.next;

                continue;
            }

            if (session == action.session) {
                if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }

                action = action.next;

                continue;
            } else if (action.commitTimestamp == 0) {
                if (action.type == ACTION_NONE) {

                    //
                } else if (action.type == ACTION_INSERT) {
                    if (mode == TransactionManager.ACTION_DUP) {
                        actionType = ACTION_INSERT;

//                        actionType = ACTION_DELETE;
                        break;
                    } else if (mode == TransactionManager.ACTION_REF) {
                        actionType = ACTION_DELETE;
                    }
                } else if (action.type == ACTION_DELETE) {
                    if (mode == TransactionManager.ACTION_DUP) {

                        //
                    } else if (mode == TransactionManager.ACTION_REF) {
                        actionType = ACTION_DELETE;
                    }
                }

                action = action.next;

                continue;
            }

            if (action.commitTimestamp < threshold) {
                if (action.type == ACTION_DELETE) {
                    if (actionType == ACTION_INSERT) {
                        actionType = ACTION_INSERT_DELETE;
                    } else {
                        actionType = action.type;
                    }
                } else if (action.type == ACTION_INSERT) {
                    actionType = action.type;
                }
            } else {
                if (action.type == ACTION_INSERT) {
                    actionType = ACTION_DELETE;
                }
            }

            action = action.next;

            continue;
        } while (action != null);

        if (actionType == ACTION_NONE || actionType == ACTION_INSERT) {
            return true;
        }

        return false;
    }
}
