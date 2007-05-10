/* Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb.rights;

import org.hsqldb.Column;
import org.hsqldb.Table;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.SchemaObject;

/**
 * Represents the set of rights
 */
public class Right {

    final boolean  isFull;
    boolean        isFullSelect;
    boolean        isFullInsert;
    boolean        isFullUpdate;
    boolean        isFullReferences;
    boolean        isFullDelete;
    OrderedHashSet selectColumnSet;
    OrderedHashSet insertColumnSet;
    OrderedHashSet updateColumnSet;
    OrderedHashSet referencesColumnSet;

    //
    Right                     adminRights;
    public static final Right fullRights = new Right(true);

    private Right(boolean isFull) {
        this.isFull = isFull;
    }

    public Right() {
        this.isFull = false;
    }

    Right(Table table) {

        isFull              = false;
        isFullDelete        = true;
        selectColumnSet     = table.getColumnSet();
        insertColumnSet     = table.getColumnSet();
        updateColumnSet     = table.getColumnSet();
        referencesColumnSet = table.getColumnSet();
    }

    public Right duplicate() {

        if (isFull) {
            return this;
        }

        Right right = new Right();

        right.add(this);

        return right;
    }

    /**
     * Supports column level GRANT
     */
    public void add(Right right) {

        if (isFull) {
            return;
        }

        isFullSelect     |= right.isFullSelect;
        isFullInsert     |= right.isFullInsert;
        isFullUpdate     |= right.isFullUpdate;
        isFullReferences |= right.isFullReferences;
        isFullDelete     |= right.isFullDelete;

        if (isFullSelect) {
            selectColumnSet = null;
        } else if (right.selectColumnSet != null) {
            if (selectColumnSet == null) {
                selectColumnSet = new OrderedHashSet();
            }

            selectColumnSet.addAll(right.selectColumnSet);
        }

        if (isFullInsert) {
            insertColumnSet = null;
        } else if (right.insertColumnSet != null) {
            if (insertColumnSet == null) {
                insertColumnSet = new OrderedHashSet();
            }

            insertColumnSet.addAll(right.insertColumnSet);
        }

        if (isFullUpdate) {
            updateColumnSet = null;
        } else if (right.updateColumnSet != null) {
            if (updateColumnSet == null) {
                updateColumnSet = new OrderedHashSet();
            }

            updateColumnSet.addAll(right.updateColumnSet);
        }

        if (isFullReferences) {
            referencesColumnSet = null;
        } else if (right.referencesColumnSet != null) {
            if (referencesColumnSet == null) {
                referencesColumnSet = new OrderedHashSet();
            }

            referencesColumnSet.addAll(right.referencesColumnSet);
        }
    }

    /**
     * supports column level REVOKE
     */
    public void remove(Right right) {

        if (right.isFullSelect) {
            isFullSelect    = false;
            selectColumnSet = null;
        } else if (right.selectColumnSet != null) {
            selectColumnSet.removeAll(right.selectColumnSet);

            if (selectColumnSet.isEmpty()) {
                selectColumnSet = null;
            }
        }

        if (right.isFullInsert) {
            isFullInsert    = false;
            insertColumnSet = null;
        } else if (right.insertColumnSet != null) {
            insertColumnSet.removeAll(right.insertColumnSet);

            if (insertColumnSet.isEmpty()) {
                insertColumnSet = null;
            }
        }

        if (right.isFullUpdate) {
            isFullUpdate    = false;
            updateColumnSet = null;
        } else if (right.updateColumnSet != null) {
            updateColumnSet.removeAll(right.updateColumnSet);

            if (updateColumnSet.isEmpty()) {
                updateColumnSet = null;
            }
        }

        if (right.isFullDelete) {
            isFullDelete = false;
        }

        if (right.isFullReferences) {
            isFullReferences    = false;
            referencesColumnSet = null;
        } else if (right.referencesColumnSet != null) {
            referencesColumnSet.removeAll(right.referencesColumnSet);

            if (referencesColumnSet.isEmpty()) {
                referencesColumnSet = null;
            }
        }
    }

    /**
     * supports column level GRANT / REVOKE
     */
    public boolean isEmpty() {

        if (isFull || isFullSelect || isFullInsert || isFullUpdate
                || isFullReferences || isFullDelete) {
            return false;
        }

        if (selectColumnSet != null && !selectColumnSet.isEmpty()) {
            return false;
        }

        if (insertColumnSet != null && !insertColumnSet.isEmpty()) {
            return false;
        }

        if (updateColumnSet != null && !updateColumnSet.isEmpty()) {
            return false;
        }

        if (referencesColumnSet != null && !referencesColumnSet.isEmpty()) {
            return false;
        }

        return true;
    }

    void removeDroppedColumns(OrderedHashSet columnSet, Table table) {

        for (int i = 0; i < columnSet.size(); i++) {
            Column column = (Column) columnSet.get(i);

            if (table.findColumn(column.getName().name) >= 0) {
                columnSet.remove(i);

                i--;
            }
        }
    }

    /**
     * Supports column level checks
     */
    boolean containsAllColumns(OrderedHashSet columnSet, Table table,
                               boolean[] columnCheckList) {

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                if (columnSet.contains(table.getColumn(i))) {
                    continue;
                }

                return false;
            }
        }

        return true;
    }

    /**
     * Supports column level checks
     */
    boolean canSelect(Table table, boolean[] columnCheckList) {

        if (isFull || isFullSelect) {
            return true;
        }

        OrderedHashSet columnSet = selectColumnSet;

        if (columnSet == null) {
            return false;
        }

        return containsAllColumns(columnSet, table, columnCheckList);
    }

    /**
     * Supports column level checks
     */
    boolean canInsert(Table table, boolean[] columnCheckList) {

        if (isFull || isFullInsert) {
            return true;
        }

        OrderedHashSet columnSet = insertColumnSet;

        if (columnSet == null) {
            return false;
        }

        return containsAllColumns(columnSet, table, columnCheckList);
    }

    /**
     * Supports column level checks
     */
    boolean canUpdate(Table table, boolean[] columnCheckList) {

        if (isFull || isFullUpdate) {
            return true;
        }

        OrderedHashSet columnSet = updateColumnSet;

        if (columnSet == null) {
            return false;
        }

        return containsAllColumns(columnSet, table, columnCheckList);
    }

    /**
     * Supports column level checks
     */
    boolean canReference(Table table, boolean[] columnCheckList) {

        if (isFull || isFullReferences) {
            return true;
        }

        OrderedHashSet columnSet = referencesColumnSet;

        if (columnSet == null) {
            return false;
        }

        return containsAllColumns(columnSet, table, columnCheckList);
    }

    boolean canDelete() {
        return isFull || isFullDelete;
    }

    /**
     * Supports column level checks
     */
    boolean canAccess(SchemaObject object, int privilegeType) {

        if (isFull) {
            return true;
        }

        switch (privilegeType) {

            case GrantConstants.SELECT :
                if (isFullSelect) {
                    return true;
                }
            case GrantConstants.INSERT :
                if (isFullInsert) {
                    return true;
                }

                return insertColumnSet != null && !insertColumnSet.isEmpty();

            case GrantConstants.UPDATE :
                if (isFullUpdate) {
                    return true;
                }

                return updateColumnSet != null && !updateColumnSet.isEmpty();

            case GrantConstants.DELETE :
                return isFullDelete;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Right");
        }
    }

    String getMethodRightsDDL() {

        if (isFull) {
            return Token.T_EXECUTE;
        }

        // must always be isFull
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "Right");
    }

    String getSequenceRightsDDL() {

        if (isFull) {
            return Token.T_USAGE;
        }

        // must always be isFull
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "Right");
    }

    /**
     * Not yet supporting column level GRANT
     */
    String getTableRightsDDL(Table table) {

        StringBuffer buf = new StringBuffer();

        if (isFull) {
            return Token.T_ALL;
        }

        if (isFullSelect) {
            buf.append(Token.T_SELECT);
            buf.append(',');
        } else if (selectColumnSet != null) {
            buf.append(Token.T_SELECT);
            getColumnList(table, selectColumnSet, buf);
            buf.append(',');
        }

        if (isFullInsert) {
            buf.append(Token.T_INSERT);
            getColumnList(table, insertColumnSet, buf);
            buf.append(',');
        } else if (insertColumnSet != null) {
            buf.append(Token.T_INSERT);
            getColumnList(table, insertColumnSet, buf);
            buf.append(',');
        }

        if (isFullUpdate) {
            buf.append(Token.T_UPDATE);
            buf.append(',');
        } else if (updateColumnSet != null) {
            buf.append(Token.T_UPDATE);
            getColumnList(table, updateColumnSet, buf);
            buf.append(',');
        }

        if (isFullDelete) {
            buf.append(Token.T_DELETE);
            buf.append(',');
        }

        if (isFullReferences) {
            buf.append(Token.T_REFERENCES);
            buf.append(',');
        } else if (referencesColumnSet != null) {
            buf.append(Token.T_REFERENCES);
            buf.append(',');
        }

        return buf.toString().substring(0, buf.length() - 1);
    }

    private static void getColumnList(Table t, OrderedHashSet set,
                                      StringBuffer buf) {

        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();

        for (int i = 0; i < set.size(); i++) {
            Column c        = (Column) set.get(i);
            int    colIndex = t.findColumn(c.getName().name);

            if (colIndex == -1) {
//                System.err.println(c.getName().name);

                continue;
            }

            colCheckList[colIndex] = true;

            count++;
        }

        if (count == 0) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Right");
        }

        buf.append('(');

        for (int i = 0, colCount = 0; i < colCheckList.length; i++) {
            if (colCheckList[i]) {
                colCount++;

                buf.append(t.getColumn(i).getName().statementName);

                if (colCount < count) {
                    buf.append(',');
                }
            }
        }

        buf.append(')');
    }

    public void setColumns(Table table) {

        if (selectColumnSet != null) {
            setColumns(table, selectColumnSet);
        }

        if (insertColumnSet != null) {
            setColumns(table, insertColumnSet);
        }

        if (updateColumnSet != null) {
            setColumns(table, updateColumnSet);
        }

        if (referencesColumnSet != null) {
            setColumns(table, referencesColumnSet);
        }
    }

    private void setColumns(Table t, OrderedHashSet set) {

        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();

        for (int i = 0; i < set.size(); i++) {
            String name     = (String) set.get(i);
            int    colIndex = t.findColumn(name);

            if (colIndex == -1) {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Right");
            }

            colCheckList[colIndex] = true;

            count++;
        }

        if (count == 0) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Right");
        }

        set.clear();

        for (int i = 0; i < colCheckList.length; i++) {
            if (colCheckList[i]) {
                set.add(t.getColumn(i));
            }
        }
    }

    /**
     * Supports column level GRANT / REVOKE
     */
    public void set(int type, OrderedHashSet set) {

        switch (type) {

            case GrantConstants.SELECT :
                if (set == null) {
                    isFullSelect = true;
                }

                selectColumnSet = set;
                break;

            case GrantConstants.DELETE :
                if (set == null) {
                    isFullDelete = true;
                }
                break;

            case GrantConstants.INSERT :
                if (set == null) {
                    isFullInsert = true;
                }

                insertColumnSet = set;
                break;

            case GrantConstants.UPDATE :
                if (set == null) {
                    isFullUpdate = true;
                }

                updateColumnSet = set;
                break;

            case GrantConstants.REFERENCES :
                if (set == null) {
                    isFullReferences = true;
                }

                referencesColumnSet = set;
                break;
        }
    }

    /**
     * Used solely by org.hsqldb.dbinfo in existing system tables lacking column
     * level reporting.<p>
     *
     * Returns names of individual rights instead of ALL
     */
    String[] getTableRightsArray() {

        if (isFull) {
            return new String[] {
                Token.T_SELECT, Token.T_INSERT, Token.T_UPDATE, Token.T_DELETE,
                Token.T_REFERENCES
            };
        }

        HsqlArrayList list  = new HsqlArrayList();
        String[]      array = new String[list.size()];

        if (isFullSelect) {
            list.add(Token.T_SELECT);
        }

        if (isFullInsert) {
            list.add(Token.T_INSERT);
        }

        if (isFullUpdate) {
            list.add(Token.T_UPDATE);
        }

        if (isFullDelete) {
            list.add(Token.T_DELETE);
        }

        if (isFullReferences) {
            list.add(Token.T_REFERENCES);
        }

        list.toArray(array);

        return array;
    }
}
