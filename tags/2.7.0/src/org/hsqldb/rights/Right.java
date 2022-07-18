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


package org.hsqldb.rights;

import org.hsqldb.Expression;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Table;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Represents the set of rights on a database object
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.7.0
 * @since 1.9.0
 */
public final class Right {

    boolean        isFull;
    boolean        isFullSelect;
    boolean        isFullInsert;
    boolean        isFullUpdate;
    boolean        isFullDelete;
    boolean        isFullReferences;
    boolean        isFullTrigger;
    boolean        isFullExecute;    // used only in temporary Right object
    boolean        isFullUsage;      // ditto
    OrderedHashSet selectColumnSet;
    OrderedHashSet insertColumnSet;
    OrderedHashSet updateColumnSet;
    OrderedHashSet referencesColumnSet;
    OrderedHashSet triggerColumnSet;
    Expression     selectFilter;
    Expression     insertFilter;
    Expression     deleteFilter;
    Expression     updateFilter;
    String         selectFilterSQL;
    String         insertFilterSQL;
    String         deleteFilterSQL;
    String         updateFilterSQL;

    //
    Right   grantableRights;
    Grantee grantor;
    Grantee grantee;

    //
    public static final OrderedHashSet emptySet   = new OrderedHashSet();
    public static final Right          fullRights = new Right(true);
    public static final Right          noRights   = new Right();

    static {
        fullRights.grantor = GranteeManager.systemAuthorisation;
    }

    public static final String[] tablePrivilegeNames = {
        Tokens.T_SELECT, Tokens.T_INSERT, Tokens.T_UPDATE, Tokens.T_DELETE,
        Tokens.T_REFERENCES, Tokens.T_TRIGGER
    };
    public static final int[] tablePrivilegeTypes = {
        GrantConstants.SELECT, GrantConstants.INSERT, GrantConstants.UPDATE,
        GrantConstants.DELETE, GrantConstants.REFERENCES,
        GrantConstants.TRIGGER
    };

    public Right() {
        this.isFull = false;
    }

    public Right(boolean full) {
        isFull = full;
    }

    public boolean isFull() {
        return isFull;
    }

    public Grantee getGrantor() {
        return grantor;
    }

    public Grantee getGrantee() {
        return grantee;
    }

    public Right getGrantableRights() {
        return grantableRights == null ? noRights
                                       : grantableRights;
    }

    Right duplicate() {

        Right right = new Right();

        right.add(this);

        right.selectFilter    = selectFilter;
        right.selectFilterSQL = selectFilterSQL;
        right.deleteFilter    = deleteFilter;
        right.deleteFilterSQL = deleteFilterSQL;
        right.insertFilter    = insertFilter;
        right.insertFilterSQL = insertFilterSQL;
        right.updateFilter    = updateFilter;
        right.updateFilterSQL = updateFilterSQL;

        return right;
    }

    public void setFilterExpression(Expression filter, String sql) {

        if (canAccess(GrantConstants.SELECT)) {
            selectFilter    = filter;
            selectFilterSQL = sql;
        }

        if (canAccess(GrantConstants.DELETE)) {
            deleteFilter    = filter;
            deleteFilterSQL = sql;
        }

        if (canAccess(GrantConstants.INSERT)) {
            insertFilter    = filter;
            insertFilterSQL = sql;
        }

        if (canAccess(GrantConstants.UPDATE)) {
            updateFilter    = filter;
            updateFilterSQL = sql;
        }
    }

    public void setFilterExpression(int rightType, Expression filter,
                                    String sql) {

        switch (rightType) {

            case GrantConstants.SELECT :
                selectFilter    = filter;
                selectFilterSQL = sql;
                break;

            case GrantConstants.DELETE :
                deleteFilter    = filter;
                deleteFilterSQL = sql;
                break;

            case GrantConstants.INSERT :
                insertFilter    = filter;
                insertFilterSQL = sql;
                break;

            case GrantConstants.UPDATE :
                updateFilter    = filter;
                updateFilterSQL = sql;
                break;

            default :
        }
    }

    /**
     * Supports column level GRANT
     */
    void add(Right right) {

        if (isFull) {
            return;
        }

        if (right.isFull) {
            clear();

            isFull = true;

            return;
        }

        isFullSelect     |= right.isFullSelect;
        isFullInsert     |= right.isFullInsert;
        isFullUpdate     |= right.isFullUpdate;
        isFullReferences |= right.isFullReferences;
        isFullDelete     |= right.isFullDelete;

        if (isFullSelect) {
            selectColumnSet = null;
        } else {
            if (right.selectColumnSet != null) {
                if (selectColumnSet == null) {
                    selectColumnSet = new OrderedHashSet();
                }

                selectColumnSet.addAll(right.selectColumnSet);
            }

            if (right.selectFilter != null) {
                selectFilter    = right.selectFilter;
                selectFilterSQL = right.selectFilterSQL;
            }
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

        if (isFullTrigger) {
            triggerColumnSet = null;
        } else if (right.triggerColumnSet != null) {
            if (triggerColumnSet == null) {
                triggerColumnSet = new OrderedHashSet();
            }

            triggerColumnSet.addAll(right.triggerColumnSet);
        }
    }

    /**
     * supports column level REVOKE
     */
    void remove(SchemaObject object, Right right) {

        if (right.isFull) {
            clear();

            return;
        }

        if (isFull) {
            isFull = false;
            isFullSelect = isFullInsert = isFullUpdate = isFullReferences =
                isFullDelete = true;
        }

        if (right.isFullDelete) {
            isFullDelete = false;
        }

        if (!isFullSelect && selectColumnSet == null) {

            //
        } else if (right.isFullSelect) {
            isFullSelect    = false;
            selectColumnSet = null;
        } else if (right.selectColumnSet != null) {
            if (isFullSelect) {
                isFullSelect    = false;
                selectColumnSet = ((Table) object).getColumnNameSet();
            }

            selectColumnSet.removeAll(right.selectColumnSet);

            if (selectColumnSet.isEmpty()) {
                selectColumnSet = null;
            }
        }

        if (!isFullInsert && insertColumnSet == null) {

            //
        } else if (right.isFullInsert) {
            isFullInsert    = false;
            insertColumnSet = null;
        } else if (right.insertColumnSet != null) {
            if (isFullInsert) {
                isFullInsert    = false;
                insertColumnSet = ((Table) object).getColumnNameSet();
            }

            insertColumnSet.removeAll(right.insertColumnSet);

            if (insertColumnSet.isEmpty()) {
                insertColumnSet = null;
            }
        }

        if (!isFullUpdate && updateColumnSet == null) {

            //
        } else if (right.isFullUpdate) {
            isFullUpdate    = false;
            updateColumnSet = null;
        } else if (right.updateColumnSet != null) {
            if (isFullUpdate) {
                isFullUpdate    = false;
                updateColumnSet = ((Table) object).getColumnNameSet();
            }

            updateColumnSet.removeAll(right.updateColumnSet);

            if (updateColumnSet.isEmpty()) {
                updateColumnSet = null;
            }
        }

        if (!isFullReferences && referencesColumnSet == null) {

            //
        } else if (right.isFullReferences) {
            isFullReferences    = false;
            referencesColumnSet = null;
        } else if (right.referencesColumnSet != null) {
            if (isFullReferences) {
                isFullReferences    = false;
                referencesColumnSet = ((Table) object).getColumnNameSet();
            }

            referencesColumnSet.removeAll(right.referencesColumnSet);

            if (referencesColumnSet.isEmpty()) {
                referencesColumnSet = null;
            }
        }

        if (!isFullTrigger && triggerColumnSet == null) {

            //
        } else if (right.isFullTrigger) {
            isFullTrigger    = false;
            triggerColumnSet = null;
        } else if (right.triggerColumnSet != null) {
            if (isFullTrigger) {
                isFullTrigger    = false;
                triggerColumnSet = ((Table) object).getColumnNameSet();
            }

            triggerColumnSet.removeAll(right.triggerColumnSet);

            if (triggerColumnSet.isEmpty()) {
                triggerColumnSet = null;
            }
        }
    }

    void clear() {

        isFull = isFullSelect = isFullInsert = isFullUpdate =
            isFullReferences = isFullDelete = false;
        selectColumnSet = insertColumnSet = updateColumnSet =
            referencesColumnSet = triggerColumnSet = null;
    }

    /**
     * supports column level GRANT / REVOKE
     */
    boolean isEmpty() {

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

        if (triggerColumnSet != null && !triggerColumnSet.isEmpty()) {
            return false;
        }

        return true;
    }

    OrderedHashSet getColumnsForAllRights(Table table) {

        if (isFull) {
            return table.getColumnNameSet();
        }

        if (isFullSelect || isFullInsert || isFullUpdate || isFullReferences) {
            return table.getColumnNameSet();
        }

        OrderedHashSet set = new OrderedHashSet();

        if (selectColumnSet != null) {
            set.addAll(selectColumnSet);
        }

        if (insertColumnSet != null) {
            set.addAll(insertColumnSet);
        }

        if (updateColumnSet != null) {
            set.addAll(updateColumnSet);
        }

        if (referencesColumnSet != null) {
            set.addAll(referencesColumnSet);
        }

        return set;
    }

    boolean contains(Right right) {

        if (isFull) {
            return true;
        }

        if (right.isFull) {
            return false;
        }

        if (!containsRights(isFullSelect, selectColumnSet,
                            right.selectColumnSet, right.isFullSelect)) {
            return false;
        }

        if (!containsRights(isFullInsert, insertColumnSet,
                            right.insertColumnSet, right.isFullInsert)) {
            return false;
        }

        if (!containsRights(isFullUpdate, updateColumnSet,
                            right.updateColumnSet, right.isFullUpdate)) {
            return false;
        }

        if (!containsRights(isFullReferences, referencesColumnSet,
                            right.referencesColumnSet,
                            right.isFullReferences)) {
            return false;
        }

        if (!containsRights(isFullTrigger, triggerColumnSet,
                            right.triggerColumnSet, right.isFullTrigger)) {
            return false;
        }

        if (!isFullDelete && right.isFullDelete) {
            return false;
        }

        return true;
    }

    void removeDroppedColumns(OrderedHashSet columnSet, Table table) {

        for (int i = 0; i < columnSet.size(); i++) {
            HsqlName name = (HsqlName) columnSet.get(i);

            if (table.findColumn(name.name) >= 0) {
                columnSet.remove(i);

                i--;
            }
        }
    }

    public OrderedHashSet getColumnsForPrivilege(Table table, int type) {

        if (isFull) {
            return table.getColumnNameSet();
        }

        switch (type) {

            case GrantConstants.SELECT :
                return isFullSelect ? table.getColumnNameSet()
                                    : selectColumnSet == null ? emptySet
                                                              : selectColumnSet;

            case GrantConstants.INSERT :
                return isFullInsert ? table.getColumnNameSet()
                                    : insertColumnSet == null ? emptySet
                                                              : insertColumnSet;

            case GrantConstants.UPDATE :
                return isFullUpdate ? table.getColumnNameSet()
                                    : updateColumnSet == null ? emptySet
                                                              : updateColumnSet;

            case GrantConstants.REFERENCES :
                return isFullReferences ? table.getColumnNameSet()
                                        : referencesColumnSet == null
                                          ? emptySet
                                          : referencesColumnSet;

            case GrantConstants.TRIGGER :
                return isFullTrigger ? table.getColumnNameSet()
                                     : triggerColumnSet == null ? emptySet
                                                                : triggerColumnSet;
        }

        return emptySet;
    }

    /**
     * Supports column level checks
     */
    static boolean containsAllColumns(OrderedHashSet columnSet, Table table,
                                      boolean[] columnCheckList) {

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                if (columnSet == null) {
                    return false;
                }

                if (columnSet.contains(table.getColumn(i).getName())) {
                    continue;
                }

                return false;
            }
        }

        return true;
    }

    private static boolean containsRights(boolean isFull,
                                          OrderedHashSet columnSet,
                                          OrderedHashSet otherColumnSet,
                                          boolean otherIsFull) {

        if (isFull) {
            return true;
        }

        if (otherIsFull) {
            return false;
        }

        if (otherColumnSet != null
                && (columnSet == null
                    || !columnSet.containsAll(otherColumnSet))) {
            return false;
        }

        return true;
    }

    /**
     * Supports column level rights
     */
    boolean canSelect(Table table, boolean[] columnCheckList) {

        if (isFull || isFullSelect) {
            return true;
        }

        return containsAllColumns(selectColumnSet, table, columnCheckList);
    }

    /**
     * Supports column level rights
     */
    boolean canInsert(Table table, boolean[] columnCheckList) {

        if (isFull || isFullInsert) {
            return true;
        }

        return containsAllColumns(insertColumnSet, table, columnCheckList);
    }

    /**
     * Supports column level rights
     */
    boolean canUpdate(Table table, boolean[] columnCheckList) {

        if (isFull || isFullUpdate) {
            return true;
        }

        return containsAllColumns(updateColumnSet, table, columnCheckList);
    }

    /**
     * Supports column level rights
     */
    boolean canReference(Table table, boolean[] columnCheckList) {

        if (isFull || isFullReferences) {
            return true;
        }

        return containsAllColumns(referencesColumnSet, table, columnCheckList);
    }

    /**
     * Supports column level rights
     */
    boolean canTrigger(Table table, boolean[] columnCheckList) {

        if (isFull || isFullTrigger) {
            return true;
        }

        return containsAllColumns(triggerColumnSet, table, columnCheckList);
    }

    boolean canDelete() {
        return isFull || isFullDelete;
    }

    boolean canAccessNonSelect() {

        if (isFull) {
            return true;
        }

        if (isFullInsert || isFullUpdate || isFullDelete || isFullReferences
                || isFullTrigger) {
            return true;
        }

        boolean result = false;

        result |= (insertColumnSet != null && !insertColumnSet.isEmpty());
        result |= (updateColumnSet != null && !updateColumnSet.isEmpty());
        result |= referencesColumnSet != null
                  && !referencesColumnSet.isEmpty();
        result |= triggerColumnSet != null && !triggerColumnSet.isEmpty();

        return result;
    }

    /**
     * Supports column level rights
     */
    public boolean canAccess(int action) {

        if (isFull) {
            return true;
        }

        switch (action) {

            case GrantConstants.DELETE :
                return isFullDelete;

            case GrantConstants.SELECT :
                if (isFullSelect) {
                    return true;
                }

                return selectColumnSet != null && !selectColumnSet.isEmpty();

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

            case GrantConstants.REFERENCES :
                if (isFullReferences) {
                    return true;
                }

                return referencesColumnSet != null
                       && !referencesColumnSet.isEmpty();

            case GrantConstants.TRIGGER :
                if (isFullTrigger) {
                    return true;
                }

                return triggerColumnSet != null && !triggerColumnSet.isEmpty();

            case GrantConstants.EXECUTE :
                return isFull;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Right");
        }
    }

    boolean canAccess(Table table, int[] columnMap) {

        if (isFull) {
            return true;
        }

        if (isFullSelect || isFullInsert || isFullUpdate || isFullDelete
                || isFullReferences || isFullTrigger) {
            return true;
        }

        boolean result = false;

        result |= (selectColumnSet != null && insertColumnSet.isEmpty());
        result |= (insertColumnSet != null && insertColumnSet.isEmpty());
        result |= (updateColumnSet != null && !updateColumnSet.isEmpty());
        result |= referencesColumnSet != null
                  && !referencesColumnSet.isEmpty();
        result |= triggerColumnSet != null && !triggerColumnSet.isEmpty();

        if (!result) {
            return false;
        }

        HashSet set = new HashSet();

        set.addAll(selectColumnSet);
        set.addAll(insertColumnSet);
        set.addAll(updateColumnSet);
        set.addAll(referencesColumnSet);
        set.addAll(triggerColumnSet);

        for (int i = 0; i < columnMap.length; i++) {
            if (!set.contains(table.getColumn(i).getName())) {
                return false;
            }
        }

        return result;
    }

    public Expression getFilterExpression(int action) {

        switch (action) {

            case GrantConstants.SELECT :
                return selectFilter;

            case GrantConstants.DELETE :
                return deleteFilter;

            case GrantConstants.INSERT :
                return insertFilter;

            case GrantConstants.UPDATE :
                return updateFilter;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Right");
        }
    }

    public Expression[] getFiltersArray() {

        return new Expression[] {
            selectFilter, deleteFilter, insertFilter, updateFilter,
        };
    }

    boolean hasFilter() {
        return selectFilter != null || deleteFilter != null
               || insertFilter != null || updateFilter != null;
    }

    /**
     * supports column level GRANT
     */
    String getTableRightsSQL(Table table) {

        StringBuilder sb = new StringBuilder();

        if (isFull) {
            return Tokens.T_ALL;
        }

        if (isFullSelect) {
            sb.append(Tokens.T_SELECT);
            appendFilterSQL(selectFilterSQL, sb);
            sb.append(',');
        } else if (selectColumnSet != null) {
            sb.append(Tokens.T_SELECT);
            appendColumnList(table, selectColumnSet, sb);
            appendFilterSQL(selectFilterSQL, sb);
            sb.append(',');
        }

        if (isFullInsert) {
            sb.append(Tokens.T_INSERT);
            appendFilterSQL(insertFilterSQL, sb);
            sb.append(',');
        } else if (insertColumnSet != null) {
            sb.append(Tokens.T_INSERT);
            appendColumnList(table, insertColumnSet, sb);
            appendFilterSQL(insertFilterSQL, sb);
            sb.append(',');
        }

        if (isFullUpdate) {
            sb.append(Tokens.T_UPDATE);
            appendFilterSQL(updateFilterSQL, sb);
            sb.append(',');
        } else if (updateColumnSet != null) {
            sb.append(Tokens.T_UPDATE);
            appendColumnList(table, updateColumnSet, sb);
            appendFilterSQL(updateFilterSQL, sb);
            sb.append(',');
        }

        if (isFullDelete) {
            sb.append(Tokens.T_DELETE);
            appendFilterSQL(deleteFilterSQL, sb);
            sb.append(',');
        }

        if (isFullReferences) {
            sb.append(Tokens.T_REFERENCES);
            sb.append(',');
        } else if (referencesColumnSet != null) {
            sb.append(Tokens.T_REFERENCES);
            sb.append(',');
        }

        if (isFullTrigger) {
            sb.append(Tokens.T_TRIGGER);
            sb.append(',');
        } else if (triggerColumnSet != null) {
            sb.append(Tokens.T_TRIGGER);
            sb.append(',');
        }

        return sb.toString().substring(0, sb.length() - 1);
    }

    static void appendFilterSQL(String sql, StringBuilder sb) {

        if (sql == null) {
            return;
        }

        sb.append(' ');
        sb.append(sql);
    }

    private static void appendColumnList(Table t, OrderedHashSet set,
                                         StringBuilder buf) {

        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name     = (HsqlName) set.get(i);
            int      colIndex = t.findColumn(name.name);

            if (colIndex == -1) {
                continue;
            }

            colCheckList[colIndex] = true;

            count++;
        }

        if (count == 0) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Right");
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

    void addNewColumn(HsqlName name) {

        if (selectColumnSet != null) {
            selectColumnSet.add(name);
        }

        if (insertColumnSet != null) {
            insertColumnSet.add(name);
        }

        if (updateColumnSet != null) {
            updateColumnSet.add(name);
        }

        if (referencesColumnSet != null) {
            referencesColumnSet.add(name);
        }

        if (triggerColumnSet != null) {
            triggerColumnSet.add(name);
        }
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

        if (triggerColumnSet != null) {
            setColumns(table, triggerColumnSet);
        }
    }

    private static void setColumns(Table t, OrderedHashSet set) {

        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();

        for (int i = 0; i < set.size(); i++) {
            String name     = (String) set.get(i);
            int    colIndex = t.findColumn(name);

            if (colIndex == -1) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            colCheckList[colIndex] = true;

            count++;
        }

        if (count == 0) {
            throw Error.error(ErrorCode.X_42501);
        }

        set.clear();

        for (int i = 0; i < colCheckList.length; i++) {
            if (colCheckList[i]) {
                set.add(t.getColumn(i).getName());
            }
        }
    }

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

            case GrantConstants.TRIGGER :
                if (set == null) {
                    isFullTrigger = true;
                }

                triggerColumnSet = set;
                break;

            case GrantConstants.EXECUTE :
                isFullExecute = true;
                break;

            case GrantConstants.USAGE :
                isFullUsage = true;
                break;

            default :
        }
    }
}
