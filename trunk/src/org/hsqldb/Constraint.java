/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.RangeVariable.RangeIteratorBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;

// fredt@users 20020225 - patch 1.7.0 by boucherb@users - named constraints
// fredt@users 20020320 - doc 1.7.0 - update
// tony_lai@users 20020820 - patch 595156 - violation of Integrity constraint name

/**
 * Implementation of a table constraint with references to the indexes used
 * by the constraint.<p>
 *
 * Partly based on Hypersonic code.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public final class Constraint implements SchemaObject {

    ConstraintCore   core;
    private HsqlName name;
    int              constType;
    boolean          isForward;

    //
    Expression      check;
    String          checkStatement;
    private boolean isNotNull;
    int             notNullColumnIndex;
    RangeVariable   rangeVariable;
    OrderedHashSet  schemaObjectNames;

    // for temp constraints only
    OrderedHashSet mainColSet;
    OrderedHashSet refColSet;

    //
    final public static Constraint[] emptyArray = new Constraint[]{};

    /**
     *  Constructor declaration for PK and UNIQUE
     */
    public Constraint(HsqlName name, Table t, Index index, int type) {

        core           = new ConstraintCore();
        this.name      = name;
        constType      = type;
        core.mainTable = t;
        core.mainIndex = index;
        core.mainCols  = index.getColumns();
    }

    /**
     *  Constructor for main constraints (foreign key references in PK table)
     */
    public Constraint(HsqlName name, Constraint fkconstraint) {

        this.name = name;
        constType = SchemaObject.ConstraintTypes.MAIN;
        core      = fkconstraint.core;
    }

    Constraint duplicate() {

        Constraint copy = new Constraint();

        copy.core      = core.duplicate();
        copy.name      = name;
        copy.constType = constType;
        copy.isForward = isForward;

        //
        copy.check              = check;
        copy.isNotNull          = isNotNull;
        copy.notNullColumnIndex = notNullColumnIndex;
        copy.rangeVariable      = rangeVariable;
        copy.schemaObjectNames  = schemaObjectNames;

        return copy;
    }

    /**
     * General constructor for foreign key constraints.
     *
     * @param name name of constraint
     * @param refCols list of referencing columns
     * @param mainTableName referenced table
     * @param mainCols list of referenced columns
     * @param type constraint type
     * @param deleteAction triggered action on delete
     * @param updateAction triggered action on update
     *
     */
    public Constraint(HsqlName name, HsqlName refTableName,
                      OrderedHashSet refCols, HsqlName mainTableName,
                      OrderedHashSet mainCols, int type, int deleteAction,
                      int updateAction, int matchType) {

        core               = new ConstraintCore();
        this.name          = name;
        constType          = type;
        mainColSet         = mainCols;
        core.refTableName  = refTableName;
        core.mainTableName = mainTableName;
        refColSet          = refCols;
        core.deleteAction  = deleteAction;
        core.updateAction  = updateAction;
        core.matchType     = matchType;
    }

    public Constraint(HsqlName name, OrderedHashSet mainCols, int type) {

        core       = new ConstraintCore();
        this.name  = name;
        constType  = type;
        mainColSet = mainCols;
    }

    void setColumnsIndexes(Table table) {

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            if (mainColSet == null) {
                core.mainCols = core.mainTable.getPrimaryKey();

                if (core.mainCols == null) {
                    throw Error.error(ErrorCode.X_42581);
                }
            } else if (core.mainCols == null) {
                core.mainCols = core.mainTable.getColumnIndexes(mainColSet);
            }

            if (core.refCols == null) {
                core.refCols = table.getColumnIndexes(refColSet);
            }
        } else if (mainColSet != null) {
            core.mainCols = table.getColumnIndexes(mainColSet);
        }
    }

    private Constraint() {}

    public int getType() {
        return SchemaObject.CONSTRAINT;
    }

    /**
     * Returns the HsqlName.
     */
    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return schemaObjectNames;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                OrderedHashSet set = new OrderedHashSet();

                set.add(core.uniqueName);

                return set;
        }

        return null;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (getConstraintType()) {

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
                if (getMainColumns().length > 1
                        || (getMainColumns().length == 1
                            && !getName().isReservedName())) {
                    if (!getName().isReservedName()) {
                        sb.append(Tokens.T_CONSTRAINT).append(' ');
                        sb.append(getName().statementName).append(' ');
                    }

                    sb.append(Tokens.T_PRIMARY).append(' ').append(
                        Tokens.T_KEY);
                    sb.append(
                        getMain().getColumnListSQL(
                            getMainColumns(), getMainColumns().length));
                }
                break;

            case SchemaObject.ConstraintTypes.UNIQUE :
                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName);
                    sb.append(' ');
                }

                sb.append(Tokens.T_UNIQUE);

                int[] col = getMainColumns();

                sb.append(getMain().getColumnListSQL(col, col.length));
                break;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                if (isForward) {
                    sb.append(Tokens.T_ALTER).append(' ').append(
                        Tokens.T_TABLE).append(' ');
                    sb.append(
                        getRef().getName().getSchemaQualifiedStatementName());
                    sb.append(' ').append(Tokens.T_ADD).append(' ');
                    getFKStatement(sb);
                } else {
                    getFKStatement(sb);
                }
                break;

            case SchemaObject.ConstraintTypes.CHECK :
                if (isNotNull()) {
                    break;
                }

                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName).append(' ');
                }

                sb.append(Tokens.T_CHECK).append('(');
                sb.append(check.getSQL());
                sb.append(')');

                // should not throw as it is already tested OK
                break;
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    /**
     * Generates the foreign key declaration for a given Constraint object.
     */
    private void getFKStatement(StringBuffer sb) {

        if (!getName().isReservedName()) {
            sb.append(Tokens.T_CONSTRAINT).append(' ');
            sb.append(getName().statementName);
            sb.append(' ');
        }

        sb.append(Tokens.T_FOREIGN).append(' ').append(Tokens.T_KEY);

        int[] col = getRefColumns();

        sb.append(getRef().getColumnListSQL(col, col.length));
        sb.append(' ').append(Tokens.T_REFERENCES).append(' ');
        sb.append(getMain().getName().getSchemaQualifiedStatementName());

        col = getMainColumns();

        sb.append(getMain().getColumnListSQL(col, col.length));

        if (getDeleteAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_DELETE).append(' ');
            sb.append(getDeleteActionString());
        }

        if (getUpdateAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_UPDATE).append(' ');
            sb.append(getUpdateActionString());
        }
    }

    public HsqlName getMainTableName() {
        return core.mainTableName;
    }

    public HsqlName getMainName() {
        return core.mainName;
    }

    public HsqlName getRefName() {
        return core.refName;
    }

    public HsqlName getUniqueName() {
        return core.uniqueName;
    }

    /**
     *  Returns the type of constraint
     */
    public int getConstraintType() {
        return constType;
    }

    /**
     *  Returns the main table
     */
    public Table getMain() {
        return core.mainTable;
    }

    /**
     *  Returns the main index
     */
    Index getMainIndex() {
        return core.mainIndex;
    }

    /**
     *  Returns the reference table
     */
    public Table getRef() {
        return core.refTable;
    }

    /**
     *  Returns the reference index
     */
    Index getRefIndex() {
        return core.refIndex;
    }

    /**
     * Returns the foreign key action rule.
     */
    private static String getActionString(int action) {

        switch (action) {

            case SchemaObject.ReferentialAction.RESTRICT :
                return Tokens.T_RESTRICT;

            case SchemaObject.ReferentialAction.CASCADE :
                return Tokens.T_CASCADE;

            case SchemaObject.ReferentialAction.SET_DEFAULT :
                return Tokens.T_SET + ' ' + Tokens.T_DEFAULT;

            case SchemaObject.ReferentialAction.SET_NULL :
                return Tokens.T_SET + ' ' + Tokens.T_NULL;

            default :
                return Tokens.T_NO + ' ' + Tokens.T_ACTION;
        }
    }

    /**
     *  The ON DELETE triggered action of (foreign key) constraint
     */
    public int getDeleteAction() {
        return core.deleteAction;
    }

    public String getDeleteActionString() {
        return getActionString(core.deleteAction);
    }

    /**
     *  The ON UPDATE triggered action of (foreign key) constraint
     */
    public int getUpdateAction() {
        return core.updateAction;
    }

    public String getUpdateActionString() {
        return getActionString(core.updateAction);
    }

    public boolean hasTriggeredAction() {

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            switch (core.deleteAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }

            switch (core.updateAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }
        }

        return false;
    }

    public int getDeferability() {
        return SchemaObject.Deferable.NOT_DEFERRABLE;
    }

    /**
     *  Returns the main table column index array
     */
    public int[] getMainColumns() {
        return core.mainCols;
    }

    /**
     *  Returns the reference table column index array
     */
    public int[] getRefColumns() {
        return core.refCols;
    }

    /**
     * Returns the SQL for the expression in CHECK clause
     */
    public String getCheckSQL() {
        return check.getSQL();
    }

    /**
     * Returns true if the expression in CHECK is a simple IS NOT NULL
     */
    public boolean isNotNull() {
        return isNotNull;
    }

    boolean hasColumnOnly(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) == 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex;

            case SchemaObject.ConstraintTypes.MAIN :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex
                       && core.mainTable == core.refTable;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return core.refCols.length == 1 && core.refCols[0] == colIndex
                       && core.mainTable == core.refTable;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumnPlus(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) > 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length != 1
                       && ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable == core.refTable);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumn(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex];

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

// fredt@users 20020225 - patch 1.7.0 by fredt - duplicate constraints

    /**
     * Compares this with another constraint column set. This is used only for
     * UNIQUE constraints.
     */
    boolean isUniqueWithColumns(int[] cols) {

        if (constType != SchemaObject.ConstraintTypes.UNIQUE
                || core.mainCols.length != cols.length) {
            return false;
        }

        return ArrayUtil.haveEqualSets(core.mainCols, cols, cols.length);
    }

    /**
     * Compares this with another constraint column set. This implementation
     * only checks FOREIGN KEY constraints.
     */
    boolean isEquivalent(Table mainTable, int[] mainCols, Table refTable,
                         int[] refCols) {

        if (constType != SchemaObject.ConstraintTypes.MAIN
                && constType != SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            return false;
        }

        if (mainTable != core.mainTable || refTable != core.refTable) {
            return false;
        }

        return ArrayUtil.areEqualSets(core.mainCols, mainCols)
               && ArrayUtil.areEqualSets(core.refCols, refCols);
    }

    /**
     * Used to update constrains to reflect structural changes in a table. Prior
     * checks must ensure that this method does not throw.
     *
     * @param session Session
     * @param oldTable reference to the old version of the table
     * @param newTable referenct to the new version of the table
     * @param colIndex index at which table column is added or removed
     * @param adjust -1, 0, +1 to indicate if column is added or removed
     * @
     */
    void updateTable(Session session, Table oldTable, Table newTable,
                     int colIndex, int adjust) {

        if (oldTable == core.mainTable) {
            core.mainTable = newTable;

            if (core.mainIndex != null) {
                core.mainIndex =
                    core.mainTable.getIndex(core.mainIndex.getName().name);
                core.mainCols = ArrayUtil.toAdjustedColumnArray(core.mainCols,
                        colIndex, adjust);
            }
        }

        if (oldTable == core.refTable) {
            core.refTable = newTable;

            if (core.refIndex != null) {
                core.refIndex =
                    core.refTable.getIndex(core.refIndex.getName().name);
                core.refCols = ArrayUtil.toAdjustedColumnArray(core.refCols,
                        colIndex, adjust);
            }
        }

        // CHECK
        if (constType == SchemaObject.ConstraintTypes.CHECK) {
            recompile(session, newTable);
        }
    }

    /**
     * Checks for foreign key or check constraint violation when
     * inserting a row into the child table.
     */
    void checkInsert(Session session, Table table, Object[] row) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                if (!isNotNull) {
                    checkCheckConstraint(session, table, row);
                }

                return;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                PersistentStore store =
                    session.sessionData.getRowStore(core.mainTable);

                if (ArrayUtil.hasNull(row, core.refCols)) {
                    if (core.matchType == OpTypes.MATCH_SIMPLE) {
                        return;
                    }

                    if (core.refCols.length == 1) {
                        return;
                    }

                    if (ArrayUtil.hasAllNull(row, core.refCols)) {
                        return;
                    }

                    // core.matchType == OpTypes.MATCH_FULL
                } else if (core.mainIndex.exists(session, store, row,
                                                 core.refCols)) {
                    return;
                } else if (core.mainTable == core.refTable) {

                    // special case: self referencing table and self referencing row
                    int compare = core.mainIndex.compareRowNonUnique(row,
                        core.refCols, row);

                    if (compare == 0) {
                        return;
                    }
                }

                String[] info = new String[] {
                    core.refName.name, core.mainTable.getName().name
                };

                throw Error.error(null, ErrorCode.X_23503,
                                  ErrorCode.CONSTRAINT, info);
        }
    }

    /*
     * Tests a row against this CHECK constraint.
     */
    void checkCheckConstraint(Session session, Table table, Object[] data) {

/*
        if (session.compiledStatementExecutor.rangeIterators[1] == null) {
            session.compiledStatementExecutor.rangeIterators[1] =
                rangeVariable.getIterator(session);
        }
*/
        RangeIteratorBase it =
            session.sessionContext.getCheckIterator(rangeVariable);

        it.currentData = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        it.currentData = null;

        if (nomatch) {
            String[] info = new String[] {
                name.name, table.tableName.name
            };

            throw Error.error(null, ErrorCode.X_23513, ErrorCode.CONSTRAINT,
                              info);
        }
    }

    void checkCheckConstraint(Session session, Table table, Object data) {

        session.sessionData.currentValue = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        session.sessionData.currentValue = null;

        if (nomatch) {
            if (table == null) {
                throw Error.error(ErrorCode.X_23513, name.name);
            } else {
                String[] info = new String[] {
                    name.name, table.tableName.name
                };

                throw Error.error(null, ErrorCode.X_23513,
                                  ErrorCode.CONSTRAINT, info);
            }
        }
    }

// fredt@users 20020225 - patch 1.7.0 - cascading deletes

    /**
     * New method to find any referencing row for a foreign key (finds row in
     * child table). If ON DELETE CASCADE is specified for this constraint, then
     * the method finds the first row among the rows of the table ordered by the
     * index and doesn't throw. Without ON DELETE CASCADE, the method attempts
     * to finds any row that exists. If no
     * row is found, null is returned. (fredt@users)
     *
     * @param session Session
     * @param row array of objects for a database row
     * @param delete should we allow 'ON DELETE CASCADE' or 'ON UPDATE CASCADE'
     * @return iterator
     * @
     */
    RowIterator findFkRef(Session session, Object[] row, boolean delete) {

        if (row == null || ArrayUtil.hasNull(row, core.mainCols)) {
            return core.refIndex.emptyIterator();
        }

        PersistentStore store = session.sessionData.getRowStore(core.refTable);

        return core.refIndex.findFirstRow(session, store, row, core.mainCols);
    }

    /**
     * For the candidate table row, finds any referring node in the main table.
     * This is used to check referential integrity when updating a node. We
     * have to make sure that the main table still holds a valid main record.
     * returns true If a valid row is found, false if there are null in the data
     * Otherwise a 'INTEGRITY VIOLATION' Exception gets thrown.
     */
    boolean checkHasMainRef(Session session, Object[] row) {

        if (ArrayUtil.hasNull(row, core.refCols)) {
            return false;
        }

        PersistentStore store =
            session.sessionData.getRowStore(core.mainTable);
        boolean exists = core.mainIndex.exists(session, store, row,
                                               core.refCols);

        if (!exists) {
            String[] info = new String[] {
                core.refName.name, core.mainTable.getName().name
            };

            throw Error.error(null, ErrorCode.X_23503, ErrorCode.CONSTRAINT,
                              info);
        }

        return exists;
    }

    /**
     * Check used before creating a new foreign key cosntraint, this method
     * checks all rows of a table to ensure they all have a corresponding
     * row in the main table.
     */
    void checkReferencedRows(Session session, Table table, int[] rowColArray) {

        Index           mainIndex = getMainIndex();
        PersistentStore mainStore = session.sessionData.getRowStore(getMain());
        RowIterator     it        = table.rowIterator(session);

        while (true) {
            Row row = it.getNextRow();

            if (row == null) {
                break;
            }

            Object[] rowData = row.getData();

            if (ArrayUtil.hasNull(rowData, rowColArray)) {
                if (core.matchType == OpTypes.MATCH_SIMPLE) {
                    continue;
                }
            } else if (mainIndex.exists(session, mainStore, rowData,
                                        rowColArray)) {
                continue;
            }

            if (ArrayUtil.hasAllNull(rowData, rowColArray)) {
                continue;
            }

            StringBuffer sb = new StringBuffer();

            for (int i = 0; i < rowColArray.length; i++) {
                Object o = rowData[rowColArray[i]];

                sb.append(
                    table.getColumnTypes()[rowColArray[i]].convertToString(o));
                sb.append(',');
            }

            String[] info = new String[] {
                getName().name, getMain().getName().name, sb.toString()
            };

            throw Error.error(null, ErrorCode.X_23503, ErrorCode.CONSTRAINT,
                              info);
        }
    }

    public Expression getCheckExpression() {
        return check;
    }

    public OrderedHashSet getCheckColumnExpressions() {

        OrderedHashSet set = new OrderedHashSet();

        Expression.collectAllExpressions(set, check,
                                         Expression.columnExpressionSet,
                                         Expression.emptyExpressionSet);

        return set;
    }

    void recompile(Session session, Table newTable) {

        String    ddl     = check.getSQL();
        Scanner   scanner = new Scanner(ddl);
        ParserDQL parser  = new ParserDQL(session, scanner);

        parser.read();

        parser.isCheckOrTriggerCondition = true;

        Expression condition = parser.XreadBooleanValueExpression();

        check             = condition;
        schemaObjectNames = parser.compileContext.getSchemaObjectNames();

        // this workaround is here to stop LIKE optimisation (for proper scripting)
        QuerySpecification s = Expression.getCheckSelect(session, newTable,
            check);

        rangeVariable = s.rangeVariables[0];

        rangeVariable.setForCheckConstraint();
    }

    void prepareCheckConstraint(Session session, Table table,
                                boolean checkValues) {

        // to ensure no subselects etc. are in condition
        check.checkValidCheckConstraint();

        if (table == null) {
            check.resolveTypes(session, null);
        } else {
            QuerySpecification s = Expression.getCheckSelect(session, table,
                check);
            Result r = s.getResult(session, 1);

            if (r.getNavigator().getSize() != 0) {
                String[] info = new String[] {
                    table.getName().name, ""
                };

                throw Error.error(null, ErrorCode.X_23513,
                                  ErrorCode.CONSTRAINT, info);
            }

            rangeVariable = s.rangeVariables[0];

            // removes reference to the Index object in range variable
            rangeVariable.setForCheckConstraint();
        }

        if (check.getType() == OpTypes.NOT
                && check.getLeftNode().getType() == OpTypes.IS_NULL
                && check.getLeftNode().getLeftNode().getType()
                   == OpTypes.COLUMN) {
            notNullColumnIndex =
                check.getLeftNode().getLeftNode().getColumnIndex();
            isNotNull = true;
        }
    }
}
