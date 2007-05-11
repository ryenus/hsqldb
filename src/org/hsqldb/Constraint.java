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
 * Copyright (c) 2001-2007, The HSQL Development Group
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
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;

// fredt@users 20020225 - patch 1.7.0 by boucherb@users - named constraints
// fredt@users 20020320 - doc 1.7.0 - update
// tony_lai@users 20020820 - patch 595156 - violation of Integrity constraint name

/**
 * Implementation of a table constraint with references to the indexes used
 * by the constraint.<p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class Constraint implements SchemaObject {

    /*
     SQL CLI codes

     Referential Constraint 0 CASCADE
     Referential Constraint 1 RESTRICT
     Referential Constraint 2 SET NULL
     Referential Constraint 3 NO ACTION
     Referential Constraint 4 SET DEFAULT
     */
    public static final int CASCADE        = 0,
                            SET_NULL       = 2,
                            NO_ACTION      = 3,
                            SET_DEFAULT    = 4,
                            INIT_DEFERRED  = 5,
                            INIT_IMMEDIATE = 6,
                            NOT_DEFERRABLE = 7;
    public static final int FOREIGN_KEY    = 0,
                            MAIN           = 1,
                            UNIQUE         = 2,
                            CHECK          = 3,
                            PRIMARY_KEY    = 4,
                            TEMP           = 5;
    ConstraintCore          core;
    private HsqlName        name;
    int                     constType;
    boolean                 isForward;

    //
    Expression    check;
    boolean       isNotNull;
    int           notNullColumnIndex;
    RangeVariable rangeVariable;

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
        constType = MAIN;
        core      = fkconstraint.core;
    }

    Constraint duplicate() {

        Constraint copy = new Constraint();

        copy.core               = core.duplicate();
        copy.name               = name;
        copy.constType          = constType;
        copy.isForward          = isForward;

        //
        copy.check              = check;
        copy.isNotNull          = isNotNull;
        copy.notNullColumnIndex = notNullColumnIndex;
        copy.rangeVariable      = rangeVariable;

        return copy;
    }

    // for temp constraints only
    OrderedHashSet mainColSet;
    OrderedHashSet refColSet;

    /**
     * General constructor for constraints.
     *
     * @param name name of constraint
     * @param refCols list of referencing columns
     * @param mainTable referenced table
     * @param mainCols list of referenced columns
     * @param type constraint type
     * @param deleteAction triggered action on delete
     * @param updateAction triggered action on update
     * @throws HsqlException
     */
    public Constraint(HsqlName name, OrderedHashSet refCols, Table mainTable,
                      OrderedHashSet mainCols, int type, int deleteAction,
                      int updateAction) {

        core              = new ConstraintCore();
        this.name         = name;
        constType         = type;
        mainColSet        = mainCols;
        core.mainTable    = mainTable;
        refColSet         = refCols;
        core.deleteAction = deleteAction;
        core.updateAction = updateAction;
    }

    public Constraint(HsqlName name, OrderedHashSet mainCols, int type) {

        core       = new ConstraintCore();
        this.name  = name;
        constType  = type;
        mainColSet = mainCols;
    }

    void setColumnsIndexes(Table table) throws HsqlException {

        if (constType == Constraint.FOREIGN_KEY) {
            if (mainColSet == null) {
                core.mainCols = core.mainTable.getPrimaryKey();

                if (core.mainCols == null) {
                    throw Trace.error(Trace.TABLE_HAS_NO_PRIMARY_KEY);
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

    /**
     * Returns the HsqlName.
     */
    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

/*
        if (check == null) {
            return null;
        }

        OrderedHashSet set = new OrderedHashSet();

        Expression.collectAllExpressions(set, check,
                                         Expression.columnExpressionSet,
                                         Expression.emptyExpressionSet);

        OrderedHashSet nameSet = new OrderedHashSet();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = ((Expression) set.get(i)).getColumn().getName();

            nameSet.add(name);
        }

        return nameSet;
 */
        return null;
    }

    public void compile(Session session) throws HsqlException {}

    public HsqlName getMainName() {
        return core.mainName;
    }

    public HsqlName getRefName() {
        return core.refName;
    }

    /**
     *  Returns the type of constraint
     */
    public int getType() {
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
     *  The ON DELETE triggered action of (foreign key) constraint
     */
    public int getDeleteAction() {
        return core.deleteAction;
    }

    /**
     *  The ON UPDATE triggered action of (foreign key) constraint
     */
    public int getUpdateAction() {
        return core.updateAction;
    }

    public int getDeferability() {
        return NOT_DEFERRABLE;
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

    public String getCheckDDL() throws HsqlException {
        return check.getDDL();
    }

    /**
     *  Returns true if an index is part this constraint and the constraint is set for
     *  a foreign key. Used for tests before dropping an index.
     */
    boolean isIndexFK(Index index) {

        if (constType == FOREIGN_KEY || constType == MAIN) {
            if (core.mainIndex == index || core.refIndex == index) {
                return true;
            }
        }

        return false;
    }

    /**
     *  Returns true if an index is part this constraint and the constraint is set for
     *  a unique constraint. Used for tests before dropping an index.
     */
    boolean isIndexUnique(Index index) {
        return (constType == UNIQUE && core.mainIndex == index);
    }

    boolean hasColumnOnly(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) == 1;

            case PRIMARY_KEY :
            case UNIQUE :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex;

            case MAIN :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex
                       && core.mainTable == core.refTable;

            case FOREIGN_KEY :
                return core.refCols.length == 1 && core.refCols[0] == colIndex
                       && core.mainTable == core.refTable;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Constraint");
        }
    }

    boolean hasColumnPlus(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) > 1;

            case PRIMARY_KEY :
            case UNIQUE :
                return core.mainCols.length != 1
                       && ArrayUtil.find(core.mainCols, colIndex) != -1;

            case MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            case FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable == core.refTable);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Constraint");
        }
    }

    boolean hasColumn(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex];

            case PRIMARY_KEY :
            case UNIQUE :
            case MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1;

            case FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Constraint");
        }
    }

// fredt@users 20020225 - patch 1.7.0 by fredt - duplicate constraints

    /**
     * Compares this with another constraint column set. This is used only for
     * UNIQUE constraints.
     */
    boolean isUniqueWithColumns(int[] cols) {

        if (constType != UNIQUE || core.mainCols.length != cols.length) {
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

        if (constType != Constraint.MAIN
                && constType != Constraint.FOREIGN_KEY) {
            return false;
        }

        if (mainTable != core.mainTable || refTable != core.refTable) {
            return false;
        }

        return ArrayUtil.areEqualSets(core.mainCols, mainCols)
               && ArrayUtil.areEqualSets(core.refCols, refCols);
    }

    /**
     *  Used to update constrains to reflect structural changes in a table.
     *  Prior checks must ensure that this method does not throw.
     *
     * @param  oldt reference to the old version of the table
     * @param  newt referenct to the new version of the table
     * @param  colindex index at which table column is added or removed
     * @param  adjust -1, 0, +1 to indicate if column is added or removed
     * @throws  HsqlException
     */
    void updateTable(Session session, Table oldTable, Table newTable,
                     int colIndex, int adjust) throws HsqlException {

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
        if (constType == CHECK) {
            recompile(session, newTable);
        }
    }

    void recompile(Session session, Table newTable) throws HsqlException {

        if (constType != CHECK) {
            return;
        }

        String     ddl       = check.getDDL();
        Tokenizer  tokenizer = new Tokenizer(ddl);
        Parser     parser    = new Parser(session, tokenizer);
        Expression condition = parser.parseExpression();

        check = condition;

        // this workaround is here to stop LIKE optimisation (for proper scripting)
        check.setLikeOptimised();

        Select s = Expression.getCheckSelect(session, newTable, check);

        rangeVariable = s.rangeVariables[0];

        rangeVariable.setForCheckConstraint();
    }

    void renameTable(String oldName, String newName) {

        if (constType != CHECK) {
            return;
        }

        OrderedHashSet cols = new OrderedHashSet();

        Expression.collectAllExpressions(cols, check, Expression.COLUMN);

        for (int i = 0; i < cols.size(); i++) {
            Expression e = (Expression) cols.get(i);

            if (e.getTableName() == oldName) {
                e.setTableName(newName);
            }
        }
    }

    /**
     * Checks for foreign key or check constraint violation when
     * inserting a row into the child table.
     */
    void checkInsert(Session session, Table table,
                     Object[] row) throws HsqlException {

        switch (constType) {

            case CHECK :
                if (!isNotNull) {
                    checkCheckConstraint(session, table, row);
                }

                return;

            case FOREIGN_KEY :
                if (Index.isNull(row, core.refCols)) {
                    return;
                }

                // a record must exist in the main table
                boolean exists = core.mainIndex.exists(session, row,
                                                       core.refCols);

                if (!exists) {

                    // special case: self referencing table and self referencing row
                    if (core.mainTable == core.refTable) {
                        boolean match = true;

                        for (int i = 0; i < core.mainCols.length; i++) {
                            if (!row[core.refCols[i]].equals(
                                    row[core.mainCols[i]])) {
                                match = false;

                                break;
                            }
                        }

                        if (match) {
                            return;
                        }
                    }

                    throw Trace.error(
                        Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                        Trace.Constraint_violation, new Object[] {
                        core.refName.name, core.mainTable.getName().name
                    });
                }
        }
    }

    /*
     * Tests a row against this CHECK constraint.
     */
    void checkCheckConstraint(Session session, Table table,
                              Object[] data) throws HsqlException {

        if (session.compiledStatementExecutor.rangeIterators[0] == null) {
            session.compiledStatementExecutor.rangeIterators[0] =
                rangeVariable.getIterator(session);
        }

        session.compiledStatementExecutor.rangeIterators[0].currentData = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        session.compiledStatementExecutor.rangeIterators[0].currentData = null;

        if (nomatch) {
            throw Trace.error(Trace.CHECK_CONSTRAINT_VIOLATION,
                              Trace.Constraint_violation, new Object[] {
                name.name, table.tableName.name
            });
        }
    }

// fredt@users 20020225 - patch 1.7.0 - cascading deletes

    /**
     * New method to find any referencing row for a
     * foreign key (finds row in child table). If ON DELETE CASCADE is
     * supported by this constraint, then the method finds the first row
     * among the rows of the table ordered by the index and doesn't throw.
     * Without ON DELETE CASCADE, the method attempts to finds any row that
     * exists, in which case it throws an exception. If no row is found,
     * null is returned.
     * (fredt@users)
     *
     * @param  row array of objects for a database row
     * @param  forDelete should we allow 'ON DELETE CASCADE' or 'ON UPDATE CASCADE'
     * @return Node object or null
     * @throws  HsqlException
     */
    RowIterator findFkRef(Session session, Object[] row,
                          boolean delete) throws HsqlException {

        if (row == null || Index.isNull(row, core.mainCols)) {
            return core.refIndex.emptyIterator();
        }

        return delete
               ? core.refIndex.findFirstRowForDelete(session, row,
                   core.mainCols)
               : core.refIndex.findFirstRow(session, row, core.mainCols);
    }

    /**
     * For the candidate table row, finds any referring node in the main table.
     * This is used to check referential integrity when updating a node. We
     * have to make sure that the main table still holds a valid main record.
     * If a valid row is found the corresponding <code>Node</code> is returned.
     * Otherwise a 'INTEGRITY VIOLATION' Exception gets thrown.
     */
    boolean hasMainRef(Session session, Object[] row) throws HsqlException {

        if (Index.isNull(row, core.refCols)) {
            return false;
        }

        boolean exists = core.mainIndex.exists(session, row, core.refCols);

        // -- there has to be a valid node in the main table
        // --
        if (!exists) {
            throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                              Trace.Constraint_violation, new Object[] {
                core.refName.name, core.refTable.getName().name
            });
        }

        return exists;
    }

    /**
     * Test used before adding a new foreign key constraint. This method
     * returns true if the given row has a corresponding row in the main
     * table. Also returns true if any column covered by the foreign key
     * constraint has a null value.
     */
    private static boolean hasReferencedRow(Session session, Object[] rowdata,
            int[] rowColArray, Index mainIndex) throws HsqlException {

        if (Index.isNull(rowdata, rowColArray)) {
            return true;
        }

        // else a record must exist in the main index
        return mainIndex.exists(session, rowdata, rowColArray);
    }

    /**
     * Check used before creating a new foreign key cosntraint, this method
     * checks all rows of a table to ensure they all have a corresponding
     * row in the main table.
     */
    static void checkReferencedRows(Session session, Table table,
                                    int[] rowColArray,
                                    Index mainIndex) throws HsqlException {

        RowIterator it = table.getPrimaryIndex().firstRow(session);

        while (true) {
            Row row = it.getNext();

            if (row == null) {
                break;
            }

            Object[] rowData = row.getData();

            if (!Constraint.hasReferencedRow(session, rowData, rowColArray,
                                             mainIndex)) {
                String colValues = "";

                for (int i = 0; i < rowColArray.length; i++) {
                    Object o = rowData[rowColArray[i]];

                    colValues += o;
                    colValues += ",";
                }

                throw Trace.error(
                    Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                    Trace.Constraint_violation, new Object[] {
                    colValues, table.getName().name
                });
            }
        }
    }

    public Expression getCheckExpression() {
        return check;
    }

    public OrderedHashSet getCheckColumnExpressions() {

        OrderedHashSet set = new OrderedHashSet();

        Expression.collectAllExpressions(set, check, Expression.COLUMN);

        return set;
    }

    void prepareCheckConstraint(Session session,
                                Table table) throws HsqlException {

        // to ensure no subselects etc. are in condition
        check.checkValidCheckConstraint();

        // this workaround is here to stop LIKE optimisation
        check.setLikeOptimised();

        Select s = Expression.getCheckSelect(session, table, check);
        Result r = s.getResult(session, 1);

        if (r.getNavigator().getSize() != 0) {
            throw Trace.error(Trace.CHECK_CONSTRAINT_VIOLATION);
        }

        rangeVariable = s.rangeVariables[0];

        // removes reference to the Index object in range variable
        rangeVariable.setForCheckConstraint();

        if (check.exprType == Expression.NOT
                && check.eArg.exprType == Expression.IS_NULL
                && check.eArg.eArg.exprType == Expression.COLUMN) {
            notNullColumnIndex = check.eArg.eArg.getColumnIndex();
            isNotNull          = true;
        }
    }
}
