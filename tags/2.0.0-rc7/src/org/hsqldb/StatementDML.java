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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.navigator.RowSetNavigatorLinkedList;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.rights.GrantConstants;
import org.hsqldb.types.Type;

/**
 * Implementation of Statement for DML statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */

// support for ON UPDATE CASCADE etc. | ON DELETE SET NULL etc. by Sebastian Kloska (kloska@users dot ...)
// support for MERGE statement by Justin Spadea (jzs9783@users dot sourceforge.net)
public class StatementDML extends StatementDMQL {

    public StatementDML(int type, int group, HsqlName schemaName) {
        super(type, group, schemaName);
    }

    /**
     * Instantiate this as a DELETE statement
     */
    StatementDML(Session session, Table targetTable,
                 RangeVariable[] rangeVars, CompileContext compileContext,
                 boolean restartIdentity) {

        super(StatementTypes.DELETE_WHERE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targetTable            = targetTable;
        this.baseTable              = targetTable.getBaseTable();
        this.targetRangeVariables   = rangeVars;
        this.restartIdentity        = restartIdentity;
        this.isTransactionStatement = true;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as an UPDATE statement.
     */
    StatementDML(Session session, Table targetTable,
                 RangeVariable rangeVars[], int[] updateColumnMap,
                 Expression[] colExpressions, boolean[] checkColumns,
                 CompileContext compileContext) {

        super(StatementTypes.UPDATE_WHERE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targetTable            = targetTable;
        this.baseTable              = targetTable.getBaseTable();
        this.updateColumnMap        = updateColumnMap;
        this.updateExpressions      = colExpressions;
        this.updateCheckColumns     = checkColumns;
        this.targetRangeVariables   = rangeVars;
        this.isTransactionStatement = true;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a MERGE statement.
     */
    StatementDML(Session session, RangeVariable[] targetRangeVars,
                 int[] insertColMap, int[] updateColMap,
                 boolean[] checkColumns, Expression mergeCondition,
                 Expression insertExpr, Expression[] updateExpr,
                 CompileContext compileContext) {

        super(StatementTypes.MERGE, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.sourceTable          = targetRangeVars[0].rangeTable;
        this.targetTable          = targetRangeVars[1].rangeTable;
        this.baseTable            = targetTable.getBaseTable();
        this.insertCheckColumns   = checkColumns;
        this.insertColumnMap      = insertColMap;
        this.updateColumnMap      = updateColMap;
        this.insertExpression     = insertExpr;
        this.updateExpressions    = updateExpr;
        this.targetRangeVariables = targetRangeVars;
        this.condition            = mergeCondition;
        isTransactionStatement    = true;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a CURSOR operation statement.
     */
    StatementDML() {
        super(StatementTypes.UPDATE_CURSOR, StatementTypes.X_SQL_DATA_CHANGE,
              null);
    }

    Result getResult(Session session) {

        Result result = null;

        switch (type) {

            case StatementTypes.UPDATE_WHERE :
                result = executeUpdateStatement(session);
                break;

            case StatementTypes.MERGE :
                result = executeMergeStatement(session);
                break;

            case StatementTypes.DELETE_WHERE :
                result = executeDeleteStatement(session);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementDML");
        }

        return result;
    }

    // this fk references -> other  :  other read lock
    void collectTableNamesForRead(OrderedHashSet set) {

        if (!baseTable.isTemp()) {
            for (int i = 0; i < baseTable.fkConstraints.length; i++) {
                set.add(baseTable.fkConstraints[i].getMain().getName());
            }

            getTriggerTableNames(set, false);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();

            if (rangeTable.isReadOnly() || rangeTable.isTemp()) {
                continue;
            }

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            set.add(name);
        }

        for (int i = 0; i < subqueries.length; i++) {
            if (subqueries[i].queryExpression != null) {
                subqueries[i].queryExpression.getBaseTableNames(set);
            }
        }
    }

    void getTriggerTableNames(OrderedHashSet set, boolean write) {

        for (int i = 0; i < baseTable.triggerList.length; i++) {
            TriggerDef td = baseTable.triggerList[i];

            switch (type) {

                case StatementTypes.INSERT :
                    if (td.getPrivilegeType() == GrantConstants.INSERT) {
                        break;
                    }

                    continue;
                case StatementTypes.UPDATE_WHERE :
                    if (td.getPrivilegeType() == GrantConstants.UPDATE) {
                        break;
                    }

                    continue;
                case StatementTypes.DELETE_WHERE :
                    if (td.getPrivilegeType() == GrantConstants.DELETE) {
                        break;
                    }

                    continue;
                case StatementTypes.MERGE :
                    if (td.getPrivilegeType() == GrantConstants.INSERT
                            || td.getPrivilegeType()
                               == GrantConstants.UPDATE) {
                        break;
                    }

                    continue;
                default :
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "StatementDML");
            }

            for (int j = 0; j < td.statements.length; j++) {
                if (write) {
                    set.addAll(td.statements[j].getTableNamesForWrite());
                } else {
                    set.addAll(td.statements[j].getTableNamesForRead());
                }
            }
        }
    }

    /**
     * Executes an UPDATE statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @return the result of executing the statement
     */
    Result executeUpdateStatement(Session session) {

        int            count          = 0;
        Expression[]   colExpressions = updateExpressions;
        HashMappedList rowset         = new HashMappedList();
        Type[]         colTypes       = baseTable.getColumnTypes();
        RangeIterator it = RangeVariable.getIterator(session,
            targetRangeVariables);
        Expression checkCondition = null;

        if (targetTable != baseTable) {
            checkCondition =
                ((TableDerived) targetTable).getQueryExpression()
                    .getMainSelect().checkQueryCondition;
        }

        while (it.next()) {
            session.sessionData.startRowProcessing();

            Row      row  = it.getCurrentRow();
            Object[] data = row.getData();
            Object[] newData = getUpdatedData(session, baseTable,
                                              updateColumnMap, colExpressions,
                                              colTypes, data);

            if (checkCondition != null) {
                it.setCurrent(newData);

                boolean check = checkCondition.testCondition(session);

                if (!check) {
                    throw Error.error(ErrorCode.X_44000);
                }
            }

            rowset.add(row, newData);
        }

/* debug 190
        if (rowset.size() == 0) {
            System.out.println(targetTable.getName().name + " zero update: session "
                               + session.getId());
        } else if (rowset.size() >1) {
           System.out.println("multiple update: session "
                              + session.getId() + ", " + rowset.size());
       }

//* debug 190 */
        count = update(session, baseTable, rowset);

        baseTable.fireTriggers(session, Trigger.UPDATE_AFTER, rowset);

        if (count == 1) {
            return Result.updateOneResult;
        }

        return new Result(ResultConstants.UPDATECOUNT, count);
    }

    static Object[] getUpdatedData(Session session, Table targetTable,
                                   int[] columnMap,
                                   Expression[] colExpressions,
                                   Type[] colTypes, Object[] oldData) {

        Object[] data = targetTable.getEmptyRowData();

        System.arraycopy(oldData, 0, data, 0, data.length);

        for (int i = 0, ix = 0; i < columnMap.length; ) {
            Expression expr = colExpressions[ix++];

            if (expr.getType() == OpTypes.ROW) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int        colIndex = columnMap[i];
                    Expression e        = expr.nodes[j];

                    // transitional - still supporting null for identity generation
                    if (targetTable.identityColumn == colIndex) {
                        if (e.getType() == OpTypes.VALUE
                                && e.valueData == null) {
                            continue;
                        }
                    }

                    if (e.getType() == OpTypes.DEFAULT) {
                        if (targetTable.identityColumn == colIndex) {
                            continue;
                        }

                        data[colIndex] =
                            targetTable.colDefaults[colIndex].getValue(
                                session);

                        continue;
                    }

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], e.dataType);
                }
            } else if (expr.getType() == OpTypes.TABLE_SUBQUERY) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int colIndex = columnMap[i];
                    Type colType =
                        expr.subQuery.queryExpression.getMetaData()
                            .columnTypes[j];

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], colType);
                }
            } else {
                int colIndex = columnMap[i];

                if (expr.getType() == OpTypes.DEFAULT) {
                    if (targetTable.identityColumn == colIndex) {
                        i++;

                        continue;
                    }

                    data[colIndex] =
                        targetTable.colDefaults[colIndex].getValue(session);

                    i++;

                    continue;
                }

                data[colIndex] = expr.getValue(session, colTypes[colIndex]);

                i++;
            }
        }

        return data;
    }

    /**
     * Executes a MERGE statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @return Result object
     */
    Result executeMergeStatement(Session session) {

        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;
        PersistentStore store = session.sessionData.getRowStore(baseTable);

        if (generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        int count = 0;

        // data generated for non-matching rows
        RowSetNavigatorClient newData = new RowSetNavigatorClient(8);

        // rowset for update operation
        HashMappedList  updateRowSet       = new HashMappedList();
        RangeVariable[] joinRangeIterators = targetRangeVariables;

        // populate insert and update lists
        RangeIterator[] rangeIterators =
            new RangeIterator[joinRangeIterators.length];

        for (int i = 0; i < joinRangeIterators.length; i++) {
            rangeIterators[i] = joinRangeIterators[i].getIterator(session);
        }

        for (int currentIndex = 0; 0 <= currentIndex; ) {
            RangeIterator it          = rangeIterators[currentIndex];
            boolean       beforeFirst = it.isBeforeFirst();

            if (it.next()) {
                if (currentIndex < joinRangeIterators.length - 1) {
                    currentIndex++;

                    continue;
                }
            } else {
                if (currentIndex == 1 && beforeFirst
                        && insertExpression != null) {
                    Type[] colTypes = baseTable.getColumnTypes();
                    Object[] data =
                        getInsertData(session, colTypes,
                                      insertExpression.nodes[0].nodes);

                    if (data != null) {
                        newData.add(data);
                    }
                }

                it.reset();

                currentIndex--;

                continue;
            }

            // row matches!
            if (updateExpressions != null) {
                Row row = it.getCurrentRow();    // this is always the second iterator
                Object[] data = getUpdatedData(session, baseTable,
                                               updateColumnMap,
                                               updateExpressions,
                                               baseTable.getColumnTypes(),
                                               row.getData());

                updateRowSet.add(row, data);
            }
        }

        // run the transaction as a whole, updating and inserting where needed
        // update any matched rows
        if (updateRowSet.size() > 0) {
            count = update(session, baseTable, updateRowSet);

            baseTable.fireTriggers(session, Trigger.UPDATE_AFTER,
                                   updateRowSet);
        }

        // insert any non-matched rows
        if (newData.getSize() > 0) {
            newData.beforeFirst();

            while (newData.hasNext()) {
                Object[] data = (Object[]) newData.getNext();

                baseTable.insertRow(session, store, data);

                if (generatedNavigator != null) {
                    Object[] generatedValues = getGeneratedColumns(data);

                    generatedNavigator.add(generatedValues);
                }
            }

            newData.beforeFirst();
            baseTable.fireTriggers(session, Trigger.INSERT_AFTER, newData);

            count += newData.getSize();
        }

        if (resultOut == null) {
            if (count == 1) {
                return Result.updateOneResult;
            }

            return new Result(ResultConstants.UPDATECOUNT, count);
        } else {
            resultOut.setUpdateCount(count);

            return resultOut;
        }
    }

    /**
     * Highest level multiple row update method. Corresponds to an SQL UPDATE.
     * To deal with unique constraints we need to perform all deletes at once
     * before the inserts. If there is a UNIQUE constraint violation limited
     * only to the duration of updating multiple rows, we don't want to abort
     * the operation. Example: UPDATE MYTABLE SET UNIQUECOL = UNIQUECOL + 1
     * After performing each cascade update, delete the main row. After all
     * cascade ops and deletes have been performed, insert new rows.<p>
     *
     * Following clauses from SQL Standard section 11.8 are enforced 9) Let ISS
     * be the innermost SQL-statement being executed. 10) If evaluation of these
     * General Rules during the execution of ISS would cause an update of some
     * site to a value that is distinct from the value to which that site was
     * previously updated during the execution of ISS, then an exception
     * condition is raised: triggered data change violation. 11) If evaluation
     * of these General Rules during the execution of ISS would cause deletion
     * of a row containing a site that is identified for replacement in that
     * row, then an exception condition is raised: triggered data change
     * violation. (fredt)
     *
     * @param session Session
     * @param table Table
     * @param updateList HashMappedList
     * @return int
     */
    int update(Session session, Table table, HashMappedList updateList) {

        HashSet path = session.sessionContext.getConstraintPath();
        HashMappedList tableUpdateList =
            session.sessionContext.getTableUpdateList();

        // set identity column where null and check columns
        for (int i = 0; i < updateList.size(); i++) {
            Row      row  = (Row) updateList.getKey(i);
            Object[] data = (Object[]) updateList.get(i);

            /**
             * @todo 1.9.0 - make optional using database property -
             * this means the identity column can be set to null to force
             * creation of a new identity value
             */
            table.setIdentityColumn(session, data);

            if (table.triggerList.length > 0) {
                table.fireTriggers(session, Trigger.UPDATE_BEFORE_ROW,
                                   row.getData(), data, updateColumnMap);
            }

            table.setGeneratedColumns(session, data);
            table.enforceRowConstraints(session, data);
        }

        if (table.isView) {
            return updateList.size();
        }

        // perform check/cascade operations
        if (session.database.isReferentialIntegrity()) {
            for (int i = 0; i < updateList.size(); i++) {
                Object[] data = (Object[]) updateList.get(i);
                Row      row  = (Row) updateList.getKey(i);

                checkCascadeUpdate(session, table, tableUpdateList, row, data,
                                   updateColumnMap, null, path);
            }
        }

        // merge any triggered change to this table with the update list
        HashMappedList triggeredList =
            (HashMappedList) tableUpdateList.get(table);

        if (triggeredList != null) {
            for (int i = 0; i < triggeredList.size(); i++) {
                Row      row  = (Row) triggeredList.getKey(i);
                Object[] data = (Object[]) triggeredList.get(i);

                mergeKeepUpdate(session, updateList, updateColumnMap,
                                table.colTypes, row, data);
            }

            triggeredList.clear();
        }

        // update lists - main list last
        for (int i = 0; i < tableUpdateList.size(); i++) {
            Table targetTable = (Table) tableUpdateList.getKey(i);
            HashMappedList updateListT =
                (HashMappedList) tableUpdateList.get(i);

            targetTable.updateRowSet(session, updateListT, null, true);
            updateListT.clear();
        }

        table.updateRowSet(session, updateList, updateColumnMap, false);
        baseTable.fireTriggers(session, Trigger.UPDATE_AFTER_ROW, updateList);
        path.clear();

        return updateList.size();
    }

    // fredt - currently deletes that fail due to referential constraints are caught
    // prior to actual delete operation

    /**
     * Executes a DELETE statement.  It is assumed that the argument is
     * of the correct type.
     *
     * @return the result of executing the statement
     */
    Result executeDeleteStatement(Session session) {

        int                       count   = 0;
        RowSetNavigatorLinkedList oldRows = new RowSetNavigatorLinkedList();
        RangeIterator it = RangeVariable.getIterator(session,
            targetRangeVariables);

        while (it.next()) {
            Row currentRow = it.getCurrentRow();

            oldRows.addRow(currentRow);
        }

        count = delete(session, baseTable, oldRows);

        if (restartIdentity && targetTable.identitySequence != null) {
            targetTable.identitySequence.reset();
        }

        if (count == 1) {
            return Result.updateOneResult;
        }

        return new Result(ResultConstants.UPDATECOUNT, count);
    }

    /**
     *  Highest level multiple row delete method. Corresponds to an SQL
     *  DELETE.
     */
    int delete(Session session, Table table, RowSetNavigator oldRows) {

        if (table.fkMainConstraints.length == 0) {
            deleteRows(session, table, oldRows);
            oldRows.beforeFirst();
            table.fireTriggers(session, Trigger.DELETE_AFTER, oldRows);

            return oldRows.getSize();
        }

        HashSet path = session.sessionContext.getConstraintPath();
        HashMappedList tableUpdateList =
            session.sessionContext.getTableUpdateList();

        if (session.database.isReferentialIntegrity()) {
            oldRows.beforeFirst();

            while (oldRows.hasNext()) {
                oldRows.next();

                Row row = oldRows.getCurrentRow();

                path.clear();
                checkCascadeDelete(session, table, tableUpdateList, row,
                                   false, path);
            }
        }

        if (session.database.isReferentialIntegrity()) {
            oldRows.beforeFirst();

            while (oldRows.hasNext()) {
                oldRows.next();

                Row row = oldRows.getCurrentRow();

                path.clear();
                checkCascadeDelete(session, table, tableUpdateList, row, true,
                                   path);
            }
        }

        oldRows.beforeFirst();

        while (oldRows.hasNext()) {
            oldRows.next();

            Row row = oldRows.getCurrentRow();

            if (!row.isDeleted(session)) {
                table.deleteNoRefCheck(session, row);
            }
        }

        for (int i = 0; i < tableUpdateList.size(); i++) {
            Table targetTable = (Table) tableUpdateList.getKey(i);
            HashMappedList updateList =
                (HashMappedList) tableUpdateList.get(i);

            if (updateList.size() > 0) {
                targetTable.updateRowSet(session, updateList, null, true);
                updateList.clear();
            }
        }

        oldRows.beforeFirst();
        table.fireTriggers(session, Trigger.DELETE_AFTER, oldRows);
        path.clear();

        return oldRows.getSize();
    }

    void deleteRows(Session session, Table table, RowSetNavigator oldRows) {

        while (oldRows.hasNext()) {
            oldRows.next();

            Row row = oldRows.getCurrentRow();

            if (!row.isDeleted(session)) {
                table.deleteNoRefCheck(session, row);
            }
        }
    }

    // fredt@users 20020225 - patch 1.7.0 - CASCADING DELETES

    /**
     *  Method is called recursively on a tree of tables from the current one
     *  until no referring foreign-key table is left. In the process, if a
     *  non-cascading foreign-key referring table contains data, an exception
     *  is thrown. Parameter delete indicates whether to delete refering rows.
     *  The method is called first to check if the row can be deleted, then to
     *  delete the row and all the refering rows.<p>
     *
     *  Support added for SET NULL and SET DEFAULT by kloska@users involves
     *  switching to checkCascadeUpdate(,,,,) when these rules are encountered
     *  in the constraint.(fredt@users)
     *
     * @param session current session
     * @param  table table to delete from
     * @param  tableUpdateList list of update lists
     * @param  row row to delete
     * @param  delete action
     * @param  path constraint path
     * @throws  HsqlException
     */
    static void checkCascadeDelete(Session session, Table table,
                                   HashMappedList tableUpdateList, Row row,
                                   boolean delete, HashSet path) {

        for (int i = 0, size = table.fkMainConstraints.length; i < size; i++) {
            Constraint c = table.fkMainConstraints[i];
            RowIterator refiterator = c.findFkRef(session, row.getData(),
                                                  delete);

            if (!refiterator.hasNext()) {
                continue;
            }

            try {
                if (c.core.deleteAction == SchemaObject.ReferentialAction
                        .NO_ACTION || c.core.deleteAction == SchemaObject
                        .ReferentialAction.RESTRICT) {
                    if (c.core.mainTable == c.core.refTable) {
                        Row refrow = refiterator.getNextRow();

                        // fredt - it's the same row
                        // this supports deleting a single row
                        // in future we can iterate over and check against
                        // the full delete row list to enable multi-row
                        // with self-referencing FK's deletes
                        if (row.equals(refrow)) {
                            continue;
                        }
                    }

                    int errorCode =
                        c.core.deleteAction == SchemaObject.ReferentialAction
                            .NO_ACTION ? ErrorCode.X_23504
                                       : ErrorCode.X_23001;
                    String[] info = new String[] {
                        c.core.refName.name, c.core.refTable.getName().name
                    };

                    throw Error.error(null, errorCode, ErrorCode.CONSTRAINT,
                                      info);
                }

                Table reftable = c.getRef();

                // shortcut when deltable has no imported constraint
                boolean hasref = reftable.fkMainConstraints.length > 0;

                // if (reftable == this) we don't need to go further and can return ??
                if (!delete && !hasref) {
                    continue;
                }

                Index    refindex  = c.getRefIndex();
                int[]    m_columns = c.getMainColumns();
                int[]    r_columns = c.getRefColumns();
                Object[] mdata     = row.getData();
                boolean isUpdate =
                    c.getDeleteAction() == SchemaObject.ReferentialAction
                        .SET_NULL || c.getDeleteAction() == SchemaObject
                        .ReferentialAction.SET_DEFAULT;

                // -- list for records to be inserted if this is
                // -- a 'ON DELETE SET [NULL|DEFAULT]' constraint
                HashMappedList rowSet = null;

                if (isUpdate) {
                    rowSet = (HashMappedList) tableUpdateList.get(reftable);

                    if (rowSet == null) {
                        rowSet = new HashMappedList();

                        tableUpdateList.add(reftable, rowSet);
                    }
                }

                // walk the index for all the nodes that reference delnode
                for (;;) {
                    Row refrow = refiterator.getNextRow();

                    if (refrow == null || refrow.isDeleted(session)
                            || refindex.compareRowNonUnique(
                                session, mdata, m_columns,
                                refrow.getData()) != 0) {
                        break;
                    }

                    // -- if the constraint is a 'SET [DEFAULT|NULL]' constraint we have to keep
                    // -- a new record to be inserted after deleting the current. We also have to
                    // -- switch over to the 'checkCascadeUpdate' method below this level
                    if (isUpdate) {
                        Object[] rnd = reftable.getEmptyRowData();

                        System.arraycopy(refrow.getData(), 0, rnd, 0,
                                         rnd.length);

                        if (c.getDeleteAction()
                                == SchemaObject.ReferentialAction.SET_NULL) {
                            for (int j = 0; j < r_columns.length; j++) {
                                rnd[r_columns[j]] = null;
                            }
                        } else {
                            for (int j = 0; j < r_columns.length; j++) {
                                ColumnSchema col =
                                    reftable.getColumn(r_columns[j]);

                                rnd[r_columns[j]] =
                                    col.getDefaultValue(session);
                            }
                        }

                        if (hasref && path.add(c)) {

                            // fredt - avoid infinite recursion on circular references
                            // these can be rings of two or more mutually dependent tables
                            // so only one visit per constraint is allowed
                            checkCascadeUpdate(session, reftable, null,
                                               refrow, rnd, r_columns, null,
                                               path);
                            path.remove(c);
                        }

                        if (delete) {

                            //  foreign key referencing own table - do not update the row to be deleted
                            if (reftable != table || !refrow.equals(row)) {
                                mergeUpdate(rowSet, refrow, rnd, r_columns);
                            }
                        }
                    } else if (hasref) {
                        if (reftable != table) {
                            if (path.add(c)) {
                                checkCascadeDelete(session, reftable,
                                                   tableUpdateList, refrow,
                                                   delete, path);
                                path.remove(c);
                            }
                        } else {

                            // fredt - we avoid infinite recursion on the fk's referencing the same table
                            // but chained rows can result in very deep recursion and StackOverflowError
                            if (refrow.getPos() != row.getPos()) {
                                checkCascadeDelete(session, reftable,
                                                   tableUpdateList, refrow,
                                                   delete, path);
                            }
                        }
                    }

                    if (delete && !isUpdate && !refrow.isDeleted(session)) {
                        reftable.deleteRowAsTriggeredAction(session, refrow);
                    }
                }
            } finally {
                refiterator.release();
            }
        }
    }

    Object[] getInsertData(Session session, Type[] colTypes,
                           Expression[] rowArgs) {

        Object[] data = baseTable.getNewRowData(session);

        session.sessionData.startRowProcessing();

        for (int i = 0; i < rowArgs.length; i++) {
            Expression e        = rowArgs[i];
            int        colIndex = insertColumnMap[i];

            if (e.opType == OpTypes.DEFAULT) {
                if (baseTable.identityColumn == colIndex) {
                    continue;
                }

                if (baseTable.colDefaults[colIndex] != null) {
                    data[colIndex] =
                        baseTable.colDefaults[colIndex].getValue(session);

                    continue;
                }

                continue;
            }

            Object value = e.getValue(session);
            Type   type  = colTypes[colIndex];

            if (colTypes[colIndex] != e.dataType) {
                value = type.convertToType(session, value, e.dataType);
            }

            data[colIndex] = value;
        }

        return data;
    }

    /**
     * Check or perform an update cascade operation on a single row. Check or
     * cascade an update (delete/insert) operation. The method takes a pair of
     * rows (new data,old data) and checks if Constraints permit the update
     * operation. A boolean arguement determines if the operation should realy
     * take place or if we just have to check for constraint violation. fredt -
     * cyclic conditions are now avoided by checking for second visit to each
     * constraint. The set of list of updates for all tables is passed and
     * filled in recursive calls.
     *
     * @param session current database session
     * @param table table to check
     * @param tableUpdateLists lists of updates
     * @param orow old row data to be deleted.
     * @param nrow new row data to be inserted.
     * @param cols indices of the columns actually changed.
     * @param ref This should be initialized to null when the method is called
     *   from the 'outside'. During recursion this will be the current table
     *   (i.e. this) to indicate from where we came. Foreign keys to this table
     *   do not have to be checked since they have triggered the update and are
     *   valid by definition.
     * @param path HashSet
     */
    static void checkCascadeUpdate(Session session, Table table,
                                   HashMappedList tableUpdateLists, Row orow,
                                   Object[] nrow, int[] cols, Table ref,
                                   HashSet path) {

        // -- We iterate through all constraints associated with this table
        // --
        for (int i = 0, size = table.fkConstraints.length; i < size; i++) {

            // -- (1) If it is a foreign key constraint we have to check if the
            // --     main table still holds a record which allows the new values
            // --     to be set in the updated columns. This test however will be
            // --     skipped if the reference table is the main table since changes
            // --     in the reference table triggered the update and therefor
            // --     the referential integrity is guaranteed to be valid.
            // --
            Constraint c = table.fkConstraints[i];

            if (ref == null || c.getMain() != ref) {

                // -- common indexes of the changed columns and the main/ref constraint
                if (ArrayUtil.countCommonElements(cols, c.getRefColumns())
                        == 0) {

                    // -- Table::checkCascadeUpdate -- NO common cols; reiterating
                    continue;
                }

                c.checkInsert(session, c.getMain(), nrow, true);
            }
        }

        for (int i = 0, size = table.fkMainConstraints.length; i < size; i++) {
            Constraint c = table.fkMainConstraints[i];

            // -- (2) If it happens to be a main constraint we check if the slave
            // --     table holds any records refering to the old contents. If so,
            // --     the constraint has to support an 'on update' action or we
            // --     throw an exception (all via a call to Constraint.findFkRef).
            // --
            // -- If there are no common columns between the reference constraint
            // -- and the changed columns, we reiterate.
            int[] common = ArrayUtil.commonElements(cols, c.getMainColumns());

            if (common == null) {

                // -- NO common cols between; reiterating
                continue;
            }

            int[] m_columns = c.getMainColumns();
            int[] r_columns = c.getRefColumns();

            // fredt - find out if the FK columns have actually changed
            boolean nochange = true;

            for (int j = 0; j < m_columns.length; j++) {

                // identity test is enough
                if (orow.getData()[m_columns[j]] != nrow[m_columns[j]]) {
                    nochange = false;

                    break;
                }
            }

            if (nochange) {
                continue;
            }

            // there must be no record in the 'slave' table
            // sebastian@scienion -- dependent on forDelete | forUpdate
            RowIterator refiterator = c.findFkRef(session, orow.getData(),
                                                  false);

            if (refiterator.hasNext()) {
                if (c.core.updateAction == SchemaObject.ReferentialAction
                        .NO_ACTION || c.core.updateAction == SchemaObject
                        .ReferentialAction.RESTRICT) {
                    int errorCode =
                        c.core.deleteAction == SchemaObject.ReferentialAction
                            .NO_ACTION ? ErrorCode.X_23504
                                       : ErrorCode.X_23001;
                    String[] info = new String[] {
                        c.core.refName.name, c.core.refTable.getName().name
                    };

                    throw Error.error(null, errorCode, ErrorCode.CONSTRAINT,
                                      info);
                }
            } else {

                // no referencing row found
                continue;
            }

            Table reftable = c.getRef();

            // -- unused shortcut when update table has no imported constraint
            boolean hasref = reftable.getNextConstraintIndex(
                0, SchemaObject.ConstraintTypes.MAIN) != -1;
            Index refindex = c.getRefIndex();

            // -- walk the index for all the nodes that reference update node
            HashMappedList rowSet =
                (HashMappedList) tableUpdateLists.get(reftable);

            if (rowSet == null) {
                rowSet = new HashMappedList();

                tableUpdateLists.add(reftable, rowSet);
            }

            for (Row refrow = refiterator.getNextRow(); ;
                    refrow = refiterator.getNextRow()) {
                if (refrow == null
                        || refindex.compareRowNonUnique(
                            session, orow.getData(), m_columns,
                            refrow.getData()) != 0) {
                    break;
                }

                Object[] rnd = reftable.getEmptyRowData();

                System.arraycopy(refrow.getData(), 0, rnd, 0, rnd.length);

                // -- Depending on the type constraint we are dealing with we have to
                // -- fill up the forign key of the current record with different values
                // -- And handle the insertion procedure differently.
                if (c.getUpdateAction()
                        == SchemaObject.ReferentialAction.SET_NULL) {

                    // -- set null; we do not have to check referential integrity any further
                    // -- since we are setting <code>null</code> values
                    for (int j = 0; j < r_columns.length; j++) {
                        rnd[r_columns[j]] = null;
                    }
                } else if (c.getUpdateAction()
                           == SchemaObject.ReferentialAction.SET_DEFAULT) {

                    // -- set default; we check referential integrity with ref==null; since we manipulated
                    // -- the values and referential integrity is no longer guaranteed to be valid
                    for (int j = 0; j < r_columns.length; j++) {
                        ColumnSchema col = reftable.getColumn(r_columns[j]);

                        rnd[r_columns[j]] = col.getDefaultValue(session);
                    }

                    if (path.add(c)) {
                        checkCascadeUpdate(session, reftable,
                                           tableUpdateLists, refrow, rnd,
                                           r_columns, null, path);
                        path.remove(c);
                    }
                } else {

                    // -- cascade; standard recursive call. We inherit values from the foreign key
                    // -- table therefor we set ref==this.
                    for (int j = 0; j < m_columns.length; j++) {
                        rnd[r_columns[j]] = nrow[m_columns[j]];
                    }

                    if (path.add(c)) {
                        checkCascadeUpdate(session, reftable,
                                           tableUpdateLists, refrow, rnd,
                                           common, table, path);
                        path.remove(c);
                    }
                }

                mergeUpdate(rowSet, refrow, rnd, r_columns);
            }
        }
    }

    /**
     *  Merges a triggered change with a previous triggered change, or adds to
     * list.
     */
    static void mergeUpdate(HashMappedList rowSet, Row row, Object[] newData,
                            int[] cols) {

        Object[] data = (Object[]) rowSet.get(row);

        if (data != null) {
            for (int j = 0; j < cols.length; j++) {
                data[cols[j]] = newData[cols[j]];
            }
        } else {
            rowSet.add(row, newData);
        }
    }

    /**
     * Merge the full triggered change with the updated row, or add to list.
     * Return false if changes conflict.
     */
    static boolean mergeKeepUpdate(Session session, HashMappedList rowSet,
                                   int[] cols, Type[] colTypes, Row row,
                                   Object[] newData) {

        Object[] data = (Object[]) rowSet.get(row);

        if (data != null) {
            if (Table.compareRows(
                    session, row
                        .getData(), newData, cols, colTypes) != 0 && Table
                            .compareRows(
                                session, newData, data, cols, colTypes) != 0) {
                return false;
            }

            for (int j = 0; j < cols.length; j++) {
                newData[cols[j]] = data[cols[j]];
            }

            rowSet.put(row, newData);
        } else {
            rowSet.add(row, newData);
        }

        return true;
    }
}