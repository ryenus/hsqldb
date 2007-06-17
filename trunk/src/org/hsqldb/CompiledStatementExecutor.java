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


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.RangeVariable.RangeIterator;
import org.hsqldb.index.Index;
import org.hsqldb.index.Index.IndexRowIterator;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.navigator.LinkedListRowSetNavigator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.types.Type;

// boucherb@users 200404xx - fixed broken CALL statement result set unwrapping;
//                           fixed broken support for prepared SELECT...INTO
// fredt@users - 1.9.0 - moved triggered referential actions from Table classe
//                     - rewrite of some methods
//                     - generated column reporting
//                     - multi-row inserts
//                     - enhanced update
//                     - merge operation, originally developed by Justin Spadea
//                     - cascading updates originally developed by Sebastian Kloska

/**
 * Provides execution of CompiledStatement objects. <p>
 *
 * If multiple threads access a CompiledStatementExecutor.execute()
 * concurrently, they must be synchronized externally, relative to both
 * this object's Session and the Session's Database object. Internally, this
 * is accomplished in Session.execute() by synchronizing on the Session
 * object's Database object.
 *
 * @author  boucherb@users
 * @author  fredt@users
 * @version 1.9.0
 * @since 1.7.2
 */
public final class CompiledStatementExecutor {

    // parameter values
    Object[] paramValues;

    // range variables
    RangeIterator[] rangeIterators;

    /**
     * Reusable set of all FK constraints that have so far been enforced while
     * a cascading insert or delete is in progress. This is emptied and passed
     * with the first call to checkCascadeDelete or checkCascadeUpdate. During
     * recursion, if an FK constraint is encountered and is already present
     * in the set, the recursion stops.
     */
    HashSet constraintPath;

    /**
     * Current list of all cascading updates on all table. This is emptied once
     * a cascading operation is over.
     */
    HashMappedList tableUpdateList;

    //
    HsqlArrayList updatedIterators;

    //
    HashSet          subqueryPopSet;
    private Session  session;
    private Database database;

    /**
     * Creates a new instance of CompiledStatementExecutor.
     *
     * @param session the context in which to perform the execution
     */
    CompiledStatementExecutor(Session session) {

        this.session   = session;
        database       = session.database;
        rangeIterators = new RangeIterator[4];
        subqueryPopSet = new HashSet();
    }

    /**
     * Executes a generic CompiledStatement. Execution includes first building
     * any subquery result dependencies and clearing them after the main result
     * is built.
     *
     * @return the result of executing the statement
     * @param cs any valid CompiledStatement
     * @param paramValues parameter values
     */
    void initialiseExec(Object[] paramValues) {
        this.paramValues = paramValues;
    }

    /**
     * Executes a generic CompiledStatement. Execution includes first building
     * any subquery result dependencies and clearing them after the main result
     * is built.
     *
     * @return the result of executing the statement
     * @param cs any valid CompiledStatement
     * @param paramValues parameter values
     */
    Result execute(CompiledStatement cs, Object[] paramValues) {

        Result result = null;

        JavaSystem.gc();

        this.paramValues = paramValues;

        try {
            if (paramValues == null && cs.parameters.length != 0) {
                throw Trace.error(Trace.ASSERT_DIRECT_EXEC_WITH_PARAM);
            }

            cs.materializeSubQueries(session, subqueryPopSet);

            result = executeImpl(cs);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, cs.sql);
        }

        // clear redundant data
        cs.dematerializeSubQueries(session);

        paramValues = null;

        if (cs.type == CompiledStatement.UPDATE
                || cs.type == CompiledStatement.DELETE
                || cs.type == CompiledStatement.MERGE) {
            if (constraintPath != null) {
                constraintPath.clear();
            }

            if (tableUpdateList != null) {
                for (int i = 0; i < tableUpdateList.size(); i++) {
                    HashMappedList updateList =
                        (HashMappedList) tableUpdateList.get(i);

                    updateList.clear();
                }
            }
        }

        // opportunity to combine the two clearance ops
        // updatedIterators may contain iterators not in range Iterator list
        for (int i = 0; i < cs.rangeIteratorCount; i++) {
            if (rangeIterators[i] != null) {
                rangeIterators[i].reset();
            }
        }

        if (updatedIterators != null) {
            for (int i = 0; i < updatedIterators.size(); i++) {
                IndexRowIterator it =
                    (IndexRowIterator) updatedIterators.get(i);

                it.release();
            }

            updatedIterators.clear();
        }

        subqueryPopSet.clear();

        return result;
    }

    /**
     * Executes a generic CompiledStatement. Execution excludes building
     * subquery result dependencies and clearing them after the main result
     * is built.
     *
     * @param cs any valid CompiledStatement
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeImpl(CompiledStatement cs) throws HsqlException {

        cs.checkAccessRights(session);

        switch (cs.type) {

            case CompiledStatement.SELECT :
                return executeSelectStatement(cs);

            case CompiledStatement.INSERT_SELECT :
                return executeInsertSelectStatement(cs);

            case CompiledStatement.INSERT_VALUES :
                return executeInsertValuesStatement(cs);

            case CompiledStatement.SELECT_INTO :
                return executeSelectIntoStatement(cs);

            case CompiledStatement.UPDATE :
                return executeUpdateStatement(cs);

            case CompiledStatement.MERGE :
                return executeMergeStatement(cs);

            case CompiledStatement.DELETE :
                return executeDeleteStatement(cs);

            case CompiledStatement.CALL :
                return executeCallStatement(cs);

            case CompiledStatement.DDL :
                return executeDDLStatement(cs);

            case CompiledStatement.SET :
                return executeSetStatement(cs);

            default :
                throw Trace.runtimeError(
                    Trace.UNSUPPORTED_INTERNAL_OPERATION,
                    "CompiledStatementExecutor.executeImpl()");
        }
    }

    /**
     * Executes a CALL statement.  It is assumed that the argument is
     * of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.CALL
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeCallStatement(CompiledStatement cs)
    throws HsqlException {

        Expression e = cs.expression;          // representing CALL
        Object     o = e.getValue(session);    // expression return value
        Result     r;

        if (o instanceof Result) {
            return (Result) o;
        }

        // TODO: Now that jdbcResultSet has a RowSetNavigator instead of
        // Result, then how to let stored procedure implemented using JDBC
        // to generate result sets for the client?
        // else if (o instanceof jdbcResultSet) {
        //    return ((jdbcResultSet) o).getResult();
        //}
        r = Result.newSingleColumnResult(CompiledStatement.RETURN_COLUMN_NAME,
                                         e.getDataType());

        Object[] row = new Object[1];

        row[0]                   = o;
        r.metaData.classNames[0] = e.getValueClassName();

        r.getNavigator().add(row);

        return r;
    }

// fredt - currently deletes that fail due to referential constraints are caught
// prior to actual delete operation, so no nested transaction is required

    /**
     * Executes a DELETE statement.  It is assumed that the argument is
     * of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.DELETE
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeDeleteStatement(CompiledStatement cs)
    throws HsqlException {

        Table                     table   = cs.targetTable;
        int                       count   = 0;
        LinkedListRowSetNavigator oldRows = new LinkedListRowSetNavigator();
        RangeIterator it = RangeVariable.getIterator(session,
            cs.targetRangeVariables);

        while (it.next()) {
            oldRows.add(it.currentRow);
        }

        count = delete(session, table, oldRows);

        return Result.newUpdateCountResult(count);
    }

    /**
     * Executes an INSERT_SELECT statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.INSERT_SELECT
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeInsertSelectStatement(CompiledStatement cs)
    throws HsqlException {

        Table           table              = cs.targetTable;
        Type[]          colTypes           = table.getColumnTypes();
        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;
        int[]           columnMap          = cs.insertColumnMap;
        boolean[]       colCheck           = cs.insertCheckColumns;

        //
        Result          result      = cs.select.getResult(session, 0);
        RowSetNavigator nav         = result.initialiseNavigator();
        Type[]          sourceTypes = result.metaData.colTypes;

        if (cs.generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(cs.generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        LinkedListRowSetNavigator newData = new LinkedListRowSetNavigator();

        while (nav.hasNext()) {
            Object[] data       = table.getNewRowData(session, colCheck);
            Object[] sourceData = (Object[]) nav.getNext();

            for (int i = 0; i < columnMap.length; i++) {
                int  j          = columnMap[i];
                Type sourceType = sourceTypes[i];

                data[j] = colTypes[j].convertToType(session, sourceData[i],
                                                    sourceType);
            }

            newData.add(data);
        }

        while (newData.hasNext()) {
            Object[] data = (Object[]) newData.getNext();

            table.insertRow(session, data);

            if (generatedNavigator != null) {
                Object[] generatedValues = cs.getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }
        }

        table.fireAfterTriggers(session, Trigger.INSERT_AFTER, newData);

        return Result.newUpdateCountResult(newData.getSize());
    }

    /**
     * Executes an INSERT_VALUES statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.INSERT_VALUES
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeInsertValuesStatement(CompiledStatement cs)
    throws HsqlException {

        Table           table              = cs.targetTable;
        Type[]          colTypes           = table.getColumnTypes();
        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;
        int[]           columnMap          = cs.insertColumnMap;
        boolean[]       colCheck           = cs.insertCheckColumns;

        if (cs.generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(cs.generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        Expression[]              list    = cs.insertExpression.argList;
        LinkedListRowSetNavigator newData = new LinkedListRowSetNavigator();

        for (int j = 0; j < list.length; j++) {
            Expression[] rowArgs = list[j].argList;
            Object[]     data    = table.getNewRowData(session, colCheck);

            for (int i = 0; i < rowArgs.length; i++) {
                Expression e        = rowArgs[i];
                int        colIndex = columnMap[i];

                if (e.exprType == Expression.DEFAULT) {
                    if (table.identityColumn == colIndex) {
                        continue;
                    }

                    data[colIndex] =
                        table.colDefaults[colIndex].getValue(session);

                    continue;
                }

                data[colIndex] = colTypes[colIndex].convertToType(session,
                        e.getValue(session), e.getDataType());
            }

            newData.add(data);
        }

        while (newData.hasNext()) {
            Object[] data = (Object[]) newData.getNext();

            table.insertRow(session, data);

            if (generatedNavigator != null) {
                Object[] generatedValues = cs.getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }
        }

        newData.beforeFirst();
        table.fireAfterTriggers(session, Trigger.INSERT_AFTER, newData);

        if (resultOut == null) {
            return Result.newUpdateCountResult(list.length);
        } else {
            resultOut.setUpdateCount(list.length);

            return resultOut;
        }
    }

    /**
     * Executes a SELECT .. INTO  statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.SELECT
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeSelectIntoStatement(CompiledStatement cs)
    throws HsqlException {

        Select select = cs.select;
        Result result;

        session.getUser().checkSchemaUpdateOrGrantRights(
            select.intoTableName.schema.name);
        session.checkDDLWrite();

        boolean exists =
            session.database.schemaManager.findUserTable(
                session, select.intoTableName.name,
                select.intoTableName.schema.name) != null;

        if (exists) {
            throw Trace.error(Trace.TABLE_ALREADY_EXISTS,
                              select.intoTableName.name);
        }

        result = select.getResult(session, 0);
        result = processSelectInto(result, select.intoTableName,
                                   select.intoType);

        session.getDatabase().setMetaDirty(false);

        return result;
    }

    /**
     * Processes a SELECT INTO for a new table.
     */
    Result processSelectInto(Result result, HsqlName intoHsqlName,
                             int intoType) throws HsqlException {

        // fredt@users 20020215 - patch 497872 by Nitin Chauhan
        // to require column labels in SELECT INTO TABLE
        int colCount = result.getColumnCount();

        for (int i = 0; i < colCount; i++) {
            if (result.metaData.colLabels[i].length() == 0) {
                throw Trace.error(Trace.LABEL_REQUIRED);
            }
        }

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        Table t = (intoType == Table.TEXT_TABLE)
                  ? new TextTable(session.database, intoHsqlName, intoType)
                  : new Table(session.database, intoHsqlName, intoType);

        TableUtil.addColumns(t, result.metaData, result.getColumnCount());
        t.createPrimaryKey();

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        if (intoType == Table.TEXT_TABLE) {

            // Use default lowercase name "<table>.csv" (with invalid
            // char's converted to underscores):
            String txtSrc = StringUtil.toLowerSubset(intoHsqlName.name, '_')
                            + ".csv";

            t.setDataSource(session, txtSrc, false, true);
        }

        logTableDDL(t);
        t.insertIntoTable(session, result);
        session.database.schemaManager.addSchemaObject(t);

        return Result.newUpdateCountResult(result.getNavigator().getSize());
    }

    /**
     *  Logs the DDL for a table created with INTO.
     *  Uses two dummy arguments for getTableDDL() as the new table has no
     *  FK constraints.
     *
     *
     * @param t table
     * @throws  HsqlException
     */
    private void logTableDDL(Table t) throws HsqlException {

        StringBuffer tableDDL;
        String       sourceDDL;

        tableDDL = new StringBuffer();

        DatabaseScript.getTableDDL(t, true, tableDDL);

        sourceDDL = DatabaseScript.getDataSourceDDL(t);

        session.database.logger.writeToLog(session, tableDDL.toString());

        if (sourceDDL != null) {
            session.database.logger.writeToLog(session, sourceDDL);
        }
    }

    private Result executeSelectStatement(CompiledStatement cs)
    throws HsqlException {
        return cs.select.getResult(session, session.getMaxRows());
    }

    /**
     * Executes an UPDATE statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.UPDATE
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeUpdateStatement(CompiledStatement cs)
    throws HsqlException {

        Table          table          = cs.targetTable;
        int            count          = 0;
        int[]          colMap         = cs.updateColumnMap;    // column map
        Expression[]   colExpressions = cs.updateExpressions;
        HashMappedList rowset         = new HashMappedList();
        Type[]         colTypes       = table.getColumnTypes();
        RangeIterator it = RangeVariable.getIterator(session,
            cs.targetRangeVariables);

        while (it.next()) {
            Row row = it.currentRow;
            Object[] data = getUpdatedData(table, colMap, colExpressions,
                                           colTypes, row.getData());

            rowset.add(row, data);
        }

        count = update(session, table, rowset, colMap);

        return Result.newUpdateCountResult(count);
    }

    private Object[] getUpdatedData(Table table, int[] colMap,
                                    Expression[] colExpressions,
                                    Type[] colTypes,
                                    Object[] oldData) throws HsqlException {

        Object[] data = table.getEmptyRowData();

        System.arraycopy(oldData, 0, data, 0, data.length);

        for (int i = 0, ix = 0; i < colMap.length; ) {
            Expression expr = colExpressions[ix++];

            if (expr.exprType == Expression.ROW) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int        colIndex = colMap[i];
                    Expression e        = expr.argList[j];

                    // transitional - still supporting null for identity generation
                    if (table.identityColumn == colIndex) {
                        if (e.exprType == Expression.VALUE
                                && e.valueData == null) {
                            continue;
                        }
                    }

                    if (e.exprType == Expression.DEFAULT) {
                        if (table.identityColumn == colIndex) {
                            continue;
                        }

                        data[colIndex] =
                            table.colDefaults[colIndex].getValue(session);

                        continue;
                    }

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], e.dataType);
                }
            } else if (expr.exprType == Expression.TABLE_SUBQUERY) {
                Object[] values = expr.getRowValue(session);

                for (int j = 0; j < values.length; j++, i++) {
                    int colIndex = colMap[i];
                    Type colType =
                        expr.subQuery.select.getMetaData().colTypes[j];

                    data[colIndex] = colTypes[colIndex].convertToType(session,
                            values[j], colType);
                }
            } else {
                int colIndex = colMap[i];

                if (expr.exprType == Expression.DEFAULT) {
                    if (table.identityColumn == colIndex) {
                        i++;

                        continue;
                    }

                    data[colIndex] =
                        table.colDefaults[colIndex].getValue(session);

                    i++;

                    continue;
                }

                data[colIndex] = expr.getValue(session, colTypes[colIndex]);

                i++;
            }
        }

        return data;
    }

    private Result executeSetStatement(CompiledStatement cs)
    throws HsqlException {

        Table        table          = cs.targetTable;
        int[]        colMap         = cs.updateColumnMap;    // column map
        Expression[] colExpressions = cs.updateExpressions;
        Type[]       colTypes       = table.getColumnTypes();
        int          index = cs.targetRangeVariables[TriggerDef.NEW_ROW].index;
        Object[]     oldData        = rangeIterators[index].currentData;
        Object[] data = getUpdatedData(table, colMap, colExpressions,
                                       colTypes, oldData);

        ArrayUtil.copyArray(data, rangeIterators[index].currentData,
                            data.length);

        return Result.updateOneResult;
    }

    /**
     * Executes a MERGE statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.MERGE
     * @return Result object
     * @throws HsqlException
     */
    private Result executeMergeStatement(CompiledStatement cs)
    throws HsqlException {

        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;

        if (cs.generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(cs.generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        Table targetTable = cs.targetTable;
        int   count       = 0;

        // data generated for non-matching rows
        LinkedListRowSetNavigator newData = new LinkedListRowSetNavigator();

        // rowset for update operation
        HashMappedList updateRowSet = new HashMappedList();
/*
        RangeVariable[] joinRangeIterators = {
            sourceRange, targetRange
        };
*/
        RangeVariable[] joinRangeIterators = cs.targetRangeVariables;

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
                if (currentIndex == 1 && beforeFirst) {
                    Object[] data = getMergeInsertData(cs);

                    if (data != null) {
                        newData.add(data);
                    }
                }

                it.reset();

                currentIndex--;

                continue;
            }

            // row matches!
            if (cs.updateExpressions != null) {
                Row row = it.currentRow;    // this is always the second iterator
                Object[] data = getUpdatedData(targetTable,
                                               cs.updateColumnMap,
                                               cs.updateExpressions,
                                               targetTable.getColumnTypes(),
                                               row.getData());

                updateRowSet.add(row, data);
            }
        }

        // run the transaction as a whole, updating and inserting where needed
        // Update any matched rows
        if (updateRowSet.size() > 0) {
            count = update(session, targetTable, updateRowSet,
                           cs.updateColumnMap);
        }

        // Insert any non-matched rows
        newData.beforeFirst();

        while (newData.hasNext()) {
            Object[] data = (Object[]) newData.getNext();

            targetTable.insertRow(session, data);

            if (generatedNavigator != null) {
                Object[] generatedValues = cs.getGeneratedColumns(data);

                generatedNavigator.add(generatedValues);
            }
        }

        targetTable.fireAfterTriggers(session, Trigger.INSERT_AFTER, newData);

        count += newData.getSize();

        if (resultOut == null) {
            return Result.newUpdateCountResult(count);
        } else {
            resultOut.setUpdateCount(count);

            return resultOut;
        }
    }

    private Object[] getMergeInsertData(CompiledStatement cs)
    throws HsqlException {

        if (cs.insertExpression == null) {
            return null;
        }

        Object[] data = cs.targetTable.getNewRowData(session,
            cs.insertCheckColumns);
        Type[]       colTypes = cs.targetTable.getColumnTypes();
        Expression[] rowArgs  = cs.insertExpression.argList[0].argList;

        for (int i = 0; i < rowArgs.length; i++) {
            Expression e        = rowArgs[i];
            int        colIndex = cs.insertColumnMap[i];

            // transitional - still supporting null for identity generation
            if (cs.targetTable.identityColumn == colIndex) {
                if (e.exprType == Expression.VALUE && e.valueData == null) {
                    continue;
                }
            }

            if (e.exprType == Expression.DEFAULT) {
                if (cs.targetTable.identityColumn == colIndex) {
                    continue;
                }

                data[colIndex] =
                    cs.targetTable.colDefaults[colIndex].getValue(session);

                continue;
            }

            data[colIndex] = colTypes[colIndex].convertToType(session,
                    e.getValue(session), e.getDataType());
        }

        return data;
    }

    /**
     * Executes a DDL statement.  It is assumed that the argument
     * is of the correct type.
     *
     * @param cs a CompiledStatement of type CompiledStatement.DDL
     * @throws HsqlException if a database access error occurs
     * @return the result of executing the statement
     */
    private Result executeDDLStatement(CompiledStatement cs)
    throws HsqlException {
        return session.executeDirectStatement(cs.sql);
    }

    public void setRangeIterator(RangeIterator iterator) {

        if (iterator.rangeVar.index >= rangeIterators.length) {
            rangeIterators =
                (RangeIterator[]) ArrayUtil.resizeArray(rangeIterators,
                    iterator.rangeVar.index + 1);
        }

        rangeIterators[iterator.rangeVar.index] = iterator;
    }

    /**
     * For cascade operations
     */
    public HashMappedList getTableUpdateList() {

        if (tableUpdateList == null) {
            tableUpdateList = new HashMappedList();
        }

        return tableUpdateList;
    }

    /**
     * For cascade operations
     */
    public HashSet getConstraintPath() {

        if (constraintPath == null) {
            constraintPath = new HashSet();
        }

        return constraintPath;
    }

    /**
     * add updatable iterators to be released in case of incomplete execution
     */
    public void addUpdatableIterator(IndexRowIterator index) {

        if (updatedIterators == null) {
            updatedIterators = new HsqlArrayList();
        }

        updatedIterators.add(index);
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
                                   boolean delete,
                                   HashSet path) throws HsqlException {

        for (int i = 0, size = table.constraintList.length; i < size; i++) {
            Constraint c = table.constraintList[i];

            if (c.getType() != Constraint.MAIN || c.getRef() == null) {
                continue;
            }

            RowIterator refiterator = c.findFkRef(session, row.getData(),
                                                  delete);

            if (!refiterator.hasNext()) {
                continue;
            }

            try {
                if (c.core.deleteAction == Constraint.NO_ACTION) {
                    if (c.core.mainTable == c.core.refTable) {
                        Row refrow = refiterator.getNext();

                        // fredt - it's the same row
                        // this supports deleting a single row
                        // in future we can iterate over and check against
                        // the full delete row list to enable multi-row
                        // with self-referencing FK's deletes
                        if (row.equals(refrow)) {
                            continue;
                        }
                    }

                    throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION,
                                      Trace.Constraint_violation,
                                      new Object[] {
                        c.core.refName.name, c.core.refTable.getName().name
                    });
                }

                Table reftable = c.getRef();

                // shortcut when deltable has no imported constraint
                boolean hasref =
                    reftable.getNextConstraintIndex(0, Constraint.MAIN) != -1;

                // if (reftable == this) we don't need to go further and can return ??
                if (delete == false && hasref == false) {
                    continue;
                }

                Index    refindex  = c.getRefIndex();
                int[]    m_columns = c.getMainColumns();
                int[]    r_columns = c.getRefColumns();
                Object[] mdata     = row.getData();
                boolean isUpdate = c.getDeleteAction() == Constraint.SET_NULL
                                   || c.getDeleteAction()
                                      == Constraint.SET_DEFAULT;

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
                    Row refrow = refiterator.getNext();

                    if (refrow == null || refrow.isCascadeDeleted()
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

                        if (c.getDeleteAction() == Constraint.SET_NULL) {
                            for (int j = 0; j < r_columns.length; j++) {
                                rnd[r_columns[j]] = null;
                            }
                        } else {
                            for (int j = 0; j < r_columns.length; j++) {
                                Column col = reftable.getColumn(r_columns[j]);

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
                            if (refrow != row) {
                                checkCascadeDelete(session, reftable,
                                                   tableUpdateList, refrow,
                                                   delete, path);
                            }
                        }
                    }

                    if (delete && !isUpdate && !refrow.isCascadeDeleted()) {
                        reftable.deleteRowAsTriggeredAction(session, refrow);
                    }
                }
            } finally {
                refiterator.release();
            }
        }
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
     * @short Check or perform and update cascade operation on a single row.
     * @throws HsqlException
     */
    static void checkCascadeUpdate(Session session, Table table,
                                   HashMappedList tableUpdateLists, Row orow,
                                   Object[] nrow, int[] cols, Table ref,
                                   HashSet path) throws HsqlException {

        // -- We iterate through all constraints associated with this table
        // --
        for (int i = 0, size = table.constraintList.length; i < size; i++) {
            Constraint c = table.constraintList[i];

            if (c.getType() == Constraint.FOREIGN_KEY && c.getRef() != null) {

                // -- (1) If it is a foreign key constraint we have to check if the
                // --     main table still holds a record which allows the new values
                // --     to be set in the updated columns. This test however will be
                // --     skipped if the reference table is the main table since changes
                // --     in the reference table triggered the update and therefor
                // --     the referential integrity is guaranteed to be valid.
                // --
                if (ref == null || c.getMain() != ref) {

                    // -- common indexes of the changed columns and the main/ref constraint
                    if (ArrayUtil.countCommonElements(cols, c.getRefColumns())
                            == 0) {

                        // -- Table::checkCascadeUpdate -- NO common cols; reiterating
                        continue;
                    }

                    c.hasMainRef(session, nrow);
                }
            } else if (c.getType() == Constraint.MAIN && c.getRef() != null) {

                // -- (2) If it happens to be a main constraint we check if the slave
                // --     table holds any records refering to the old contents. If so,
                // --     the constraint has to support an 'on update' action or we
                // --     throw an exception (all via a call to Constraint.findFkRef).
                // --
                // -- If there are no common columns between the reference constraint
                // -- and the changed columns, we reiterate.
                int[] common = ArrayUtil.commonElements(cols,
                    c.getMainColumns());

                if (common == null) {

                    // -- NO common cols between; reiterating
                    continue;
                }

                int[] m_columns = c.getMainColumns();
                int[] r_columns = c.getRefColumns();

                // fredt - find out if the FK columns have actually changed
                boolean nochange = true;

                for (int j = 0; j < m_columns.length; j++) {
                    if (!orow.getData()[m_columns[j]].equals(
                            nrow[m_columns[j]])) {
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
                    if (c.core.updateAction == Constraint.NO_ACTION) {
                        throw Trace.error(Trace.INTEGRITY_CONSTRAINT_VIOLATION,
                                          Trace.Constraint_violation,
                                          new Object[] {
                            c.core.refName.name, c.core.refTable.getName().name
                        });
                    }
                } else {

                    // no referencing row found
                    continue;
                }

                Table reftable = c.getRef();

                // -- unused shortcut when update table has no imported constraint
                boolean hasref =
                    reftable.getNextConstraintIndex(0, Constraint.MAIN) != -1;
                Index refindex = c.getRefIndex();

                // -- walk the index for all the nodes that reference update node
                HashMappedList rowSet =
                    (HashMappedList) tableUpdateLists.get(reftable);

                if (rowSet == null) {
                    rowSet = new HashMappedList();

                    tableUpdateLists.add(reftable, rowSet);
                }

                for (Row refrow = refiterator.getNext(); ;
                        refrow = refiterator.getNext()) {
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
                    if (c.getUpdateAction() == Constraint.SET_NULL) {

                        // -- set null; we do not have to check referential integrity any further
                        // -- since we are setting <code>null</code> values
                        for (int j = 0; j < r_columns.length; j++) {
                            rnd[r_columns[j]] = null;
                        }
                    } else if (c.getUpdateAction() == Constraint.SET_DEFAULT) {

                        // -- set default; we check referential integrity with ref==null; since we manipulated
                        // -- the values and referential integrity is no longer guaranteed to be valid
                        for (int j = 0; j < r_columns.length; j++) {
                            Column col = reftable.getColumn(r_columns[j]);

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
                                   Object[] newData) throws HsqlException {

        Object[] data = (Object[]) rowSet.get(row);

        if (data != null) {
            if (Index.compareRows(
                    session, row
                        .getData(), newData, cols, colTypes) != 0 && Index
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

    /**
     *  Highest level multiple row delete method. Corresponds to an SQL
     *  DELETE.
     */
    int delete(Session session, Table table,
               RowSetNavigator oldRows) throws HsqlException {

        HashSet        path            = getConstraintPath();
        HashMappedList tableUpdateList = getTableUpdateList();

        if (database.isReferentialIntegrity()) {
            oldRows.beforeFirst();

            while (oldRows.hasNext()) {
                Row row = (Row) oldRows.getNext();

                path.clear();
                checkCascadeDelete(session, table, tableUpdateList, row,
                                   false, path);
            }
        }

        // check transactions
        database.txManager.checkDelete(session, oldRows);

        for (int i = 0; i < tableUpdateList.size(); i++) {
            HashMappedList updateList =
                (HashMappedList) tableUpdateList.get(i);

            if (updateList.size() > 0) {
                database.txManager.checkDelete(session, updateList);
            }
        }

        if (database.isReferentialIntegrity()) {
            oldRows.beforeFirst();

            while (oldRows.hasNext()) {
                Row row = (Row) oldRows.getNext();

                path.clear();
                checkCascadeDelete(session, table, tableUpdateList, row, true,
                                   path);
            }
        }

        oldRows.beforeFirst();

        while (oldRows.hasNext()) {
            Row row = (Row) oldRows.getNext();

            if (!row.isCascadeDeleted()) {
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

        if (table.hasTrigger(Trigger.DELETE_AFTER)) {
            table.fireAfterTriggers(session, Trigger.DELETE_AFTER, oldRows);
        }

        path.clear();

        return oldRows.getSize();
    }

    /**
     * Highest level multiple row update method. Corresponds to an SQL UPDATE.
     * To DEAL with unique constraints we need to perform all deletes at once
     * before the inserts. If there is a UNIQUE constraint violation limited
     * only to the duration of updating multiple rows, we don't want to abort
     * the operation. Example: UPDATE MYTABLE SET UNIQUECOL = UNIQUECOL + 1
     * After performing each cascade update, delete the main row. After all
     * cascade ops and deletes have been performed, insert new rows.<p>
     *
     * The
     * following clauses from SQL Standard section 11.8 are enforced 9) Let ISS
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
     * @param cols int[]
     * @throws HsqlException
     * @return int
     */
    int update(Session session, Table table, HashMappedList updateList,
               int[] cols) throws HsqlException {

        HashSet        path        = getConstraintPath();
        HashMappedList tUpdateList = getTableUpdateList();

        // set identity column where null and check columns
        for (int i = 0; i < updateList.size(); i++) {
            Row      row  = (Row) updateList.getKey(i);
            Object[] data = (Object[]) updateList.get(i);

            // this means the identity column can be set to null to force
            // creation of a new identity value
            table.setIdentityColumn(session, data);

            if (table.triggerLists[Trigger.UPDATE_BEFORE] != null) {
                table.fireBeforeTriggers(session, Trigger.UPDATE_BEFORE,
                                         row.getData(), data, cols);
            }

            table.enforceRowConstraints(data);
        }

        // perform check/cascade operations
        if (database.isReferentialIntegrity()) {
            for (int i = 0; i < updateList.size(); i++) {
                Object[] data = (Object[]) updateList.get(i);
                Row      row  = (Row) updateList.getKey(i);

                checkCascadeUpdate(session, table, tUpdateList, row, data,
                                   cols, null, path);
            }
        }

        // merge any triggered change to this table with the update list
        HashMappedList triggeredList = (HashMappedList) tUpdateList.get(table);

        if (triggeredList != null) {
            for (int i = 0; i < triggeredList.size(); i++) {
                Row      row  = (Row) triggeredList.getKey(i);
                Object[] data = (Object[]) triggeredList.get(i);

                mergeKeepUpdate(session, updateList, cols, table.colTypes,
                                row, data);
            }

            triggeredList.clear();
        }

        // check transactions
        for (int i = 0; i < tUpdateList.size(); i++) {
            HashMappedList updateListT = (HashMappedList) tUpdateList.get(i);

            database.txManager.checkDelete(session, updateListT);
        }

        database.txManager.checkDelete(session, updateList);

        // update lists - main list last
        for (int i = 0; i < tUpdateList.size(); i++) {
            Table          targetTable = (Table) tUpdateList.getKey(i);
            HashMappedList updateListT = (HashMappedList) tUpdateList.get(i);

            targetTable.updateRowSet(session, updateListT, null, true);
            updateListT.clear();
        }

        table.updateRowSet(session, updateList, cols, false);
        path.clear();

        return updateList.size();
    }
}
