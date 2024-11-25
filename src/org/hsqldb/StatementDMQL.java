/* Copyright (c) 2001-2025, The HSQL Development Group
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
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.map.ValuePool;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rights.GrantConstants;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.Right;

/**
 * Statement implementation for DML and base DQL statements.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.7.2
 */

// fredt@users 20040404 - patch 1.7.2 - fixed type resolution for parameters
// campbell-burnet@users 200404xx - patch 1.7.2 - changed parameter naming scheme for SQLCI client usability/support
// fredt@users 20050609 - 1.8.0 - fixed EXPLAIN PLAN by implementing describe(Session)
// fredt@users - 1.9.0 - support for generated column reporting
// fredt@users - 1.9.0 - support for multi-row inserts
public abstract class StatementDMQL extends Statement {

    /** target table for INSERT_XXX, UPDATE, DELETE and MERGE */
    Table targetTable;
    Table baseTable;

    /** column map of query expression */
    int[]           baseColumnMap;
    RangeVariable[] targetRangeVariables = RangeVariable.emptyArray;

    /** source table for MERGE */
    Table sourceTable;

    /** condition expression for UPDATE, MERGE and DELETE */
    Expression condition;

    /** for TRUNCATE variation of DELETE */
    boolean restartIdentity;

    /** column map for INSERT operation direct or via MERGE */
    int[] insertColumnMap = ValuePool.emptyIntArray;

    /** column map for UPDATE operation direct or via MERGE */
    int[] updateColumnMap     = ValuePool.emptyIntArray;
    int[] baseUpdateColumnMap = ValuePool.emptyIntArray;

    /** Column value Expressions for UPDATE and MERGE. */
    Expression[] updateExpressions = Expression.emptyArray;

    /** Column value Expressions for MERGE */
    Expression[][] multiColumnValues;

    /** INSERT_VALUES */
    Expression insertExpression;

    /**
     * Flags indicating which columns' values will/will not be
     * explicitly set.
     */
    boolean[] insertCheckColumns;
    boolean[] updateCheckColumns;

    /**
     * VIEW check
     */
    Expression    updatableTableCheck;
    RangeVariable checkRangeVariable;

    /**
     * Select to be evaluated when this is an INSERT_SELECT or
     * SELECT statement
     */
    QueryExpression queryExpression;

    /**
     * Name of cursor
     */
    HsqlName cursorName;

    /**
     * Subqueries inverse usage depth order
     */
    TableDerived[] subqueries = TableDerived.emptyArray;

    /**
     * Total number of RangeIterator objects used
     */
    int rangeIteratorCount;

    /**
     * Database objects used
     */
    NumberSequence[] sequences;
    Routine[]        routines;
    RangeVariable[]  rangeVariables;

    StatementDMQL(int type, int group, HsqlName schemaName) {

        super(type, group);

        this.schemaName             = schemaName;
        this.isTransactionStatement = true;
    }

    void setBaseIndexColumnMap() {
        if (targetTable != baseTable) {
            baseColumnMap = targetTable.getBaseTableColumnMap();
        }
    }

    public Result execute(Session session) {

        Result result;

        if (targetTable != null
                && session.isReadOnly()
                && !targetTable.isTemp()) {
            HsqlException e = Error.error(ErrorCode.X_25006);

            return Result.newErrorResult(e);
        }

        if (isExplain) {
            return getExplainResult(session);
        }

        try {
            if (subqueries.length > 0) {
                materializeSubQueries(session);
            }

            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t);

            result.getException().setStatementType(group, type);
        } finally {
            clearStructures(session);
        }

        return result;
    }

    private Result getExplainResult(Session session) {

        Result result = Result.newSingleColumnStringResult(
            "OPERATION",
            describe(session));
        OrderedHashSet<HsqlName> set = getReferences();

        result.navigator.add(new Object[]{ "Object References" });

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = set.get(i);

            result.navigator.add(
                new Object[]{ name.getSchemaQualifiedStatementName() });
        }

        result.navigator.add(new Object[]{ "Read Locks" });

        for (int i = 0; i < readTableNames.length; i++) {
            HsqlName name = readTableNames[i];

            result.navigator.add(
                new Object[]{ name.getSchemaQualifiedStatementName() });
        }

        result.navigator.add(new Object[]{ "WriteLocks" });

        for (int i = 0; i < writeTableNames.length; i++) {
            HsqlName name = writeTableNames[i];

            result.navigator.add(
                new Object[]{ name.getSchemaQualifiedStatementName() });
        }

        return result;
    }

    abstract Result getResult(Session session);

    abstract void collectTableNamesForRead(OrderedHashSet<HsqlName> set);

    abstract void collectTableNamesForWrite(OrderedHashSet<HsqlName> set);

    boolean[] getInsertOrUpdateColumnCheckList() {

        switch (type) {

            case StatementTypes.INSERT :
                return insertCheckColumns;

            case StatementTypes.UPDATE_WHERE :
                return updateCheckColumns;

            case StatementTypes.MERGE :
                boolean[] check = (boolean[]) ArrayUtil.duplicateArray(
                    insertCheckColumns);

                ArrayUtil.orBooleanArray(updateCheckColumns, check);

                return check;
        }

        return null;
    }

    void materializeSubQueries(Session session) {

        HashSet<TableDerived> subqueryPopFlags = new HashSet<>();

        for (int i = 0; i < subqueries.length; i++) {
            TableDerived td = subqueries[i];

            if (!subqueryPopFlags.add(td)) {
                continue;
            }

            if (!td.isCorrelated()) {
                td.materialise(session);
            }
        }
    }

    TableDerived[] getSubqueries(Session session) {

        OrderedHashSet<TableDerived> subQueries = null;

        for (int i = 0; i < targetRangeVariables.length; i++) {
            if (targetRangeVariables[i] == null) {
                continue;
            }

            OrderedHashSet<TableDerived> set =
                targetRangeVariables[i].getSubqueries();

            subQueries = OrderedHashSet.addAll(subQueries, set);
        }

        for (int i = 0; i < updateExpressions.length; i++) {
            subQueries = updateExpressions[i].collectAllSubqueries(subQueries);
        }

        if (insertExpression != null) {
            subQueries = insertExpression.collectAllSubqueries(subQueries);
        }

        if (condition != null) {
            subQueries = condition.collectAllSubqueries(subQueries);
        }

        if (queryExpression != null) {
            OrderedHashSet<TableDerived> set = queryExpression.getSubqueries();

            subQueries = OrderedHashSet.addAll(subQueries, set);
        }

        if (updatableTableCheck != null) {
            OrderedHashSet<TableDerived> set =
                updatableTableCheck.getSubqueries();

            subQueries = OrderedHashSet.addAll(subQueries, set);
        }

        if (subQueries == null || subQueries.isEmpty()) {
            return TableDerived.emptyArray;
        }

        TableDerived[] subQueryArray = new TableDerived[subQueries.size()];

        subQueries.toArray(subQueryArray);

        return subQueryArray;
    }

    void setDatabaseObjects(Session session, CompileContext compileContext) {

        parameters = compileContext.getParameters();

        setParameterMetaData();

        subqueries         = getSubqueries(session);
        rangeIteratorCount = compileContext.getRangeVarCount();
        rangeVariables     = compileContext.getAllRangeVariables();
        sequences          = compileContext.getSequences();
        routines           = compileContext.getRoutines();

        OrderedHashSet<HsqlName> set = new OrderedHashSet<>();

        collectTableNamesForWrite(set);

        if (set.size() > 0) {
            writeTableNames = new HsqlName[set.size()];

            set.toArray(writeTableNames);
            set.clear();
        }

        collectTableNamesForRead(set);
        set.removeAll(writeTableNames);

        if (set.size() > 0) {
            readTableNames = new HsqlName[set.size()];

            set.toArray(readTableNames);
        }

        if (readTableNames.length == 0 && writeTableNames.length == 0) {
            if (type == StatementTypes.SELECT_CURSOR
                    || type == StatementTypes.SELECT_SINGLE) {
                isTransactionStatement = false;
            }
        }

        references = compileContext.getSchemaObjectNames();

        if (targetTable != null) {
            references.add(targetTable.getName());

            if (targetTable == baseTable) {
                if (insertCheckColumns != null) {
                    targetTable.getColumnNames(insertCheckColumns, references);
                }

                if (updateCheckColumns != null) {
                    targetTable.getColumnNames(updateCheckColumns, references);
                }
            }
        }
    }

    /**
     * Determines if the authorizations are adequate
     * to execute the compiled object. Completion requires the list of
     * all database objects in a compiled statement.
     */
    void checkAccessRights(Session session) {

        if (targetTable != null && !targetTable.isTemp()) {
            if (!session.isProcessingScript()) {
                targetTable.checkDataReadOnly();
            }

            Grantee owner = targetTable.getOwner();

            if (owner != null && owner.isSystem()) {
                if (!session.getUser().isSystem()) {
                    throw Error.error(
                        ErrorCode.X_42501,
                        targetTable.getName().name);
                }
            }

            session.checkReadWrite();
        }

        if (session.isAdmin()) {
            return;
        }

        for (int i = 0; i < sequences.length; i++) {
            session.getGrantee().checkAccess(sequences[i]);
        }

        for (int i = 0; i < routines.length; i++) {
            if (routines[i].isLibraryRoutine()) {
                continue;
            }

            session.getGrantee().checkAccess(routines[i]);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            if (range.isViewSubquery) {
                continue;
            }

            if (range.rangeTable.getSchemaName()
                    == SqlInvariants.SESSION_SCHEMA_HSQLNAME) {
                continue;
            }

            if (range.rangeTable.getSchemaName()
                    == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            Right right = session.getGrantee()
                                 .checkSelect(
                                     range.rangeTable,
                                     range.usedColumns);
            Expression expr = right.getFilterExpression(GrantConstants.SELECT);

            if (expr != null) {
                expr = expr.duplicate();

                range.setFilterExpression(session, expr);

                OrderedHashSet<TableDerived> set = expr.collectAllSubqueries(
                    null);

                if (set != null && set.size() > 0) {
                    for (int j = 0; j < set.size(); j++) {
                        TableDerived subquery = set.get(j);

                        subqueries = (TableDerived[]) ArrayUtil.toAdjustedArray(
                            subqueries,
                            subquery,
                            subqueries.length,
                            1);
                    }
                }
            }
        }

        Expression expr = null;
        Right      right;

        switch (type) {

            case StatementTypes.CALL : {
                break;
            }

            case StatementTypes.INSERT : {
                right = session.getGrantee()
                               .checkInsert(targetTable, insertCheckColumns);
                expr = right.getFilterExpression(GrantConstants.INSERT);
                break;
            }

            case StatementTypes.SELECT_CURSOR :
                break;

            case StatementTypes.DELETE_WHERE : {
                right = session.getGrantee().checkDelete(targetTable);
                expr  = right.getFilterExpression(GrantConstants.DELETE);
                break;
            }

            case StatementTypes.UPDATE_WHERE : {
                right = session.getGrantee()
                               .checkUpdate(targetTable, updateCheckColumns);
                expr = right.getFilterExpression(GrantConstants.UPDATE);
                break;
            }

            case StatementTypes.MERGE : {
                session.getGrantee()
                       .checkInsert(targetTable, insertCheckColumns);

                right = session.getGrantee()
                               .checkUpdate(targetTable, updateCheckColumns);
                expr = right.getFilterExpression(GrantConstants.UPDATE);
                break;
            }
        }

        if (expr != null) {
            expr = expr.duplicate();

            targetRangeVariables[0].setFilterExpression(session, expr);

            OrderedHashSet<TableDerived> set = expr.collectAllSubqueries(null);

            if (set != null && set.size() > 0) {
                for (int j = 0; j < set.size(); j++) {
                    TableDerived subquery = set.get(j);

                    subqueries = (TableDerived[]) ArrayUtil.toAdjustedArray(
                        subqueries,
                        subquery,
                        subqueries.length,
                        1);
                }
            }
        }
    }

    /**
     * Returns the metadata, which is empty if the CompiledStatement does not
     * generate a Result.
     */
    public ResultMetaData getResultMetaData() {

        switch (type) {

            case StatementTypes.DELETE_WHERE :
            case StatementTypes.INSERT :
            case StatementTypes.UPDATE_WHERE :
            case StatementTypes.MERGE :
                return ResultMetaData.emptyResultMetaData;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementDMQL");
        }
    }

    /* @todo 1.9.0 - build the metadata only once and reuse */

    /**
     * Returns the metadata for the placeholder parameters.
     */
    public ResultMetaData getParametersMetaData() {
        return parameterMetaData;
    }

    /**
     * Retrieves a String representation of this object.
     */
    public String describe(Session session) {

        try {
            return describeImpl(session);
        } catch (Throwable e) {

            // e.printStackTrace();
            return e.toString();
        }
    }

    /**
     * Provides the toString() implementation.
     */
    String describeImpl(Session session) {

        StringBuilder sb     = new StringBuilder();
        int           blanks = 0;

        switch (type) {

            case StatementTypes.SELECT_CURSOR : {
                sb.append(queryExpression.describe(session, 0));
                appendParams(sb).append('\n');
                appendSubqueries(session, sb, 2);

                return sb.toString();
            }

            case StatementTypes.INSERT : {
                if (queryExpression == null) {
                    sb.append("INSERT VALUES").append('[').append('\n');
                    appendMultiColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    appendParams(sb).append('\n');
                    appendSubqueries(session, sb, 2).append(']');

                    return sb.toString();
                } else {
                    sb.append("INSERT SELECT").append('[').append('\n');
                    appendColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    sb.append(queryExpression.describe(session, blanks))
                      .append('\n');
                    appendParams(sb).append('\n');
                    appendSubqueries(session, sb, 2).append(']');

                    return sb.toString();
                }
            }

            case StatementTypes.UPDATE_WHERE : {
                sb.append("UPDATE").append('[').append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);

                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session, blanks))
                      .append('\n');
                }

                appendParams(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');

                return sb.toString();
            }

            case StatementTypes.DELETE_WHERE : {
                sb.append("DELETE").append('[').append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);

                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session, blanks))
                      .append('\n');
                }

                appendParams(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');

                return sb.toString();
            }

            case StatementTypes.CALL : {
                sb.append("CALL");
                sb.append('[').append(']');

                return sb.toString();
            }

            case StatementTypes.MERGE : {
                sb.append("MERGE");
                sb.append('[').append('\n');
                appendMultiColumns(sb, insertColumnMap).append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);

                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session, blanks))
                      .append('\n');
                }

                appendParams(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');

                return sb.toString();
            }

            default : {
                return "UNKNOWN";
            }
        }
    }

    private StringBuilder appendSubqueries(
            Session session,
            StringBuilder sb,
            int blanks) {

        sb.append("SUBQUERIES[");

        for (int i = 0; i < subqueries.length; i++) {
            sb.append("\n[level=").append(subqueries[i].depth).append('\n');

            if (subqueries[i].queryExpression == null) {
                for (int j = 0; j < blanks; j++) {
                    sb.append(' ');
                }

                sb.append("value expression");
            } else {
                sb.append(
                    subqueries[i].queryExpression.describe(session, blanks));
            }

            sb.append("]");
        }

        sb.append(']');

        return sb;
    }

    private StringBuilder appendTable(StringBuilder sb) {
        sb.append("TABLE[").append(targetTable.getName().name).append(']');

        return sb;
    }

    private StringBuilder appendSourceTable(StringBuilder sb) {

        sb.append("SOURCE TABLE[")
          .append(sourceTable.getName().name)
          .append(']');

        return sb;
    }

    private StringBuilder appendColumns(StringBuilder sb, int[] columnMap) {

        if (columnMap == null || updateExpressions.length == 0) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int i = 0; i < columnMap.length; i++) {
            sb.append('\n')
              .append(columnMap[i])
              .append(':')
              .append(' ')
              .append(targetTable.getColumn(columnMap[i]).getNameString());
        }

        for (int i = 0; i < updateExpressions.length; i++) {
            sb.append('[').append(updateExpressions[i]).append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuilder appendMultiColumns(
            StringBuilder sb,
            int[] columnMap) {

        // todo - multiColVals is always null
        if (columnMap == null || multiColumnValues == null) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int j = 0; j < multiColumnValues.length; j++) {
            for (int i = 0; i < columnMap.length; i++) {
                sb.append('\n')
                  .append(columnMap[i])
                  .append(':')
                  .append(' ')
                  .append(targetTable.getColumn(columnMap[i]).getName().name)
                  .append('[')
                  .append(multiColumnValues[j][i])
                  .append(']');
            }
        }

        sb.append(']');

        return sb;
    }

    private StringBuilder appendParams(StringBuilder sb) {

        sb.append("PARAMETERS=[");

        for (int i = 0; i < parameters.length; i++) {
            sb.append('\n')
              .append('@')
              .append(i)
              .append('[')
              .append(parameters[i].describe(null, 0))
              .append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuilder appendCondition(Session session, StringBuilder sb) {

        return condition == null
               ? sb.append("CONDITION[]\n")
               : sb.append("CONDITION[")
                   .append(condition.describe(session, 0))
                   .append("]\n");
    }

    public void resolve(Session session) {}

    public final boolean isCatalogLock(int model) {
        return false;
    }

    public boolean isCatalogChange() {
        return false;
    }

    public void clearStructures(Session session) {
        session.sessionContext.clearStructures(this);
    }
}
