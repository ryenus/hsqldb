/* Copyright (c) 2001-2010, The HSQL Development Group
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
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

/**
 * Implementation of Statement for PSM and trigger assignment.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementSet extends StatementDMQL {

    Expression expression;

    //
    ColumnSchema[] variables;
    int[]          variableIndexes;
    Type[]         sourceTypes;

    //
    final int               operationType;
    public final static int TRIGGER_SET  = 1;
    public final static int SELECT_INTO  = 2;
    public final static int VARIABLE_SET = 3;

    /**
     * Instantiate this as a SET statement.
     */
    StatementSet(Session session, Table table, RangeVariable rangeVars[],
                 int[] updateColumnMap, Expression[] colExpressions,
                 CompileContext compileContext) {

        super(StatementTypes.ASSIGNMENT, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.operationType     = TRIGGER_SET;
        this.targetTable       = table;
        this.baseTable         = targetTable.getBaseTable();
        this.updateColumnMap   = updateColumnMap;
        this.updateExpressions = colExpressions;
        this.updateCheckColumns =
            targetTable.getColumnCheckList(updateColumnMap);
        this.targetRangeVariables = rangeVars;
        isTransactionStatement    = false;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    StatementSet(Session session, CompileContext compileContext,
                 ColumnSchema[] variables, Expression e, int[] indexes) {

        super(StatementTypes.ASSIGNMENT, StatementTypes.X_SQL_CONTROL, null);

        this.operationType     = VARIABLE_SET;
        isTransactionStatement = false;
        this.expression        = e;
        this.variables         = variables;
        variableIndexes        = indexes;
        sourceTypes            = expression.getNodeDataTypes();

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    StatementSet(Session session, CompileContext compileContext,
                 ColumnSchema[] variables, QueryExpression query,
                 int[] indexes) {

        super(StatementTypes.ASSIGNMENT, StatementTypes.X_SQL_CONTROL, null);

        this.operationType     = SELECT_INTO;
        isTransactionStatement = false;
        this.queryExpression   = query;
        this.variables         = variables;
        variableIndexes        = indexes;
        sourceTypes            = query.getColumnTypes();

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    SubQuery[] getSubqueries(Session session) {

        OrderedHashSet subQueries = null;

        if (expression != null) {
            subQueries = expression.collectAllSubqueries(subQueries);
        }

        if (subQueries == null || subQueries.size() == 0) {
            return SubQuery.emptySubqueryArray;
        }

        SubQuery[] subQueryArray = new SubQuery[subQueries.size()];

        subQueries.toArray(subQueryArray);
        ArraySort.sort(subQueryArray, 0, subQueryArray.length,
                       subQueryArray[0]);

        for (int i = 0; i < subqueries.length; i++) {
            subQueryArray[i].prepareTable(session);
        }

        return subQueryArray;
    }

    Result getResult(Session session) {

        Result result = null;

        switch (operationType) {

            case StatementSet.TRIGGER_SET :
                result = executeSetStatement(session);
                break;

            case StatementSet.SELECT_INTO : {
                Object[] values = queryExpression.getSingleRowValues(session);

                if (values == null) {
                    result = Result.updateZeroResult;

                    break;
                }

                for (int i = 0; i < values.length; i++) {
                    values[i] =
                        variables[i].getDataType().convertToType(session,
                            values[i], sourceTypes[i]);
                }

                result = executeAssignment(session, values);

                break;
            }
            case StatementSet.VARIABLE_SET : {
                Object[] values = getExpressionValues(session);

                if (values == null) {
                    result = Result.updateZeroResult;

                    break;
                }

                for (int i = 0; i < values.length; i++) {
                    values[i] =
                        variables[i].getDataType().convertToType(session,
                            values[i], sourceTypes[i]);
                }

                result = executeAssignment(session, values);

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSet");
        }

        return result;
    }

    public void resolve(Session session) {

        references = new OrderedHashSet();

        switch (operationType) {

            case StatementSet.TRIGGER_SET :
                for (int i = 0; i < updateExpressions.length; i++) {
                    updateExpressions[i].collectObjectNames(references);
                }
                break;

            case StatementSet.SELECT_INTO :
            case StatementSet.VARIABLE_SET : {
                if (expression != null) {
                    expression.collectObjectNames(references);
                }

                if (queryExpression != null) {
                    queryExpression.collectObjectNames(references);
                }

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSet");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (operationType) {

            case StatementSet.TRIGGER_SET :
                return sql;

            case StatementSet.VARIABLE_SET : {

                /** @todo - cover row assignment */
                sb.append(Tokens.T_SET).append(' ');
                sb.append(variables[0].getName().statementName).append(' ');
                sb.append('=').append(' ').append(expression.getSQL());

                break;
            }
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(Tokens.T_STATEMENT);

        return sb.toString();
    }

    public Result execute(Session session) {

        Result result;

        try {
            if (subqueries.length > 0) {
                materializeSubQueries(session);
            }

            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, null);
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    public String describe(Session session) {
        return "";
    }

    Result executeSetStatement(Session session) {

        Table        table          = targetTable;
        int[]        colMap         = updateColumnMap;    // column map
        Expression[] colExpressions = updateExpressions;
        Type[]       colTypes       = table.getColumnTypes();
        int index = targetRangeVariables[TriggerDef.NEW_ROW].rangePosition;
        Object[] oldData =
            session.sessionContext.rangeIterators[index].getCurrent();
        Object[] data = StatementDML.getUpdatedData(session, table, colMap,
            colExpressions, colTypes, oldData);

        ArrayUtil.copyArray(data, oldData, data.length);

        return Result.updateOneResult;
    }

    // this fk references -> other  :  other read lock
    void collectTableNamesForRead(OrderedHashSet set) {

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

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForRead());
        }
    }

    void collectTableNamesForWrite(OrderedHashSet set) {}

    Object[] getExpressionValues(Session session) {

        Object[] values;

        if (expression.getType() == OpTypes.ROW) {
            values = expression.getRowValue(session);
        } else if (expression.getType() == OpTypes.ROW_SUBQUERY) {
            values = expression.subQuery.queryExpression.getSingleRowValues(
                session);

            if (values == null) {

                // todo - verify semantics
                return null;
            }
        } else {
            values    = new Object[1];
            values[0] = expression.getValue(session);
        }

        return values;
    }

    Result executeAssignment(Session session, Object[] values) {

        for (int j = 0; j < values.length; j++) {
            Object[] data = ValuePool.emptyObjectArray;

            switch (variables[j].getType()) {

                case SchemaObject.PARAMETER :
                    data = session.sessionContext.routineArguments;
                    break;

                case SchemaObject.VARIABLE :
                    data = session.sessionContext.routineVariables;
                    break;
            }

            int colIndex = variableIndexes[j];

            data[colIndex] = values[j];
        }

        return Result.updateZeroResult;
    }
}
