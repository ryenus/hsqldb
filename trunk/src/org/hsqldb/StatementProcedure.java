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

import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

/**
 * Implementation of Statement for callable procedures.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementProcedure extends StatementDMQL {

    /** Expression to evaluate */
    Expression expression;

    /** Routine to execute */
    Routine procedure;

    /** arguments to Routine */
    Expression[]   arguments = Expression.emptyArray;
    ResultMetaData resultMetaData;

    /**
     * Constructor for CALL statements for expressions.
     */
    StatementProcedure(Session session, Expression expression,
                       CompileContext compileContext) {

        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());

        this.expression = expression;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);

        if (procedure != null) {
            session.getGrantee().checkAccess(procedure);
        }
    }

    /**
     * Constructor for CALL statements for procedures.
     */
    StatementProcedure(Session session, Routine procedure,
                       Expression[] arguments, CompileContext compileContext) {

        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());

        this.procedure = procedure;
        this.arguments = arguments;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    Result getResult(Session session) {
        return expression == null ? getProcedureResult(session)
                                  : getExpressionResult(session);
    }

    Result getProcedureResult(Session session) {

        Object[] data = ValuePool.emptyObjectArray;

        if (arguments.length > 0) {
            data = new Object[arguments.length];
        }

        for (int i = 0; i < arguments.length; i++) {
            Expression e     = arguments[i];
            Object     value = e.getValue(session);

            if (e != null) {
                Type targetType = procedure.getParameter(i).getDataType();

                data[i] = targetType.convertToType(session, value,
                                                   e.getDataType());
            }
        }

        session.sessionContext.push();

        session.sessionContext.routineArguments = data;
        session.sessionContext.routineVariables = ValuePool.emptyObjectArray;

        Result result = Result.updateZeroResult;

        if (procedure.isPSM()) {
            result = executePSMProcedure(session);
        } else {
            result = executeJavaProcedure(session);
        }

        Object[] callArguments = session.sessionContext.routineArguments;

        session.sessionContext.pop();

        if (result.isError()) {
            return result;
        }

        if (result.isSimpleValue()) {
            result = Result.updateZeroResult;
        }

        boolean returnParams = false;

        for (int i = 0; i < procedure.getParameterCount(); i++) {
            ColumnSchema param = procedure.getParameter(i);
            int          mode  = param.getParameterMode();

            if (mode != SchemaObject.ParameterModes.PARAM_IN) {
                if (this.arguments[i].isDynamicParam()) {
                    int paramIndex = arguments[i].parameterIndex;

                    session.sessionContext.dynamicArguments[paramIndex] =
                        callArguments[i];
                    returnParams = true;
                } else {
                    int varIndex = arguments[i].getColumnIndex();

                    session.sessionContext.routineVariables[varIndex] =
                        callArguments[i];
                }
            }
        }

        if (returnParams) {
            result = Result.newCallResponse(
                this.getParametersMetaData().getParameterTypes(), this.id,
                session.sessionContext.dynamicArguments);
        }

        return result;
    }

    Result executePSMProcedure(Session session) {

        int variableCount = procedure.getVariableCount();

        if (variableCount > 0) {
            session.sessionContext.routineVariables =
                new Object[variableCount];
        }

        Result result = procedure.statement.execute(session);

        if (!result.isError()) {
            result = Result.updateZeroResult;
        }

        return result;
    }

    Result executeJavaProcedure(Session session) {

        Result   result        = Result.updateZeroResult;
        int      extraArg      = procedure.javaMethodWithConnection ? 1
                                                                    : 0;
        Object[] callArguments = session.sessionContext.routineArguments;
        Object[] data          = new Object[callArguments.length + extraArg];

        data = procedure.convertArgsToJava(session, callArguments);

        if (procedure.javaMethodWithConnection) {
            data[0] = session.getInternalConnection();
        }

        result = procedure.invokeJavaMethod(session, data);

        procedure.convertArgsToSQL(session, callArguments, data);

        return result;
    }

    Result getExpressionResult(Session session) {

        Object o;    // expression return value
        Result r;

        session.sessionData.startRowProcessing();

        o = expression.getValue(session);

        if (o instanceof Result) {
            return (Result) o;
        }

        if (resultMetaData == null) {
            getResultMetaData();
        }

        r = Result.newSingleColumnResult(resultMetaData);

        Object[] row;

        if (expression.getDataType().isArrayType()) {
            row    = new Object[1];
            row[0] = o;
        } else if (o instanceof Object[]) {
            row = (Object[]) o;
        } else {
            row    = new Object[1];
            row[0] = o;
        }

        r.getNavigator().add(row);

        return r;
    }

    SubQuery[] getSubqueries(Session session) {

        OrderedHashSet subQueries = null;

        if (expression != null) {
            subQueries = expression.collectAllSubqueries(subQueries);
        }

        for (int i = 0; i < arguments.length; i++) {
            subQueries = arguments[i].collectAllSubqueries(subQueries);
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

    public ResultMetaData getResultMetaData() {

        if (resultMetaData != null) {
            return resultMetaData;
        }

        switch (type) {

            case StatementTypes.CALL : {
                if (expression == null) {
                    return ResultMetaData.emptyResultMetaData;
                }

                // TODO:
                //
                // 1.) standard to register metadata for columns of
                // the primary result set, if any, generated by call
                //
                // 2.) Represent the return value, if any (which is
                // not, in truth, a result set), as an OUT parameter
                //
                // For now, I've reverted a bunch of code I had in place
                // and instead simply reflect things as the are, describing
                // a single column result set that communicates
                // the return value.  If the expression generating the
                // return value has a void return type, a result set
                // is described whose single column is of type NULL
                ResultMetaData md = ResultMetaData.newResultMetaData(1);
                ColumnBase column =
                    new ColumnBase(null, null, null,
                                   StatementDMQL.RETURN_COLUMN_NAME);

                column.setType(expression.getDataType());

                md.columns[0] = column;

                md.prepareData();

                resultMetaData = md;

                return md;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementProcedure");
        }
    }

    /**
     * Returns the metadata for the placeholder parameters.
     */
    public ResultMetaData getParametersMetaData() {

        /** @todo - change the auto-names to the names of params */
        return super.getParametersMetaData();
    }

    void collectTableNamesForRead(OrderedHashSet set) {

        if (expression == null) {
            set.addAll(procedure.getTableNamesForRead());
        } else {
            for (int i = 0; i < subqueries.length; i++) {
                if (subqueries[i].queryExpression != null) {
                    subqueries[i].queryExpression.getBaseTableNames(set);
                }
            }

            for (int i = 0; i < routines.length; i++) {
                set.addAll(routines[i].getTableNamesForRead());
            }
        }
    }

    void collectTableNamesForWrite(OrderedHashSet set) {

        if (expression == null) {
            set.addAll(procedure.getTableNamesForWrite());
        }
    }
}
