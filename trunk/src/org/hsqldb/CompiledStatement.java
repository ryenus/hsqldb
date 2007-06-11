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
import org.hsqldb.Parser.CompileContext;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;
import org.hsqldb.lib.ArrayUtil;

/**
 * A simple structure class for holding the products of
 * statement compilation for later execution.
 *
 * @author  boucherb@users
 * @version 1.9.0
 * @since 1.7.2
 */

// fredt@users 20040404 - patch 1.7.2 - fixed type resolution for parameters
// boucherb@users 200404xx - patch 1.7.2 - changed parameter naming scheme for SQLCI client usability/support
// fredt@users 20050609 - 1.8.0 - fixed EXPLAIN PLAN by implementing describe(Session)
// fredt@users - 1.9.0 - support for generated column reporting
// fredt@users - 1.9.0 - support for multi-row inserts
public final class CompiledStatement {

    public static final String PCOL_PREFIX        = "@p";
    static final String        RETURN_COLUMN_NAME = "@p0";

    // enumeration of allowable CompiledStatement types
    public static final int UNKNOWN       = 0;
    public static final int INSERT_VALUES = 1;
    public static final int INSERT_SELECT = 2;
    public static final int UPDATE        = 3;
    public static final int DELETE        = 4;
    public static final int SELECT        = 5;
    public static final int SELECT_INTO   = 6;
    public static final int CALL          = 7;
    public static final int MERGE         = 8;
    public static final int SET           = 9;
    public static final int DDL           = 10;

    // resusables
    static final Expression[] parameters0 = new Expression[0];
    static final Type[]       paramTypes0 = new Type[0];
    static final SubQuery[]   subqueries0 = new SubQuery[0];

    /** id in CompiledStatementManager */
    int id;

    /** false when cleared */
    boolean isValid = true;

    /** target table for INSERT_XXX, UPDATE and DELETE and MERGE */
    Table           targetTable;
    RangeVariable[] targetRangeVariables;

    /** source table for MERGE */
    Table sourceTable;

    /** condition expression for UPDATE, MERGE and DELETE */
    Expression condition;

    /** column map for INSERT_XXX, UPDATE */
    int[] columnMap;

    /** column map for INSERT operation via MERGE */
    int[] insertColumnMap;

    /** column map for UPDATE operation via MERGE */
    int[] updateColumnMap;

    /** Column value Expressions for UPDATE and MERGE. */
    Expression[] updateExpressions;

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

    /** Expression to be evaluated when this is a CALL statement. */
    Expression expression;

    /**
     * Select to be evaluated when this is an INSERT_SELECT or
     * SELECT statement
     */
    Select select;

    /**
     * Parse-order array of Expression objects, all of iType == PARAM ,
     * involved in some way in any INSERT_XXX, UPDATE, DELETE, SELECT or
     * CALL CompiledStatement
     */
    Expression[] parameters;

    /**
     * Type[] contains type of each parameter
     */
    Type[] paramTypes;

    /**
     * int[] contains column indexes for generated values
     */
    int[] generatedIndexes;

    /**
     * ResultMetaData for generated values
     */
    ResultMetaData generatedResultMetaData;

    /**
     * Subqueries inverse usage depth order
     */
    SubQuery[] subqueries;

    /**
     * The type of this CompiledStatement. <p>
     *
     * One of: <p>
     *
     * <ol>
     *  <li>UNKNOWN
     *  <li>INSERT_VALUES
     *  <li>INSERT_SELECT
     *  <li>UPDATE
     *  <li>DELETE
     *  <li>SELECT
     *  <li>CALL
     *  <li>MERGE
     *  <li>DDL
     * </ol>
     */
    int type;

    /**
     * The SQL string that produced this compiled statement
     */
    String sql;

    /**
     * The default schema name used to resolve names in the sql
     */
    final HsqlName schemaHsqlName;

    /**
     * Total number of RangeIterator objects used
     */
    int rangeIteratorCount;

    /**
     * Database objects used
     */
    OrderedHashSet sequenceExpressions;
    OrderedHashSet routineExpressions;
    HsqlArrayList  rangeVariables;

    /**
     * Creates a new instance of CompiledStatement for DDL
     *
     */
    CompiledStatement(HsqlName schema) {

        parameters     = parameters0;
        paramTypes     = paramTypes0;
        subqueries     = subqueries0;
        type           = DDL;
        schemaHsqlName = schema;
    }

    /**
     * Initializes this as a DELETE statement
     */
    CompiledStatement(Session session, RangeVariable[] rangeVars,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName       = session.currentSchema;
        this.targetTable          = rangeVars[0].rangeTable;
        this.targetRangeVariables = rangeVars;
        type                      = DELETE;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as an UPDATE statement.
     */
    CompiledStatement(Session session, RangeVariable rangeVars[],
                      int[] updateColumnMap, Expression[] colExpressions,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName    = session.currentSchema;
        this.targetTable       = rangeVars[0].rangeTable;
        this.updateColumnMap   = updateColumnMap;
        this.updateExpressions = colExpressions;
        this.updateCheckColumns =
            targetTable.getColumnCheckList(updateColumnMap);
        this.targetRangeVariables = rangeVars;
        type                      = UPDATE;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as an INSERT_VALUES statement.
     */
    CompiledStatement(Session session, Table targetTable, int[] columnMap,
                      Expression insertExpression, boolean[] checkColumns,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName     = session.currentSchema;
        this.targetTable        = targetTable;
        this.insertColumnMap    = columnMap;
        this.insertCheckColumns = checkColumns;
        this.insertExpression   = insertExpression;
        type                    = INSERT_VALUES;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as an INSERT_SELECT statement.
     */
    CompiledStatement(Session session, Table targetTable, int[] columnMap,
                      boolean[] checkColumns, Select select,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName     = session.currentSchema;
        this.targetTable        = targetTable;
        this.insertColumnMap    = columnMap;
        this.insertCheckColumns = checkColumns;
        this.select             = select;
        type                    = INSERT_SELECT;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a SELECT statement.
     */
    CompiledStatement(Session session, Select select,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName = session.currentSchema;
        this.select         = select;
        type                = (select.intoTableName == null) ? SELECT
                                                             : SELECT_INTO;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a CALL statement.
     */
    CompiledStatement(Session session, Expression expression,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName = session.currentSchema;
        this.expression     = expression;
        type                = CALL;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a MERGE statement.
     */
    CompiledStatement(Session session, RangeVariable[] targetRangeVars,
                      int[] insertColMap, int[] updateColMap,
                      boolean[] checkColumns, Expression mergeCondition,
                      Expression insertExpr, Expression[] updateExpr,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName       = session.currentSchema;
        this.sourceTable          = targetRangeVars[0].rangeTable;
        this.targetTable          = targetRangeVars[1].rangeTable;
        this.insertCheckColumns   = checkColumns;
        this.insertColumnMap      = insertColMap;
        this.updateColumnMap      = updateColMap;
        this.insertExpression     = insertExpr;
        this.updateExpressions    = updateExpr;
        this.targetRangeVariables = targetRangeVars;
        this.condition            = mergeCondition;
        type                      = MERGE;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * Instantiate this as a SET statement.
     */
    CompiledStatement(Session session, Table table, RangeVariable rangeVars[],
                      int[] updateColumnMap, Expression[] colExpressions,
                      CompileContext compileContext) throws HsqlException {

        this.schemaHsqlName    = session.currentSchema;
        this.targetTable       = table;
        this.updateColumnMap   = updateColumnMap;
        this.updateExpressions = colExpressions;
        this.updateCheckColumns =
            targetTable.getColumnCheckList(updateColumnMap);
        this.targetRangeVariables = rangeVars;
        type                      = SET;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    /**
     * For the output of the statement
     */
    public ResultMetaData getGeneratedColumnMetaData() {
        return generatedResultMetaData;
    }

    /**
     * For the creation of the statement
     */
    public void setGeneratedColumnInfo(int generate, ResultMetaData meta) {

        // can support INSERT_SELECT also
        if (type != this.INSERT_VALUES) {
            return;
        }

        int colIndex = targetTable.getIdentityColumn();

        if (colIndex == -1) {
            return;
        }

        switch (generate) {

            case ResultConstants.RETURN_NO_GENERATED_KEYS :
                return;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES :
                int[] columnIndexes = meta.getGeneratedColumnIndexes();

                if (columnIndexes.length != 1) {
                    return;
                }

                if (columnIndexes[0] != colIndex) {
                    return;
                }

            // fall through
            case ResultConstants.RETURN_GENERATED_KEYS :
                generatedIndexes = new int[]{ colIndex };
                break;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES :
                String[] columnNames = meta.getGeneratedColumnNames();

                if (columnNames.length != 1) {
                    return;
                }

                if (targetTable.findColumn(columnNames[0]) != colIndex) {
                    return;
                }

                generatedIndexes = new int[]{ colIndex };
                break;
        }

        generatedResultMetaData =
            ResultMetaData.newResultMetaData(generatedIndexes.length);

        for (int i = 0; i < generatedIndexes.length; i++) {
            Column.setMetaDataColumnInfo(
                generatedResultMetaData, i, targetTable,
                targetTable.getColumn(generatedIndexes[i]));
        }
    }

    Object[] getGeneratedColumns(Object[] data) {

        if (generatedIndexes == null) {
            return null;
        }

        Object[] values = new Object[generatedIndexes.length];

        for (int i = 0; i < generatedIndexes.length; i++) {
            values[i] = data[generatedIndexes[i]];
        }

        return values;
    }

    boolean hasGeneratedColumns() {
        return generatedIndexes != null;
    }

    boolean[] getInsertOrUpdateColumnCheckList() {

        switch (type) {

            case INSERT_SELECT :
            case INSERT_VALUES :
                return insertCheckColumns;

            case UPDATE :
                return updateCheckColumns;

            case MERGE :
                boolean[] check =
                    (boolean[]) ArrayUtil.duplicateArray(insertCheckColumns);

                ArrayUtil.orBooleanArray(updateCheckColumns, check);

                return check;
        }

        return null;
    }

    private void setParameters(Expression[] params) {

        this.parameters = params;

        Type[] types = new Type[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            parameters[i].parameterIndex = i;
            types[i]                     = parameters[i].getDataType();
        }

        this.paramTypes = types;
    }

    void materializeSubQueries(Session session,
                               Set subqueryPopFlags) throws HsqlException {

        for (int i = 0; i < subqueries.length; i++) {
            SubQuery sq = subqueries[i];

            // VIEW working tables may be reused in a single query but they are filled only once
            if (!subqueryPopFlags.add(sq)) {
                continue;
            }

            if (!sq.isCorrelated) {
                sq.materialise(session);
            }
        }
    }

    void dematerializeSubQueries(Session session) {

        if (subqueries == null) {
            return;
        }

        for (int i = 0; i < subqueries.length; i++) {
            subqueries[i].dematerialiseAll(session);
        }
    }

    void clearVariables() {

        isValid            = false;
        targetTable        = null;
        condition          = null;
        insertColumnMap    = null;
        updateColumnMap    = null;
        updateExpressions  = null;
        insertExpression   = null;
        insertCheckColumns = null;
        expression         = null;
        select             = null;
        parameters         = null;
        paramTypes         = null;
        subqueries         = null;
    }

    void setDatabseObjects(CompileContext compileContext)
    throws HsqlException {

        Expression[] params = compileContext.getParameters();

        setParameters(params);

        subqueries          = compileContext.getSubqueries();
        rangeIteratorCount  = compileContext.getRangeVarCount();
        rangeVariables      = compileContext.rangeVariables;
        sequenceExpressions = compileContext.usedSequences;
        routineExpressions  = compileContext.usedRoutineNames;
    }

    /**
     * Determines if the authorizations are adequate
     * to execute the compiled object. Completion requires the list of
     * all database objects in a compiled statement.
     */
    void checkAccessRights(Session session) throws HsqlException {

        if (targetTable != null) {
            targetTable.checkDataReadOnly();
        }

        if (session.isAdmin()) {
            return;
        }

        if (sequenceExpressions != null) {
            for (int i = 0; i < sequenceExpressions.size(); i++) {
                NumberSequence s = (NumberSequence) sequenceExpressions.get(i);

                session.getUser().checkAccess(s);
            }
        }

        if (routineExpressions != null) {
            for (int i = 0; i < routineExpressions.size(); i++) {
                String s = (String) routineExpressions.get(i);

                session.getUser().checkAccess(s);
            }
        }

        if (rangeVariables != null) {
            for (int i = 0; i < rangeVariables.size(); i++) {
                RangeVariable range = (RangeVariable) rangeVariables.get(i);

                if (range.rangeTable.getSchemaName()
                        == SchemaManager.SYSTEM_SCHEMA_HSQLNAME) {
                    continue;
                }

                session.getUser().checkSelect(range.rangeTable,
                                              range.usedColumns);
            }
        }

        switch (type) {

            case CALL : {
                break;
            }
            case INSERT_SELECT : {
                session.checkReadWrite();
                session.getUser().checkInsert(targetTable, insertCheckColumns);
                targetTable.checkDataReadOnly();

                // fall through
            }
            case SELECT :
                break;

            case DELETE : {

                // session level user rights
                session.checkReadWrite();
                session.getUser().checkDelete(targetTable);

                // object readonly
                targetTable.checkDataReadOnly();

                break;
            }
            case INSERT_VALUES : {
                session.checkReadWrite();
                session.getUser().checkInsert(targetTable, insertCheckColumns);
                targetTable.checkDataReadOnly();

                break;
            }
            case UPDATE : {
                session.checkReadWrite();
                session.getUser().checkUpdate(targetTable, updateCheckColumns);
                targetTable.checkDataReadOnly();

                break;
            }
            case MERGE : {
                session.checkReadWrite();
                session.getUser().checkInsert(targetTable, insertCheckColumns);
                session.getUser().checkUpdate(targetTable, updateCheckColumns);
                targetTable.checkDataReadOnly();

                break;
            }
            case DDL : {
                session.checkReadWrite();
            }
        }
    }

    /**
     * Returns the metadata, which is empty if the CompiledStatement does not
     * generate a Result.
     */
    ResultMetaData getResultMetaData() {

        switch (type) {

            case CALL : {

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

                md.colNames[0]   = CompiledStatement.RETURN_COLUMN_NAME;
                md.colTypes[0]   = expression.getDataType();
                md.classNames[0] = expression.getValueClassName();

                // no more setup for r; all the defaults apply
                return md;
            }
            case SELECT :
                return select.getMetaData();

            case SELECT_INTO :
            case DELETE :
            case INSERT_SELECT :
            case INSERT_VALUES :
            case UPDATE :
            case DDL :
                return ResultMetaData.emptyMetaData;

            default :
                throw Trace.runtimeError(
                    Trace.UNSUPPORTED_INTERNAL_OPERATION,
                    "CompiledStatement.getResultMetaData()");
        }
    }

    /**
     * Returns the metadata for the placeholder parameters.
     */
    ResultMetaData getParametersMetaData() {

        ResultMetaData metaData;
        int            offset;
        int            idx;
        boolean        hasReturnValue;

        offset = 0;

// NO:  Not yet
//        hasReturnValue = (type == CALL && !expression.isProcedureCall());
//
//        if (hasReturnValue) {
//            outlen++;
//            offset = 1;
//        }
        metaData = ResultMetaData.newParameterMetaData(parameters.length);

// NO: Not yet
//        if (hasReturnValue) {
//            e = expression;
//            out.sName[0]       = DIProcedureInfo.RETURN_COLUMN_NAME;
//            out.sClassName[0]  = e.getValueClassName();
//            out.colType[0]     = e.getDataType();
//            out.colSize[0]     = e.getColumnSize();
//            out.colScale[0]    = e.getColumnScale();
//            out.nullability[0] = e.nullability;
//            out.isIdentity[0]  = false;
//            out.paramMode[0]   = expression.PARAM_OUT;
//        }
        for (int i = 0; i < parameters.length; i++) {
            idx = i + offset;

            // always i + 1.  We currently use the convention of @p0 to name the
            // return value OUT parameter
            metaData.colNames[idx] = CompiledStatement.PCOL_PREFIX + (i + 1);

            // sLabel is meaningless in this context.
            metaData.classNames[idx] = parameters[i].getValueClassName();
            metaData.colTypes[idx]   = parameters[i].dataType;

            // TODO: The rules should be: - always nullable unless:
            //
            //                              1.) type of site is Java primitive,
            //                              e.g. a SQL-invoked routine
            //                              parameter that maps to a
            //                              primitive Java method argument
            //
            //                              2.) (future) the site is declared
            //                              not null, e.g. a SQL language
            //                              routine parameter explicitly
            //                              declared not null.
            metaData.colNullable[idx] = parameters[i].nullability;
            metaData.isIdentity[idx]  = parameters[i].isIdentity;

            // currently will always be Expression.PARAM_IN
            metaData.paramModes[idx] = parameters[i].paramMode;
        }

        return metaData;
    }

    /**
     * Retrieves a String representation of this object.
     */
    public String describe(Session session) {

        try {
            return describeImpl(session);
        } catch (Exception e) {
            e.printStackTrace();

            return e.toString();
        }
    }

    /**
     * Provides the toString() implementation.
     */
    private String describeImpl(Session session) throws Exception {

        StringBuffer sb;

        sb = new StringBuffer();

        switch (type) {

            case SELECT : {
                sb.append(select.describe(session));
                appendParms(sb).append('\n');
                appendSubqueries(sb);

                return sb.toString();
            }
            case INSERT_VALUES : {
                sb.append("INSERT VALUES");
                sb.append('[').append('\n');
                appendMultiColumns(sb, insertColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            case INSERT_SELECT : {
                sb.append("INSERT SELECT");
                sb.append('[').append('\n');
                appendColumns(sb, insertColumnMap).append('\n');
                appendTable(sb).append('\n');
                sb.append(select.describe(session)).append('\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            case UPDATE : {
                sb.append("UPDATE");
                sb.append('[').append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            case DELETE : {
                sb.append("DELETE");
                sb.append('[').append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            case CALL : {
                sb.append("CALL");
                sb.append('[');
                sb.append(expression.describe(session)).append('\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            case MERGE : {
                sb.append("MERGE");
                sb.append('[').append('\n');
                appendMultiColumns(sb, insertColumnMap).append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[2].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(sb).append(']');

                return sb.toString();
            }
            default : {
                return "UNKNOWN";
            }
        }
    }

    private StringBuffer appendSubqueries(StringBuffer sb) {

        sb.append("SUBQUERIES[");

        for (int i = 0; i < subqueries.length; i++) {
            sb.append("\n[level=").append(subqueries[i].level).append('\n');

            if (subqueries[i].select != null) {
                sb.append("org.hsqldb.Select@").append(
                    Integer.toHexString(subqueries[i].select.hashCode()));
            }

            sb.append("]");
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendTable(StringBuffer sb) {

        sb.append("TABLE[").append(targetTable.getName().name).append(']');

        return sb;
    }

    private StringBuffer appendSourceTable(StringBuffer sb) {

        sb.append("SOURCE TABLE[").append(sourceTable.getName().name).append(
            ']');

        return sb;
    }

    private StringBuffer appendColumns(StringBuffer sb, int[] columnMap) {

        if (columnMap == null || updateExpressions == null) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int i = 0; i < columnMap.length; i++) {
            sb.append('\n').append(columnMap[i]).append(':').append(
                ' ').append(
                targetTable.getColumn(columnMap[i]).columnName.name).append(
                '[').append(updateExpressions[i]).append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendColumns(StringBuffer sb) {

        sb.append("COLUMNS=[");
/*
        for (int i = 0; i < columnMap.length; i++) {
            sb.append('\n').append(columnMap[i]).append(':');
            .append(' ').append(
                targetTable.getColumn(columnMap[i]).columnName.name).append(
                '[').append(columnExpressions[i]).append(']');

        }
 */
        sb.append(']');

        return sb;
    }

    private StringBuffer appendMultiColumns(StringBuffer sb, int[] columnMap) {

        if (columnMap == null || multiColumnValues == null) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int j = 0; j < multiColumnValues.length; j++) {
            for (int i = 0; i < columnMap.length; i++) {
                sb.append('\n').append(columnMap[i]).append(':').append(
                    ' ').append(
                    targetTable.getColumn(
                        columnMap[i]).columnName.name).append('[').append(
                            multiColumnValues[j][i]).append(']');
            }
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendParms(StringBuffer sb) {

        sb.append("PARAMETERS=[");

        for (int i = 0; i < parameters.length; i++) {
            sb.append('\n').append('@').append(i).append('[').append(
                parameters[i]).append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendCondition(Session session, StringBuffer sb) {

        return condition == null ? sb.append("CONDITION[]\n")
                                 : sb.append("CONDITION[").append(
                                     condition.describe(session)).append(
                                     "]\n");
    }
}
