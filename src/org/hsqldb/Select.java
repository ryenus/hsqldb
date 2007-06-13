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
import org.hsqldb.Parser.CompileContext;
import org.hsqldb.RangeVariable.RangeIterator;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.navigator.DataRowSetNavigator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;

// fredt@users - 1.9.0 - complete rewrite of column resolution and query processing

/**
 * The compiled representation of an SQL SELECT.
 *
 * The query processing functionality of this class was inherited from Hypersonic.
 * It was completely rewritten in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author fredt@users
 *
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class Select {

    boolean         isDistinctSelect;
    public boolean  isAggregated;
    public boolean  isAggregateSorted;
    private boolean isGrouped;
    private HashSet groupColumnNames;
    RangeVariable[] rangeVariables;
    HsqlArrayList   rangeVariableList;
    int[]           rangeSequence;
    Expression      limitCondition;
    Expression      queryCondition;           // null means no condition
    Expression      havingCondition;          // null means none
    Expression[]    exprColumns;              // 'result', 'group' and 'order' columns
    HsqlArrayList   exprColumnList;
    public int      visibleColumnCount;       // columns in 'result'
    int             groupByColumnCount;       // columns in 'group by'
    int             havingColumnCount;        // columns in 'having' (0 or 1)
    public int      orderByColumnCount;       // columns in 'order by'
    public int      orderByStart;
    public int      orderByLimitIndex;        // index of first possible column beyond order by cols
    public int[] sortOrder;
    public int[] sortDirection;
    boolean      sortUnion;                   // if true, sort the result of the full union
    HsqlName     intoTableName;               // null means not select..into
    public int   intoType;

    //
    boolean                 isMain;           // has no union or is first in union chain
    Select[]                unionArray;       // only set in the first Select in a union chain
    int                     unionMaxDepth;    // max unionDepth in chain
    Select                  unionSelect;      // next select in union chain or null
    public int              unionType;        // type of union op with next select
    int                     unionDepth;
    public static final int NOUNION   = 0,
                            UNION     = 1,
                            UNIONALL  = 2,
                            INTERSECT = 3,
                            EXCEPT    = 4;
    private boolean         simpleLimit;      // true if maxrows can be uses as is
    ResultMetaData          resultMetaData;
    Expression[]            aggregates;

    //
    OrderedHashSet unresolvedColumns;

    //
    CompileContext compileContext;

    //
    OrderedHashSet tempSet = new OrderedHashSet();

    //
    boolean isResolved;

    private Select() {}

    Select(CompileContext compileContext, boolean isMain) {

        this.compileContext = compileContext;
        this.isMain         = isMain;
        rangeVariableList   = new HsqlArrayList();
        exprColumnList      = new HsqlArrayList();
    }

    void addRangeVariable(RangeVariable rangeVar) {
        rangeVariableList.add(rangeVar);
    }

    void finaliseRangeVariables() {

        if (rangeVariables == null
                || rangeVariables.length < rangeVariableList.size()) {
            rangeVariables = new RangeVariable[rangeVariableList.size()];

            rangeVariableList.toArray(rangeVariables);
/*
            rangeSequence = new int[rangeVariables.length];

            ArrayUtil.fillSequence(rangeSequence);
*/
        }
    }

    void addSelectColumnExpression(Expression e) throws HsqlException {

        if (e.exprType == Expression.ROW) {

            // SQL error messsage
            throw Trace.error(Trace.WRONG_DATA_TYPE, e.getDDL());
        }

        exprColumnList.add(e);
    }

    void addQueryCondition(Expression e) {
        queryCondition = e;
    }

    void addGroupByColumnExpression(Expression e) throws HsqlException {

        if (e.exprType == Expression.ROW) {

            // SQL error messsage
            throw Trace.error(Trace.UNRESOLVED_TYPE, e.getDDL());
        }

        exprColumnList.add(e);

        isGrouped = true;

        groupByColumnCount++;
    }

    void addHavingExpression(Expression e) {

        exprColumnList.add(e);

        havingCondition   = e;
        havingColumnCount = 1;
    }

    void addOrderByExpression(Expression e) throws HsqlException {

        exprColumnList.add(e);

        orderByColumnCount++;
    }

    void addUnionOrderByExpression(Expression e) throws HsqlException {

        exprColumnList.add(e);

        orderByColumnCount++;
    }

    public void checkUniqueColumnNameSet(OrderedHashSet columns)
    throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable  range = (RangeVariable) rangeVariableList.get(i);
            HashMappedList columnList = range.rangeTable.columnList;

            for (int j = 0; j < columnList.size(); j++) {
                String name = ((Column) columnList.get(j)).columnName.name;

                if (range.namedJoinColumns != null
                        && range.namedJoinColumns.contains(name)) {
                    continue;
                }

                if (columns.contains(name)) {
                    if (set.contains(name)) {
                        throw Trace.error(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE);
                    }

                    set.add(name);
                }
            }
        }
    }

    public OrderedHashSet getUniqueColumnNameSet() throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable  range = (RangeVariable) rangeVariableList.get(i);
            HashMappedList columnList = range.rangeTable.columnList;

            for (int j = 0; j < columnList.size(); j++) {
                String name = ((Column) columnList.get(j)).columnName.name;

                if (range.namedJoinColumns != null
                        && range.namedJoinColumns.contains(name)) {
                    continue;
                }

                if (set.contains(name)) {
                    throw Trace.error(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE);
                }

                set.add(name);
            }
        }

        return set;
    }

    int addAllJoinedColumns(HsqlArrayList columnList, int position) {

        HsqlArrayList currentColumnList = new HsqlArrayList();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].addTableColumns(currentColumnList);
        }

        for (int i = 0; i < currentColumnList.size(); i++) {
            columnList.add(position, currentColumnList.get(i));

            position++;
        }

        return position;
    }

    void finaliseAndResolve(boolean resolveAll) throws HsqlException {

        finaliseColumns();
        resolveNonSelectColumnRefernces();

        if (resolveAll) {
            checkColumnsResolved();
        }
    }

    void finaliseColumns() {

        exprColumns = new Expression[exprColumnList.size()];

        exprColumnList.toArray(exprColumns);

        orderByStart = visibleColumnCount + groupByColumnCount
                       + havingColumnCount;
        orderByLimitIndex = orderByStart + orderByColumnCount;

        if (orderByColumnCount != 0) {
            for (int i = 0; i < visibleColumnCount; i++) {
                exprColumns[i].queryTableColumnIndex = i;
            }
        }
    }

    void checkColumnsResolved() throws HsqlException {
        Expression.checkColumnsResolved(unresolvedColumns);
    }

    void finishPrepare(Session session) throws HsqlException {

        if (isMain) {
            prepareUnions();
        }

        if (areColumnsResolved()) {
            resolveTypesAndPrepare();
        }
    }

    /**
     * Resolves all column expressions in the GROUP BY clause and beyond.
     * Replaces any alias column expression in the ORDER BY cluase
     * with the actual select column expression.
     *
     * @throws HsqlException
     */
    void resolveNonSelectColumnRefernces() throws HsqlException {

        for (int i = visibleColumnCount; i < orderByStart; i++) {
            resolveColumnReferences(exprColumns[i]);
        }

        if (queryCondition != null) {
            resolveColumnReferences(queryCondition);
        }

        // replace the aliases with expressions
        // replace column names with expressions and resolve the table columns
        for (int i = orderByStart; i < orderByLimitIndex; i++) {
            Expression orderBy = exprColumns[i];

            replaceColumnIndexInOrderBy(orderBy);

            if (orderBy.eArg.queryTableColumnIndex != -1) {
                continue;
            }

            orderBy.replaceAliasInOrderBy(exprColumns, visibleColumnCount,
                                          null);
            resolveColumnReferences(orderBy);
        }
    }

    private void replaceColumnIndexInOrderBy(Expression orderBy)
    throws HsqlException {

        Expression e = orderBy.eArg;

        if (e.getType() != Expression.VALUE) {
            return;
        }

        // order by 1,2,3
        if (e.getDataType().type == Types.SQL_INTEGER) {
            int i = ((Integer) e.getValue(null)).intValue();

            if (0 < i && i <= visibleColumnCount) {
                orderBy.eArg = (Expression) exprColumnList.get(i - 1);

                orderBy.setAggregateSpec();
            }

            return;
        }

        throw Trace.error(Trace.INVALID_ORDER_BY);
    }

    /**
     * Uses SQL standard rules to determine the types of UNION columns
     */
    private void resolveUnionColumnTypes() throws HsqlException {

        for (int i = 0; i < visibleColumnCount; i++) {
            Type type = null;

            for (int j = 0; j < unionArray.length; j++) {
                Type exprType = unionArray[j].exprColumns[i].dataType;

                type = Type.getAggregatedType(exprType, type);
            }

            for (int j = 0; j < unionArray.length; j++) {
                unionArray[j].exprColumns[i].setDataType(type);
            }
        }
    }

    public void collectAllBaseColumnExpressions(OrderedHashSet set) {

        for (Select select = this; select != null;
                select = select.unionSelect) {
            for (int i = 0; i < orderByLimitIndex; i++) {
                exprColumns[i].collectAllBaseColumnExpressions(set);
            }

            if (queryCondition != null) {
                queryCondition.collectAllBaseColumnExpressions(set);
            }
        }
    }

    void collectRangeVariables(RangeVariable[] rangeVars, Set set) {

        for (int i = 0; i < orderByLimitIndex; i++) {
            exprColumns[i].collectRangeVariables(rangeVars, set);
        }

        if (queryCondition != null) {
            queryCondition.collectRangeVariables(rangeVars, set);
        }
    }

    void markRangeVariables(RangeVariable[] rangeVars, boolean[] flags) {

        for (int i = 0; i < orderByLimitIndex; i++) {
            exprColumns[i].markRangeVariables(rangeVars, flags);
        }

        if (queryCondition != null) {
            queryCondition.markRangeVariables(rangeVars, flags);
        }
    }

    boolean hasOuterReference(RangeVariable range) {

        if (unresolvedColumns == null) {
            return false;
        }

        for (int i = 0; i < unresolvedColumns.size(); i++) {
            if (((Expression) unresolvedColumns.get(i)).hasReference(range)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Sets the types of all the expressions used in this Select.
     *
     * @throws HsqlException
     */
    void resolveTypes() throws HsqlException {

        for (int i = 0; i < orderByLimitIndex; i++) {
            Expression e = exprColumns[i];

            e.resolveTypes(null);

            if (e.exprType == Expression.ROW) {
                throw Trace.error(Trace.WRONG_DATA_TYPE);
            }
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            Expression e = rangeVariables[i].nonIndexJoinCondition;

            if (e != null) {
                e.resolveTypes(null);
            }
        }

        if (queryCondition != null) {
            Expression e = queryCondition;

            e.resolveTypes(null);
        }
    }

    void resolveColumnReferences(Expression e) throws HsqlException {

        finaliseRangeVariables();

        unresolvedColumns = e.resolveColumnReferences(rangeVariables,
                unresolvedColumns);
    }

    boolean areColumnsResolved() {
        return unresolvedColumns == null || unresolvedColumns.isEmpty();
    }

    /**
     * Attempts to resolve unresolved correlated column Expressions in this
     * Select and assigns any still unresolved column to the given unresolved
     * Expression set. Unresolved columns for union selects have already
     * been added to this.
     */
    OrderedHashSet resolveCorrelatedColumns(RangeVariable[] f,
            OrderedHashSet unresolvedSet) throws HsqlException {

        if (unresolvedColumns == null) {
            return unresolvedSet;
        }

        for (int i = 0; i < unresolvedColumns.size(); i++) {
            Expression e = (Expression) unresolvedColumns.get(i);

            unresolvedSet = e.resolveColumnReferences(f, unresolvedSet);
        }

        return unresolvedSet;
    }

    private void setRangeVariableConditions() throws HsqlException {

        RangeVariableResolver rangeResolver =
            new RangeVariableResolver(rangeVariables, queryCondition,
                                      compileContext);

        rangeResolver.processConditions();

        rangeVariables = rangeResolver.rangeVariables;
        queryCondition = null;
    }

    /**
     * Resolves expressions and pepares the metadata for the result.
     * Called externally.
     */
    void resolveTypesAndPrepare() throws HsqlException {

        if (isResolved) {
            return;
        }

        if (unionArray == null) {
            resolveTypes();
            setRangeVariableConditions();
            checkConflicts();
            setResultMetaData();
        } else {
            for (int i = 0; i < unionArray.length; i++) {
                unionArray[i].resolveTypes();
                unionArray[i].setRangeVariableConditions();
            }

            resolveUnionColumnTypes();

            for (int i = 0; i < unionArray.length; i++) {
                unionArray[i].checkConflicts();
                unionArray[i].setResultMetaData();
            }
        }

        isResolved = true;
    }

    private void checkConflicts() throws HsqlException {

        // - 1.9.0 is standard compliant but has more extended support for
        //   referencing columns
        // - check there is no direct aggregate expression in group by
        // - check each expression in select list can be
        //   decomposed into the expressions in group by or any aggregates
        //   this allows direct function of group by expressions, but
        //   doesn't allow indirect functions. e.g.
        //     select 2*abs(cola) , sum(colb) from t group by abs(cola) // ok
        //     select 2*(cola + 10) , sum(colb) from t group by cola + 10 // ok
        //     select abs(cola) , sum(colb) from t group by cola // ok
        //     select 2*cola + 20 , sum(colb) from t group by cola + 10 // not allowed although correct
        //     select cola , sum(colb) from t group by abs(cola) // not allowed because incorrect
        // - group by can introduce invisible, derived columns into the query table
        // - check the having expression can be decomposed into
        //   select list expresions plus group by expressions
        // - having cannot introduce additional, derived columns
        // - having cannot reference columns not in the select or group by list
        // - if there is any aggregate in select list but no group by, no
        //   non-aggregates is allowed
        // - check order by columns
        // - if distinct select, order by must be composed of the select list columns
        // - if grouped by, then order by should be decomposed into the
        //   select list plus group by list
        // - references to column aliases are allowed only in order by (Standard
        //   compliance) and take precendence over references to non-alias
        //   column names.
        // - references to table / correlation and column list in correlation
        //   names are handles according to the Standard
        //  fredt@users
        tempSet.clear();

        for (int i = 0; i < visibleColumnCount; i++) {
            if (exprColumns[i].dataType == null) {
                throw Trace.error(Trace.UNRESOLVED_TYPE);
            }

            Expression.collectAllExpressions(tempSet, exprColumns[i],
                                             Expression.aggregateFunctionSet,
                                             Expression.subqueryExpressionSet);

            if (!tempSet.isEmpty()) {
                tempSet.clear();

                isAggregated = true;

                break;
            }
        }

        if (havingColumnCount != 0) {
            Expression.collectAllExpressions(
                tempSet, exprColumns[visibleColumnCount + groupByColumnCount],
                Expression.aggregateFunctionSet,
                Expression.subqueryExpressionSet);

            if (!tempSet.isEmpty()) {
                tempSet.clear();

                isAggregated = true;
            }
        }

        if (isGrouped) {
            for (int i = visibleColumnCount;
                    i < visibleColumnCount + groupByColumnCount; i++) {
                Expression.collectAllExpressions(
                    tempSet, exprColumns[i], Expression.aggregateFunctionSet,
                    Expression.subqueryExpressionSet);

                if (!tempSet.isEmpty()) {
                    throw Trace.error(Trace.INVALID_GROUP_BY,
                                      ((Expression) tempSet.get(0)).getDDL());
                }
            }

            for (int i = 0; i < visibleColumnCount; i++) {
                if (!exprColumns[i].isComposedOf(
                        exprColumns, visibleColumnCount,
                        visibleColumnCount + groupByColumnCount,
                        Expression.aggregateFunctionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }

            if (!tempSet.isEmpty() && !resolveForGroupBy(tempSet)) {
                throw Trace.error(Trace.NOT_IN_AGGREGATE_OR_GROUP_BY,
                                  ((Expression) tempSet.get(0)).getDDL());
            }
        } else if (isAggregated) {
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression.collectAllExpressions(
                    tempSet, exprColumns[i], Expression.columnExpressionSet,
                    Expression.aggregateFunctionSet);

                if (!tempSet.isEmpty()) {
                    throw Trace.error(Trace.NOT_IN_AGGREGATE_OR_GROUP_BY,
                                      ((Expression) tempSet.get(0)).getDDL());
                }
            }
        }

        if (havingColumnCount != 0) {
            if (unresolvedColumns != null) {
                tempSet.addAll(unresolvedColumns);
            }

            for (int i = 0; i < visibleColumnCount + groupByColumnCount; i++) {
                tempSet.add(exprColumns[i]);
            }

            if (!exprColumns[visibleColumnCount + groupByColumnCount]
                    .isComposedOf(tempSet, Expression.aggregateFunctionSet)) {
                int error = isAggregated || isGrouped
                            ? Trace.NOT_IN_AGGREGATE_OR_GROUP_BY
                            : Trace.INVALID_HAVING;

                throw Trace
                    .error(error,
                           exprColumns[visibleColumnCount + groupByColumnCount]
                               .getDDL());
            }

            tempSet.clear();
        }

        if (isDistinctSelect || unionSelect != null) {
            for (int i = orderByStart; i < orderByLimitIndex; i++) {
                if (exprColumns[i].queryTableColumnIndex != -1) {
                    continue;
                }

                if (!exprColumns[i].isComposedOf(
                        exprColumns, 0, visibleColumnCount,
                        Expression.emptyExpressionSet)) {
                    int error = Trace.INVALID_ORDER_BY_IN_DISTINCT_SELECT;

                    throw Trace.error(error, exprColumns[i].getDDL());
                }
            }
        } else if (isGrouped) {
            for (int i = orderByStart; i < orderByLimitIndex; i++) {
                if (exprColumns[i].queryTableColumnIndex != -1) {
                    continue;
                }

                if (!exprColumns[i].isComposedOf(
                        exprColumns, 0,
                        visibleColumnCount + groupByColumnCount,
                        Expression.emptyExpressionSet)) {
                    int error = Trace.INVALID_ORDER_BY;

                    throw Trace.error(error, exprColumns[i].getDDL());
                }
            }
        }

        prepareSort();

        simpleLimit = (isDistinctSelect == false && isGrouped == false
                       && unionSelect == null && orderByColumnCount == 0);
    }

    boolean resolveForGroupBy(OrderedHashSet unresolvedSet) {

        for (int i = visibleColumnCount;
                i < visibleColumnCount + groupByColumnCount; i++) {
            Expression e = exprColumns[i];

            if (e.exprType == Expression.COLUMN) {
                RangeVariable range    = e.getRangeVariable();
                int           colIndex = e.getColumnIndex();

                range.columnsInGroupBy[colIndex] = true;
            }
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            range.hasKeyedColumnInGroupBy =
                range.rangeTable.hasUniqueNotNullIndexForColumns(
                    range.columnsInGroupBy);
        }

        OrderedHashSet set = null;

        for (int i = 0; i < unresolvedSet.size(); i++) {
            Expression e = (Expression) unresolvedSet.get(i);

            set = e.getUnkeyedColumns(set);
        }

        return set == null;
    }

    /**
     * This is called only on the first Select in a UNION chain.
     */
    private void prepareUnions() throws HsqlException {

        int count = 0;

        for (Select current = this; current != null;
                current = current.unionSelect, count++) {}

        if (count == 1) {
            if (unionDepth != 0) {
                throw Trace.error(Trace.MISSING_CLOSEBRACKET);
            }

            return;
        }

        unionArray = new Select[count];
        count      = 0;

        for (Select current = this; current != null;
                current = current.unionSelect, count++) {
            unionArray[count] = current;
            unionMaxDepth = current.unionDepth > unionMaxDepth
                            ? current.unionDepth
                            : unionMaxDepth;

            if (current != this && current.unresolvedColumns != null) {
                unresolvedColumns.addAll(current.unresolvedColumns);
            }
        }

        if (unionArray[unionArray.length - 1].unionDepth != 0) {
            throw Trace.error(Trace.MISSING_CLOSEBRACKET);
        }
    }

    private void setResultMetaData() throws HsqlException {

        resultMetaData = ResultMetaData.newResultMetaData(orderByLimitIndex);

        for (int i = 0; i < orderByLimitIndex; i++) {
            Expression e = exprColumns[i];

            resultMetaData.colTypes[i] = e.getDataType();

            if (i < visibleColumnCount) {
                String colname = e.getAlias();

                if (colname == null || colname.length() == 0) {
                    colname = HsqlNameManager.getAutoColumnNameString(i);

                    e.setAlias(colname, false);
                }

                resultMetaData.colLabels[i]     = e.getAlias();
                resultMetaData.isLabelQuoted[i] = e.isAliasQuoted();
                resultMetaData.schemaNames[i]   = e.getTableSchemaName();
                resultMetaData.tableNames[i]    = e.getTableName();
                resultMetaData.colNames[i]      = e.getColumnName();

                if (resultMetaData.isTableColumn(i)) {
                    resultMetaData.colNullable[i] = e.nullability;
                    resultMetaData.isIdentity[i]  = e.isIdentity;
                    resultMetaData.isWritable[i]  = e.isWritable;
                }

                resultMetaData.classNames[i] = e.getValueClassName();
            }
        }
    }

    private int getLimitStart(Session session) throws HsqlException {

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getArg().getValue(session);

            if (limit != null) {
                return limit.intValue();
            }
        }

        return 0;
    }

    /**
     * For SELECT LIMIT n m ....
     * finds cases where the result does not have to be fully built and
     * returns an adjusted rowCount with LIMIT params.
     */
    private int getLimitCount(Session session,
                              int rowCount) throws HsqlException {

        int limitCount = 0;

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getArg2().getValue(session);

            if (limit != null) {
                limitCount = limit.intValue();
            }
        }

        if (rowCount != 0 && (limitCount == 0 || rowCount < limitCount)) {
            limitCount = rowCount;
        }

        return limitCount;
    }

    /**
     * translate the rowCount into total number of rows needed from query,
     * including any rows skipped at the beginning
     */
    private int getMaxRowCount(Session session,
                               int rowCount) throws HsqlException {

        int limitStart = getLimitStart(session);
        int limitCount = getLimitCount(session, rowCount);

        if (!simpleLimit) {
            rowCount = Integer.MAX_VALUE;
        } else {
            if (rowCount == 0) {
                rowCount = limitCount;
            }

            if (rowCount == 0 || rowCount > Integer.MAX_VALUE - limitStart) {
                rowCount = Integer.MAX_VALUE;
            } else {
                rowCount += limitStart;
            }
        }

        return rowCount;
    }

    private void prepareSort() {

        if (orderByColumnCount == 0) {
            return;
        }

        sortOrder     = new int[orderByColumnCount];
        sortDirection = new int[orderByColumnCount];

        for (int i = orderByStart, j = 0; i < orderByLimitIndex; i++, j++) {
            int colindex = i;

            if (exprColumns[i].eArg.queryTableColumnIndex != -1) {
                colindex = exprColumns[i].eArg.queryTableColumnIndex;
            }

            sortOrder[j]     = colindex;
            sortDirection[j] = exprColumns[i].isDescending() ? -1
                                                             : 1;
        }
    }

    /**
     * Returns a single value result or throws if the result has more than
     * one row with one value.
     *
     * @param session context
     * @return the single valued result
     * @throws HsqlException
     */
    Object getValue(Session session) throws HsqlException {

        Result r    = getResult(session, 2);    // 2 records are required for test
        int    size = r.getNavigator().getSize();
        int    len  = r.getColumnCount();

        if (size == 0) {
            return null;
        } else if (size == 1) {
            r.initialiseNavigator();

            Object o = r.getSingleRowData()[0];

            return o;
        } else {
            throw Trace.error(Trace.CARDINALITY_VIOLATION_NO_SUBCLASS);
        }
    }

    /**
     * Returns a single row value result or throws if the result has more than
     * one row.
     *
     * @param session context
     * @return the single row result
     * @throws HsqlException
     */
    Object[] getValues(Session session) throws HsqlException {

        Result r    = getResult(session, 2);    // 2 records are required for test
        int    size = r.getNavigator().getSize();

        if (size == 0) {
            return new Object[visibleColumnCount];
        } else if (size == 1) {
            return r.getSingleRowData();
        } else {
            throw Trace.error(Trace.CARDINALITY_VIOLATION_NO_SUBCLASS);
        }
    }

    /**
     * Returns the result of executing this Select.
     *
     * @param maxrows may be 0 to indicate no limit on the number of rows.
     * Positive values limit the size of the result set.
     * @return the result of executing this Select
     * @throws HsqlException if a database access error occurs
     */
    Result getResult(Session session, int maxrows) throws HsqlException {

        Result r;

        if (unionArray == null) {
            r = getSingleResult(session, maxrows);
        } else {
            r = getResultMain(session, maxrows);
        }

        // fredt - now there is no need for the sort and group columns
        r.setColumnCount(visibleColumnCount);
        r.getNavigator().reset();

        return r;
    }

    private Result getResultMain(Session session,
                                 int rowCount) throws HsqlException {

        Result[] unionResults = new Result[unionArray.length];

        for (int i = 0; i < unionArray.length; i++) {
            unionResults[i] = unionArray[i].getSingleResult(session,
                    Integer.MAX_VALUE);
        }

        for (int depth = unionMaxDepth; depth >= 0; depth--) {
            for (int pass = 0; pass < 2; pass++) {
                for (int i = 0; i < unionArray.length - 1; i++) {
                    if (unionResults[i] != null
                            && unionArray[i].unionDepth >= depth) {
                        if (pass == 0
                                && unionArray[i].unionType
                                   != Select.INTERSECT) {
                            continue;
                        }

                        if (pass == 1
                                && unionArray[i].unionType
                                   == Select.INTERSECT) {
                            continue;
                        }

                        int nextIndex = i + 1;

                        for (; nextIndex < unionArray.length; nextIndex++) {
                            if (unionResults[nextIndex] != null) {
                                break;
                            }
                        }

                        if (nextIndex == unionArray.length) {
                            break;
                        }

                        unionArray[i].mergeResults(session, unionResults[i],
                                                   unionResults[nextIndex]);

                        unionResults[nextIndex] = unionResults[i];
                        unionResults[i]         = null;
                    }
                }
            }
        }

        Result result = unionResults[unionResults.length - 1];

        if (sortUnion) {
            DataRowSetNavigator nav =
                (DataRowSetNavigator) result.getNavigator();

            nav.sortOrder();
            nav.trim(getLimitStart(session), getLimitCount(session, rowCount));
        }

        return result;
    }

    /**
     * Merges the second result into the first using the unionMode
     * set operation.
     */
    private void mergeResults(Session session, Result first,
                              Result second) throws HsqlException {

        DataRowSetNavigator navigator =
            (DataRowSetNavigator) first.initialiseNavigator();

        switch (unionType) {

            case UNION :
                navigator.union((DataRowSetNavigator) second.getNavigator());
                break;

            case UNIONALL :
                navigator.unionAll(
                    (DataRowSetNavigator) second.getNavigator());
                break;

            case INTERSECT :
                navigator.intersect(
                    (DataRowSetNavigator) second.getNavigator());
                break;

            case EXCEPT :
                navigator.except((DataRowSetNavigator) second.getNavigator());
                break;
        }
    }

    private Result getSingleResult(Session session,
                                   int rowCount) throws HsqlException {

        Result r = buildResult(session, getMaxRowCount(session, rowCount));
        DataRowSetNavigator nav = (DataRowSetNavigator) r.getNavigator();

        if (isDistinctSelect) {
            nav.removeDuplicates();
        }

        if (!sortUnion) {
            nav.sortOrder();
            nav.trim(getLimitStart(session), getLimitCount(session, rowCount));
        }

        return r;
    }

    private Result buildResult(Session session,
                               int limitcount) throws HsqlException {

        int           fullJoinIndex = 0;
        GroupedResult gResult       = new GroupedResult(session, this);
        RangeIterator[] rangeIterators =
            new RangeIterator[rangeVariables.length];

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i] = rangeVariables[i].getIterator(session);
        }

        for (int currentIndex = 0; ; ) {
            if (currentIndex < fullJoinIndex) {
                boolean end = true;

                for (int i = fullJoinIndex + 1; i < rangeVariables.length;
                        i++) {
                    if (rangeVariables[i].isRightJoin) {
                        rangeIterators[i] = rangeVariables[i].getFullIterator(
                            session, rangeIterators[i]);
                        fullJoinIndex = i;
                        currentIndex  = i;
                        end           = false;

                        break;
                    }
                }

                if (end) {
                    break;
                }
            }

            RangeIterator it = rangeIterators[currentIndex];

            if (it.next()) {
                if (currentIndex < rangeVariables.length - 1) {
                    currentIndex++;

                    continue;
                }
            } else {
                it.reset();

                currentIndex--;

                continue;
            }

            Object[] row = new Object[exprColumns.length];

            // gets the group by column values first.
            for (int i = gResult.groupBegin; i < gResult.groupEnd; i++) {
                row[i] = exprColumns[i].getValue(session);
            }

            row = gResult.getRow(row);

            // Get all other values
            for (int i = 0; i < gResult.groupBegin; i++) {
                row[i] = isAggregated && exprColumns[i].isAggregate()
                         ? exprColumns[i].updateAggregatingValue(session,
                             row[i])
                         : exprColumns[i].getValue(session);
            }

            for (int i = gResult.groupEnd; i < orderByLimitIndex; i++) {
                row[i] = isAggregated && exprColumns[i].isAggregate()
                         ? exprColumns[i].updateAggregatingValue(session,
                             row[i])
                         : exprColumns[i].getValue(session);
            }

            /*

             // new aggregates
             Object[] row = new Object[len + aggregates.length];

             // gets the group by column values first.
             for (int i = gResult.groupBegin; i < gResult.groupEnd; i++) {
                 row[i] = exprColumns[i].getValue(session);
             }

             row = gResult.getRow(row);

             // Get all other values
             for (int i = 0; i < gResult.groupBegin; i++) {
                 row[i] = isAggregated && exprColumns[i].isAggregate()
                          ? row[i]
                          : exprColumns[i].getValue(session);
             }

             for (int i = gResult.groupEnd; i < len; i++) {
                 row[i] = isAggregated && exprColumns[i].isAggregate()
                          ? row[i]
                          : exprColumns[i].getValue(session);
             }

             for (int i = 0; i < aggregates.length; i++) {
                 if (isAggregated && aggregates[i].isAggregate()) {
                     row[iPostOrderIndex + i] = aggregates[i].updateAggregatingValue(session,
                             row[iPostOrderIndex + i]);
                 } else {
                     throw Trace.runtimeError(
                         Trace.UNSUPPORTED_INTERNAL_OPERATION, "Select");
                 }
             }

             // end new aggregates
             */
            gResult.addRow(row);

            if (gResult.size() >= limitcount) {
                break;
            }
        }

        if (isAggregated && !isGrouped && gResult.size() == 0) {
            Object[] row = new Object[exprColumns.length];

            for (int i = 0; i < exprColumns.length; i++) {
                row[i] = exprColumns[i].isAggregate() ? null
                                                      : exprColumns[i]
                                                      .getValue(session);
            }

            gResult.addRow(row);
        }

        Result          result = gResult.getResult();
        RowSetNavigator nav    = result.getNavigator();

        nav.reset();

        if (havingColumnCount > 0) {
            while (nav.hasNext()) {
                Object[] row = (Object[]) nav.getNext();

                if (!Boolean.TRUE.equals(
                        row[visibleColumnCount + groupByColumnCount])) {
                    nav.remove();
                }
            }
        }

        nav.reset();

        return result;
    }

    /**
     * Skeleton under development. Needs a lot of work.
     */
    public StringBuffer getDDL() throws HsqlException {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_SELECT).append(' ');

        //limitStart;
        //limitCount;
        for (int i = 0; i < visibleColumnCount; i++) {
            sb.append(exprColumns[i].getDDL());

            if (i < visibleColumnCount - 1) {
                sb.append(',');
            }
        }

        sb.append(Token.T_FROM);

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable rangeVar = rangeVariables[i];

            if (i != 0) {
                if (rangeVar.isLeftJoin || rangeVariables[i].isRightJoin) {
                    sb.append(Token.T_OUTER).append(' ');
                    sb.append(Token.T_JOIN).append(' ');
                }

                // todo rangeVar conditions
            }

            // otherwise use a comma delimited table list
            sb.append(',');
        }

        // if has GROUP BY
        sb.append(' ').append(Token.T_GROUP).append(' ');

        for (int i = visibleColumnCount;
                i < visibleColumnCount + groupByColumnCount; i++) {
            sb.append(exprColumns[i].getDDL());

            if (i < visibleColumnCount + groupByColumnCount - 1) {
                sb.append(',');
            }
        }

        // if has HAVING
        sb.append(' ').append(Token.T_HAVING).append(' ');

        for (int i = visibleColumnCount + groupByColumnCount; i < orderByStart;
                i++) {
            sb.append(exprColumns[i].getDDL());

            if (i < orderByStart - 1) {
                sb.append(',');
            }
        }

        if (unionSelect != null) {
            switch (unionType) {

                case EXCEPT :
                    sb.append(' ').append(Token.T_EXCEPT).append(' ');
                    break;

                case INTERSECT :
                    sb.append(' ').append(Token.T_INTERSECT).append(' ');
                    break;

                case UNION :
                    sb.append(' ').append(Token.T_UNION).append(' ');
                    break;

                case UNIONALL :
                    sb.append(' ').append(Token.T_UNION).append(' ').append(
                        Token.T_ALL).append(' ');
                    break;
            }
        }

        // if has ORDER BY
        int groupByEnd   = visibleColumnCount + groupByColumnCount;
        int orderByStart = groupByEnd + havingColumnCount;
        int orderByEnd   = orderByStart + orderByColumnCount;

        sb.append(' ').append(Token.T_ORDER).append(Token.T_BY).append(' ');

        for (int i = orderByStart; i < orderByEnd; i++) {
            sb.append(exprColumns[i].getDDL());

            if (i < visibleColumnCount + groupByColumnCount - 1) {
                sb.append(',');
            }
        }

        return sb;
    }

    public ResultMetaData getMetaData() {
        return resultMetaData;
    }

    public String describe(Session session) {

        StringBuffer sb;
        String       temp;

/*
        // temporary :  it is currently unclear whether this may affect
        // later attempts to retrieve an actual result (calls getResult(1)
        // in preProcess mode).  Thus, toString() probably should not be called
        // on Select objects that will actually be used to retrieve results,
        // only on Select objects used by EXPLAIN PLAN FOR
        try {
            getResult(session, 1);
        } catch (HsqlException e) {}
*/
        sb = new StringBuffer();

        sb.append(super.toString()).append("[\n");

        if (intoTableName != null) {
            sb.append("into table=[").append(intoTableName.name).append("]\n");
        }

        if (limitCondition != null) {
            sb.append("offset=[").append(
                limitCondition.getArg().describe(session)).append("]\n");
            sb.append("limit=[").append(
                limitCondition.getArg2().describe(session)).append("]\n");
        }

        sb.append("isDistinctSelect=[").append(isDistinctSelect).append("]\n");
        sb.append("isGrouped=[").append(isGrouped).append("]\n");
        sb.append("isAggregated=[").append(isAggregated).append("]\n");
        sb.append("columns=[");

        int columns = visibleColumnCount + groupByColumnCount
                      + havingColumnCount;

        for (int i = 0; i < columns; i++) {
            sb.append(exprColumns[i].describe(session));
        }

        sb.append("\n]\n");
        sb.append("range variables=[\n");

        for (int i = 0; i < rangeVariables.length; i++) {
            sb.append("[\n");
            sb.append(rangeVariables[i].describe(session));
            sb.append("\n]");
        }

        sb.append("]\n");

        temp = queryCondition == null ? "null"
                                      : queryCondition.describe(session);

        sb.append("queryCondition=[").append(temp).append("]\n");

        temp = havingCondition == null ? "null"
                                       : havingCondition.describe(session);

        sb.append("havingCondition=[").append(temp).append("]\n");
        sb.append("groupColumns=[").append(groupColumnNames).append("]\n");

        if (unionSelect != null) {
            switch (unionType) {

                case EXCEPT :
                    sb.append(" EXCEPT ");
                    break;

                case INTERSECT :
                    sb.append(" INTERSECT ");
                    break;

                case UNION :
                    sb.append(" UNION ");
                    break;

                case UNIONALL :
                    sb.append(" UNION ALL ");
                    break;

                default :
                    sb.append(" UNKNOWN SET OPERATION ");
            }

            sb.append("[\n").append(unionSelect.describe(session)).append(
                "]\n");
        }

        return sb.toString();
    }
}
