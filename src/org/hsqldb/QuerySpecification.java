/* Copyright (c) 2001-2024, The HSQL Development Group
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

import java.util.Locale;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.RangeVariable.RangeIteratorRight;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.map.ValuePool;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.navigator.RowSetNavigatorDataTable;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Implementation of an SQL query specification, including SELECT.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.7.4
 * @since 1.9.0
 */
public class QuerySpecification extends QueryExpression {

    //
    public int                           resultRangePosition;
    public boolean                       isDistinctSelect;
    public boolean                       isAggregated;
    public boolean                       isGrouped;
    public boolean                       isGroupingSets;
    boolean                              isDistinctGroups;
    public boolean                       isOrderSensitive;
    public boolean                       isSimpleDistinct;
    RangeVariable[]                      rangeVariables;
    private HsqlArrayList<RangeVariable> rangeVariableList;
    int                                  startInnerRange = -1;
    int                                  endInnerRange   = -1;
    Expression                           queryCondition;
    Expression                           checkQueryCondition;
    Expression[]                         exprColumns;
    HsqlArrayList<Expression>            exprColumnList;
    GroupSet                             groupSet;
    private int                          groupByColumnCount;    // (0 or more)
    private int                          havingColumnCount;     // (0 or 1)
    public int                           indexLimitVisible;
    private int                          indexLimitRowId;
    private int                          indexStartHaving;
    public int                           indexStartOrderBy;
    public int                           indexStartAggregates;
    private int                          indexLimitExpressions;
    public int                           indexLimitData;
    private boolean                      isSimpleCount;
    private boolean                      isSingleMemoryTable;

    //
    public boolean isUniqueResultRows;

    //
    Type[] resultColumnTypes;

    //
    private ArrayListIdentity<Expression> aggregateList;

    //
    private ArrayListIdentity<Expression> resolvedSubqueryExpressions = null;

    //
    private boolean[] aggregateCheck;

    //
    private OrderedHashSet<Expression> tempSet = new OrderedHashSet<>();

    //
    int[]         columnMap;
    private Table baseTable;

    //
    public Index groupIndex;

    //
    private RangeGroup[] outerRanges;

    //
    QuerySpecification(
            Session session,
            Table table,
            CompileContext compileContext,
            boolean isValueList) {

        this(compileContext);

        this.isValueList = isValueList;

        RangeVariable range = new RangeVariable(
            table,
            null,
            null,
            null,
            compileContext);

        range.addTableColumns(exprColumnList, 0, null);

        indexLimitVisible = exprColumnList.size();

        addRangeVariable(session, range);

        sortAndSlice    = SortAndSlice.noSort;
        isBaseMergeable = true;
        isMergeable     = true;
        isTable         = true;
    }

    QuerySpecification(CompileContext compileContext) {

        super(compileContext);

        resultRangePosition = compileContext.getNextResultRangeVarIndex();
        rangeVariableList   = new HsqlArrayList<>();
        exprColumnList      = new HsqlArrayList<>();
        sortAndSlice        = SortAndSlice.noSort;
        isBaseMergeable     = true;
        isMergeable         = true;
    }

    void addRangeVariable(Session session, RangeVariable rangeVar) {
        rangeVariableList.add(rangeVar);
    }

    public TableDerived getValueListTable() {

        if (isValueList) {
            RangeVariable range = null;

            if (rangeVariables == null) {
                if (rangeVariableList.size() == 1) {
                    range = rangeVariableList.get(0);
                }
            } else if (rangeVariables.length == 1) {
                range = rangeVariables[0];
            }

            if (range != null) {
                return (TableDerived) range.getTable();
            }
        }

        return null;
    }

    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
    }

    public int getCurrentRangeVariableCount() {
        return rangeVariableList.size();
    }

    // range variable sub queries are resolved fully
    private void resolveRangeVariables(
            Session session,
            RangeGroup[] rangeGroups) {

        if (rangeVariables == null
                || rangeVariables.length < rangeVariableList.size()) {
            rangeVariables = new RangeVariable[rangeVariableList.size()];

            rangeVariableList.toArray(rangeVariables);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeGroup rangeGroup;

            if (rangeVariables[i].isLateral) {
                RangeVariable[] rangeVars =
                    (RangeVariable[]) ArrayUtil.resizeArray(
                        rangeVariables,
                        i);

                rangeGroup = new RangeGroupSimple(rangeVars, this);
            } else if (rangeGroups == RangeGroup.emptyArray) {
                rangeGroup = RangeGroup.emptyGroup;
            } else {
                rangeGroup = new RangeGroupSimple(
                    RangeVariable.emptyArray,
                    this);
            }

            rangeVariables[i].resolveRangeTable(
                session,
                rangeGroup,
                rangeGroups);
        }
    }

    void addSelectColumnExpression(Expression e) {

        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (indexLimitVisible > 0) {
            if (e.opType == OpTypes.MULTICOLUMN) {
                if (((ExpressionColumn) e).getTableName() == null) {
                    throw Error.error(ErrorCode.X_42578);
                }
            }

            Expression first = exprColumnList.get(0);

            if (first.opType == OpTypes.MULTICOLUMN
                    && ((ExpressionColumn) first).getTableName() == null) {
                throw Error.error(ErrorCode.X_42578);
            }
        }

        exprColumnList.add(e);

        indexLimitVisible++;
    }

    void addQueryCondition(Expression e) {
        queryCondition = e;
    }

    void setDistinctSelect() {
        isDistinctSelect = true;
    }

    void setDistinctGroups() {
        isDistinctGroups = true;
    }

    void addGroupingSets(Expression[] groupingExpressions) {
        groupSet = new GroupSet(groupingExpressions, isDistinctGroups);
    }

    /**
     * fully resolve all group by columns
     * add each group by expression to exprColumnList
     */
    void resolveColumnReferencesInGroupingSets(
            Session session,
            RangeGroup[] rangeGroups) {

        if (groupSet == null) {
            return;
        }

        Expression[] groupExpressions = groupSet.groupExpressions;

        for (int i = 0; i < groupExpressions.length; i++) {
            Expression e = groupExpressions[i];

            tempSet.clear();
            e.resolveColumnReferences(
                session,
                this,
                rangeVariables.length,
                rangeGroups,
                tempSet,
                false);

            if (!tempSet.isEmpty()) {
                if (session.database.sqlEnforceRefs) {
                    ExpressionColumn.checkColumnsResolved(tempSet);
                    continue;
                }

                // when a column reference does not resolve to range variables,
                // resolve to aliases in select list
                // this is non-standard and probably should be allowed only
                // for basic group by lists
                Expression resolved = e.replaceAliasInOrderBy(
                    session,
                    exprColumnList,
                    indexLimitVisible);

                if (resolved != e) {
                    groupExpressions[i] = resolved;
                }

                tempSet.clear();
                resolved.resolveColumnReferences(
                    session,
                    this,
                    rangeVariables.length,
                    RangeGroup.emptyArray,
                    tempSet,
                    false);
                ExpressionColumn.checkColumnsResolved(tempSet);
            }
        }

        tempSet.clear();
        addGroupingExpressions(groupExpressions, tempSet);

        Iterator<Expression> it = tempSet.iterator();

        while (it.hasNext()) {
            Expression e = it.next();

            if (e.getType() == OpTypes.ROW) {
                throw Error.error(ErrorCode.X_42564);
            }

            e.resultTableColumnIndex = indexLimitVisible + groupByColumnCount;    //

            exprColumnList.add(e.resultTableColumnIndex, e);

            groupByColumnCount++;
        }

        groupSet.process();
    }

    /**
     * collect all expressions in an extended group by expression
     * set the flags for grouping set and group by
     */
    void addGroupingExpressions(
            Expression[] nodes,
            OrderedHashSet<Expression> set) {

        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];

            if (e.groupingType != OpTypes.NONE) {
                isGroupingSets = true;
            }

            if (e.opType == OpTypes.ROW || e.opType == OpTypes.VALUELIST) {
                for (int j = 0; j < e.nodes.length; j++) {
                    addGroupingExpressions(e.nodes, set);
                }
            } else {
                if (e.opType == OpTypes.NONE) {
                    continue;
                }

                nodes[i]  = set.getOrAdd(e);
                isGrouped = true;
            }
        }
    }

    void addHavingExpression(Expression e) {
        exprColumnList.add(e);

        havingColumnCount = 1;
    }

    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice = sortAndSlice;
    }

    public void resolveReferences(Session session, RangeGroup[] rangeGroups) {

        if (isReferencesResolved) {
            return;
        }

        outerRanges = rangeGroups;

        resolveRangeVariables(session, rangeGroups);
        resolveColumnReferencesForAsterisk();

        // must be after asterisk expansion
        resolveColumnReferencesInGroupingSets(session, rangeGroups);
        setColumnIndexes();
        resolveColumnReferences(session, rangeGroups);
        finaliseColumns();
        setReferenceableColumns();

        unresolvedExpressions = Expression.resolveColumnSet(
            session,
            RangeVariable.emptyArray,
            rangeGroups,
            unresolvedExpressions);
        unionColumnTypes     = new Type[indexLimitVisible];
        isReferencesResolved = true;
    }

    public boolean hasReference(RangeVariable range) {

        if (unresolvedExpressions == null) {
            return false;
        }

        for (int i = 0; i < unresolvedExpressions.size(); i++) {
            if (unresolvedExpressions.get(i).hasReference(range)) {
                return true;
            }
        }

        return false;
    }

    public boolean areColumnsResolved() {
        return super.areColumnsResolved();
    }

    public void resolveTypes(Session session) {

        if (isResolved) {
            return;
        }

        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);
        resolveTypesPartThree(session);
        ArrayUtil.copyArray(
            resultTable.colTypes,
            unionColumnTypes,
            unionColumnTypes.length);
    }

    void resolveTypesPartOne(Session session) {

        if (isPartOneResolved) {
            return;
        }

        resolveExpressionTypes(session);
        resolveAggregates();

        for (int i = 0; i < unionColumnTypes.length; i++) {
            unionColumnTypes[i] = Type.getAggregateType(
                unionColumnTypes[i],
                exprColumns[i].getDataType());
        }

        isPartOneResolved = true;
    }

    /**
     * additional resolution for union
     */
    void resolveTypesPartTwoRecursive(Session session) {

        for (int i = 0; i < unionColumnTypes.length; i++) {
            Type type = unionColumnTypes[i];

            exprColumns[i].setDataType(session, type);
        }

        setResultColumnTypes();
        createResultMetaData(session);
        createTable(session);
    }

    void resolveTypesPartTwo(Session session) {

        if (isPartTwoResolved) {
            return;
        }

        resolveGroups();
        resolveGroupingSets();

        for (int i = 0; i < unionColumnTypes.length; i++) {
            Type type = unionColumnTypes[i];

            if (type == null) {
                if (session.database.sqlEnforceTypes) {
                    throw Error.error(ErrorCode.X_42567);
                }

                type                = Type.SQL_VARCHAR_DEFAULT;
                unionColumnTypes[i] = type;
            }

            exprColumns[i].setDataType(session, type);

            if (exprColumns[i].dataType.isArrayType()
                    && exprColumns[i].dataType.collectionBaseType() == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            if (exprColumns[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        checkLobUsage();
        setMergeability();
        setUpdatability(session);
        setResultColumnTypes();
        createResultMetaData(session);
        createTable(session);
        mergeQuery(session);

        isPartTwoResolved = true;
    }

    void resolveTypesPartThree(Session session) {

        if (isResolved) {
            return;
        }

        sortAndSlice.setSortIndex(this);
        setRangeVariableConditions(session);
        setDistinctConditions(session);
        setAggregateConditions(session);
        sortAndSlice.setSortRange(this);

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(session, rangeVariables);
        }

        setResultNullability();

        rangeVariableList = null;
        tempSet           = null;
        compileContext    = null;
        outerRanges       = null;
        isResolved        = true;
    }

    public void addExtraConditions(Expression e) {

        if (isAggregated || isGrouped) {
            return;
        }

        queryCondition = ExpressionLogical.andExpressions(queryCondition, e);
    }

    /**
     * Resolves all column expressions in the GROUP BY clause and beyond.
     * Replaces any alias column expression in the ORDER BY clause
     * with the actual select column expression.
     */
    private void resolveColumnReferences(
            Session session,
            RangeGroup[] rangeGroups) {

        if (isDistinctSelect || isGrouped) {
            acceptsSequences = false;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            Expression e = rangeVariables[i].getJoinCondition();

            if (e == null) {
                continue;
            }

            resolveColumnReferencesAndAllocate(
                session,
                e,
                i + 1,
                rangeGroups,
                false);
        }

        resolveColumnReferencesAndAllocate(
            session,
            queryCondition,
            rangeVariables.length,
            rangeGroups,
            false);

        if (resolvedSubqueryExpressions != null) {

            // subqueries in conditions not to be converted to SIMPLE_COLUMN
            resolvedSubqueryExpressions.setSize(0);
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e = exprColumnList.get(i);

            resolveColumnReferencesAndAllocate(
                session,
                e,
                rangeVariables.length,
                rangeGroups,
                acceptsSequences);

            if (!isGrouped && !isDistinctSelect) {
                List<TableDerived> list = e.collectAllSubqueries(null);

                if (list != null) {
                    isMergeable = false;
                }

                List<Expression> set = e.collectAllExpressions(
                    null,
                    OpTypes.sequenceExpressionSet,
                    OpTypes.subqueryAggregateExpressionSet);

                if (set != null) {
                    isOrderSensitive = true;
                    isMergeable      = false;
                    isBaseMergeable  = false;
                }
            }
        }

        for (int i = indexStartHaving; i < indexStartOrderBy; i++) {
            Expression e = exprColumnList.get(i);

            resolveColumnReferencesAndAllocate(
                session,
                e,
                rangeVariables.length,
                rangeGroups,
                false);
        }

        resolveColumnReferencesInOrderBy(session, rangeGroups, sortAndSlice);
    }

    void resolveColumnReferencesInOrderBy(
            Session session,
            RangeGroup[] rangeGroups,
            SortAndSlice sortAndSlice) {

        // replace the aliases with expressions
        // replace column names with expressions and resolve the table columns
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            ExpressionOrderBy e = (ExpressionOrderBy) sortAndSlice.exprList.get(
                i);

            replaceColumnIndexInOrderBy(e);

            if (e.getLeftNode().resultTableColumnIndex != -1) {
                continue;
            }

            if (sortAndSlice.sortUnion) {
                if (e.getLeftNode().getType() != OpTypes.COLUMN) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }

            e.replaceAliasInOrderBy(session, exprColumnList, indexLimitVisible);
            resolveColumnReferencesAndAllocate(
                session,
                e,
                rangeVariables.length,
                RangeGroup.emptyArray,
                false);
        }

        if (sortAndSlice.limitCondition != null) {
            unresolvedExpressions =
                sortAndSlice.limitCondition.resolveColumnReferences(
                    session,
                    this,
                    rangeGroups,
                    unresolvedExpressions);
        }

        sortAndSlice.prepare(indexStartOrderBy);
    }

    private boolean resolveColumnReferences(
            Session session,
            Expression e,
            int rangeCount,
            boolean withSequences) {

        if (e == null) {
            return true;
        }

        int oldSize = unresolvedExpressions == null
                      ? 0
                      : unresolvedExpressions.size();

        unresolvedExpressions = e.resolveColumnReferences(
            session,
            this,
            rangeCount,
            RangeGroup.emptyArray,
            unresolvedExpressions,
            withSequences);

        int newSize = unresolvedExpressions == null
                      ? 0
                      : unresolvedExpressions.size();

        return oldSize == newSize;
    }

    private void resolveColumnReferencesForAsterisk() {

        for (int pos = 0; pos < indexLimitVisible; ) {
            Expression e = exprColumnList.get(pos);

            if (e.getType() == OpTypes.MULTICOLUMN) {
                exprColumnList.remove(pos);

                String tablename = ((ExpressionColumn) e).getTableName();

                if (tablename == null) {
                    addAllJoinedColumns(e);
                } else {
                    boolean resolved = false;

                    for (int i = 0; i < rangeVariables.length; i++) {
                        RangeVariable range =
                            rangeVariables[i].getRangeForTableName(
                                tablename);

                        if (range != null) {
                            HashSet<String> exclude = getAllNamedJoinColumns();

                            rangeVariables[i].addTableColumns(
                                range,
                                e,
                                exclude);

                            resolved = true;
                            break;
                        }
                    }

                    if (!resolved) {
                        throw Error.error(ErrorCode.X_42501, tablename);
                    }
                }

                for (int i = 0; i < e.nodes.length; i++) {
                    exprColumnList.add(pos, e.nodes[i]);

                    pos++;
                }

                indexLimitVisible += e.nodes.length - 1;
            } else {
                pos++;
            }
        }
    }

    private void resolveColumnReferencesAndAllocate(
            Session session,
            Expression expression,
            int rangeCount,
            RangeGroup[] rangeGroups,
            boolean withSequences) {

        if (expression == null) {
            return;
        }

        List<Expression> list = expression.resolveColumnReferences(
            session,
            this,
            rangeCount,
            rangeGroups,
            null,
            withSequences);

        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Expression e        = list.get(i);
                boolean    resolved = true;

                if (e.isSelfAggregate()) {
                    for (int j = 0; j < e.nodes.length; j++) {
                        List<Expression> colList =
                            e.nodes[j].resolveColumnReferences(
                                session,
                                this,
                                rangeCount,
                                RangeGroup.emptyArray,
                                null,
                                false);

                        for (int k = 0; k < rangeGroups.length; k++) {
                            if (rangeGroups[k].isVariable()) {
                                colList = Expression.resolveColumnSet(
                                    session,
                                    rangeGroups[k].getRangeVariables(),
                                    RangeGroup.emptyArray,
                                    colList);
                            }
                        }

                        resolved &= colList == null;
                    }
                } else {
                    resolved = resolveColumnReferences(
                        session,
                        e,
                        rangeCount,
                        withSequences);
                }

                if (resolved) {
                    if (e.isSelfAggregate()) {
                        addAggregateToList(expression, e);
                    }

                    if (resolvedSubqueryExpressions == null) {
                        resolvedSubqueryExpressions =
                            new ArrayListIdentity<Expression>();
                    }

                    resolvedSubqueryExpressions.add(e);
                } else {
                    if (unresolvedExpressions == null) {
                        unresolvedExpressions = new ArrayListIdentity<>();
                    }

                    unresolvedExpressions.add(e);
                }
            }
        }
    }

    private void addAggregateToList(Expression parent, Expression e) {

        if (aggregateList == null) {
            aggregateList = new ArrayListIdentity<>();
        }

        aggregateList.add(e);

        isAggregated = true;

        parent.setAggregate();
        e.setCorrelatedReferences(this);
    }

    private HashSet<String> getAllNamedJoinColumns() {

        HashSet<String> set = null;

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable range = rangeVariableList.get(i);

            if (range.namedJoinColumns != null) {
                if (set == null) {
                    set = new HashSet<>();
                }

                set.addAll(range.namedJoinColumns);
            }
        }

        return set;
    }

    public Expression getEquiJoinExpressions(
            OrderedHashSet<String> nameSet,
            RangeVariable rightRange,
            boolean fullList) {

        HashSet<String>        set             = new HashSet<>();
        Expression             result          = null;
        OrderedHashSet<String> joinColumnNames = new OrderedHashSet<>();

        for (int i = rangeVariableList.size() - 1; i >= 0; i--) {
            RangeVariable range = rangeVariableList.get(i);
            OrderedHashMap<String, ColumnSchema> columnList =
                range.rangeTable.columnList;

            for (int j = 0; j < columnList.size(); j++) {
                ColumnSchema column       = columnList.get(j);
                String       name         = range.getColumnAlias(j).name;
                boolean      columnInList = nameSet.contains(name);
                boolean namedJoin = range.namedJoinColumns != null
                                    && range.namedJoinColumns.contains(name);
                boolean      repeated     = !namedJoin && !set.add(name);

                if (repeated && (!fullList || columnInList)) {
                    throw Error.error(ErrorCode.X_42578, name);
                }

                if (!columnInList) {
                    continue;
                }

                joinColumnNames.add(name);

                int leftPosition = range.rangeTable.getColumnIndex(
                    column.getNameString());
                int rightPosition = rightRange.rangeTable.getColumnIndex(name);
                Expression e = new ExpressionLogical(
                    range,
                    leftPosition,
                    rightRange,
                    rightPosition);
                ExpressionColumn col = range.getColumnExpression(name);

                if (col == null) {
                    col = new ExpressionColumn(
                        new Expression[]{ e.getLeftNode(), e.getRightNode() },
                        name);

                    range.addNamedJoinColumnExpression(name, col, leftPosition);

                    result = ExpressionLogical.andExpressions(result, e);

                    rightRange.addNamedJoinColumnExpression(
                        name,
                        col,
                        rightPosition);
                } else if (rightRange.getColumnExpression(name) == null
                           && (!range.isLeftJoin || range.isRightJoin)) {
                    if (range.isLeftJoin && range.isRightJoin) {
                        e = new ExpressionLogical(col, e.getRightNode());
                    }

                    col.nodes = (Expression[]) ArrayUtil.resizeArray(
                        col.nodes,
                        col.nodes.length + 1);
                    col.nodes[col.nodes.length - 1] = e.getRightNode();
                    result = ExpressionLogical.andExpressions(result, e);

                    rightRange.addNamedJoinColumnExpression(
                        name,
                        col,
                        rightPosition);
                }
            }

            if (!range.isJoin) {
                break;
            }
        }

        if (fullList && !joinColumnNames.containsAll(nameSet)) {
            throw Error.error(ErrorCode.X_42501);
        }

        rightRange.addNamedJoinColumns(joinColumnNames);

        return result;
    }

    private void addAllJoinedColumns(Expression e) {

        HsqlArrayList<Expression> list = new HsqlArrayList<>();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].addTableColumns(list);
        }

        Expression[] nodes = new Expression[list.size()];

        list.toArray(nodes);

        e.nodes = nodes;
    }

    private void setColumnIndexes() {

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e = exprColumnList.get(i);

            e.resultTableColumnIndex = i;
        }

        indexLimitRowId      = indexLimitVisible;
        indexStartHaving     = indexLimitRowId + groupByColumnCount;
        indexStartOrderBy    = indexStartHaving + havingColumnCount;
        indexStartAggregates = indexStartOrderBy
                               + sortAndSlice.getOrderLength();
        indexLimitData       = indexLimitExpressions = indexStartAggregates;
    }

    private void finaliseColumns() {

        exprColumns = new Expression[indexLimitExpressions];

        exprColumnList.toArray(exprColumns);

        exprColumnList = null;

        if (sortAndSlice.hasOrder()) {
            for (int i = 0; i < sortAndSlice.getOrderLength(); i++) {
                exprColumns[indexStartOrderBy + i] = sortAndSlice.exprList.get(
                    i);
            }
        }
    }

    private int replaceColumnIndexInOrderBy(Expression orderBy) {

        Expression e = orderBy.getLeftNode();

        if (e.getType() != OpTypes.VALUE) {
            return -1;
        }

        Type type = e.getDataType();

        if (type != null && type.typeCode == Types.SQL_INTEGER) {
            int i = ((Integer) e.getValue(null)).intValue();

            if (0 < i && i <= indexLimitVisible) {
                orderBy.setLeftNode(exprColumnList.get(i - 1));

                return i;
            }
        }

        throw Error.error(ErrorCode.X_42576);
    }

    OrderedHashSet<RangeVariable> collectRangeVariables(
            RangeVariable[] rangeVars,
            OrderedHashSet<RangeVariable> set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectRangeVariables(rangeVars, set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectRangeVariables(rangeVars, set);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            set = rangeVariables[i].collectRangeVariables(rangeVars, set);
        }

        return set;
    }

    OrderedHashSet<RangeVariable> collectRangeVariables(
            OrderedHashSet<RangeVariable> set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectRangeVariables(set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectRangeVariables(set);
        }

        return set;
    }

    /**
     * Sets the types of all the expressions used in this SELECT list.
     */
    public void resolveExpressionTypes(Session session) {

        Expression rowExpression = new Expression(OpTypes.ROW, exprColumns);

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            e.resolveTypes(session, rowExpression);

            if (e.getType() == OpTypes.ROW) {
                throw Error.error(ErrorCode.X_42565);
            }

            if (e.getType() == OpTypes.ROW_SUBQUERY && e.getDegree() > 1) {
                throw Error.error(ErrorCode.X_42565);
            }

            if (e.getDataType() != null
                    && e.getDataType().typeCode == Types.SQL_ROW) {
                throw Error.error(ErrorCode.X_42565);
            }
        }

        rowExpression.nodes = new Expression[1];

        for (int i = 0; i < rangeVariables.length; i++) {
            Expression e = rangeVariables[i].getJoinCondition();

            if (e != null) {
                rowExpression.setLeftNode(e);
                e.resolveTypes(session, rowExpression);

                e = rowExpression.getLeftNode();

                rangeVariables[i].setJoinCondition(e);

                if (e.getDataType() != Type.SQL_BOOLEAN) {
                    throw Error.error(ErrorCode.X_42568);
                }
            }
        }

        if (queryCondition != null) {
            rowExpression.setLeftNode(queryCondition);
            queryCondition.resolveTypes(session, rowExpression);

            queryCondition = rowExpression.getLeftNode();

            if (queryCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (havingColumnCount != 0) {
            if (exprColumns[indexStartHaving].getDataType()
                    != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (sortAndSlice.limitCondition != null) {
            sortAndSlice.limitCondition.resolveTypes(session, null);
        }
    }

    private void resolveAggregates() {

        tempSet.clear();

        if (isAggregated) {
            aggregateCheck = new boolean[indexStartAggregates];

            tempSet.addAll(aggregateList);

            indexLimitData = indexLimitExpressions = exprColumns.length
                    + tempSet.size();
            exprColumns = (Expression[]) ArrayUtil.resizeArray(
                exprColumns,
                indexLimitExpressions);

            for (int i = indexStartAggregates, j = 0; i < indexLimitExpressions;
                    i++, j++) {
                Expression e = tempSet.get(j);

                exprColumns[i]          = e.duplicate();
                exprColumns[i].nodes    = e.nodes;    // keep original nodes
                exprColumns[i].dataType = e.dataType;
            }

            tempSet.clear();
        }
    }

    private void setRangeVariableConditions(Session session) {

        RangeVariableResolver rangeResolver = new RangeVariableResolver(
            session,
            this);

        rangeResolver.processConditions();

        rangeVariables = rangeResolver.rangeVariables;

        if (rangeVariables.length > 1) {
            isMergeable = false;
        }
    }

    private void setDistinctConditions(Session session) {

        if (!isDistinctSelect && !isGrouped) {
            return;
        }

        if (isAggregated) {
            return;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isRightJoin) {
                return;
            }
        }

        RangeVariable range = null;
        int[]         colMap;

        if (isGrouped) {
            colMap = new int[groupByColumnCount];

            for (int i = 0; i < groupByColumnCount; i++) {
                if (exprColumns[indexLimitRowId + i].getType()
                        != OpTypes.COLUMN) {
                    return;
                }

                if (range == null) {
                    range = exprColumns[indexLimitRowId + i].getRangeVariable();
                } else {
                    if (range != exprColumns[indexLimitRowId + i].getRangeVariable()) {
                        return;
                    }
                }

                colMap[i] = exprColumns[i].columnIndex;
            }
        } else {
            colMap = new int[indexLimitVisible];
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            if (exprColumns[i].getType() != OpTypes.COLUMN) {
                return;
            }

            if (range == null) {
                range = exprColumns[i].getRangeVariable();
            } else {
                if (range != exprColumns[i].getRangeVariable()) {
                    return;
                }
            }

            if (!isGrouped) {
                colMap[i] = exprColumns[i].columnIndex;
            }
        }

        if (range != rangeVariables[0]) {
            return;
        }

        boolean check = ArrayUtil.areAllIntIndexesAsBooleanArray(
            colMap,
            range.usedColumns);

        if (!check) {
            return;
        }

        if (!range.hasAnyIndexCondition()) {
            Index index = range.rangeTable.getIndexForAllColumns(colMap);

            if (index != null) {
                range.setSortIndex(index, false);
            }
        }

        isSimpleDistinct = range.setDistinctColumnsOnIndex(colMap);
    }

    private void setAggregateConditions(Session session) {

        if (!isAggregated) {
            return;
        }

        if (isGrouped) {
            setGroupedAggregateConditions(session);
        } else if (!sortAndSlice.hasOrder()
                   && !sortAndSlice.hasLimit()
                   && aggregateList.size() == 1
                   && indexLimitVisible == 1) {
            Expression e      = exprColumns[indexStartAggregates];
            int        opType = e.getType();
            Expression expr   = e.getLeftNode();

            switch (opType) {

                case OpTypes.MAX :
                case OpTypes.MIN : {
                    if (e.hasCondition()) {
                        break;
                    }

                    SortAndSlice slice = new SortAndSlice();

                    slice.isGenerated = true;

                    slice.addLimitCondition(ExpressionOp.limitOneExpression);

                    if (slice.prepareSpecial(session, this)) {
                        this.sortAndSlice = slice;
                    }

                    break;
                }

                case OpTypes.COUNT : {
                    if (!e.hasCondition()
                            && rangeVariables.length == 1
                            && queryCondition == null) {
                        if (expr.getType() == OpTypes.ASTERISK) {
                            isSimpleCount = true;
                            break;
                        } else if (expr.getNullability()
                                   == SchemaObject.Nullability.NO_NULLS) {
                            if (e.isDistinctAggregate) {
                                if (expr.opType == OpTypes.COLUMN) {
                                    Table t = expr.getRangeVariable()
                                                  .getTable();

                                    if (t.getPrimaryKey().length == 1) {
                                        if (t.getColumn(t.getPrimaryKey()[0])
                                                == expr.getColumn()) {
                                            isSimpleCount = true;
                                            break;
                                        }
                                    }
                                }
                            } else {
                                isSimpleCount = true;
                                break;
                            }
                        }
                    }

                    break;
                }

                default :
            }
        }
    }

    private void setGroupedAggregateConditions(Session session) {

        //
    }

    void checkLobUsage() {}

    private void resolveGroups() {

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
        //   select list expressions plus group by expressions
        // - having cannot introduce additional, derived columns
        // - having cannot reference columns not in the select or group by list
        // - if there is any aggregate in select list but no group by, no
        //   non-aggregates is allowed
        // - check order by columns
        // - if distinct select, order by must be composed of the select list columns
        // - if grouped by, then order by should be decomposed into the
        //   select list plus group by list
        // - references to column aliases are allowed only in order by (Standard
        //   compliance) and take precedence over references to non-alias
        //   column names.
        // - references to table / correlation and column list in correlation
        //   names are handled according to the Standard
        //  fredt@users
        OrderedHashSet<Expression> extraSet = null;

        if (isAggregated || isGrouped) {
            int orderColumnCount = sortAndSlice.getOrderLength();

            for (int i = indexStartOrderBy;
                    i < indexStartOrderBy + orderColumnCount; i++) {
                Expression e = exprColumns[i];
                boolean check = e.getLeftNode()
                                 .isComposedOf(
                                     exprColumns,
                                     0,
                                     indexLimitVisible + groupByColumnCount,
                                     OpTypes.aggregateFunctionSet);

                if (!check) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        tempSet.clear();

        if (isGrouped) {
            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet,
                    OpTypes.aggregateFunctionSet,
                    OpTypes.subqueryExpressionSet);

                if (!tempSet.isEmpty()) {
                    throw Error.error(
                        ErrorCode.X_42572,
                        tempSet.get(0).getSQL());
                }
            }

            for (int i = 0; i < indexLimitVisible; i++) {
                if (!exprColumns[i].isComposedOf(exprColumns,
                                                 indexLimitVisible,
                                                 indexLimitVisible
                                                 + groupByColumnCount,
                                                 OpTypes.aggregateFunctionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }

            if (!tempSet.isEmpty()) {
                if (!resolveForGroupBy(tempSet)) {
                    throw Error.error(
                        ErrorCode.X_42574,
                        tempSet.get(0).getSQL());
                }

                extraSet = new OrderedHashSet<>();

                extraSet.addAll(tempSet);
            }
        } else if (isAggregated) {
            for (int i = 0; i < indexLimitVisible; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet,
                    OpTypes.columnExpressionSet,
                    OpTypes.aggregateFunctionSet);

                for (int j = 0; j < tempSet.size(); j++) {
                    Expression e = tempSet.get(j);

                    for (int k = 0; k < rangeVariables.length; k++) {
                        if (rangeVariables[k] == e.getRangeVariable()) {
                            throw Error.error(ErrorCode.X_42574, e.getSQL());
                        }
                    }
                }

                tempSet.clear();
            }
        }

        tempSet.clear();

        if (havingColumnCount != 0) {
            Expression condition = exprColumns[indexStartHaving];

            if (unresolvedExpressions != null) {
                tempSet.addAll(unresolvedExpressions);
            }

            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                tempSet.add(exprColumns[i]);
            }

            if (extraSet != null) {
                tempSet.addAll(extraSet);
            }

            if (!condition.isComposedOf(tempSet,
                                        outerRanges,
                                        OpTypes.subqueryAggregateExpressionSet)) {
                throw Error.error(ErrorCode.X_42573);
            }

            tempSet.clear();
        }

        if (isDistinctSelect) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = sortAndSlice.exprList.get(i);

                if (e.resultTableColumnIndex != -1) {
                    continue;
                }

                if (!e.isComposedOf(exprColumns,
                                    0,
                                    indexLimitVisible,
                                    OpTypes.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        if (isGrouped) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = sortAndSlice.exprList.get(i);

                if (e.resultTableColumnIndex != -1) {
                    continue;
                }

                if (!e.hasAggregate()
                        && !e.isComposedOf(exprColumns,
                                           0,
                                           indexLimitVisible
                                           + groupByColumnCount,
                                           OpTypes.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        OrderedHashSet<Expression> expressions = new OrderedHashSet<>();

        for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
            Expression e = exprColumns[i];

            e.resultTableColumnIndex = i;

            expressions.add(e);
        }

        for (int i = 0; i < indexStartHaving; i++) {
            if (exprColumns[i].hasAggregate()) {
                continue;
            }

            Expression e = exprColumns[i];

            e.resultTableColumnIndex = i;

            expressions.add(e);
        }

        if (!isAggregated) {
            return;
        }

        // order by with aggregate
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            Expression e = sortAndSlice.exprList.get(i);

            if (e.getLeftNode().hasAggregate()) {
                e.setAggregate();
            }
        }

        for (int i = indexStartOrderBy; i < indexStartAggregates; i++) {
            if (exprColumns[i].getLeftNode().hasAggregate()) {
                exprColumns[i].setAggregate();
            }
        }

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            if (!e.hasAggregate() /* && !e.isCorrelated() */) {
                continue;
            }

            aggregateCheck[i] = true;
            exprColumns[i] = e.replaceExpressions(
                expressions,
                resultRangePosition);
        }

        if (resolvedSubqueryExpressions != null) {
            for (int i = 0; i < resolvedSubqueryExpressions.size(); i++) {
                Expression e = resolvedSubqueryExpressions.get(i);

                e.replaceExpressions(expressions, resultRangePosition);
            }
        }
    }

    /**
     * replace all expressions in select list and having condition with
     * pointers to columns for group by and aggregate expressions
     */
    public void resolveGroupingSets() {

        if (!isGrouped) {
            return;
        }

        tempSet.clear();

        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            tempSet.add(exprColumns[i]);
        }

        for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
            tempSet.add(exprColumns[i]);
        }

        if (isGroupingSets) {
            for (int i = 0; i < indexLimitVisible; i++) {
                exprColumns[i] = exprColumns[i].replaceExpressions(
                    tempSet,
                    resultRangePosition);
            }
        }

        for (int i = indexStartHaving; i < indexStartHaving + havingColumnCount;
                i++) {
            exprColumns[i] = exprColumns[i].replaceExpressions(
                tempSet,
                resultRangePosition);
        }
    }

    boolean resolveForGroupBy(List<Expression> unresolvedSet) {

        for (int i = indexLimitVisible;
                i < indexLimitVisible + groupByColumnCount; i++) {
            Expression e = exprColumns[i];

            if (e.getType() == OpTypes.COLUMN) {
                RangeVariable range    = e.getRangeVariable();
                int           colIndex = e.getColumnIndex();

                range.columnsInGroupBy[colIndex] = true;
            }
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            range.hasKeyedColumnInGroupBy =
                range.rangeTable.getUniqueNotNullColumnGroup(
                    range.columnsInGroupBy) != null;
        }

        OrderedHashSet<Expression> set = null;

        for (int i = 0; i < unresolvedSet.size(); i++) {
            Expression e = unresolvedSet.get(i);

            set = e.getUnkeyedColumns(set);
        }

        return set == null;
    }

    /**
     * Returns the result of executing this Select.
     *
     * @param maxRows may be 0 to indicate no limit on the number of rows.
     * Positive values limit the size of the result set.
     * @return the result of executing this Select
     */
    Result getResult(Session session, int maxRows) {

        RowSetNavigatorData navigator  = new RowSetNavigatorData(session, this);
        int[] limits = sortAndSlice.getLimits(session, this, maxRows);
        int                 skipCount  = 0;
        int                 limitCount = limits[2];

        if (sortAndSlice.skipFullResult) {
            skipCount  = limits[0];
            limitCount = limits[1];
        }

        Result r = buildResult(session, navigator, skipCount, limitCount);

        navigator = (RowSetNavigatorData) r.getNavigator();

        if (isDistinctSelect) {
            navigator.removeDuplicates();
        }

        if (sortAndSlice.hasOrder() && !sortAndSlice.skipSort) {
            navigator.sortOrder();
        }

        if (limits != SortAndSlice.defaultLimits
                && !sortAndSlice.skipFullResult) {
            navigator.trim(limits[0], limits[1]);
        }

        r.getNavigator().reset();

        return r;
    }

    private Result buildResult(
            Session session,
            RowSetNavigatorData navigator,
            int skipCount,
            int limitCount) {

        Result  result          = Result.newResult(navigator);
        boolean isResultGrouped = isGrouped && !isSimpleDistinct;

        result.metaData = resultMetaData;

        if (isUpdatable) {
            result.rsProperties = ResultProperties.updatablePropsValue;
        }

        if (isSimpleCount) {
            getSimpleCountResult(session, navigator);

            return result;
        }

        if (limitCount == 0) {
            return result;
        }

        if (isGroupingSets) {
            session.sessionContext.setGroup(null);
        }

        int memoryRowLimit = (!isAggregated
                              && !isSingleMemoryTable
                              && !isGroupingSets)
                             ? session.resultMaxMemoryRows
                             : 0;
        int fullJoinIndex = 0;
        RangeIterator[] rangeIterators =
            new RangeIterator[rangeVariables.length];

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i] = rangeVariables[i].getIterator(session);
        }

        session.sessionContext.rownum = 1;

        for (int currentIndex = 0; ; ) {
            if (currentIndex < fullJoinIndex) {

                // finished current span
                // or finished outer rows on right navigator
                boolean end = true;

                for (int i = fullJoinIndex + 1; i < rangeVariables.length;
                        i++) {
                    if (rangeVariables[i].isRightJoin) {
                        fullJoinIndex = i;
                        currentIndex  = i;
                        end           = false;

                        ((RangeIteratorRight) rangeIterators[i]).setOnOuterRows();
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

            session.sessionData.startRowProcessing();

            Object[] data  = new Object[indexLimitData];
            int      start = 0;

            if (isGroupingSets) {
                start = indexLimitVisible;
            }

            for (int i = start; i < indexStartAggregates; i++) {
                if (isAggregated && aggregateCheck[i]) {
                    continue;
                } else if (havingColumnCount > 0 && i == indexStartHaving) {
                    continue;
                } else {
                    data[i] = exprColumns[i].getValue(session);
                }
            }

            for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
                if (i == indexLimitVisible + ResultMetaData.SysOffsets.rowId) {
                    data[i] = Long.valueOf(it.getRowId());
                } else if (i == indexLimitVisible
                           + ResultMetaData.SysOffsets.rowNum) {
                    data[i] = Long.valueOf(session.sessionContext.rownum - 1);
                } else if (i == indexLimitVisible
                           + ResultMetaData.SysOffsets.row) {
                    if (isSingleMemoryTable) {
                        data[i] = it.getCurrentRow();
                    }
                }
            }

            session.sessionContext.rownum++;

            if (skipCount > 0) {
                skipCount--;
                continue;
            }

            Object[] groupData = null;

            if (isAggregated || isResultGrouped) {
                groupData = navigator.getGroupData(data);

                if (groupData != null) {
                    data = groupData;
                }
            }

            for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
                data[i] = exprColumns[i].updateAggregatingValue(
                    session,
                    (SetFunction) data[i]);
            }

            if (groupData == null) {
                navigator.add(data);

                if (isSimpleDistinct) {
                    for (int i = 1; i < rangeVariables.length; i++) {
                        rangeIterators[i].reset();
                    }

                    currentIndex = 0;
                }
            } else if (isAggregated) {
                navigator.updateData(groupData, data);
            }

            int rowCount = navigator.getSize();

            if (rowCount == memoryRowLimit) {
                navigator = new RowSetNavigatorDataTable(
                    session,
                    this,
                    navigator);

                result.setNavigator(navigator);

                memoryRowLimit = 0;
            }

            if (isAggregated || isResultGrouped) {
                if (!sortAndSlice.isGenerated) {
                    continue;
                }
            }

            if (rowCount >= limitCount) {
                break;
            }
        }

        navigator.reset();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i].reset();
        }

        if (!isGroupingSets && !isAggregated && havingColumnCount == 0) {
            return result;
        }

        session.sessionContext.setRangeIterator(navigator);

        if (isGroupingSets) {
            session.sessionContext.setGroupSet(groupSet);

            Iterator   groupsIterator = groupSet.getIterator();
            int        baseResultSize = navigator.getSize();
            Object[][] baseResult     = navigator.removeDataTable();

            if (groupSet.nullSets != 0) {
                Object[] data = new Object[indexLimitData];

                for (int i = 0; i < indexStartAggregates; i++) {
                    data[i] = exprColumns[i].getValue(session);
                }

                navigator.add(data);
                navigator.next();

                if (isAggregated) {
                    for (int i = 0; i < baseResultSize; i++) {
                        Object[] row = baseResult[i];

                        for (int j = indexStartAggregates;
                                j < indexLimitExpressions; j++) {
                            data[j] = exprColumns[j].updateAggregatingValue(
                                session,
                                (SetFunction) data[j],
                                (SetFunction) row[j]);
                        }
                    }

                    for (int i = indexStartAggregates;
                            i < indexLimitExpressions; i++) {
                        data[i] = exprColumns[i].getAggregatedValue(
                            session,
                            (SetFunction) data[i]);
                    }

                    for (int i = 0; i < indexStartAggregates; i++) {
                        if (aggregateCheck[i]) {
                            data[i] = exprColumns[i].getValue(session);
                        }
                    }
                }

                for (int i = 1; i < groupSet.nullSets; i++) {
                    navigator.add(data);
                    navigator.next();
                }
            }

            while (groupsIterator.hasNext()) {
                navigator.resetRowMap();

                List set = (List) groupsIterator.next();

                session.sessionContext.setGroup(set);

                for (int i = 0; i < baseResultSize; i++) {
                    Object[] row  = baseResult[i];
                    Object[] data = new Object[indexLimitData];

                    for (int j = indexLimitVisible; j < indexStartHaving; j++) {
                        if (set.contains(j)) {
                            data[j] = row[j];
                        }
                    }

                    Object[] groupData = navigator.getGroupDataAndPosition(
                        data);

                    if (groupData == null) {
                        navigator.add(data);    // must populate before positioning
                        navigator.absolute(navigator.getSize() - 1);
                    } else {
                        data = groupData;
                    }

                    for (int j = indexStartAggregates;
                            j < indexLimitExpressions; j++) {
                        data[j] = exprColumns[j].updateAggregatingValue(
                            session,
                            (SetFunction) data[j],
                            (SetFunction) row[j]);
                    }

                    for (int j = 0; j < indexLimitVisible; j++) {
                        if (!isAggregated || !aggregateCheck[j]) {
                            data[j] = exprColumns[j].getValue(session);
                        }
                    }

                    navigator.updateData(groupData, data);
                }
            }
        }

        navigator.reset();

        if (isAggregated) {
            if (!isResultGrouped && navigator.getSize() == 0) {
                Object[] data = new Object[exprColumns.length];

                for (int i = 0; i < indexStartAggregates; i++) {
                    if (!aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }

                navigator.add(data);
            }

            if (isGroupingSets) {
                for (int i = 0; i < groupSet.nullSets; i++) {
                    navigator.next();
                }
            }

            while (navigator.next()) {
                Object[] data = navigator.getCurrent();

                for (int i = indexStartAggregates; i < indexLimitExpressions;
                        i++) {
                    data[i] = exprColumns[i].getAggregatedValue(
                        session,
                        (SetFunction) data[i]);
                }

                for (int i = 0; i < indexStartAggregates; i++) {
                    if (aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }
            }
        }

        navigator.reset();

        if (havingColumnCount != 0) {
            while (navigator.next()) {
                Object[] data = navigator.getCurrent();
                boolean test = exprColumns[indexStartHaving].testCondition(
                    session);

                if (!test) {
                    navigator.removeCurrent();
                }
            }

            navigator.reset();
        }

        session.sessionContext.unsetRangeIterator(navigator);

        return result;
    }

    private void getSimpleCountResult(
            Session session,
            RowSetNavigatorData navigator) {

        Object[] data  = new Object[indexLimitData];
        Table    table = rangeVariables[0].getTable();

        table.materialise(session);

        PersistentStore store = table.getRowStore(session);
        long            count = store.elementCount(session);

        data[indexStartAggregates] = ValuePool.getLong(count);

        navigator.add(data);
        navigator.reset();
        session.sessionContext.setRangeIterator(navigator);

        if (navigator.next()) {
            data = navigator.getCurrent();

            for (int i = 0; i < indexStartAggregates; i++) {
                data[i] = exprColumns[i].getValue(session);
            }
        }

        session.sessionContext.unsetRangeIterator(navigator);
    }

    void setReferenceableColumns() {

        accessibleColumns = new boolean[indexLimitVisible];

        IntValueHashMap<String> aliases = new IntValueHashMap<>();

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];
            String     alias      = expression.getAlias();

            if (alias.isEmpty()) {
                SimpleName name = HsqlNameManager.getAutoColumnName(i);

                expression.setAlias(name);
                continue;
            }

            int index = aliases.get(alias, -1);

            if (index == -1) {
                aliases.put(alias, i);

                accessibleColumns[i] = true;
            } else {
                accessibleColumns[index] = false;
            }
        }
    }

    void setColumnAliases(SimpleName[] names) {

        if (names.length != indexLimitVisible) {
            throw Error.error(ErrorCode.X_42593);
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].setAlias(names[i]);
        }
    }

    private void createResultMetaData(Session session) {

        resultMetaData = ResultMetaData.newResultMetaData(
            resultColumnTypes,
            columnMap,
            indexLimitVisible,
            indexLimitRowId);

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression   e           = exprColumns[i];
            ColumnSchema tableColumn = null;
            ColumnBase   column;

            tableColumn                   = e.getColumn();
            resultMetaData.columnTypes[i] = e.getDataType();

            if (tableColumn == null) {
                column = new ColumnBase();
            } else {
                column = new ColumnBase(
                    session.database.getCatalogName(),
                    tableColumn,
                    lowerCaseResultIdentifier);
            }

            column.setType(e.getDataType());

            SimpleName alias    = e.getSimpleName();
            String     colLabel = alias == null
                                  ? ""
                                  : alias.name;

            if (lowerCaseResultIdentifier) {
                if (!alias.isNameQuoted) {
                    colLabel = colLabel.toLowerCase(Locale.ENGLISH);
                }
            }

            resultMetaData.columns[i]      = column;
            resultMetaData.columnLabels[i] = colLabel;
        }
    }

    private void setResultNullability() {

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e           = exprColumns[i];
            byte       nullability = e.getNullability();

            if (e.opType == OpTypes.COLUMN) {
                RangeVariable range = e.getRangeVariable();

                if (range != null) {
                    if (range.rangePositionInJoin >= startInnerRange
                            && range.rangePositionInJoin < endInnerRange) {

                        //
                    } else {
                        nullability = SchemaObject.Nullability.NULLABLE;
                    }
                }
            }

            resultMetaData.columns[i].setNullability(nullability);
        }
    }

    void createTable(Session session) {

        createResultTable(session);

        mainIndex = resultTable.getPrimaryIndex();

        if (sortAndSlice.hasOrder() && !sortAndSlice.skipSort) {
            orderIndex = sortAndSlice.getNewIndex(session, resultTable);
        }

        if (isDistinctSelect || isFullOrder) {
            createFullIndex(session);
        }

        if (isGrouped) {
            int[] groupCols = new int[groupByColumnCount];

            for (int i = 0; i < groupByColumnCount; i++) {
                groupCols[i] = indexLimitRowId + i;
            }

            groupIndex = resultTable.createAndAddIndexStructure(
                session,
                null,
                groupCols,
                null,
                null,
                false,
                false,
                false);
        } else if (isAggregated) {
            groupIndex = mainIndex;
        }

        if (isUpdatable && view == null) {
            int[] idCols = new int[]{
                indexLimitVisible + ResultMetaData.SysOffsets.rowId };

            idIndex = resultTable.createAndAddIndexStructure(
                session,
                null,
                idCols,
                null,
                null,
                false,
                false,
                false);

            int[] rowNumCols = new int[]{
                indexLimitVisible + ResultMetaData.SysOffsets.rowNum };

            rowNumIndex = resultTable.createAndAddIndexStructure(
                session,
                null,
                rowNumCols,
                null,
                null,
                false,
                false,
                false);
            mainIndex = rowNumIndex;
        }
    }

    private void createFullIndex(Session session) {

        int[] fullCols = new int[indexLimitVisible];

        ArrayUtil.fillSequence(fullCols);

        fullIndex = resultTable.createAndAddIndexStructure(
            session,
            null,
            fullCols,
            null,
            null,
            false,
            false,
            false);
        resultTable.fullIndex = fullIndex;
    }

    private void setResultColumnTypes() {

        resultColumnTypes = new Type[indexLimitData];

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e = exprColumns[i];

            resultColumnTypes[i] = e.getDataType();
        }

        for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
            if (i == indexLimitVisible + ResultMetaData.SysOffsets.rowId) {
                resultColumnTypes[i] = Type.SQL_BIGINT;
            } else if (i == indexLimitVisible
                       + ResultMetaData.SysOffsets.rowStatus) {
                resultColumnTypes[i] = Type.SQL_INTEGER;
            } else if (i == indexLimitVisible
                       + ResultMetaData.SysOffsets.rowNum) {
                resultColumnTypes[i] = Type.SQL_BIGINT;
            } else if (i == indexLimitVisible + ResultMetaData.SysOffsets.row) {
                resultColumnTypes[i] = Type.SQL_ALL_TYPES;
            }
        }

        for (int i = indexLimitRowId; i < indexLimitData; i++) {
            Expression e    = exprColumns[i];
            Type       type = e.getDataType();

            if (type.getCollation() != e.collation && e.collation != null) {
                type = Type.getType(type, e.collation);
            }

            resultColumnTypes[i] = type;
        }
    }

    void createResultTable(Session session) {

        HsqlName tableName =
            session.database.nameManager.getSubqueryTableName();
        int tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                        ? TableBase.SYSTEM_SUBQUERY
                        : TableBase.RESULT_TABLE;
        OrderedHashMap<String, ColumnSchema> columnList =
            new OrderedHashMap<>();

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e          = exprColumns[i];
            SimpleName simpleName = e.getSimpleName();
            String     nameString = simpleName.name;
            HsqlName name =
                session.database.nameManager.newColumnSchemaHsqlName(
                    tableName,
                    simpleName);

            if (!accessibleColumns[i]) {
                nameString = HsqlNameManager.getAutoNoNameColumnString(i);
            }

            ColumnSchema column = new ColumnSchema(
                name,
                e.dataType,
                true,
                false,
                null);

            columnList.add(nameString, column);
        }

        resultTable = new TableDerived(
            session.database,
            tableName,
            tableType,
            resultColumnTypes,
            columnList,
            ValuePool.emptyIntArray);
    }

    public String getSQL() {

        StringBuilder sb = new StringBuilder();
        int           limit;

        sb.append(Tokens.T_SELECT).append(' ');

        limit = indexLimitVisible;

        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(exprColumns[i].getSQL());
        }

        sb.append(Tokens.T_FROM);

        limit = rangeVariables.length;

        for (int i = 0; i < limit; i++) {
            RangeVariable rangeVar = rangeVariables[i];

            if (i > 0) {
                if (rangeVar.isLeftJoin && rangeVar.isRightJoin) {
                    sb.append(Tokens.T_FULL).append(' ');
                } else if (rangeVar.isLeftJoin) {
                    sb.append(Tokens.T_LEFT).append(' ');
                } else if (rangeVar.isRightJoin) {
                    sb.append(Tokens.T_RIGHT).append(' ');
                }

                sb.append(Tokens.T_JOIN).append(' ');
            }

            sb.append(rangeVar.getTable().getName().statementName);
        }

        if (isGrouped) {
            sb.append(' ')
              .append(Tokens.T_GROUP)
              .append(' ')
              .append(Tokens.T_BY);

            limit = indexLimitVisible + groupByColumnCount;

            for (int i = indexLimitVisible; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());

                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }

        if (havingColumnCount != 0) {
            sb.append(' ')
              .append(Tokens.T_HAVING)
              .append(' ')
              .append(exprColumns[indexStartHaving].getSQL());
        }

        if (sortAndSlice.hasOrder()) {
            limit = indexStartOrderBy + sortAndSlice.getOrderLength();

            sb.append(' ')
              .append(Tokens.T_ORDER)
              .append(' ')
              .append(Tokens.T_BY)
              .append(' ');

            for (int i = indexStartOrderBy; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());

                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }

        if (sortAndSlice.hasLimit()) {
            sb.append(sortAndSlice.limitCondition.getLeftNode().getSQL());
        }

        return sb.toString();
    }

    public ResultMetaData getMetaData() {
        return resultMetaData;
    }

    public String describe(Session session, int blanks) {

        String        temp;
        StringBuilder sb = new StringBuilder();
        StringBuilder b  = new StringBuilder(blanks);

        for (int i = 0; i < blanks; i++) {
            b.append(' ');
        }

        sb.append(b)
          .append("isDistinctSelect=[")
          .append(isDistinctSelect)
          .append("]\n")
          .append(b)
          .append("isGrouped=[")
          .append(isGrouped)
          .append("]\n")
          .append(b)
          .append("isAggregated=[")
          .append(isAggregated)
          .append("]\n")
          .append(b)
          .append("columns=[");

        for (int i = 0; i < indexLimitVisible; i++) {
            int index = i;

            if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                index = exprColumns[i].columnIndex;
            }

            sb.append(b);

            temp = exprColumns[index].describe(session, 2);

            sb.append(temp, 0, temp.length() - 1);

            if (resultMetaData.columns[i].getNullability()
                    == SchemaObject.Nullability.NO_NULLS) {
                sb.append(" not nullable\n");
            } else {
                sb.append(" nullable\n");
            }
        }

        sb.append("\n").append(b).append("]\n");

        for (int i = 0; i < rangeVariables.length; i++) {
            sb.append(b)
              .append("[")
              .append("range variable ")
              .append(i + 1)
              .append("\n")
              .append(rangeVariables[i].describe(session, blanks + 2))
              .append(b)
              .append("]");
        }

        sb.append(b).append("]\n");

        temp = queryCondition == null
               ? "null"
               : queryCondition.describe(session, blanks);

        if (isGrouped) {
            sb.append(b).append("groupColumns=[");

            for (int i = indexLimitRowId;
                    i < indexLimitRowId + groupByColumnCount; i++) {
                int index = i;

                if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                    index = exprColumns[i].columnIndex;
                }

                sb.append(exprColumns[index].describe(session, blanks));
            }

            sb.append(b).append("]\n");
        }

        if (havingColumnCount != 0) {
            temp = exprColumns[indexStartHaving].describe(session, blanks);

            sb.append(b).append("havingCondition=[").append(temp).append("]\n");
        }

        if (sortAndSlice.hasOrder()) {
            sb.append(b).append("order by=[\n");

            for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                sb.append(b)
                  .append(
                      sortAndSlice.exprList.get(i).describe(session, blanks));
            }

            if (sortAndSlice.primaryTableIndex != null) {
                sb.append(b).append("uses index");
            }

            sb.append(b).append("]\n");
        }

        if (sortAndSlice.hasLimit()) {
            if (sortAndSlice.limitCondition.getLeftNode() != null) {
                sb.append(b)
                  .append("offset=[")
                  .append(
                      sortAndSlice.limitCondition.getLeftNode()
                                                 .describe(session, b.length()))
                  .append("]\n");
            }

            if (sortAndSlice.limitCondition.getRightNode() != null) {
                sb.append(b)
                  .append("limit=[")
                  .append(
                      sortAndSlice.limitCondition.getRightNode()
                                                 .describe(session, b.length()))
                  .append("]\n");
            }
        }

        return sb.toString();
    }

    void setMergeability() {

        isOrderSensitive |= sortAndSlice.hasLimit() || sortAndSlice.hasOrder();

        if (isOrderSensitive) {
            isMergeable = false;
        }

        if (isAggregated) {
            isMergeable = false;
        }

        if (isGrouped || isDistinctSelect) {
            isBaseMergeable = false;
            isMergeable     = false;
        }

        if (rangeVariables.length != 1) {
            isBaseMergeable = false;
            isMergeable     = false;
        }
    }

    void setUpdatability(Session session) {

        if (!isUpdatable) {
            return;
        }

        isUpdatable = false;

        if (isGrouped || isDistinctSelect || isAggregated) {
            return;
        }

        if (!isBaseMergeable) {
            return;
        }

        if (!isTopLevel) {
            return;
        }

        if (sortAndSlice.hasLimit() || sortAndSlice.hasOrder()) {
            if (!session.database.sqlSyntaxDb2) {
                return;
            }
        }

        RangeVariable rangeVar  = rangeVariables[0];
        Table         table     = rangeVar.getTable();
        Table         baseTable = table.getBaseTable();

        if (baseTable == null) {
            return;
        }

        isInsertable = table.isInsertable();
        isUpdatable  = table.isUpdatable();

        if (!isInsertable && !isUpdatable) {
            return;
        }

        IntValueHashMap<String> columns = new IntValueHashMap<>();
        boolean[]               checkList;
        int[]                   baseColumnMap = table.getBaseTableColumnMap();
        int[]                   columnMap     = new int[indexLimitVisible];

        if (queryCondition != null) {
            HashSet<HsqlName> nameSet = collectReferencesInSubQueries(
                queryCondition);

            if (nameSet != null) {
                if (nameSet.contains(table.getName())
                        || nameSet.contains(baseTable.getName())) {
                    isUpdatable  = false;
                    isInsertable = false;

                    return;
                }
            }
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];

            if (expression.getType() == OpTypes.COLUMN) {
                String name = expression.getColumn().getName().name;

                if (columns.containsKey(name)) {
                    columns.put(name, 1);
                    continue;
                }

                columns.put(name, 0);
            } else {
                HashSet<HsqlName> nameSet = collectReferencesInSubQueries(
                    expression);

                if (nameSet != null) {
                    if (nameSet.contains(table.getName())) {
                        isUpdatable  = false;
                        isInsertable = false;

                        return;
                    }
                }
            }
        }

        isUpdatable = false;

        for (int i = 0; i < indexLimitVisible; i++) {
            if (accessibleColumns[i]) {
                Expression expression = exprColumns[i];

                if (expression.getType() == OpTypes.COLUMN) {
                    String name = expression.getColumn().getName().name;

                    if (columns.get(name) == 0) {
                        int index = table.findColumn(name);

                        columnMap[i] = baseColumnMap[index];

                        if (columnMap[i] != -1) {
                            isUpdatable = true;
                        }

                        continue;
                    }
                }
            }

            columnMap[i] = -1;
            isInsertable = false;
        }

        if (isInsertable) {
            checkList = baseTable.getColumnCheckList(columnMap);

            for (int i = 0; i < checkList.length; i++) {
                if (checkList[i]) {
                    continue;
                }

                ColumnSchema column = baseTable.getColumn(i);

                if (column.isIdentity()
                        || column.isGenerated()
                        || column.hasDefault()
                        || column.isNullable()) {}
                else {
                    isInsertable = false;
                    break;
                }
            }
        }

        if (!isUpdatable) {
            isInsertable = false;
        }

        if (isUpdatable) {
            this.columnMap = columnMap;
            this.baseTable = baseTable;

            if (view != null) {
                return;
            }

            indexLimitRowId += ResultMetaData.SysOffsets.limitWithRow;

            if (!baseTable.isFileBased()) {
                isSingleMemoryTable = true;
            }

            indexLimitData = indexLimitRowId;
        }
    }

    /**
     * isBaseMergeable is simply a flag to allow merging the current query
     * isMergeable is a flag to allow this to act as base for a query
     *
     */
    void mergeQuery(Session session) {

        RangeVariable   rangeVar            = rangeVariables[0];
        Table           table               = rangeVar.getTable();
        Expression      localQueryCondition = queryCondition;
        QueryExpression baseQueryExpression = table.getQueryExpression();

        if (isBaseMergeable
                && baseQueryExpression != null
                && baseQueryExpression.isMergeable) {
            QuerySpecification baseSelect = baseQueryExpression.getMainSelect();

            rangeVariables[0] = baseSelect.rangeVariables[0];

            rangeVariables[0].resetConditions();

            for (int i = 0; i < indexLimitExpressions; i++) {
                Expression e = exprColumns[i];

                exprColumns[i] = e.replaceColumnReferences(
                    session,
                    rangeVar,
                    baseSelect.exprColumns);
            }

            if (localQueryCondition != null) {
                localQueryCondition =
                    localQueryCondition.replaceColumnReferences(
                        session,
                        rangeVar,
                        baseSelect.exprColumns);
            }

            Expression baseQueryCondition = baseSelect.queryCondition;

            checkQueryCondition = baseSelect.checkQueryCondition;
            queryCondition = ExpressionLogical.andExpressions(
                baseQueryCondition,
                localQueryCondition);
        }

        if (view != null) {
            switch (view.getCheckOption()) {

                case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = localQueryCondition;
                    break;

                case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = queryCondition;
                    break;
            }
        }
    }

    private HashSet<HsqlName> collectReferencesInSubQueries(
            Expression expression) {

        tempSet.clear();
        expression.collectAllExpressions(
            tempSet,
            OpTypes.subqueryExpressionSet,
            OpTypes.emptyExpressionSet);

        if (tempSet.isEmpty()) {
            return null;
        }

        HashSet<HsqlName> nameSet = new HashSet<>();

        for (int i = 0; i < tempSet.size(); i++) {
            Expression e = tempSet.get(i);

            e.collectObjectNames(nameSet);
        }

        return nameSet;
    }

    public OrderedHashSet<TableDerived> getSubqueries() {

        OrderedHashSet<TableDerived> set = null;

        for (int i = 0; i < indexLimitExpressions; i++) {
            set = exprColumns[i].collectAllSubqueries(set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectAllSubqueries(set);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            OrderedHashSet<TableDerived> temp =
                rangeVariables[i].getSubqueries();

            set = OrderedHashSet.addAll(set, temp);
        }

        return set;
    }

    public Table getBaseTable() {
        return baseTable;
    }

    public OrderedHashSet<Expression> collectOuterColumnExpressions(
            OrderedHashSet<Expression> set,
            OrderedHashSet<Expression> exclude) {

        set = collectAllExpressions(
            set,
            OpTypes.columnExpressionSet,
            OpTypes.subqueryAggregateExpressionSet);

        if (set == null) {
            return null;
        }

        for (int i = set.size() - 1; i >= 0; i--) {
            Expression col = set.get(i);

            if (ArrayUtil.find(rangeVariables, col.getRangeVariable()) >= 0) {
                set.remove(i);
            }

            if (exclude.contains(col)) {
                set.remove(i);
            }
        }

        if (set.isEmpty()) {
            set = null;
        }

        return set;
    }

    public OrderedHashSet<Expression> collectAllExpressions(
            OrderedHashSet<Expression> set,
            OrderedIntHashSet typeSet,
            OrderedIntHashSet stopAtTypeSet) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        if (queryCondition != null) {
            set = queryCondition.collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        return set;
    }

    public void collectObjectNames(Set<HsqlName> set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectObjectNames(set);
        }

        if (queryCondition != null) {
            queryCondition.collectObjectNames(set);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            HsqlName name = rangeVariables[i].getTable().getName();

            set.add(name);
        }
    }

    public void replaceColumnReferences(
            Session session,
            RangeVariable range,
            Expression[] list) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i] = exprColumns[i].replaceColumnReferences(
                session,
                range,
                list);
        }

        if (queryCondition != null) {
            queryCondition = queryCondition.replaceColumnReferences(
                session,
                range,
                list);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].replaceColumnReferences(session, range, list);
        }
    }

    public void replaceRangeVariables(
            RangeVariable[] ranges,
            RangeVariable[] newRanges) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].replaceRangeVariables(ranges, newRanges);
        }

        if (queryCondition != null) {
            queryCondition.replaceRangeVariables(ranges, newRanges);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].getSubqueries();
        }
    }

    public void replaceExpressions(
            OrderedHashSet<Expression> expressions,
            int resultRangePosition) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i] = exprColumns[i].replaceExpressions(
                expressions,
                resultRangePosition);
        }

        if (queryCondition != null) {
            queryCondition = queryCondition.replaceExpressions(
                expressions,
                resultRangePosition);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].replaceExpressions(
                expressions,
                resultRangePosition);
        }
    }

    /**
     * Not for views. Only used on root node.
     */
    public void setReturningResult() {

        setReturningResultSet();

        acceptsSequences = true;
        isTopLevel       = true;
    }

    void setReturningResultSet() {
        persistenceScope = TableBase.SCOPE_SESSION;
    }

    public boolean isSingleColumn() {
        return indexLimitVisible == 1;
    }

    public String[] getColumnNames() {

        String[] names = new String[indexLimitVisible];

        for (int i = 0; i < indexLimitVisible; i++) {
            names[i] = exprColumns[i].getAlias();
        }

        return names;
    }

    public Type[] getColumnTypes() {

        if (resultColumnTypes.length == indexLimitVisible) {
            return resultColumnTypes;
        }

        Type[] types = new Type[indexLimitVisible];

        ArrayUtil.copyArray(resultColumnTypes, types, types.length);

        return types;
    }

    public int getColumnCount() {
        return indexLimitVisible;
    }

    public int[] getBaseTableColumnMap() {
        return columnMap;
    }

    public Expression getCheckCondition() {
        return queryCondition;
    }

    void getBaseTableNames(OrderedHashSet<HsqlName> set) {

        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();

            if (rangeTable.isView()) {
                continue;
            }

            if (rangeTable.isDataReadOnly() || rangeTable.isTemp()) {
                continue;
            }

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            set.add(name);
        }
    }

    /**
     * returns true if almost equivalent
     */
    boolean isEquivalent(QueryExpression other) {

        if (!(other instanceof QuerySpecification)) {
            return false;
        }

        QuerySpecification otherSpec = (QuerySpecification) other;

        if (!Expression.equals(exprColumns, otherSpec.exprColumns)) {
            return false;
        }

        if (!Expression.equals(queryCondition, otherSpec.queryCondition)) {
            return false;
        }

        if (rangeVariables.length != otherSpec.rangeVariables.length) {
            return false;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].getTable()
                    != otherSpec.rangeVariables[i].getTable()) {
                return false;
            }
        }

        return true;
    }
}
