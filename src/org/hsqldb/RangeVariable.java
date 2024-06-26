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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.RangeGroup.RangeGroupSimple;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.OrderedLongHashSet;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;

/**
 * Metadata for range variables, including conditions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public class RangeVariable {

    static final RangeVariable[] emptyArray = new RangeVariable[]{};

    //
    public static final int TABLE_RANGE       = 1;
    public static final int TRANSITION_RANGE  = 2;
    public static final int PARAMETER_RANGE   = 3;
    public static final int VARIALBE_RANGE    = 4;
    public static final int PLACEHOLDER_RANGE = 5;

    //
    Table                            rangeTable;
    final SimpleName                 tableAlias;
    private OrderedHashSet<String>   columnAliases;
    private SimpleName[]             columnAliasNames;
    private OrderedHashSet<HsqlName> columnNames;
    OrderedHashSet<String>           namedJoinColumns;
    HashMap<String, Expression>      namedJoinColumnExpressions;
    boolean[]                        columnsInGroupBy;
    boolean                          hasKeyedColumnInGroupBy;
    boolean[]                        usedColumns;
    boolean[]                        updatedColumns;
    boolean[]                        namedJoinColumnCheck;

    //
    RangeVariableConditions[] joinConditions;
    RangeVariableConditions[] whereConditions;
    int                       subRangeCount;

    // non-index conditions
    Expression joinCondition;

    // system period condition
    ExpressionPeriodOp periodCondition;

    // role based condition
    Expression filterCondition;

    //
    boolean isLateral;
    boolean isLeftJoin;     // table joined with LEFT / FULL OUTER JOIN
    boolean isRightJoin;    // table joined with RIGHT / FULL OUTER JOIN
    boolean isJoin;

    //
    boolean hasLateral;
    boolean hasLeftJoin;
    boolean hasRightJoin;

    //
    int level;

    //
    int indexDistinctCount;

    //
    int rangePositionInJoin;

    //
    int rangePosition;

    //
    boolean isViewSubquery;

    // for variable and parameter lists
    OrderedHashMap<String, ColumnSchema> variables;

    // variable, parameter, table
    int rangeType;

    //
    boolean isGenerated;

    public RangeVariable(
            OrderedHashMap<String, ColumnSchema> variables,
            SimpleName rangeName,
            boolean isVariable,
            int rangeType) {

        this.variables   = variables;
        this.rangeType   = rangeType;
        rangeTable       = null;
        tableAlias       = rangeName;
        columnsInGroupBy = null;
        usedColumns      = null;
        joinConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                true) };
        whereConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                false) };

        switch (rangeType) {

            case TRANSITION_RANGE :
                usedColumns = new boolean[variables.size()];
                break;

            case PARAMETER_RANGE :
            case VARIALBE_RANGE :
            case PLACEHOLDER_RANGE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "RangeVariable");
        }
    }

    public RangeVariable(
            Table table,
            SimpleName alias,
            OrderedHashSet<String> columnList,
            SimpleName[] columnNameList,
            CompileContext compileContext) {

        rangeType        = TABLE_RANGE;
        rangeTable       = table;
        tableAlias       = alias;
        columnAliases    = columnList;
        columnAliasNames = columnNameList;
        joinConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                true) };
        whereConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                false) };

        compileContext.registerRangeVariable(this);

        if (rangeTable.getColumnCount() != 0) {
            setRangeTableVariables();
        }
    }

    public RangeVariable(Table table, int position) {

        rangeType        = TABLE_RANGE;
        rangeTable       = table;
        tableAlias       = null;
        columnsInGroupBy = rangeTable.getNewColumnCheckList();
        usedColumns      = rangeTable.getNewColumnCheckList();
        rangePosition    = position;
        joinConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                true) };
        whereConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                false) };
    }

    public void setRangeTableVariables() {

        if (columnAliasNames != null
                && rangeTable.getColumnCount() != columnAliasNames.length) {
            throw Error.error(ErrorCode.X_42593);
        }

        columnsInGroupBy              = rangeTable.getNewColumnCheckList();
        usedColumns                   = rangeTable.getNewColumnCheckList();
        joinConditions[0].rangeIndex  = rangeTable.getDefaultIndex();
        whereConditions[0].rangeIndex = rangeTable.getDefaultIndex();
    }

    public void setJoinType(boolean isLeft, boolean isRight) {

        isJoin      = true;
        isLeftJoin  = isLeft;
        isRightJoin = isRight;

        if (isRightJoin) {
            whereConditions[0].rangeIndex = rangeTable.getPrimaryIndex();
        }
    }

    public void setSystemPeriodCondition(ExpressionPeriodOp condition) {
        periodCondition = condition;
    }

    public void setFilterExpression(Session session, Expression expr) {

        if (expr != null) {
            RangeGroup ranges = new RangeGroupSimple(
                new RangeVariable[]{ this },
                false);

            expr.resolveColumnReferences(
                session,
                ranges,
                RangeGroup.emptyArray,
                null);
            expr.resolveTypes(session, null);

            filterCondition = expr;
        }
    }

    public void addNamedJoinColumns(OrderedHashSet<String> columns) {
        namedJoinColumns = columns;
    }

    public void addColumn(int columnIndex) {
        if (usedColumns != null) {
            usedColumns[columnIndex] = true;
        }
    }

    public void addAllColumns() {
        if (usedColumns != null) {
            ArrayUtil.fillArray(usedColumns, true);
        }
    }

    public void addNamedJoinColumnExpression(
            String name,
            Expression e,
            int position) {

        if (namedJoinColumnExpressions == null) {
            namedJoinColumnExpressions = new HashMap<>();
        }

        namedJoinColumnExpressions.put(name, e);

        if (namedJoinColumnCheck == null) {
            namedJoinColumnCheck = rangeTable.getNewColumnCheckList();
        }

        namedJoinColumnCheck[position] = true;
    }

    public ExpressionColumn getColumnExpression(String name) {
        return namedJoinColumnExpressions == null
               ? null
               : (ExpressionColumn) namedJoinColumnExpressions.get(name);
    }

    public Table getTable() {
        return rangeTable;
    }

    public boolean hasAnyTerminalCondition() {

        for (int i = 0; i < joinConditions.length; i++) {
            if (joinConditions[0].terminalCondition != null) {
                return true;
            }
        }

        for (int i = 0; i < whereConditions.length; i++) {
            if (whereConditions[0].terminalCondition != null) {
                return true;
            }
        }

        return false;
    }

    public boolean hasAnyIndexCondition() {

        for (int i = 0; i < joinConditions.length; i++) {
            if (joinConditions[0].indexedColumnCount > 0) {
                return true;
            }
        }

        for (int i = 0; i < whereConditions.length; i++) {
            if (whereConditions[0].indexedColumnCount > 0) {
                return true;
            }
        }

        return false;
    }

    public boolean hasSingleIndexCondition() {
        return joinConditions.length == 1
               && joinConditions[0].indexedColumnCount > 0;
    }

    public boolean setDistinctColumnsOnIndex(int[] colMap) {

        if (joinConditions.length != 1) {
            return false;
        }

        int[] indexColMap = joinConditions[0].rangeIndex.getColumns();

        if (colMap.length > indexColMap.length) {
            return false;
        }

        if (colMap.length == indexColMap.length) {
            if (ArrayUtil.haveEqualSets(colMap, indexColMap, colMap.length)) {
                indexDistinctCount = colMap.length;

                return true;
            }
        }

        if (ArrayUtil.haveEqualArrays(colMap, indexColMap, colMap.length)) {
            indexDistinctCount = colMap.length;

            return true;
        }

        return false;
    }

    /**
     * Used for sort
     */
    public Index getSortIndex() {

        if (joinConditions.length == 1) {
            return joinConditions[0].rangeIndex;
        } else {
            return null;
        }
    }

    /**
     * Used for sort
     */
    public boolean setSortIndex(Index index, boolean reversed) {

        if (joinConditions.length == 1) {
            if (joinConditions[0].indexedColumnCount == 0) {
                joinConditions[0].rangeIndex = index;
                joinConditions[0].reversed   = reversed;

                return true;
            }
        }

        return false;
    }

    public boolean reverseOrder() {

        if (joinConditions.length == 1) {
            return joinConditions[0].reverseIndexCondition();
        }

        return false;
    }

    public OrderedHashSet<HsqlName> getColumnNames() {

        if (columnNames == null) {
            columnNames = new OrderedHashSet<>();

            rangeTable.getColumnNames(usedColumns, columnNames);
        }

        return columnNames;
    }

    public OrderedHashSet<String> getUniqueColumnNameSet() {

        OrderedHashSet<String> set = new OrderedHashSet<>();

        if (columnAliases != null) {
            set.addAll(columnAliases);

            return set;
        }

        for (int i = 0; i < rangeTable.columnList.size(); i++) {
            String  name  = rangeTable.getColumn(i).getName().name;
            boolean added = set.add(name);

            if (!added) {
                throw Error.error(ErrorCode.X_42578, name);
            }
        }

        return set;
    }

    public int findColumn(
            String schemaName,
            String tableName,
            String columnName) {

        if (namedJoinColumnExpressions != null
                && namedJoinColumnExpressions.containsKey(columnName)) {
            if (tableName != null && !resolvesTableName(tableName)) {
                return -1;
            }
        }

        if (resolvesSchemaAndTableName(schemaName, tableName)) {
            return findColumn(columnName);
        }

        return -1;
    }

    /**
     * Returns index for column
     *
     * @param columnName name of column
     * @return int index or -1 if not found
     */
    public int findColumn(String columnName) {

        if (variables != null) {
            return variables.getIndex(columnName);
        } else if (columnAliases != null) {
            return columnAliases.getIndex(columnName);
        } else {
            return rangeTable.findColumn(columnName);
        }
    }

    public ColumnSchema getColumn(int i) {

        if (variables == null) {
            return rangeTable.getColumn(i);
        } else {
            return variables.get(i);
        }
    }

    public SimpleName getColumnAlias(int i) {

        if (columnAliases == null) {
            return rangeTable.getColumn(i).getName();
        } else {
            return columnAliasNames[i];
        }
    }

    public boolean hasColumnAlias() {
        return columnAliases != null;
    }

    public boolean hasTableAlias() {
        return tableAlias != null;
    }

    public boolean isVariable() {
        return variables != null;
    }

    public SimpleName getTableAlias() {
        return tableAlias == null
               ? rangeTable.getName()
               : tableAlias;
    }

    public RangeVariable getRangeForTableName(String name) {

        if (resolvesTableName(name)) {
            return this;
        }

        return null;
    }

    private boolean resolvesSchemaAndTableName(
            String schemaName,
            String tableName) {
        return resolvesSchemaName(schemaName) && resolvesTableName(tableName);
    }

    private boolean resolvesTableName(String name) {

        if (name == null) {
            return true;
        }

        if (variables != null) {
            if (tableAlias != null) {
                return name.equals(tableAlias.name);
            }

            return false;
        }

        if (tableAlias == null) {
            return name.equals(rangeTable.getName().name);
        } else {
            return name.equals(tableAlias.name);
        }
    }

    private boolean resolvesSchemaName(String name) {

        if (name == null) {
            return true;
        }

        if (variables != null) {
            return false;
        }

        if (tableAlias != null) {
            return false;
        }

        return name.equals(rangeTable.getSchemaName().name);
    }

    public PeriodDefinition findPeriod(
            String schemaName,
            String tableName,
            String periodName) {

        if (resolvesSchemaAndTableName(schemaName, tableName)) {
            PeriodDefinition period = rangeTable.getApplicationPeriod();

            if (period != null && period.getName().name.equals(periodName)) {
                return period;
            }

            period = rangeTable.getSystemPeriod();

            if (period != null && period.getName().name.equals(periodName)) {
                return period;
            }
        }

        return null;
    }

    /**
     * Add all columns to a list of expressions
     */
    public void addTableColumns(HsqlArrayList<Expression> exprList) {

        if (namedJoinColumns != null) {
            int count    = exprList.size();
            int position = 0;

            for (int i = 0; i < count; i++) {
                Expression e          = exprList.get(i);
                String     columnName = e.getColumnName();

                if (namedJoinColumns.contains(columnName)) {
                    if (position != i) {
                        exprList.remove(i);
                        exprList.add(position, e);
                    }

                    e = getColumnExpression(columnName);

                    exprList.set(position, e);

                    position++;
                }
            }
        }

        addTableColumns(exprList, exprList.size(), namedJoinColumns);
    }

    /**
     * Add all columns to a list of expressions
     */
    public int addTableColumns(
            HsqlArrayList<Expression> exprList,
            int position,
            HashSet<String> exclude) {

        Table table = getTable();
        int   count = table.getColumnCount();

        for (int i = 0; i < count; i++) {
            ColumnSchema column     = table.getColumn(i);
            String       columnName = columnAliases == null
                                      ? column.getName().name
                                      : columnAliases.get(i);

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new ExpressionColumn(this, i);

            exprList.add(position++, e);
        }

        return position;
    }

    public void addTableColumns(
            RangeVariable subRange,
            Expression expression,
            HashSet<String> exclude) {

        if (subRange == this) {
            Table table = getTable();
            int   count = table.getColumnCount();

            addTableColumns(expression, 0, count, exclude);
        }
    }

    protected int getFirstColumnIndex(RangeVariable subRange) {

        if (subRange == this) {
            return 0;
        }

        return -1;
    }

    protected void addTableColumns(
            Expression expression,
            int start,
            int count,
            HashSet<String> exclude) {

        Table                     table = getTable();
        HsqlArrayList<Expression> list  = new HsqlArrayList<>();

        for (int i = start; i < start + count; i++) {
            ColumnSchema column     = table.getColumn(i);
            String       columnName = columnAliases == null
                                      ? column.getName().name
                                      : columnAliases.get(i);

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new ExpressionColumn(this, i);

            list.add(e);
        }

        Expression[] nodes = new Expression[list.size()];

        list.toArray(nodes);

        expression.nodes = nodes;
    }

    /**
     * Removes reference to Index to avoid possible memory leaks after alter
     * table or drop index
     */
    public void setForCheckConstraint() {
        joinConditions[0].rangeIndex  = null;
        whereConditions[0].rangeIndex = null;
        rangePosition                 = 0;
    }

    /**
     * used before condition processing
     */
    public Expression getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(Expression e) {
        joinCondition = e;
    }

    public void addJoinCondition(Expression e) {
        joinCondition = ExpressionLogical.andExpressions(joinCondition, e);
    }

    public void resetConditions() {

        Index index = joinConditions[0].rangeIndex;

        joinConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                true) };
        joinConditions[0].rangeIndex = index;

        //
        index                         = whereConditions[0].rangeIndex;
        whereConditions = new RangeVariableConditions[]{
            new RangeVariableConditions(
                this,
                false) };
        whereConditions[0].rangeIndex = index;
    }

    public OrderedHashSet<TableDerived> getSubqueries() {

        OrderedHashSet<TableDerived> set = null;

        if (joinCondition != null) {
            set = joinCondition.collectAllSubqueries(set);
        }

        if (rangeTable instanceof TableDerived) {
            QueryExpression queryExpression = rangeTable.getQueryExpression();

            if (queryExpression == null) {
                Expression dataExpression = rangeTable.getDataExpression();

                if (dataExpression != null) {
                    set = dataExpression.collectAllSubqueries(set);
                }
            } else {
                OrderedHashSet<TableDerived> temp =
                    queryExpression.getSubqueries();

                set = OrderedHashSet.addAll(set, temp);
                set = OrderedHashSet.add(set, (TableDerived) rangeTable);
            }
        }

        return set;
    }

    OrderedHashSet<RangeVariable> collectRangeVariables(
            RangeVariable[] rangeVars,
            OrderedHashSet<RangeVariable> set) {

        QueryExpression queryExpression = rangeTable.getQueryExpression();
        Expression      dataExpression  = rangeTable.getDataExpression();

        if (queryExpression != null) {
            set = queryExpression.collectRangeVariables(rangeVars, set);
        }

        if (dataExpression != null) {
            set = dataExpression.collectRangeVariables(rangeVars, set);
        }

        return set;
    }

    public OrderedHashSet<Expression> collectAllExpressions(
            OrderedHashSet<Expression> set,
            OrderedIntHashSet typeSet,
            OrderedIntHashSet stopAtTypeSet) {

        if (joinCondition != null) {
            set = joinCondition.collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        QueryExpression queryExpression = rangeTable.getQueryExpression();
        Expression      dataExpression  = rangeTable.getDataExpression();

        if (queryExpression != null) {
            set = queryExpression.collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        if (dataExpression != null) {
            set = dataExpression.collectAllExpressions(
                set,
                typeSet,
                stopAtTypeSet);
        }

        return set;
    }

    public void replaceColumnReferences(
            Session session,
            RangeVariable range,
            Expression[] list) {

        QueryExpression queryExpression = rangeTable.getQueryExpression();
        Expression      dataExpression  = rangeTable.getDataExpression();

        if (dataExpression != null) {
            dataExpression.replaceColumnReferences(session, range, list);
        }

        if (queryExpression != null) {
            queryExpression.replaceColumnReferences(session, range, list);
        }

        if (joinCondition != null) {
            joinCondition = joinCondition.replaceColumnReferences(
                session,
                range,
                list);
        }

        for (int i = 0; i < joinConditions.length; i++) {
            joinConditions[i].replaceColumnReferences(session, range, list);
        }

        for (int i = 0; i < whereConditions.length; i++) {
            whereConditions[i].replaceColumnReferences(session, range, list);
        }
    }

    public void replaceRangeVariables(
            RangeVariable[] ranges,
            RangeVariable[] newRanges) {
        if (joinCondition != null) {
            joinCondition.replaceRangeVariables(ranges, newRanges);
        }
    }

    public void replaceExpressions(
            OrderedHashSet<Expression> expressions,
            int resultRangePosition) {

        QueryExpression queryExpression = rangeTable.getQueryExpression();
        Expression      dataExpression  = rangeTable.getDataExpression();

        if (dataExpression != null) {
            dataExpression.replaceExpressions(expressions, resultRangePosition);
        }

        if (queryExpression != null) {
            queryExpression.replaceExpressions(
                expressions,
                resultRangePosition);
        }

        if (joinCondition != null) {
            joinCondition = joinCondition.replaceExpressions(
                expressions,
                resultRangePosition);
        }

        for (int i = 0; i < joinConditions.length; i++) {
            joinConditions[i].replaceExpressions(
                expressions,
                resultRangePosition);
        }

        for (int i = 0; i < whereConditions.length; i++) {
            whereConditions[i].replaceExpressions(
                expressions,
                resultRangePosition);
        }
    }

    public void resolveRangeTable(
            Session session,
            RangeGroup rangeGroup,
            RangeGroup[] rangeGroups) {

        QueryExpression queryExpression = rangeTable.getQueryExpression();
        Expression      dataExpression  = rangeTable.getDataExpression();

        if (queryExpression == null && dataExpression == null) {
            return;
        }

        rangeGroups = (RangeGroup[]) ArrayUtil.toAdjustedArray(
            rangeGroups,
            rangeGroup,
            rangeGroups.length,
            1);

        if (dataExpression != null) {
            List<Expression> unresolved =
                dataExpression.resolveColumnReferences(
                    session,
                    RangeGroup.emptyGroup,
                    rangeGroups,
                    null);

            unresolved = Expression.resolveColumnSet(
                session,
                RangeVariable.emptyArray,
                RangeGroup.emptyArray,
                unresolved);

            ExpressionColumn.checkColumnsResolved(unresolved);
            dataExpression.resolveTypes(session, null);
            setRangeTableVariables();
        }

        if (queryExpression != null) {
            queryExpression.resolveReferences(session, rangeGroups);

            List<Expression> unresolved =
                queryExpression.getUnresolvedExpressions();

            unresolved = Expression.resolveColumnSet(
                session,
                RangeVariable.emptyArray,
                RangeGroup.emptyArray,
                unresolved);

            ExpressionColumn.checkColumnsResolved(unresolved);
            queryExpression.resolveTypesPartOne(session);
            queryExpression.resolveTypesPartTwo(session);
            rangeTable.prepareTable(session);
            setRangeTableVariables();
        }
    }

    void resolveRangeTableTypes(Session session, RangeVariable[] ranges) {

        QueryExpression queryExpression = rangeTable.getQueryExpression();

        if (queryExpression != null) {
            if (queryExpression instanceof QuerySpecification) {
                QuerySpecification qs = (QuerySpecification) queryExpression;

                if (qs.isGrouped || qs.isAggregated || qs.isOrderSensitive) {

                    //
                } else {
                    moveConditionsToInner(session, ranges);
                }
            }

            queryExpression.resolveTypesPartThree(session);
        }
    }

    void moveConditionsToInner(Session session, RangeVariable[] ranges) {

        Expression[]              colExpr;
        int                       exclude;
        HsqlArrayList<Expression> conditionsList;
        Expression                condition = null;

        if (whereConditions.length > 1) {
            return;
        }

        if (joinConditions.length > 1) {
            return;
        }

        for (int i = 0; i < ranges.length; i++) {
            if (ranges[i].isLeftJoin || ranges[i].isRightJoin) {
                return;
            }
        }

        exclude        = ArrayUtil.find(ranges, this);
        conditionsList = new HsqlArrayList<>();

        addConditionsToList(conditionsList, joinConditions[0].indexCond);

        if (joinConditions[0].indexCond != null
                && joinConditions[0].indexCond[0]
                   != joinConditions[0].indexEndCond[0]) {
            addConditionsToList(conditionsList, joinConditions[0].indexEndCond);
        }

        addConditionsToList(conditionsList, whereConditions[0].indexCond);
        addConditionsToList(conditionsList, whereConditions[0].indexEndCond);
        RangeVariableResolver.decomposeAndConditions(
            session,
            joinConditions[0].nonIndexCondition,
            conditionsList);
        RangeVariableResolver.decomposeAndConditions(
            session,
            whereConditions[0].nonIndexCondition,
            conditionsList);

        for (int i = conditionsList.size() - 1; i >= 0; i--) {
            Expression e = conditionsList.get(i);

            if (e == null || e.isTrue() || e.hasReference(ranges, exclude)) {
                conditionsList.remove(i);
            }
        }

        if (conditionsList.isEmpty()) {
            if (rangeTable.isView()) {
                ((TableDerived) rangeTable).resetToView();
            }

            return;
        }

        OrderedHashSet<TableDerived> subquerySet = null;

        for (int i = 0; i < conditionsList.size(); i++) {
            Expression e = conditionsList.get(i);

            subquerySet = e.collectAllSubqueries(subquerySet);

            if (subquerySet != null) {
                return;
            }
        }

        QueryExpression queryExpression = rangeTable.getQueryExpression();

        colExpr = ((QuerySpecification) queryExpression).exprColumns;

        for (int i = 0; i < conditionsList.size(); i++) {
            Expression e = conditionsList.get(i);

            e = e.duplicate();
            e = e.replaceColumnReferences(session, this, colExpr);

            OrderedHashSet<RangeVariable> set = e.collectRangeVariables(null);

            if (set != null) {
                for (int j = 0; j < set.size(); j++) {
                    RangeVariable range = set.get(j);

                    if (this != range
                            && range.rangeType == RangeVariable.TABLE_RANGE) {
                        queryExpression.setCorrelated();
                        break;
                    }
                }
            }

            condition = ExpressionLogical.andExpressions(condition, e);
        }

        queryExpression.addExtraConditions(condition);
    }

    private static void addConditionsToList(
            HsqlArrayList<Expression> list,
            Expression[] array) {

        if (array == null) {
            return;
        }

        for (int i = 0; i < array.length; i++) {
            if (array[i] != null) {
                if (array[i].isSingleColumnCondition
                        || array[i].isSingleColumnNull
                        || array[i].isSingleColumnNotNull) {
                    list.add(array[i]);
                }
            }
        }
    }

    /**
     * Retrieves a String representation of this obejct. <p>
     *
     * The returned String describes this object's table, alias
     * access mode, index, join mode, Start, End and And conditions.
     *
     * @return a String representation of this object
     */
    public String describe(Session session, int blanks) {

        StringBuilder sb;
        StringBuilder b = new StringBuilder(blanks);

        for (int i = 0; i < blanks; i++) {
            b.append(' ');
        }

        sb = new StringBuilder();

        String temp = "INNER";

        if (isLeftJoin) {
            temp = "LEFT OUTER";

            if (isRightJoin) {
                temp = "FULL";
            }
        } else if (isRightJoin) {
            temp = "RIGHT OUTER";
        }

        sb.append(b)
          .append("join type=")
          .append(temp)
          .append("\n")
          .append(b)
          .append("table=")
          .append(rangeTable.getName().name)
          .append("\n");

        if (tableAlias != null) {
            sb.append(b).append("alias=").append(tableAlias.name).append("\n");
        }

        RangeVariableConditions[] conditions = joinConditions;

        if (whereConditions[0].hasIndexCondition()) {
            conditions = whereConditions;
        }

        sb.append(b)
          .append("cardinality=")
          .append(rangeTable.getRowStore(session).elementCount())
          .append("\n");

        boolean fullScan = !conditions[0].hasIndexCondition();

        sb.append(b);

        if (conditions == whereConditions) {
            if (joinConditions[0].nonIndexCondition != null) {
                sb.append("join condition = [")
                  .append(
                      joinConditions[0].nonIndexCondition.describe(session,
                              blanks))
                  .append(b)
                  .append("]\n")
                  .append(b);
            }
        }

        sb.append("access=").append(fullScan
                                    ? "FULL SCAN"
                                    : "INDEX PRED").append("\n");

        for (int i = 0; i < conditions.length; i++) {
            if (i > 0) {
                sb.append(b).append("OR condition = [");
            } else {
                sb.append(b);

                if (conditions == whereConditions) {
                    sb.append("where condition = [");
                } else {
                    sb.append("join condition = [");
                }
            }

            sb.append(conditions[i].describe(session, blanks + 2))
              .append(b)
              .append("]\n");
        }

        if (conditions == joinConditions) {
            sb.append(b);

            if (whereConditions[0].nonIndexCondition != null) {
                sb.append("where condition = [")
                  .append(
                      whereConditions[0].nonIndexCondition.describe(session,
                              blanks))
                  .append(b)
                  .append("]\n")
                  .append(b);
            }
        }

        return sb.toString();
    }

    public RangeIteratorMain getIterator(Session session) {

        RangeIteratorMain it;

        if (this.isRightJoin) {
            it = new RangeIteratorRight(session, this, null);
        } else {
            it = new RangeIteratorMain(session, this);
        }

        session.sessionContext.setRangeIterator(it);

        return it;
    }

    public static RangeIterator getIterator(
            Session session,
            RangeVariable[] rangeVars) {

        if (rangeVars.length == 1) {
            return rangeVars[0].getIterator(session);
        }

        RangeIteratorMain[] iterators = new RangeIteratorMain[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            iterators[i] = rangeVars[i].getIterator(session);
        }

        return new RangeIteratorJoined(iterators);
    }

    public static class RangeIteratorBase implements RangeIterator {

        Session         session;
        int             rangePosition;
        RowIterator     it = RangeIterator.emptyRowIterator;
        PersistentStore store;
        boolean         isBeforeFirst;
        RangeVariable   rangeVar;

        private RangeIteratorBase() {}

        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        public boolean next() {

            if (isBeforeFirst) {
                isBeforeFirst = false;
            } else {
                if (it == RangeIterator.emptyRowIterator) {
                    return false;
                }
            }

            if (session.abortTransaction) {
                throw Error.error(ErrorCode.X_40000);
            }

            if (session.abortAction) {
                throw Error.error(ErrorCode.X_40502);
            }

            return it.next();
        }

        public Row getCurrentRow() {
            return it.getCurrentRow();
        }

        public Object[] getCurrent() {
            return it.getCurrent();
        }

        public Object getField(int col) {
            return it.getField(col);
        }

        public void setCurrent(Object[] data) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RangeVariable");
        }

        public long getRowId() {
            return getCurrentRow().getId();
        }

        public void removeCurrent() {}

        public void reset() {

            it.release();

            it            = RangeIterator.emptyRowIterator;
            isBeforeFirst = true;
        }

        public int getRangePosition() {
            return rangePosition;
        }

        public boolean hasNext() {
            throw Error.runtimeError(ErrorCode.U_S0500, "RangeVariable");
        }

        public void release() {
            it.release();
        }
    }

    public static class RangeIteratorMain extends RangeIteratorBase {

        boolean                   hasLeftOuterRow;
        boolean                   isFullIterator;
        RangeVariableConditions[] conditions;
        RangeVariableConditions[] whereConditions;
        RangeVariableConditions[] joinConditions;
        ExpressionPeriodOp        periodCondition;
        Expression                filterCondition;
        int                       condIndex = 0;

        //
        OrderedLongHashSet lookup;

        //
        Object[] currentJoinData = null;

        RangeIteratorMain() {
            super();
        }

        private RangeIteratorMain(Session session, RangeVariable rangeVar) {

            this.rangePosition = rangeVar.rangePosition;
            this.store         = rangeVar.rangeTable.getRowStore(session);
            this.session       = session;
            this.rangeVar      = rangeVar;
            isBeforeFirst      = true;
            whereConditions    = rangeVar.whereConditions;
            joinConditions     = rangeVar.joinConditions;
            periodCondition    = rangeVar.periodCondition;
            filterCondition    = rangeVar.filterCondition;

            if (rangeVar.isRightJoin) {
                lookup = new OrderedLongHashSet();
            }

            conditions = rangeVar.joinConditions;

            if (rangeVar.whereConditions[0].hasIndexCondition()) {
                conditions = rangeVar.whereConditions;
            }
        }

        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        public boolean next() {

            if (session.abortTransaction) {
                throw Error.error(ErrorCode.X_40000);
            }

            if (session.abortAction) {
                throw Error.error(ErrorCode.X_40502);
            }

            while (condIndex < conditions.length) {
                if (isBeforeFirst) {
                    isBeforeFirst = false;

                    initialiseIterator();
                }

                boolean result = findNext();

                if (result) {
                    return true;
                }

                it.release();

                it            = RangeIterator.emptyRowIterator;
                isBeforeFirst = true;

                condIndex++;
            }

            condIndex = 0;

            return false;
        }

        public void removeCurrent() {}

        public void reset() {

            it.release();

            it            = RangeIterator.emptyRowIterator;
            isBeforeFirst = true;
            condIndex     = 0;
        }

        public int getRangePosition() {
            return rangeVar.rangePosition;
        }

        /**
         */
        protected void initialiseIterator() {

            if (condIndex == 0) {
                hasLeftOuterRow = rangeVar.isLeftJoin;
            }

            if (conditions[condIndex].isFalse) {
                it = RowIterator.emptyRowIterator;

                return;
            }

            rangeVar.rangeTable.materialiseCorrelated(session);

            if (conditions[condIndex].indexCond == null) {
                if (conditions[condIndex].reversed) {
                    it = conditions[condIndex].rangeIndex.lastRow(
                        session,
                        store,
                        rangeVar.indexDistinctCount,
                        null);
                } else {
                    it = conditions[condIndex].rangeIndex.firstRow(
                        session,
                        store,
                        null,
                        rangeVar.indexDistinctCount,
                        null);
                }
            } else {
                getFirstRow();

                if (!conditions[condIndex].isJoin) {
                    hasLeftOuterRow = false;
                }
            }
        }

        private void getFirstRow() {

            if (currentJoinData == null
                    || currentJoinData.length
                       < conditions[condIndex].indexedColumnCount) {
                currentJoinData =
                    new Object[conditions[condIndex].indexedColumnCount];
            }

            int opType = conditions[condIndex].opType;

            for (int i = 0; i < conditions[condIndex].indexedColumnCount; i++) {
                int range    = 0;
                int tempType = conditions[condIndex].opTypes[i];

                if (tempType == OpTypes.IS_NULL
                        || tempType == OpTypes.NOT
                        || tempType == OpTypes.MAX) {
                    currentJoinData[i] = null;
                    continue;
                }

                Type valueType =
                    conditions[condIndex].indexCond[i].getRightNode()
                            .getDataType();
                Object value = conditions[condIndex].indexCond[i].getRightNode()
                        .getValue(session);
                Type targetType =
                    conditions[condIndex].indexCond[i].getLeftNode()
                            .getDataType();

                if (i == 0 && value == null) {
                    it = RowIterator.emptyRowIterator;

                    return;
                }

                if (targetType != valueType) {
                    range = targetType.compareToTypeRange(value);

                    if (range == 0) {
                        if (targetType.typeComparisonGroup
                                != valueType.typeComparisonGroup) {
                            value = targetType.convertToType(
                                session,
                                value,
                                valueType);
                        }
                    }
                }

                if (i == 0) {
                    int exprType = conditions[condIndex].indexCond[0].getType();

                    if (range < 0) {
                        switch (exprType) {

                            case OpTypes.GREATER :
                            case OpTypes.GREATER_EQUAL :
                            case OpTypes.GREATER_EQUAL_PRE :
                                opType = OpTypes.NOT;
                                value  = null;
                                break;

                            default :
                                it = RowIterator.emptyRowIterator;

                                return;
                        }
                    } else if (range > 0) {
                        switch (exprType) {

                            case OpTypes.NOT :
                                value = null;
                                break;

                            case OpTypes.SMALLER :
                            case OpTypes.SMALLER_EQUAL :
                                if (conditions[condIndex].reversed) {
                                    opType = OpTypes.MAX;
                                    value  = null;
                                    break;
                                }

                            // fall through
                            default :
                                it = RowIterator.emptyRowIterator;

                                return;
                        }
                    }
                }

                currentJoinData[i] = value;
            }

            it = conditions[condIndex].rangeIndex.findFirstRow(
                session,
                store,
                currentJoinData,
                conditions[condIndex].indexedColumnCount,
                rangeVar.indexDistinctCount,
                opType,
                conditions[condIndex].reversed,
                null);
        }

        /**
         * Advances to the next available value. <p>
         *
         * @return true if a next value is available upon exit
         */
        private boolean findNext() {

            boolean result = false;

            while (true) {
                if (session.abortTransaction) {
                    throw Error.error(ErrorCode.X_40000);
                }

                if (session.abortAction) {
                    throw Error.error(ErrorCode.X_40502);
                }

                if (it.next()) {}
                else {
                    it.release();

                    it = RangeIterator.emptyRowIterator;
                    break;
                }

                if (periodCondition != null) {
                    if (!periodCondition.testCondition(session)) {
                        continue;
                    }
                }

                if (filterCondition != null) {
                    if (!filterCondition.testCondition(session)) {
                        continue;
                    }
                }

                if (conditions[condIndex].terminalCondition != null) {
                    if (!conditions[condIndex].terminalCondition.testCondition(
                            session)) {
                        break;
                    }
                }

                if (conditions[condIndex].indexEndCondition != null) {
                    if (!conditions[condIndex].indexEndCondition.testCondition(
                            session)) {
                        if (!conditions[condIndex].isJoin) {
                            hasLeftOuterRow = false;
                        }

                        break;
                    }
                }

                if (joinConditions[condIndex].nonIndexCondition != null) {
                    if (!joinConditions[condIndex].nonIndexCondition.testCondition(
                            session)) {
                        continue;
                    }
                }

                if (whereConditions[condIndex].nonIndexCondition != null) {
                    if (!whereConditions[condIndex].nonIndexCondition.testCondition(
                            session)) {
                        hasLeftOuterRow = false;

                        addFoundRow();
                        continue;
                    }
                }

                Expression e = conditions[condIndex].excludeConditions;

                if (e != null && e.testCondition(session)) {
                    continue;
                }

                addFoundRow();

                hasLeftOuterRow = false;

                return true;
            }

            it.release();

            it = RangeIterator.emptyRowIterator;

            if (hasLeftOuterRow && condIndex == conditions.length - 1) {
                result = (whereConditions[condIndex].nonIndexCondition == null
                          || whereConditions[condIndex].nonIndexCondition.testCondition(
                              session));
                hasLeftOuterRow = false;
            }

            return result;
        }

        private void addFoundRow() {

            if (rangeVar.isRightJoin) {
                long position = it.getRowId();

                lookup.add(position);
            }
        }
    }

    public static class RangeIteratorRight extends RangeIteratorMain {

        private RangeIteratorRight(
                Session session,
                RangeVariable rangeVar,
                RangeIteratorMain main) {
            super(session, rangeVar);

            isFullIterator = true;
        }

        boolean isOnRightOuterRows;

        public void setOnOuterRows() {

            conditions         = rangeVar.whereConditions;
            isOnRightOuterRows = true;
            hasLeftOuterRow    = false;
            condIndex          = 0;

            initialiseIterator();
        }

        public boolean next() {

            if (isOnRightOuterRows) {
                if (it == RangeIterator.emptyRowIterator) {
                    return false;
                }

                return findNextRight();
            } else {
                return super.next();
            }
        }

        private boolean findNextRight() {

            boolean result = false;

            while (true) {
                if (it.next()) {}
                else {
                    it = RangeIterator.emptyRowIterator;
                    break;
                }

                if (conditions[condIndex].indexEndCondition != null
                        && !conditions[condIndex].indexEndCondition.testCondition(
                            session)) {
                    break;
                }

                if (conditions[condIndex].nonIndexCondition != null
                        && !conditions[condIndex].nonIndexCondition.testCondition(
                            session)) {
                    continue;
                }

                if (!lookupAndTest()) {
                    continue;
                }

                result = true;
                break;
            }

            if (result) {
                return true;
            }

            it.release();

            return result;
        }

        private boolean lookupAndTest() {

            long    position = it.getRowId();
            boolean result   = !lookup.contains(position);

            if (result) {
                if (conditions[condIndex].nonIndexCondition != null) {
                    result =
                        conditions[condIndex].nonIndexCondition.testCondition(
                            session);
                }
            }

            return result;
        }
    }

    public static class RangeIteratorJoined extends RangeIteratorBase {

        RangeIteratorMain[] rangeIterators;
        int                 currentIndex = 0;
        RangeIterator       currentRange = null;

        public RangeIteratorJoined(RangeIteratorMain[] rangeIterators) {
            this.rangeIterators = rangeIterators;
            isBeforeFirst       = true;
        }

        public Row getCurrentRow() {
            return currentRange.getCurrentRow();
        }

        public Object[] getCurrent() {
            return currentRange.getCurrent();
        }

        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        public boolean next() {

            while (currentIndex >= 0) {
                currentRange = rangeIterators[currentIndex];

                if (currentRange.next()) {
                    if (currentIndex < rangeIterators.length - 1) {
                        currentIndex++;
                        continue;
                    }

                    return true;
                } else {
                    currentRange.reset();

                    currentIndex--;
                }
            }

            currentRange = null;

            for (int i = 0; i < rangeIterators.length; i++) {
                rangeIterators[i].reset();
            }

            return false;
        }

        public void removeCurrent() {}

        public void release() {

            it.release();

            for (int i = 0; i < rangeIterators.length; i++) {
                rangeIterators[i].reset();
            }
        }

        public void reset() {

            super.reset();

            for (int i = 0; i < rangeIterators.length; i++) {
                rangeIterators[i].reset();
            }
        }

        public int getRangePosition() {
            return 0;
        }
    }

    static final class RangeIteratorCheck implements RangeIterator {

        final int rangePosition;
        Object[]  currentData;

        RangeIteratorCheck() {
            this.rangePosition = 0;
        }

        RangeIteratorCheck(int rangePosition) {
            this.rangePosition = rangePosition;
        }

        public Object getField(int col) {
            return currentData[col];
        }

        public boolean next() {
            return false;
        }

        public Row getCurrentRow() {
            return null;
        }

        public Object[] getCurrent() {
            return currentData;
        }

        public boolean hasNext() {
            return false;
        }

        public long getPos() {
            return -1;
        }

        public int getStorageSize() {
            return 0;
        }

        public void release() {}

        public void removeCurrent() {}

        public long getRowId() {
            return 0L;
        }

        public boolean isBeforeFirst() {
            return false;
        }

        public void setCurrent(Object[] data) {
            this.currentData = data;
        }

        public void reset() {}

        public int getRangePosition() {
            return rangePosition;
        }
    }

    public static class RangeVariableConditions {

        final RangeVariable rangeVar;
        Expression[]        indexCond;
        Expression[]        indexEndCond;
        int[]               opTypes;
        int[]               opTypesEnd;
        Expression          indexEndCondition;
        int                 indexedColumnCount;
        Index               rangeIndex;
        final boolean       isJoin;
        Expression          excludeConditions;
        Expression          nonIndexCondition;
        Expression          terminalCondition;
        int                 opType;
        int                 opTypeEnd;
        boolean             isFalse;
        boolean             reversed;
        boolean             hasIndex;

        RangeVariableConditions(RangeVariable rangeVar, boolean isJoin) {
            this.rangeVar = rangeVar;
            this.isJoin   = isJoin;
        }

        RangeVariableConditions(RangeVariableConditions base) {
            this.rangeVar     = base.rangeVar;
            this.isJoin       = base.isJoin;
            nonIndexCondition = base.nonIndexCondition;
        }

        boolean hasIndexCondition() {
            return indexedColumnCount > 0;
        }

        boolean hasIndex() {
            return hasIndex;
        }

        void addCondition(Expression e) {

            if (e == null) {
                return;
            }

            if (e instanceof ExpressionLogical) {
                if (((ExpressionLogical) e).isTerminal) {
                    terminalCondition = e;
                }
            }

            addToNonIndexCondition(e);

            isFalse = Expression.EXPR_FALSE.equals(nonIndexCondition);

            if (rangeIndex == null || rangeIndex.getColumnCount() == 0) {
                return;
            }

            if (indexedColumnCount == 0) {
                return;
            }

            if (e.getIndexableExpression(rangeVar) == null) {
                return;
            }

            int   colIndex  = e.getLeftNode().getColumnIndex();
            int[] indexCols = rangeIndex.getColumns();

            switch (e.getType()) {

                case OpTypes.GREATER :
                case OpTypes.GREATER_EQUAL :
                case OpTypes.GREATER_EQUAL_PRE : {

                    // replaces existing condition
                    if (opType == OpTypes.NOT) {
                        if (indexCols[indexedColumnCount - 1] == colIndex) {
                            addToNonIndexCondition(
                                indexCond[indexedColumnCount - 1]);

                            indexCond[indexedColumnCount - 1] = e;
                            opType                            = e.opType;
                            opTypes[indexedColumnCount - 1]   = e.opType;

                            if (e.getType() == OpTypes.GREATER_EQUAL_PRE
                                    && indexedColumnCount == 1) {
                                indexEndCond[indexedColumnCount - 1] =
                                    ExpressionLogical.andExpressions(
                                        indexEndCond[indexedColumnCount - 1],
                                        e.nodes[2]);
                            }
                        }
                    } else {
                        addToIndexConditions(e);
                    }

                    break;
                }

                case OpTypes.SMALLER :
                case OpTypes.SMALLER_EQUAL : {
                    if (opType == OpTypes.GREATER
                            || opType == OpTypes.GREATER_EQUAL
                            || opType == OpTypes.GREATER_EQUAL_PRE
                            || opType == OpTypes.NOT) {
                        if (opTypeEnd != OpTypes.MAX) {
                            break;
                        }

                        if (indexCols[indexedColumnCount - 1] == colIndex) {
                            indexEndCond[indexedColumnCount - 1] = e;

                            addToIndexEndCondition(e);

                            opTypeEnd                          = e.opType;
                            opTypesEnd[indexedColumnCount - 1] = e.opType;
                        }
                    } else {
                        addToIndexEndConditions(e);
                    }

                    break;
                }

                default :
            }
        }

        private boolean addToIndexConditions(Expression e) {

            if (opType == OpTypes.EQUAL || opType == OpTypes.IS_NULL) {
                if (indexedColumnCount < rangeIndex.getColumnCount()) {
                    if (rangeIndex.getColumns()[indexedColumnCount]
                            == e.getLeftNode().getColumnIndex()) {
                        indexCond[indexedColumnCount]  = e;
                        opType                         = e.opType;
                        opTypes[indexedColumnCount]    = e.opType;
                        opTypeEnd                      = OpTypes.MAX;
                        opTypesEnd[indexedColumnCount] = OpTypes.MAX;

                        indexedColumnCount++;

                        return true;
                    }
                }
            }

            return false;
        }

        private boolean addToIndexEndConditions(Expression e) {

            if (opType == OpTypes.EQUAL || opType == OpTypes.IS_NULL) {
                if (indexedColumnCount < rangeIndex.getColumnCount()) {
                    if (rangeIndex.getColumns()[indexedColumnCount]
                            == e.getLeftNode().getColumnIndex()) {
                        Expression condition =
                            ExpressionLogical.newNotNullCondition(
                                e.getLeftNode());

                        indexCond[indexedColumnCount]    = condition;
                        indexEndCond[indexedColumnCount] = e;

                        addToIndexEndCondition(e);

                        opType                         = OpTypes.NOT;
                        opTypes[indexedColumnCount]    = OpTypes.NOT;
                        opTypeEnd                      = e.opType;
                        opTypesEnd[indexedColumnCount] = e.opType;

                        indexedColumnCount++;

                        return true;
                    }
                }
            }

            return false;
        }

        /**
         *
         * @param exprList has the same length as index column count
         * @param index Index to use
         * @param colCount number of columns searched
         */
        void addIndexCondition(
                Expression[] exprList,
                Index index,
                int colCount) {

            int indexColCount = index.getColumnCount();

            rangeIndex   = index;
            indexCond    = new Expression[indexColCount];
            indexEndCond = new Expression[indexColCount];
            opTypes      = new int[indexColCount];
            opTypesEnd   = new int[indexColCount];
            opType       = exprList[0].opType;
            opTypes[0]   = exprList[0].opType;

            switch (opType) {

                case OpTypes.NOT :
                    indexCond     = exprList;
                    opTypeEnd     = OpTypes.MAX;
                    opTypesEnd[0] = OpTypes.MAX;
                    break;

                case OpTypes.GREATER :
                case OpTypes.GREATER_EQUAL :
                case OpTypes.GREATER_EQUAL_PRE :
                    indexCond = exprList;

                    if (exprList[0].getType() == OpTypes.GREATER_EQUAL_PRE) {
                        indexEndCond[0] = indexEndCondition =
                            exprList[0].nodes[2];
                    }

                    opTypeEnd     = OpTypes.MAX;
                    opTypesEnd[0] = OpTypes.MAX;
                    break;

                case OpTypes.SMALLER :
                case OpTypes.SMALLER_EQUAL : {
                    Expression e = exprList[0].getLeftNode();

                    e               = new ExpressionLogical(OpTypes.IS_NULL, e);
                    e               = new ExpressionLogical(OpTypes.NOT, e);
                    indexCond[0]    = e;
                    indexEndCond[0] = indexEndCondition = exprList[0];
                    opTypeEnd       = opType;
                    opTypesEnd[0]   = opType;
                    opType          = OpTypes.NOT;
                    opTypes[0]      = OpTypes.NOT;
                    break;
                }

                case OpTypes.IS_NULL :
                case OpTypes.EQUAL : {
                    indexCond = exprList;

                    for (int i = 0; i < colCount; i++) {
                        Expression e = exprList[i];

                        indexEndCond[i] = e;

                        addToIndexEndCondition(e);

                        opType        = e.opType;
                        opTypes[i]    = e.opType;
                        opTypesEnd[i] = e.opType;
                    }

                    opTypeEnd = opType;
                    break;
                }

                default :
                    throw Error.runtimeError(
                        ErrorCode.U_S0500,
                        "RangeVariable");
            }

            indexedColumnCount = colCount;
            hasIndex           = true;
        }

        boolean addToIndexEndCondition(Expression e) {

            indexEndCondition = ExpressionLogical.andExpressions(
                indexEndCondition,
                e);

            return true;
        }

        boolean addToNonIndexCondition(Expression e) {

            nonIndexCondition = ExpressionLogical.andExpressions(
                nonIndexCondition,
                e);

            return true;
        }

        private boolean reverseIndexCondition() {

            if (indexedColumnCount == 0) {
                reversed = true;

                return true;
            }

            if (opType == OpTypes.EQUAL || opType == OpTypes.IS_NULL) {

                //
            } else {
                indexEndCondition = null;

                Expression[] temp = indexCond;

                indexCond    = indexEndCond;
                indexEndCond = temp;

                int[] temptypes = opTypes;

                opTypes    = opTypesEnd;
                opTypesEnd = temptypes;

                for (int i = 0; i < indexedColumnCount; i++) {
                    Expression e = indexEndCond[i];

                    addToIndexEndCondition(e);
                }

                if (indexedColumnCount > 1
                        && opTypes[indexedColumnCount - 1] == OpTypes.MAX) {
                    indexedColumnCount--;

                    opTypes[indexedColumnCount]    = 0;
                    opTypesEnd[indexedColumnCount] = 0;
                }

                opType    = opTypes[indexedColumnCount - 1];
                opTypeEnd = opTypesEnd[indexedColumnCount - 1];
            }

            reversed = true;

            return true;
        }

        String describe(Session session, int blanks) {

            StringBuilder sb = new StringBuilder();
            StringBuilder b  = new StringBuilder(blanks);

            for (int i = 0; i < blanks; i++) {
                b.append(' ');
            }

            sb.append("index=").append(rangeIndex.getName().name).append("\n");

            if (hasIndexCondition()) {
                if (indexedColumnCount > 0) {
                    sb.append(b).append("start conditions=[");

                    for (int j = 0; j < indexedColumnCount; j++) {
                        if (indexCond != null && indexCond[j] != null) {
                            sb.append(indexCond[j].describe(session, blanks));
                        }
                    }

                    sb.append("]\n");
                }

                if (this.opTypeEnd != OpTypes.EQUAL
                        && indexEndCondition != null) {
                    String temp = indexEndCondition.describe(session, blanks);

                    sb.append(b)
                      .append("end condition=[")
                      .append(temp)
                      .append("]\n");
                }
            }

            if (nonIndexCondition != null) {
                String temp = nonIndexCondition.describe(session, blanks);

                sb.append(b)
                  .append("other condition=[")
                  .append(temp)
                  .append("]\n");
            }

            return sb.toString();
        }

        private void replaceColumnReferences(
                Session session,
                RangeVariable range,
                Expression[] list) {

            if (indexCond != null) {
                for (int i = 0; i < indexCond.length; i++) {
                    if (indexCond[i] != null) {
                        indexCond[i] = indexCond[i].replaceColumnReferences(
                            session,
                            range,
                            list);
                    }
                }
            }

            if (indexEndCond != null) {
                for (int i = 0; i < indexEndCond.length; i++) {
                    if (indexEndCond[i] != null) {
                        indexEndCond[i] =
                            indexEndCond[i].replaceColumnReferences(
                                session,
                                range,
                                list);
                    }
                }
            }

            if (indexEndCondition != null) {
                indexEndCondition = indexEndCondition.replaceColumnReferences(
                    session,
                    range,
                    list);
            }

            if (excludeConditions != null) {
                excludeConditions = excludeConditions.replaceColumnReferences(
                    session,
                    range,
                    list);
            }

            if (nonIndexCondition != null) {
                nonIndexCondition = nonIndexCondition.replaceColumnReferences(
                    session,
                    range,
                    list);
            }

            if (terminalCondition != null) {
                terminalCondition = terminalCondition.replaceColumnReferences(
                    session,
                    range,
                    list);
            }
        }

        private void replaceExpressions(
                OrderedHashSet<Expression> expressions,
                int resultRangePosition) {

            if (indexCond != null) {
                for (int i = 0; i < indexCond.length; i++) {
                    if (indexCond[i] != null) {
                        indexCond[i] = indexCond[i].replaceExpressions(
                            expressions,
                            resultRangePosition);
                    }
                }
            }

            if (indexEndCond != null) {
                for (int i = 0; i < indexEndCond.length; i++) {
                    if (indexEndCond[i] != null) {
                        indexEndCond[i] = indexEndCond[i].replaceExpressions(
                            expressions,
                            resultRangePosition);
                    }
                }
            }

            if (indexEndCondition != null) {
                indexEndCondition = indexEndCondition.replaceExpressions(
                    expressions,
                    resultRangePosition);
            }

            if (excludeConditions != null) {
                excludeConditions = excludeConditions.replaceExpressions(
                    expressions,
                    resultRangePosition);
            }

            if (nonIndexCondition != null) {
                nonIndexCondition = nonIndexCondition.replaceExpressions(
                    expressions,
                    resultRangePosition);
            }

            if (terminalCondition != null) {
                terminalCondition = terminalCondition.replaceExpressions(
                    expressions,
                    resultRangePosition);
            }
        }
    }
}
