/* Copyright (c) 2001-2011, The HSQL Development Group
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
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.navigator.RowSetNavigatorDataTable;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Implementation of an SQL query expression
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */

/**
 * @todo 1.9.0 - review these
 * - work out usage of getMainSelect etc and add relevant methods
 * - Result metadata for the final result of QueryExpression
 *
 */
public class QueryExpression {

    public static final int NOUNION       = 0,
                            UNION         = 1,
                            UNION_ALL     = 2,
                            INTERSECT     = 3,
                            INTERSECT_ALL = 4,
                            EXCEPT_ALL    = 5,
                            EXCEPT        = 6,
                            UNION_TERM    = 7;

    //
    int                     columnCount;
    private QueryExpression leftQueryExpression;
    private QueryExpression rightQueryExpression;
    SortAndSlice            sortAndSlice;
    private int             unionType;
    private boolean         unionCorresponding;
    private OrderedHashSet  unionCorrespondingColumns;
    int[]                   unionColumnMap;
    Type[]                  unionColumnTypes;
    boolean                 isFullOrder;

    //
    HsqlList unresolvedExpressions;

    //
    boolean isResolved;

    //
    int persistenceScope = TableBase.SCOPE_STATEMENT;

    //
    ResultMetaData resultMetaData;
    boolean[]      accessibleColumns;

    //
    View    view;
    boolean isMergeable;
    boolean isUpdatable;
    boolean isInsertable;
    boolean isCheckable;
    boolean isTopLevel;
    boolean acceptsSequences;

    //
    public TableBase resultTable;
    public Index     mainIndex;
    public Index     fullIndex;
    public Index     orderIndex;
    public Index     idIndex;

    //
    CompileContext compileContext;

    QueryExpression(CompileContext compileContext) {
        this.compileContext = compileContext;
        sortAndSlice        = SortAndSlice.noSort;
    }

    public QueryExpression(CompileContext compileContext,
                           QueryExpression leftQueryExpression) {

        this(compileContext);

        sortAndSlice             = SortAndSlice.noSort;
        this.leftQueryExpression = leftQueryExpression;
    }

    void addUnion(QueryExpression queryExpression, int unionType) {

        sortAndSlice              = SortAndSlice.noSort;
        this.rightQueryExpression = queryExpression;
        this.unionType            = unionType;

        setFullOrder();
    }

    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice      = sortAndSlice;
        sortAndSlice.sortUnion = true;
    }

    public void setUnionCorresoponding() {
        unionCorresponding = true;
    }

    public void setUnionCorrespondingColumns(OrderedHashSet names) {
        unionCorrespondingColumns = names;
    }

    public void setFullOrder() {

        isFullOrder = true;

        if (leftQueryExpression == null) {
            if (isResolved) {
                ((QuerySpecification) this).createFullIndex(null);
            }

            return;
        }

        leftQueryExpression.setFullOrder();
        rightQueryExpression.setFullOrder();
    }

    public void resolve(Session session) {

        resolveReferences(session, RangeVariable.emptyArray);
        ExpressionColumn.checkColumnsResolved(unresolvedExpressions);
        resolveTypes(session);
    }

    public void resolve(Session session, RangeVariable[] outerRanges,
                        Type[] targetTypes) {

        resolveReferences(session, outerRanges);

        if (unresolvedExpressions != null) {
            for (int i = 0; i < unresolvedExpressions.size(); i++) {
                Expression e = (Expression) unresolvedExpressions.get(i);
                HsqlList list = e.resolveColumnReferences(session,
                    outerRanges, null);

                ExpressionColumn.checkColumnsResolved(list);
            }
        }

        resolveTypesPartOne(session);

        if (targetTypes != null) {
            for (int i = 0;
                    i < unionColumnTypes.length && i < targetTypes.length;
                    i++) {
                if (unionColumnTypes[i] == null) {
                    unionColumnTypes[i] = targetTypes[i];
                }
            }
        }

        resolveTypesPartTwo(session);
    }

    public void resolveReferences(Session session,
                                  RangeVariable[] outerRanges) {

        leftQueryExpression.resolveReferences(session, outerRanges);
        rightQueryExpression.resolveReferences(session, outerRanges);
        addUnresolvedExpressions(leftQueryExpression.unresolvedExpressions);
        addUnresolvedExpressions(rightQueryExpression.unresolvedExpressions);

        if (!unionCorresponding) {
            columnCount = leftQueryExpression.getColumnCount();

            int rightCount = rightQueryExpression.getColumnCount();

            if (columnCount != rightCount) {
                throw Error.error(ErrorCode.X_42594);
            }

            unionColumnTypes = new Type[columnCount];
            leftQueryExpression.unionColumnMap =
                rightQueryExpression.unionColumnMap = new int[columnCount];

            ArrayUtil.fillSequence(leftQueryExpression.unionColumnMap);
            resolveColumnRefernecesInUnionOrderBy();

            accessibleColumns = leftQueryExpression.accessibleColumns;

            return;
        }

        String[] leftNames  = leftQueryExpression.getColumnNames();
        String[] rightNames = rightQueryExpression.getColumnNames();

        if (unionCorrespondingColumns == null) {
            unionCorrespondingColumns = new OrderedHashSet();

            OrderedIntHashSet leftColumns  = new OrderedIntHashSet();
            OrderedIntHashSet rightColumns = new OrderedIntHashSet();

            for (int i = 0; i < leftNames.length; i++) {
                String name  = leftNames[i];
                int    index = ArrayUtil.find(rightNames, name);

                if (name.length() > 0 && index != -1) {
                    if (!leftQueryExpression.accessibleColumns[i]) {
                        throw Error.error(ErrorCode.X_42578);
                    }

                    if (!rightQueryExpression.accessibleColumns[index]) {
                        throw Error.error(ErrorCode.X_42578);
                    }

                    leftColumns.add(i);
                    rightColumns.add(index);
                    unionCorrespondingColumns.add(name);
                }
            }

            if (unionCorrespondingColumns.isEmpty()) {
                throw Error.error(ErrorCode.X_42578);
            }

            leftQueryExpression.unionColumnMap  = leftColumns.toArray();
            rightQueryExpression.unionColumnMap = rightColumns.toArray();
        } else {
            leftQueryExpression.unionColumnMap =
                new int[unionCorrespondingColumns.size()];
            rightQueryExpression.unionColumnMap =
                new int[unionCorrespondingColumns.size()];

            for (int i = 0; i < unionCorrespondingColumns.size(); i++) {
                String name  = (String) unionCorrespondingColumns.get(i);
                int    index = ArrayUtil.find(leftNames, name);

                if (index == -1) {
                    throw Error.error(ErrorCode.X_42501);
                }

                if (!leftQueryExpression.accessibleColumns[index]) {
                    throw Error.error(ErrorCode.X_42578);
                }

                leftQueryExpression.unionColumnMap[i] = index;
                index = ArrayUtil.find(rightNames, name);

                if (index == -1) {
                    throw Error.error(ErrorCode.X_42501);
                }

                if (!rightQueryExpression.accessibleColumns[index]) {
                    throw Error.error(ErrorCode.X_42578);
                }

                rightQueryExpression.unionColumnMap[i] = index;
            }
        }

        columnCount      = unionCorrespondingColumns.size();
        unionColumnTypes = new Type[columnCount];

        resolveColumnRefernecesInUnionOrderBy();

        accessibleColumns = new boolean[columnCount];

        ArrayUtil.fillArray(accessibleColumns, true);
    }

    /**
     * Only simple column reference or column position allowed
     */
    void resolveColumnRefernecesInUnionOrderBy() {

        int orderCount = sortAndSlice.getOrderLength();

        if (orderCount == 0) {
            return;
        }

        String[] unionColumnNames = getColumnNames();

        for (int i = 0; i < orderCount; i++) {
            Expression sort = (Expression) sortAndSlice.exprList.get(i);
            Expression e    = sort.getLeftNode();

            if (e.getType() == OpTypes.VALUE) {
                if (e.getDataType().typeCode == Types.SQL_INTEGER) {
                    int index = ((Integer) e.getValue(null)).intValue();

                    if (0 < index && index <= unionColumnNames.length) {
                        sort.getLeftNode().queryTableColumnIndex = index - 1;

                        continue;
                    }
                }
            } else if (e.getType() == OpTypes.COLUMN) {
                int index = ArrayUtil.find(unionColumnNames,
                                           e.getColumnName());

                if (index >= 0) {
                    sort.getLeftNode().queryTableColumnIndex = index;

                    continue;
                }
            }

            throw Error.error(ErrorCode.X_42576);
        }

        sortAndSlice.prepare(null);
    }

    private void addUnresolvedExpressions(HsqlList expressions) {

        if (expressions == null) {
            return;
        }

        if (unresolvedExpressions == null) {
            unresolvedExpressions = new ArrayListIdentity();
        }

        unresolvedExpressions.addAll(expressions);
    }

    public void resolveTypes(Session session) {

        if (isResolved) {
            return;
        }

        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);

        isResolved = true;
    }

    void resolveTypesPartOne(Session session) {

        ArrayUtil.projectRowReverse(leftQueryExpression.unionColumnTypes,
                                    leftQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        leftQueryExpression.resolveTypesPartOne(session);
        ArrayUtil.projectRow(leftQueryExpression.unionColumnTypes,
                             leftQueryExpression.unionColumnMap,
                             unionColumnTypes);
        ArrayUtil.projectRowReverse(rightQueryExpression.unionColumnTypes,
                                    rightQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        rightQueryExpression.resolveTypesPartOne(session);
        ArrayUtil.projectRow(rightQueryExpression.unionColumnTypes,
                             rightQueryExpression.unionColumnMap,
                             unionColumnTypes);
    }

    void resolveTypesPartTwo(Session session) {

        ArrayUtil.projectRowReverse(leftQueryExpression.unionColumnTypes,
                                    leftQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        leftQueryExpression.resolveTypesPartTwo(session);
        ArrayUtil.projectRowReverse(rightQueryExpression.unionColumnTypes,
                                    rightQueryExpression.unionColumnMap,
                                    unionColumnTypes);
        rightQueryExpression.resolveTypesPartTwo(session);

        //
        ResultMetaData leftMeta  = leftQueryExpression.getMetaData();
        ResultMetaData rightMeta = rightQueryExpression.getMetaData();

        for (int i = 0; i < leftQueryExpression.unionColumnMap.length; i++) {
            int        leftIndex  = leftQueryExpression.unionColumnMap[i];
            int        rightIndex = rightQueryExpression.unionColumnMap[i];
            ColumnBase column     = leftMeta.columns[leftIndex];
            byte leftNullability =
                leftMeta.columns[leftIndex].getNullability();
            byte rightNullability =
                rightMeta.columns[rightIndex].getNullability();

            if (column instanceof ColumnSchema
                    && rightMeta.columns[rightIndex] instanceof ColumnBase) {
                column = new ColumnBase();

                column.setType(leftQueryExpression.unionColumnTypes[i]);
                column.setNullability(leftMeta.columns[leftIndex].getNullability());

                leftMeta.columns[leftIndex] = column;
            }

            if (rightNullability == SchemaObject.Nullability
                    .NULLABLE || (rightNullability == SchemaObject.Nullability
                        .NULLABLE_UNKNOWN && leftNullability == SchemaObject
                        .Nullability.NO_NULLS)) {
                if (column instanceof ColumnSchema) {
                    column = new ColumnBase();

                    column.setType(leftQueryExpression.unionColumnTypes[i]);

                    leftMeta.columns[leftIndex] = column;
                }

                column.setNullability(rightNullability);
            }
        }

        if (unionCorresponding) {
            resultMetaData = leftQueryExpression.getMetaData().getNewMetaData(
                leftQueryExpression.unionColumnMap);

            createTable(session);
        }

        if (sortAndSlice.hasOrder()) {
            QueryExpression queryExpression = this;

            while (true) {
                if (queryExpression.leftQueryExpression == null
                        || queryExpression.unionCorresponding) {
                    sortAndSlice.setIndex(session,
                                          queryExpression.resultTable);

                    break;
                }

                queryExpression = queryExpression.leftQueryExpression;
            }
        }
/*
        // disallow lobs
        ResultMetaData meta = getMetaData();

        for (int i = 0, count = meta.getColumnCount(); i < count; i++) {
            Type dataType = meta.columnTypes[i];

            if (dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
*/
    }

    public Object[] getValues(Session session) {

        Result r    = getResult(session, 2);
        int    size = r.getNavigator().getSize();

        if (size == 0) {
            return new Object[r.metaData.getColumnCount()];
        } else if (size == 1) {
            return r.getSingleRowData();
        } else {
            throw Error.error(ErrorCode.X_21000);
        }
    }

    public Object[] getSingleRowValues(Session session) {

        Result r    = getResult(session, 2);
        int    size = r.getNavigator().getSize();

        if (size == 0) {
            return null;
        } else if (size == 1) {
            return r.getSingleRowData();
        } else {
            throw Error.error(ErrorCode.X_21000);
        }
    }

    public Object getValue(Session session) {

        Object[] values = getValues(session);

        return values[0];
    }

    Result getResult(Session session, int maxRows) {

        int    currentMaxRows = unionType == UNION_ALL ? maxRows
                                                       : 0;
        Result first = leftQueryExpression.getResult(session, currentMaxRows);
        RowSetNavigatorData navigator =
            (RowSetNavigatorData) first.getNavigator();
        Result second = rightQueryExpression.getResult(session,
            currentMaxRows);
        RowSetNavigatorData rightNavigator =
            (RowSetNavigatorData) second.getNavigator();

        if (unionCorresponding) {
            RowSetNavigatorData rowSet;
            boolean memory =
                session.resultMaxMemoryRows == 0
                || (navigator.getSize() < session.resultMaxMemoryRows
                    && rightNavigator.getSize() < session.resultMaxMemoryRows);

            if (memory) {
                rowSet = new RowSetNavigatorData(session, this);
            } else {
                rowSet = new RowSetNavigatorDataTable(session, this);
            }

            rowSet.copy(navigator, leftQueryExpression.unionColumnMap);
            navigator.close();

            navigator = rowSet;

            first.setNavigator(navigator);

            first.metaData = this.getMetaData();

            if (memory) {
                rowSet = new RowSetNavigatorData(session, this);
            } else {
                rowSet = new RowSetNavigatorDataTable(session, this);
            }

            rowSet.copy(rightNavigator, rightQueryExpression.unionColumnMap);
            navigator.close();

            rightNavigator = rowSet;
        }

        switch (unionType) {

            case UNION :
                navigator.union(session, rightNavigator);
                break;

            case UNION_ALL :
                navigator.unionAll(session, rightNavigator);
                break;

            case INTERSECT :
                navigator.intersect(session, rightNavigator);
                break;

            case INTERSECT_ALL :
                navigator.intersectAll(session, rightNavigator);
                break;

            case EXCEPT :
                navigator.except(session, rightNavigator);
                break;

            case EXCEPT_ALL :
                navigator.exceptAll(session, rightNavigator);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "QueryExpression");
        }

        if (sortAndSlice.hasOrder()) {
            navigator.sortOrderUnion(session, sortAndSlice);
        }

        if (sortAndSlice.hasLimit()) {
            navigator.trim(sortAndSlice.getLimitStart(session),
                           sortAndSlice.getLimitCount(session, maxRows));
        }

        navigator.reset();

        return first;
    }

    Result getResultRecursive(Session session, TableDerived table) {

        Result              tempResult;
        RowSetNavigatorData tempNavigator;
        RowSetNavigatorData rowSet = new RowSetNavigatorData(session, this);
        Result              result = Result.newResult(rowSet);

        rowSet.copy(table.getSubQuery().getNavigator(session), unionColumnMap);

        result.metaData = resultMetaData;

        for (int round = 0; ; round++) {
            tempResult    = rightQueryExpression.getResult(session, 0);
            tempNavigator = (RowSetNavigatorData) tempResult.getNavigator();

            if (tempNavigator.isEmpty()) {
                break;
            }

            switch (unionType) {

                case UNION :
                    rowSet.union(session, tempNavigator);
                    break;

                case UNION_ALL :
                    rowSet.unionAll(session, tempNavigator);
                    break;

                default :
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "QueryExpression");
            }

            table.clearAllData(session);
            tempNavigator.reset();
            table.insertIntoTable(session, tempResult);

            if (round > 256) {
                throw Error.error(ErrorCode.GENERAL_ERROR);
            }
        }

        table.clearAllData(session);
        rowSet.reset();

        return result;
    }

    public OrderedHashSet getSubqueries() {

        OrderedHashSet subqueries = leftQueryExpression.getSubqueries();

        return OrderedHashSet.addAll(subqueries,
                                     rightQueryExpression.getSubqueries());
    }

    public boolean isSingleColumn() {
        return leftQueryExpression.isSingleColumn();
    }

    public ResultMetaData getMetaData() {

        if (resultMetaData != null) {
            return resultMetaData;
        }

        return leftQueryExpression.getMetaData();
    }

    public QuerySpecification getMainSelect() {

        if (leftQueryExpression == null) {
            return (QuerySpecification) this;
        }

        return leftQueryExpression.getMainSelect();
    }

    /** @todo 1.9.0 review */
    public String describe(Session session, int blanks) {

        StringBuffer sb;
        String       temp;
        String       b = ValuePool.spaceString.substring(0, blanks);

        sb = new StringBuffer();

        switch (unionType) {

            case UNION :
                temp = Tokens.T_UNION;
                break;

            case UNION_ALL :
                temp = Tokens.T_UNION + ' ' + Tokens.T_ALL;
                break;

            case INTERSECT :
                temp = Tokens.T_INTERSECT;
                break;

            case INTERSECT_ALL :
                temp = Tokens.T_INTERSECT + ' ' + Tokens.T_ALL;
                break;

            case EXCEPT :
                temp = Tokens.T_EXCEPT;
                break;

            case EXCEPT_ALL :
                temp = Tokens.T_EXCEPT + ' ' + Tokens.T_ALL;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "QueryExpression");
        }

        sb.append(b).append(temp).append("\n");
        sb.append(b).append("Left Query=[\n");
        sb.append(b).append(leftQueryExpression.describe(session, blanks + 2));
        sb.append(b).append("]\n");
        sb.append(b).append("Right Query=[\n");
        sb.append(b).append(rightQueryExpression.describe(session,
                blanks + 2));
        sb.append(b).append("]\n");

        return sb.toString();
    }

    public HsqlList getUnresolvedExpressions() {
        return unresolvedExpressions;
    }

    public boolean areColumnsResolved() {
        return unresolvedExpressions == null
               || unresolvedExpressions.isEmpty();
    }

    String[] getColumnNames() {

        if (unionCorrespondingColumns == null) {
            return leftQueryExpression.getColumnNames();
        }

        String[] names = new String[unionCorrespondingColumns.size()];

        unionCorrespondingColumns.toArray(names);

        return names;
    }

    public Type[] getColumnTypes() {
        return unionColumnTypes;
    }

    public int getColumnCount() {

        if (unionCorrespondingColumns == null) {
            int left  = leftQueryExpression.getColumnCount();
            int right = rightQueryExpression.getColumnCount();

            if (left != right) {
                throw Error.error(ErrorCode.X_42594);
            }

            return left;
        }

        return unionCorrespondingColumns.size();
    }

    public OrderedHashSet collectAllExpressions(OrderedHashSet set,
            OrderedIntHashSet typeSet, OrderedIntHashSet stopAtTypeSet) {

        set = leftQueryExpression.collectAllExpressions(set, typeSet,
                stopAtTypeSet);

        if (rightQueryExpression != null) {
            set = rightQueryExpression.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }

        return set;
    }

    public void collectObjectNames(Set set) {

        leftQueryExpression.collectObjectNames(set);

        if (rightQueryExpression != null) {
            rightQueryExpression.collectObjectNames(set);
        }
    }

    public HashMappedList getColumns() {

        this.getResultTable();

        return ((TableDerived) getResultTable()).columnList;
    }

    /**
     * Used prior to type resolution
     */
    public void setView(View view) {

        this.isUpdatable      = true;
        this.view             = view;
        this.acceptsSequences = true;
        this.isTopLevel       = true;
    }

    /**
     * Used in views after full type resolution
     */
    public void setTableColumnNames(HashMappedList list) {

        if (resultTable != null) {
            ((TableDerived) resultTable).columnList = list;

            return;
        }

        leftQueryExpression.setTableColumnNames(list);
    }

    void createTable(Session session) {

        createResultTable(session);

        mainIndex = resultTable.getPrimaryIndex();

        if (sortAndSlice.hasOrder()) {
            orderIndex = sortAndSlice.getNewIndex(session, resultTable);
        }

        int[] fullCols = new int[columnCount];

        ArrayUtil.fillSequence(fullCols);

        fullIndex = resultTable.createAndAddIndexStructure(session, null,
                fullCols, null, null, false, false, false);
        resultTable.fullIndex = fullIndex;
    }

    void createResultTable(Session session) {

        HsqlName       tableName;
        HashMappedList columnList;
        int            tableType;

        tableName = session.database.nameManager.getSubqueryTableName();
        tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                    ? TableBase.SYSTEM_SUBQUERY
                    : TableBase.RESULT_TABLE;
        columnList = leftQueryExpression.getUnionColumns();

        try {
            resultTable = new TableDerived(session.database, tableName,
                                           tableType, unionColumnTypes,
                                           columnList, null, null);
        } catch (Exception e) {}
    }

    public void setColumnsDefined() {

        if (leftQueryExpression != null) {
            leftQueryExpression.setColumnsDefined();
        }
    }

    /**
     * Not for views. Only used on root node.
     */
    public void setReturningResult() {

        if (compileContext.getSequences().length > 0) {
            throw Error.error(ErrorCode.X_42598);
        }

        isTopLevel = true;

        setReturningResultSet();
    }

    /**
     * Sets the scope to SESSION for the QueryExpression object that creates
     * the table
     */
    void setReturningResultSet() {

        if (unionCorresponding) {
            persistenceScope = TableBase.SCOPE_SESSION;

            return;
        }

        leftQueryExpression.setReturningResultSet();
    }

    private HashMappedList getUnionColumns() {

        if (unionCorresponding || leftQueryExpression == null) {
            HashMappedList columns = ((TableDerived) resultTable).columnList;
            HashMappedList list    = new HashMappedList();

            for (int i = 0; i < unionColumnMap.length; i++) {
                ColumnSchema column = (ColumnSchema) columns.get(i);

                list.add(column.getName().name, column);
            }

            return list;
        }

        return leftQueryExpression.getUnionColumns();
    }

    public HsqlName[] getResultColumnNames() {

        if (resultTable == null) {
            return leftQueryExpression.getResultColumnNames();
        }

        HashMappedList list = ((TableDerived) resultTable).columnList;
        HsqlName[]     resultColumnNames = new HsqlName[list.size()];

        for (int i = 0; i < resultColumnNames.length; i++) {
            resultColumnNames[i] = ((ColumnSchema) list.get(i)).getName();
        }

        return resultColumnNames;
    }

    public TableBase getResultTable() {

        if (resultTable != null) {
            return resultTable;
        }

        if (leftQueryExpression != null) {
            return leftQueryExpression.getResultTable();
        }

        return null;
    }

    //
    public Table getBaseTable() {
        return null;
    }

    public boolean isUpdatable() {
        return isUpdatable;
    }

    public boolean isInsertable() {
        return isInsertable;
    }

    public int[] getBaseTableColumnMap() {
        return null;
    }

    public Expression getCheckCondition() {
        return null;
    }

    public boolean hasReference(RangeVariable range) {

        if (leftQueryExpression.hasReference(range)) {
            return true;
        }

        if (rightQueryExpression.hasReference(range)) {
            return true;
        }

        return false;
    }

    void getBaseTableNames(OrderedHashSet set) {
        leftQueryExpression.getBaseTableNames(set);
        rightQueryExpression.getBaseTableNames(set);
    }

    boolean isEquivalent(QueryExpression other) {

        return leftQueryExpression.isEquivalent(other.leftQueryExpression)
               && unionType == other.unionType
               && (rightQueryExpression == null
                   ? other.rightQueryExpression == null
                   : rightQueryExpression.isEquivalent(
                       other.rightQueryExpression));
    }

    public void replaceColumnReference(RangeVariable range,
                                       Expression[] list) {
        leftQueryExpression.replaceColumnReference(range, list);
        rightQueryExpression.replaceColumnReference(range, list);
    }

    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {
        leftQueryExpression.replaceRangeVariables(ranges, newRanges);
        rightQueryExpression.replaceRangeVariables(ranges, newRanges);
    }
}
