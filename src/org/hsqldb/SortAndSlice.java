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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;

/**
 * Implementation of ORDER BY and LIMIT properties of query expressions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public final class SortAndSlice {

    public static final SortAndSlice noSort = new SortAndSlice();
    static final int[] defaultLimits = new int[]{ 0, Integer.MAX_VALUE,
            Integer.MAX_VALUE };

    //
    public int[]              sortOrder;
    public boolean[]          sortDescending;
    public boolean[]          sortNullsLast;
    public Collation[]        collations;
    boolean                   hasCollation;
    boolean                   sortUnion;
    HsqlArrayList<Expression> exprList = new HsqlArrayList<>();
    ExpressionOp              limitCondition;
    public int                columnCount;
    boolean                   hasNullsLast;
    boolean                   noZeroLimit;
    boolean                   zeroLimitIsZero;
    boolean                   usingIndex;
    boolean                   descendingSort;
    public boolean skipSort = false;          // true when result can be used as is
    public boolean skipFullResult = false;    // true when result can be sliced as is
    public Index              index;
    public Table              primaryTable;
    public Index              primaryTableIndex;
    public int[]              colIndexes;
    public boolean            isGenerated;

    public SortAndSlice() {}

    public HsqlArrayList<Expression> getExpressionList() {
        return exprList;
    }

    public boolean hasOrder() {
        return exprList.size() > 0;
    }

    public boolean hasLimit() {
        return limitCondition != null;
    }

    public int getOrderLength() {
        return exprList.size();
    }

    public void addOrderExpression(Expression e) {
        exprList.add(e);
    }

    public void addLimitCondition(ExpressionOp expression) {
        limitCondition = expression;
    }

    public void setStrictLimit() {
        noZeroLimit = true;
    }

    public void setZeroLimitIsZero() {
        zeroLimitIsZero = true;
    }

    public void setUsingIndex() {
        usingIndex = true;
    }

    public void prepareSingleColumn(int colIndex) {

        sortOrder      = new int[1];
        sortDescending = new boolean[1];
        sortNullsLast  = new boolean[1];
        sortOrder[0]   = colIndex;
        columnCount    = 1;
    }

    public void prepareMultiColumn(int count) {

        sortOrder      = new int[count];
        sortDescending = new boolean[count];
        sortNullsLast  = new boolean[count];
        columnCount    = count;

        for (int i = 0; i < count; i++) {
            sortOrder[i] = i;
        }
    }

    public void prepareExtraColumn(int degree) {

        columnCount = exprList.size();

        if (columnCount == 0) {
            return;
        }

        sortOrder      = new int[columnCount + degree];
        sortDescending = new boolean[columnCount + degree];
        sortNullsLast  = new boolean[columnCount + degree];

        ArrayUtil.fillSequence(sortOrder);

        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);

            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
            hasNullsLast      |= sortNullsLast[i];
        }
    }

    public void prepare(int startColumn) {

        columnCount = exprList.size();

        if (columnCount == 0) {
            return;
        }

        sortOrder      = new int[columnCount];
        sortDescending = new boolean[columnCount];
        sortNullsLast  = new boolean[columnCount];
        collations     = new Collation[columnCount];

        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);
            int colIndex           = sort.getLeftNode().resultTableColumnIndex;

            if (colIndex == -1) {
                sortOrder[i] = startColumn + i;
            } else {
                sortOrder[i] = colIndex;
            }

            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
            collations[i]     = sort.collation;
            hasNullsLast      |= sortNullsLast[i];

            if (sort.collation != null) {
                hasCollation = true;
            }
        }
    }

    void setSortIndex(QuerySpecification select) {

        if (this == noSort) {
            return;
        }

        if (isGenerated) {
            return;
        }

        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort     = (ExpressionOrderBy) exprList.get(i);
            Type              dataType = sort.getLeftNode().getDataType();

            if (dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }

        if (select == null) {
            return;
        }

        if (select.isDistinctSelect
                || select.isGrouped
                || select.isAggregated) {
            if (!select.isSimpleDistinct) {
                return;
            }
        }

        if (columnCount == 0) {
            if (limitCondition == null) {
                return;
            }

            skipFullResult = true;

            return;
        }

        if (hasCollation) {
            return;
        }

        colIndexes = new int[columnCount];

        boolean isNullable = false;

        for (int i = 0; i < columnCount; i++) {
            Expression e = exprList.get(i).getLeftNode();

            if (e.getType() != OpTypes.COLUMN) {
                return;
            }

            if (e.getRangeVariable() != select.rangeVariables[0]) {
                return;
            }

            colIndexes[i] = e.columnIndex;

            if (e.getColumn().getNullability()
                    != SchemaObject.Nullability.NO_NULLS) {
                isNullable = true;
            }
        }

        if (hasNullsLast && isNullable) {
            return;
        }

        int count = ArrayUtil.countTrueElements(sortDescending);

        descendingSort = count == columnCount;

        if (!descendingSort && count > 0) {
            return;
        }

        primaryTable      = select.rangeVariables[0].getTable();
        primaryTableIndex = primaryTable.getFullIndexForColumns(colIndexes);
    }

    void setSortRange(QuerySpecification select) {

        if (this == noSort) {
            return;
        }

        if (primaryTableIndex == null) {
            if (select.isSimpleDistinct) {
                setSortIndex(select);
            }

            if (primaryTableIndex == null) {
                return;
            }
        }

        Index rangeIndex = select.rangeVariables[0].getSortIndex();

        if (rangeIndex == null) {

            // multi-index
            return;
        }

        if (primaryTable != select.rangeVariables[0].rangeTable) {
            return;
        }

        if (rangeIndex == primaryTableIndex) {
            if (descendingSort) {
                if (select.isDistinctSelect) {
                    return;
                }

                boolean reversed = select.rangeVariables[0].reverseOrder();

                if (!reversed) {
                    return;
                }
            }

            skipSort       = true;
            skipFullResult = true;
        } else if (!select.rangeVariables[0].joinConditions[0].hasIndexCondition()) {
            if (select.rangeVariables[0].setSortIndex(primaryTableIndex,
                    descendingSort)) {
                skipSort       = true;
                skipFullResult = true;
            }
        }
    }

    public boolean prepareSpecial(Session session, QuerySpecification select) {

        Expression e      = select.exprColumns[select.indexStartAggregates];
        int        opType = e.getType();

        e = e.getLeftNode();

        if (e.getType() != OpTypes.COLUMN) {
            return false;
        }

        if (e.getRangeVariable() != select.rangeVariables[0]) {
            return false;
        }

        Index rangeIndex = select.rangeVariables[0].getSortIndex();

        if (rangeIndex == null) {
            return false;
        }

        if (select.rangeVariables[0].hasAnyTerminalCondition()) {
            return false;
        }

        if (select.rangeVariables[0].hasSingleIndexCondition()) {
            int[] colIndexes = rangeIndex.getColumns();

            if (colIndexes[0] != e.getColumnIndex()) {
                return false;
            }

            if (opType == OpTypes.MAX) {
                select.rangeVariables[0].reverseOrder();
            }
        } else if (select.rangeVariables[0].hasAnyIndexCondition()) {
            return false;
        } else {
            Table table = select.rangeVariables[0].getTable();
            Index index = table.getIndexForColumn(session, e.getColumnIndex());

            if (index == null) {
                return false;
            }

            Expression[] conditions = new Expression[]{
                ExpressionLogical.newNotNullCondition(
                    e) };

            select.rangeVariables[0].joinConditions[0].addIndexCondition(
                conditions,
                index,
                1);

            if (opType == OpTypes.MAX) {
                select.rangeVariables[0].reverseOrder();
            }
        }

        columnCount    = 1;
        sortOrder      = new int[columnCount];
        sortDescending = new boolean[columnCount];
        sortNullsLast  = new boolean[columnCount];
        skipSort       = true;
        skipFullResult = true;

        return true;
    }

    int[] getLimits(Session session, QueryExpression qe, int maxRows) {

        if (this == noSort && maxRows == 0) {
            return defaultLimits;
        }

        int     skipRows   = 0;
        int     limitRows  = Integer.MAX_VALUE;
        int     limitFetch = Integer.MAX_VALUE;
        boolean hasLimits  = false;

        if (hasLimit()) {
            Integer value = (Integer) limitCondition.getLeftNode()
                    .getValue(session);

            if (value == null || value.intValue() < 0) {
                throw Error.error(ErrorCode.X_2201X);
            }

            skipRows  = value.intValue();
            hasLimits = skipRows != 0;

            if (limitCondition.getRightNode() != null) {
                value = (Integer) limitCondition.getRightNode()
                                                .getValue(session);

                if (value == null
                        || value.intValue() < 0
                        || (noZeroLimit && value.intValue() == 0)) {
                    throw Error.error(ErrorCode.X_2201W);
                }

                if (value.intValue() == 0 && !zeroLimitIsZero) {
                    limitRows = Integer.MAX_VALUE;
                } else {
                    limitRows = value.intValue();
                    hasLimits = true;
                }
            }
        }

        if (maxRows != 0) {
            if (maxRows < limitRows) {
                limitRows = maxRows;
            }

            hasLimits = true;
        }

        boolean simpleLimit = false;

        if (qe instanceof QuerySpecification) {
            QuerySpecification select = (QuerySpecification) qe;

            if (!select.isDistinctSelect && !select.isGrouped) {
                simpleLimit = true;
            }

            if (select.isSimpleDistinct) {
                simpleLimit = true;
            }
        }

        if (hasLimits) {
            if (simpleLimit
                    && (!hasOrder() || skipSort)
                    && (!hasLimit() || skipFullResult)) {
                if (limitFetch > skipRows + limitRows) {
                    limitFetch = skipRows + limitRows;
                }
            }

            return new int[]{ skipRows, limitRows, limitFetch };
        }

        return defaultLimits;
    }

    public void setIndex(Session session, TableBase table) {
        index = getNewIndex(session, table);
    }

    public Index getNewIndex(Session session, TableBase table) {

        if (hasOrder()) {
            Index orderIndex = table.createAndAddIndexStructure(
                session,
                null,
                sortOrder,
                sortDescending,
                sortNullsLast,
                false,
                false,
                false);

            if (hasCollation) {
                for (int i = 0; i < columnCount; i++) {
                    if (collations[i] != null) {
                        Type type = orderIndex.getColumnTypes()[i];

                        type = Type.getType(
                            type.typeCode,
                            type.getCharacterSet(),
                            collations[i],
                            type.precision,
                            type.scale);
                        orderIndex.getColumnTypes()[i] = type;
                    }
                }
            }

            return orderIndex;
        }

        return null;
    }
}
