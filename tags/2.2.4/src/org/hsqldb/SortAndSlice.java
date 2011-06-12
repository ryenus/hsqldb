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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;

/*
 * Implementation of ORDER BY and LIMIT properties of query expressions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public final class SortAndSlice {

    static final SortAndSlice noSort = new SortAndSlice();

    //
    public int[]       sortOrder;
    public boolean[]   sortDescending;
    public boolean[]   sortNullsLast;
    public Collation[] collations;
    boolean            sortUnion;
    HsqlArrayList      exprList = new HsqlArrayList();
    ExpressionOp       limitCondition;
    int                columnCount;
    boolean            hasNullsLast;
    boolean            strictLimit;
    boolean            zeroLimit;
    public boolean     skipSort       = false;    // true when result can be used as is
    public boolean     skipFullResult = false;    // true when result can be sliced as is
    int[]          columnIndexes;
    public Index   index;
    public boolean isGenerated;

    SortAndSlice() {}

    public HsqlArrayList getExpressionList() {
        return exprList;
    }

    public boolean hasOrder() {
        return exprList.size() != 0;
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
        strictLimit = true;
    }

    public void setZeroLimit() {
        zeroLimit = true;
    }

    public void prepareSingleColumn(int colIndex) {

        sortOrder      = new int[1];
        sortDescending = new boolean[1];
        sortNullsLast  = new boolean[1];
        sortOrder[0]   = colIndex;
    }

    public void prepare(int degree) {

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

    public void prepare(QuerySpecification select) {

        columnCount = exprList.size();

        if (columnCount == 0) {
            return;
        }

        sortOrder      = new int[columnCount];
        sortDescending = new boolean[columnCount];
        sortNullsLast  = new boolean[columnCount];

        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);

            if (sort.getLeftNode().queryTableColumnIndex == -1) {
                sortOrder[i] = select.indexStartOrderBy + i;
            } else {
                sortOrder[i] = sort.getLeftNode().queryTableColumnIndex;
            }

            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
            hasNullsLast      |= sortNullsLast[i];

            if (sort.collation != null) {
                if (collations == null) {
                    collations = new Collation[columnCount];
                }

                collations[i] = sort.collation;
            }
        }
    }

    void setSortRange(QuerySpecification select) {

        if (isGenerated) {
            return;
        }

        if (columnCount == 0) {
            if (limitCondition == null) {
                return;
            }

            if (select.isDistinctSelect || select.isGrouped
                    || select.isAggregated) {
                return;
            }

            skipFullResult = true;

            return;
        }

        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort     = (ExpressionOrderBy) exprList.get(i);
            Type              dataType = sort.getLeftNode().getDataType();

            if (dataType.isArrayType() || dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }

        if (select == null || hasNullsLast) {
            return;
        }

        if (select.isDistinctSelect || select.isGrouped
                || select.isAggregated) {
            return;
        }

        int[] colIndexes = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            Expression e = ((Expression) exprList.get(i)).getLeftNode();

            if (e.getType() != OpTypes.COLUMN) {
                return;
            }

            if (((ExpressionColumn) e).getRangeVariable()
                    != select.rangeVariables[0]) {
                return;
            }

            colIndexes[i] = e.columnIndex;
        }

        this.columnIndexes = colIndexes;

        if (columnIndexes == null) {
            return;
        }

        if (collations != null) {
            return;
        }

        Index rangeIndex = select.rangeVariables[0].getSortIndex();

        if (rangeIndex == null) {
            return;
        }

        colIndexes = rangeIndex.getColumns();

        int     count         = ArrayUtil.countTrueElements(sortDescending);
        boolean allDescending = count == columnCount;

        if (!allDescending && count > 0) {
            return;
        }

        if (!select.rangeVariables[0].hasIndexCondition()) {
            Table table = select.rangeVariables[0].getTable();
            Index index = table.getFullIndexForColumns(columnIndexes);

            if (index != null) {
                if (select.rangeVariables[0].setSortIndex(index,
                        allDescending)) {
                    skipSort       = true;
                    skipFullResult = true;
                }
            }
        } else if (ArrayUtil.haveEqualArrays(columnIndexes, colIndexes,
                                             columnIndexes.length)) {
            if (allDescending) {
                boolean reversed = select.rangeVariables[0].reverseOrder();

                if (!reversed) {
                    return;
                }
            }

            skipSort       = true;
            skipFullResult = true;
        }
    }

    public boolean prepareSpecial(Session session, QuerySpecification select) {

        Expression e      = select.exprColumns[select.indexStartAggregates];
        int        opType = e.getType();

        e = e.getLeftNode();

        if (e.getType() != OpTypes.COLUMN) {
            return false;
        }

        if (((ExpressionColumn) e).getRangeVariable()
                != select.rangeVariables[0]) {
            return false;
        }

        Index rangeIndex = select.rangeVariables[0].getSortIndex();

        if (rangeIndex == null) {
            return false;
        }

        int[] colIndexes = rangeIndex.getColumns();

        if (select.rangeVariables[0].hasIndexCondition()) {
            if (colIndexes[0] != ((ExpressionColumn) e).getColumnIndex()) {
                return false;
            }

            if (opType == OpTypes.MAX) {
                select.rangeVariables[0].reverseOrder();
            }
        } else {
            Table table = select.rangeVariables[0].getTable();
            Index index = table.getIndexForColumn(
                session, ((ExpressionColumn) e).getColumnIndex());

            if (index == null) {
                return false;
            }

            if (!select.rangeVariables[0].setSortIndex(index,
                    opType == OpTypes.MAX)) {
                return false;
            }
        }

        columnCount      = 1;
        sortOrder        = new int[columnCount];
        sortDescending   = new boolean[columnCount];
        sortNullsLast    = new boolean[columnCount];
        columnIndexes    = new int[columnCount];
        columnIndexes[0] = e.columnIndex;
        skipSort         = true;
        skipFullResult   = true;

        return true;
    }

    public int getLimitStart(Session session) {

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getLeftNode().getValue(session);

            if (limit != null) {
                return limit.intValue();
            }
        }

        return 0;
    }

    public int getLimitCount(Session session, int rowCount) {

        int limitCount = 0;

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getRightNode().getValue(session);

            if (limit != null) {
                limitCount = limit.intValue();
            }
        }

        if (rowCount != 0 && (limitCount == 0 || rowCount < limitCount)) {
            limitCount = rowCount;
        }

        return limitCount;
    }

    public void setIndex(Session session, TableBase table) {
        index = getNewIndex(session, table);
    }

    public Index getNewIndex(Session session, TableBase table) {

        if (hasOrder()) {
            Index orderIndex = table.createAndAddIndexStructure(session, null,
                sortOrder, sortDescending, sortNullsLast, false, false, false);

            if (collations != null) {
                for (int i = 0; i < columnCount; i++) {
                    if (collations[i] != null) {
                        Type type = orderIndex.getColumnTypes()[i];

                        type = Type.getType(type.typeCode,
                                            type.getCharacterSet(),
                                            collations[i], type.precision,
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
