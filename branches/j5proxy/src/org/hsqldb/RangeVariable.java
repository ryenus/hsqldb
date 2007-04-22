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

import org.hsqldb.Parser.CompileContext;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

/**
 * Metadata for range variables, including conditions.
 *
 * @author fredt@users;
 * @version 1.9.0
 * @since 1.9.0
 */
final class RangeVariable {

    final Table            rangeTable;
    private final String   tableAlias;
    private OrderedHashSet columnAliases;
    OrderedHashSet         namedJoinColumns;
    Index                  rangeIndex;
    private final Object[] emptyData;
    final boolean[]        columnsInGroupBy;
    boolean                hasKeyedColumnInGroupBy;
    final boolean[]        usedColumns;

    // index conditions
    Expression indexCondition;
    Expression indexEndCondition;
    boolean    isJoinIndex;
    boolean    hasIndexCondition;                 // index conditions have been assigned to this

    // non-index consitions
    Expression nonIndexJoinCondition;
    Expression nonIndexWhereCondition;

    //
    boolean              isOuterJoin;             // table joined with OUTER JOIN
    boolean              isFullJoin;              // table joined with FULL OUTER JOIN
    boolean              isMultiFindFirst;        // findFirst() uses multi-column index
    private Expression[] findFirstExpressions;    // expressions for column values
    private int          multiColumnCount;

    //
    int index;

    /**
     * Constructor declaration
     *
     * @param t Table
     * @param alias String
     * @param columnList OrderedHashSet
     * @param outerjoin boolean
     */
    RangeVariable(Table t, String alias, OrderedHashSet columnList,
                  CompileContext compileContext) {

        rangeTable       = t;
        tableAlias       = alias;
        columnAliases    = columnList;
        emptyData        = rangeTable.getEmptyRowData();
        columnsInGroupBy = rangeTable.getNewColumnCheckList();
        usedColumns      = rangeTable.getNewColumnCheckList();
        rangeIndex       = rangeTable.getPrimaryIndex();

        compileContext.registerRangeVariable(this);
    }

    void setJoinType(boolean outer, boolean full) throws HsqlException {
        isOuterJoin = outer;
        isFullJoin  = full;
    }

    public void addNamedJoinColumns(OrderedHashSet columns) {
        namedJoinColumns = columns;
    }

    public void addColumn(int columnIndex) {
        usedColumns[columnIndex] = true;
    }

    /**
     * Returns this object's backing table.
     *
     * @return table
     */
    Table getTable() {
        return rangeTable;
    }

    /**
     * Retruns index for column
     *
     * @param columnName name of column
     * @return int index or -1 if not found
     */
    public int findColumn(String columnName) {

        if (columnAliases != null) {
            return columnAliases.getIndex(columnName);
        } else {
            return rangeTable.findColumn(columnName);
        }
    }

    boolean resolvesTableName(Expression e) {

        if (e.tableName == null) {
            return true;
        }

        if (e.schema == null) {
            if (tableAlias == null) {
                if (e.tableName.equals(rangeTable.tableName.name)) {
                    return true;
                }
            } else if (e.tableName.equals(tableAlias)) {
                return true;
            }
        } else {
            if (e.tableName.equals(rangeTable.tableName.name)
                    && e.schema.equals(rangeTable.tableName.schema.name)) {
                return true;
            }
        }

        return false;
    }

    boolean resolvesTableName(String name) {

        if (name == null) {
            return true;
        }

        if (tableAlias == null) {
            if (name.equals(rangeTable.tableName.name)) {
                return true;
            }
        } else if (name.equals(tableAlias)) {
            return true;
        }

        return false;
    }

    boolean resolvesSchemaName(String name) {

        if (name == null) {
            return true;
        }

        if (tableAlias != null) {
            return false;
        }

        return name.equals(rangeTable.tableName.schema.name);
    }

    /**
     * Add all columns to a list of expressions
     */
    void addTableColumns(HsqlArrayList exprList) {

        OrderedHashSet exclude = namedJoinColumns;

        if (exclude != null) {
            int count    = exprList.size();
            int position = 0;

            for (int i = 0; i < count; i++) {
                Expression e          = (Expression) exprList.get(i);
                String     columnName = e.getColumnName();

                if (namedJoinColumns.contains(columnName)) {
                    if (position != i) {
                        exprList.remove(i);
                        exprList.add(position, e);

                        position++;
                    }
                }
            }
        }

        Table table = getTable();
        int   count = table.getColumnCount();

        for (int i = 0; i < count; i++) {
            Column column     = table.getColumn(i);
            String columnName = column.getName().name;

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new Expression(this, column);

            exprList.add(e);
        }
    }

    /**
     * Add all columns to a list of expressions
     */
    int addTableColumns(HsqlArrayList expList, int position) {

        Table          table   = getTable();
        int            count   = table.getColumnCount();
        OrderedHashSet exclude = namedJoinColumns;

        for (int i = 0; i < count; i++) {
            Column column     = table.getColumn(i);
            String columnName = column.getName().name;

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new Expression(this, column);

            expList.add(position++, e);
        }

        return position;
    }

    /**
     * Removes reference to Index to avoid possible memory leaks after alter
     * table or drop index
     */
    void setForCheckConstraint() {
        rangeIndex = null;
    }

    /**
     *
     * @param e condition
     * @param index Index object
     * @param isJoin whether a join or not
     */
    void addIndexCondition(Expression e, Index index, boolean isJoin) {

        rangeIndex        = index;
        hasIndexCondition = true;
        isJoinIndex       = isJoin;

        switch (e.getType()) {

            case Expression.IS_NULL :
                indexEndCondition = e;
                break;

            case Expression.EQUAL :
                indexCondition    = e;
                indexEndCondition = indexCondition;
                break;

            case Expression.GREATER :
            case Expression.GREATER_EQUAL :
                indexCondition = e;
                break;

            case Expression.SMALLER :
            case Expression.SMALLER_EQUAL :
                indexEndCondition = e;
                break;

            default :
                Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                   "Expression");
        }
    }

    /**
     *
     * @param e a join condition
     */
    void addJoinCondition(Expression e) {

        if (nonIndexJoinCondition == null) {
            nonIndexJoinCondition = e;
        } else {
            Expression and = new Expression(Expression.AND,
                                            nonIndexJoinCondition, e);

            nonIndexJoinCondition = and;
        }
    }

    /**
     *
     * @param e a where condition
     */
    void addWhereCondition(Expression e) {

        if (nonIndexWhereCondition == null) {
            nonIndexWhereCondition = e;
        } else {
            Expression and = new Expression(Expression.AND,
                                            nonIndexWhereCondition, e);

            nonIndexWhereCondition = and;
        }
    }

    void addCondition(Expression e, boolean isJoin) {

        if (isJoin) {
            addJoinCondition(e);
        } else {
            addWhereCondition(e);
        }
    }

    /**
     * Only multiple EQUAL conditions are used
     *
     * @param exprList list of expressions
     * @param index Index to muse
     * @param isJoin whether a join or not
     */
    void addIndexCondition(Expression[] exprList, Index index, int colCount,
                           boolean isJoin) {

        rangeIndex        = index;
        hasIndexCondition = true;
        isJoinIndex       = isJoin;

        for (int i = 0; i < colCount; i++) {
            Expression e = exprList[i];

            if (indexEndCondition == null) {
                indexEndCondition = e;
            } else {
                Expression and = new Expression(Expression.AND,
                                                indexEndCondition, e);

                indexEndCondition = and;
            }
        }

        if (colCount == 1) {
            indexCondition = exprList[0];
        } else {
            findFirstExpressions = exprList;
            isMultiFindFirst     = true;
            multiColumnCount     = colCount;
        }
    }

// boucheb@users 20030415 - added for debugging support

    /**
     * Retreives a String representation of this obejct. <p>
     *
     * The returned String describes this object's table, alias
     * access mode, index, join mode, Start, End and And conditions.
     *
     * @return a String representation of this object
     */
    public String describe(Session session) {

        StringBuffer sb;
        String       temp;
        Index        index;
        Index        primaryIndex;
        int[]        primaryKey;
        boolean      hidden;
        boolean      fullScan;

        sb           = new StringBuffer();
        index        = rangeIndex;
        primaryIndex = rangeTable.getPrimaryIndex();
        primaryKey   = rangeTable.getPrimaryKey();
        hidden       = false;
        fullScan     = (indexCondition == null && indexEndCondition == null);

        if (index == null) {
            index = primaryIndex;
        }

        if (index == primaryIndex && primaryKey.length == 0) {
            hidden   = true;
            fullScan = true;
        }

        sb.append(super.toString()).append('\n');
        sb.append("table=[").append(rangeTable.getName().name).append("]\n");
        sb.append("alias=[").append(tableAlias).append("]\n");
        sb.append("access=[").append(fullScan ? "FULL SCAN"
                                              : "INDEX PRED").append("]\n");
        sb.append("index=[");
        sb.append(index == null ? "NONE"
                                : index.getName() == null ? "UNNAMED"
                                                          : index.getName()
                                                          .name);
        sb.append(hidden ? "[HIDDEN]]\n"
                         : "]\n");
        sb.append("isOuterJoin=[").append(isOuterJoin).append("]\n");

        temp = indexCondition == null ? "null"
                                      : indexCondition.describe(session);

        sb.append("eStart=[").append(temp).append("]\n");

        temp = indexEndCondition == null ? "null"
                                         : indexEndCondition.describe(
                                             session);

        sb.append("eEnd=[").append(temp).append("]\n");

        temp = nonIndexJoinCondition == null ? "null"
                                             : nonIndexJoinCondition.describe(
                                             session);

        sb.append("eAnd=[").append(temp).append("]");

        return sb.toString();
    }

    public RangeIterator getIterator(Session session) throws HsqlException {

        RangeIterator it = new RangeIterator(session, this);

        session.compiledStatementExecutor.setRangeIterator(it);

        return it;
    }

    public RangeIterator getFullIterator(Session session,
                                         RangeIterator mainIterator)
                                         throws HsqlException {

        RangeIterator it = new FullRangeIterator(session, this, mainIterator);

        session.compiledStatementExecutor.setRangeIterator(it);

        return it;
    }

    public static RangeIterator getIterator(Session session,
            RangeVariable[] rangeVars) throws HsqlException {

        if (rangeVars.length == 1) {
            return rangeVars[0].getIterator(session);
        }

        RangeIterator[] iterators = new RangeIterator[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            iterators[i] = rangeVars[i].getIterator(session);
        }

        return new JoinedRangeIterator(iterators);
    }

    public static class RangeIterator {

        protected RowIterator it;
        Object[]              currentData;
        Row                   currentRow;
        boolean               hasOuterRow;
        boolean               isBeforeFirst;
        boolean               isFullIterator;
        Session               session;
        RangeVariable         rangeVar;

        //
        Table lookupTable;

        RangeIterator() {}

        public RangeIterator(Session session,
                             RangeVariable rangeVar) {

            this.session  = session;
            this.rangeVar = rangeVar;
            isBeforeFirst = true;

            if (rangeVar.isFullJoin) {
                lookupTable = TableUtil.newLookupTable(session.database);
            }
        }

        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        public boolean next() throws HsqlException {

            if (isBeforeFirst) {
                isBeforeFirst = false;

                initialiseIterator();
            } else {
                if (it == null) {
                    return false;
                }
            }

            return findNext();
        }

        public boolean remove() {
            return false;
        }

        public void reset() {

            if (it != null) {
                it.release();
            }

            it            = null;
            currentData   = rangeVar.emptyData;
            currentRow    = null;
            hasOuterRow   = false;
            isBeforeFirst = true;
        }

        /**
         */
        protected void initialiseIterator() throws HsqlException {

            hasOuterRow = rangeVar.isOuterJoin;

            if (rangeVar.isMultiFindFirst) {
                getFirstRowMulti();

                if (!rangeVar.isJoinIndex) {
                    hasOuterRow = false;
                }
            } else if (rangeVar.indexCondition == null) {
                if (rangeVar.indexEndCondition == null
                        || rangeVar.indexEndCondition.getType()
                           == Expression.IS_NULL) {
                    it = rangeVar.rangeIndex.firstRow(session);
                } else {
                    it = rangeVar.rangeIndex.findFirstRowNotNull(session);
                }
            } else {
                getFirstRow();

                if (!rangeVar.isJoinIndex) {
                    hasOuterRow = false;
                }
            }
        }

        /**
         */
        private void getFirstRow() throws HsqlException {

            Object value =
                rangeVar.indexCondition.getArg2().getValue(session);
            Type valueType  = rangeVar.indexCondition.getArg2().getDataType();
            Type targetType = rangeVar.indexCondition.getArg().getDataType();
            int  exprType   = rangeVar.indexCondition.getType();
            int  range      = 0;

            if (targetType != valueType) {
                range = targetType.compareToTypeRange(value);
            }

            if (range == 0) {
                value = targetType.convertToType(session, value, valueType);
                it = rangeVar.rangeIndex.findFirstRow(session, value,
                                                      exprType);
            } else if (range < 0) {
                switch (exprType) {

                    case Expression.GREATER_EQUAL :
                    case Expression.GREATER :
                        it = rangeVar.rangeIndex.findFirstRowNotNull(session);
                        break;

                    default :
                        it = rangeVar.rangeIndex.emptyIterator();
                }
            } else {
                switch (exprType) {

                    case Expression.SMALLER_EQUAL :
                    case Expression.SMALLER :
                        it = rangeVar.rangeIndex.findFirstRowNotNull(session);
                        break;

                    default :
                        it = rangeVar.rangeIndex.emptyIterator();
                }
            }

            return;
        }

        /**
         * Uses multiple EQUAL expressions
         */
        private void getFirstRowMulti() throws HsqlException {

            boolean convertible = true;
            Object[] currentJoinData =
                new Object[rangeVar.rangeIndex.getVisibleColumns()];

            for (int i = 0; i < rangeVar.multiColumnCount; i++) {
                Type valueType =
                    rangeVar.findFirstExpressions[i].getArg2().getDataType();
                Type targetType =
                    rangeVar.findFirstExpressions[i].getArg().getDataType();
                Object value =
                    rangeVar.findFirstExpressions[i].getArg2().getValue(
                        session);

                if (targetType.compareToTypeRange(value) != 0) {
                    convertible = false;

                    break;
                }

                currentJoinData[i] = targetType.convertToType(session, value,
                        valueType);
            }

            it = convertible
                 ? rangeVar.rangeIndex.findFirstRow(session, currentJoinData,
                     rangeVar.multiColumnCount)
                 : rangeVar.rangeIndex.emptyIterator();

            if (!it.hasNext()) {
                ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_OBJECT,
                                     currentJoinData, 0,
                                     currentJoinData.length);
            }
        }

        /**
         * Advances to the next available value. <p>
         *
         * @return true if a next value is available upon exit
         *
         * @throws HsqlException if a database access error occurs
         */
        protected boolean findNext() throws HsqlException {

            boolean result = false;

            while (true) {
                currentRow = it.getNext();

                if (currentRow == null) {
                    break;
                }

                currentData = currentRow.getData();

                if (rangeVar.indexEndCondition != null
                        &&!rangeVar.indexEndCondition.testCondition(
                            session)) {
                    if (!rangeVar.isJoinIndex) {
                        hasOuterRow = false;
                    }

                    break;
                }

                if (rangeVar.nonIndexJoinCondition != null
                        &&!rangeVar.nonIndexJoinCondition.testCondition(
                            session)) {
                    continue;
                }

                if (rangeVar.nonIndexWhereCondition != null
                        &&!rangeVar.nonIndexWhereCondition.testCondition(
                            session)) {
                    hasOuterRow = false;

                    continue;
                }

                addFoundRow();

                result = true;

                break;
            }

            if (result) {
                hasOuterRow = false;

                return true;
            }

            it.release();

            currentRow  = null;
            currentData = rangeVar.emptyData;

            if (hasOuterRow) {
                result = (rangeVar.nonIndexWhereCondition == null
                          || rangeVar.nonIndexWhereCondition.testCondition(
                              session));
            }

            hasOuterRow = false;

            return result;
        }

        private void addFoundRow() throws HsqlException {

            if (rangeVar.isFullJoin) {
                try {
                    lookupTable.insert(
                        session,
                        new Object[]{
                            ValuePool.getInt(currentRow.getPos()) });
                } catch (HsqlException e) {}
            }
        }
    }

    public static class FullRangeIterator extends RangeIterator {

        public FullRangeIterator(Session session, RangeVariable rangeVar,
                                 RangeIterator rangeIterator)
                                 throws HsqlException {

            this.session  = session;
            this.rangeVar = rangeVar;
            isBeforeFirst = true;
            lookupTable   = rangeIterator.lookupTable;
            it            = rangeVar.rangeIndex.firstRow(session);
        }

        protected void initialiseIterator() throws HsqlException {}

        protected boolean findNext() throws HsqlException {

            boolean result;

            while (true) {
                currentRow = it.getNext();

                if (currentRow == null) {
                    result = false;

                    break;
                }

                RowIterator lookupIterator =
                    lookupTable.indexList[0].findFirstRow(session,
                        ValuePool.getInt(currentRow.getPos()),
                        Expression.EQUAL);

                result = !lookupIterator.hasNext();

                lookupIterator.release();

                if (result) {
                    if (rangeVar.nonIndexWhereCondition != null
                            &&!rangeVar.nonIndexWhereCondition.testCondition(
                                session)) {
                        continue;
                    }

                    isBeforeFirst = false;
                    currentData   = currentRow.getData();

                    return true;
                }
            }

            it.release();

            currentRow  = null;
            currentData = rangeVar.emptyData;

            return result;
        }
    }

    public static class JoinedRangeIterator extends RangeIterator {

        RangeIterator[] rangeIterators;
        int             currentIndex = 0;

        public JoinedRangeIterator(RangeIterator[] rangeIterators) {
            this.rangeIterators = rangeIterators;
        }

        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        public boolean next() throws HsqlException {

            while (currentIndex >= 0) {
                RangeIterator it = rangeIterators[currentIndex];

                if (it.next()) {
                    if (currentIndex < rangeIterators.length - 1) {
                        currentIndex++;

                        continue;
                    }

                    currentRow  = rangeIterators[currentIndex].currentRow;
                    currentData = currentRow.getData();

                    return true;
                } else {
                    it.reset();

                    currentIndex--;

                    continue;
                }
            }

            currentData =
                rangeIterators[rangeIterators.length - 1].rangeVar.emptyData;
            currentRow = null;

            for (int i = 0; i < rangeIterators.length; i++) {
                rangeIterators[i].reset();
            }

            return false;
        }

        public void reset() {}
    }
}
