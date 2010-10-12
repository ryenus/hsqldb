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

import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.RangeVariable.RangeIteratorMain;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.Error;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.index.Index;
import org.hsqldb.store.ValuePool;
import org.hsqldb.ParserDQL.CompileContext;

public class RangeVariableJoined extends RangeVariable {

    RangeVariable[] rangeArray;

    RangeVariableJoined(Table table, SimpleName alias,
                        OrderedHashSet columnList,
                        SimpleName[] columnNameList,
                        CompileContext compileContext) {

        super(table, alias, columnList, columnNameList, compileContext);

        QuerySpecification qs =
            (QuerySpecification) this.rangeTable.getQueryExpression();

        this.rangeArray = qs.rangeVariables;
    }

    public void setRangeTableVariables() {
        super.setRangeTableVariables();
    }

    public RangeVariable duplicate() {

        RangeVariable r = null;

        try {
            r = (RangeVariable) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RangeVariable");
        }

        r.resetConditions();

        return r;
    }

    void setJoinType(boolean isLeft, boolean isRight) {
        super.setJoinType(isLeft, isRight);
    }

    public void addNamedJoinColumns(OrderedHashSet columns) {
        super.addNamedJoinColumns(columns);
    }

    public void addColumn(int columnIndex) {
        super.addColumn(columnIndex);
    }

    public void addAllColumns() {
        super.addAllColumns();
    }

    void addNamedJoinColumnExpression(String name, Expression e) {
        super.addNamedJoinColumnExpression(name, e);
    }

    ExpressionColumn getColumnExpression(String name) {
        return super.getColumnExpression(name);
    }

    Table getTable() {
        return super.getTable();
    }

    boolean hasIndexCondition() {
        return super.hasIndexCondition();
    }

    boolean setDistinctColumnsOnIndex(int[] colMap) {
        return super.setDistinctColumnsOnIndex(colMap);
    }

    /**
     * Used for sort
     */
    Index getSortIndex() {
        return super.getSortIndex();
    }

    /**
     * Used for sort
     */
    boolean setSortIndex(Index index, boolean reversed) {
        return super.setSortIndex(index, reversed);
    }

    boolean reverseOrder() {
        return super.reverseOrder();
    }

    public OrderedHashSet getColumnNames() {
        return super.getColumnNames();
    }

    public OrderedHashSet getUniqueColumnNameSet() {
        return super.getUniqueColumnNameSet();
    }

    public int findColumn(ExpressionColumn e) {

        if (tableAlias != null) {
            return super.findColumn(e);
        }

        int count = 0;

        for (int i = 0; i < rangeArray.length; i++) {
            int colIndex = rangeArray[i].findColumn(e);

            if (colIndex > -1) {
                return count + colIndex;
            }

            count += rangeArray[i].rangeTable.getColumnCount();
        }

        return -1;
    }

    /**
     * Retruns index for column
     *
     * @param columnName name of column
     * @return int index or -1 if not found
     */
    public int findColumn(String columnName) {
        return super.findColumn(columnName);
    }

    ColumnSchema getColumn(String columnName) {
        return super.getColumn(columnName);
    }

    ColumnSchema getColumn(int i) {
        return super.getColumn(i);
    }

    String getColumnAlias(int i) {
        return super.getColumnAlias(i);
    }

    public SimpleName getColumnAliasName(int i) {
        return super.getColumnAliasName(i);
    }

    boolean hasColumnAlias() {
        return super.hasColumnAlias();
    }

    String getTableAlias() {
        return super.getTableAlias();
    }

    SimpleName getTableAliasName() {
        return super.getTableAliasName();
    }

    boolean resolvesTableName(ExpressionColumn e) {

        if (tableAlias != null) {
            return super.resolvesTableName(e);
        }

        for (int i = 0; i < rangeArray.length; i++) {
            if (rangeArray[i].resolvesTableName(e)) {
                return true;
            }
        }

        return false;
    }

    public boolean resolvesTableName(String name) {

        if (tableAlias != null) {
            return super.resolvesTableName(name);
        }

        for (int i = 0; i < rangeArray.length; i++) {
            if (rangeArray[i].resolvesTableName(name)) {
                return true;
            }
        }

        return false;
    }

    boolean resolvesSchemaName(String name) {
        return super.resolvesSchemaName(name);
    }

    /**
     * Add all columns to a list of expressions
     */
    void addTableColumns(HsqlArrayList exprList) {
        super.addTableColumns(exprList);
    }

    /**
     * Add all columns to a list of expressions
     */
    int addTableColumns(HsqlArrayList exprList, int position,
                        HashSet exclude) {
        return super.addTableColumns(exprList, position, exclude);
    }

    void addTableColumns(Expression expression, HashSet exclude) {
        super.addTableColumns(expression, exclude);
    }

    /**
     * Removes reference to Index to avoid possible memory leaks after alter
     * table or drop index
     */
    void setForCheckConstraint() {
        super.setForCheckConstraint();
    }

    /**
     * used before condition processing
     */
    Expression getJoinCondition() {
        return super.getJoinCondition();
    }

    void addJoinCondition(Expression e) {
        super.addJoinCondition(e);
    }

    void resetConditions() {
        super.resetConditions();
    }

    OrderedHashSet getSubqueries() {
        return super.getSubqueries();
    }

    public void replaceColumnReference(RangeVariable range,
                                       Expression[] list) {}

    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {
        super.replaceRangeVariables(ranges, newRanges);
    }

    public void resolveRangeTable(Session session,
                                  RangeVariable[] rangeVariables,
                                  int rangeCount,
                                  RangeVariable[] outerRanges) {
        super.resolveRangeTable(session, rangeVariables, rangeCount,
                                RangeVariable.emptyArray);
    }

    /**
     * Retreives a String representation of this obejct. <p>
     *
     * The returned String describes this object's table, alias
     * access mode, index, join mode, Start, End and And conditions.
     *
     * @return a String representation of this object
     */
    public String describe(Session session, int blanks) {

        RangeVariableConditions[] conditionsArray = joinConditions;
        StringBuffer              sb;
        String b = ValuePool.spaceString.substring(0, blanks);

        sb = new StringBuffer();

        String temp = "INNER";

        if (isLeftJoin) {
            temp = "LEFT OUTER";

            if (isRightJoin) {
                temp = "FULL";
            }
        } else if (isRightJoin) {
            temp = "RIGHT OUTER";
        }

        sb.append(b).append("join type=").append(temp).append("\n");
        sb.append(b).append("table=").append(rangeTable.getName().name).append(
            "\n");

        if (tableAlias != null) {
            sb.append(b).append("alias=").append(tableAlias.name).append("\n");
        }

        boolean fullScan = !conditionsArray[0].hasIndexCondition();

        sb.append(b).append("access=").append(fullScan ? "FULL SCAN"
                                                       : "INDEX PRED").append(
                                                       "\n");

        for (int i = 0; i < conditionsArray.length; i++) {
            RangeVariableConditions conditions = this.joinConditions[i];

            if (i > 0) {
                sb.append(b).append("OR condition = [");
            } else {
                sb.append(b).append("condition = [");
            }

            sb.append(conditions.describe(session, blanks + 2));
            sb.append(b).append("]\n");
        }

        return sb.toString();
    }

    public RangeIteratorMain getIterator(Session session) {
        return super.getIterator(session);
    }
}
