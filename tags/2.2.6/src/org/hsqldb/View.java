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
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.OrderedHashSet;

// fredt@users 20020420 - patch523880 by leptipre@users - VIEW support - modified
// fredt@users 20031227 - remimplementated as compiled query

/**
 * Represents an SQL VIEW based on a query expression
 *
 * @author leptipre@users
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 1.7.0
 */
public class View extends TableDerived {

    SubQuery       viewSubQuery;
    private String statement;

    //
    private HsqlName[] columnNames;

    /**
     * List of subqueries in this view in order of materialization. Last
     * element is the view itself.
     */
    SubQuery[] viewSubqueries;

    /**
     * Names of SCHEMA objects referenced in VIEW
     */
    private OrderedHashSet schemaObjectNames;

    /**
     * check option
     */
    private int check;

    //
    private Table baseTable;

    //
    Expression checkExpression;

    //
    boolean isTriggerInsertable;
    boolean isTriggerUpdatable;
    boolean isTriggerDeletable;

    View(Database db, HsqlName name, HsqlName[] columnNames, int check) {

        super(db, name, TableBase.VIEW_TABLE);

        this.columnNames = columnNames;
        this.check       = check;
    }

    public int getType() {
        return SchemaObject.VIEW;
    }

    public OrderedHashSet getReferences() {
        return schemaObjectNames;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    /**
     * Compiles the query expression and sets up the columns.
     */
    public void compile(Session session, SchemaObject parentObject) {

        ParserDQL p = new ParserDQL(session, new Scanner(statement));

        p.read();

        viewSubQuery    = p.XreadViewSubquery(this);
        queryExpression = viewSubQuery.queryExpression;

        if (getColumnCount() == 0) {
            if (columnNames == null) {
                columnNames =
                    viewSubQuery.queryExpression.getResultColumnNames();
            }

            if (columnNames.length
                    != viewSubQuery.queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593, getName().statementName);
            }

            TableUtil.setColumnsInSchemaTable(
                this, columnNames, queryExpression.getColumnTypes());
        }

        //
        OrderedHashSet set = queryExpression.getSubqueries();

        if (set == null) {
            viewSubqueries = new SubQuery[]{ viewSubQuery };
        } else {
            set.add(viewSubQuery);

            viewSubqueries = new SubQuery[set.size()];

            set.toArray(viewSubqueries);
            ArraySort.sort(viewSubqueries, 0, viewSubqueries.length,
                           viewSubqueries[0]);
        }

        for (int i = 0; i < viewSubqueries.length; i++) {
            if (viewSubqueries[i].parentView == null) {
                viewSubqueries[i].parentView = this;
            }

            viewSubqueries[i].prepareTable(session);
        }

        //
        viewSubQuery.getTable().view       = this;
        viewSubQuery.getTable().columnList = columnList;
        schemaObjectNames = p.compileContext.getSchemaObjectNames();
        baseTable                          = queryExpression.getBaseTable();

        if (baseTable == null) {
            return;
        }

        switch (check) {

            case SchemaObject.ViewCheckModes.CHECK_NONE :
                break;

            case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                checkExpression = queryExpression.getCheckCondition();
                break;

            case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_VIEW);
        sb.append(' ');
        sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
        sb.append('(');

        int count = getColumnCount();

        for (int j = 0; j < count; j++) {
            sb.append(getColumn(j).getName().statementName);

            if (j < count - 1) {
                sb.append(',');
            }
        }

        sb.append(')').append(' ').append(Tokens.T_AS).append(' ');
        sb.append(getStatement());

        return sb.toString();
    }

    public int[] getUpdatableColumns() {
        return queryExpression.getBaseTableColumnMap();
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public boolean isTriggerInsertable() {
        return isTriggerInsertable;
    }

    public boolean isTriggerUpdatable() {
        return isTriggerUpdatable;
    }

    public boolean isTriggerDeletable() {
        return isTriggerDeletable;
    }

    public boolean isInsertable() {
        return isTriggerInsertable ? false : super.isInsertable();
    }

    public boolean isUpdatable() {
        return isTriggerUpdatable ? false : super.isUpdatable();
    }


    void addTrigger(TriggerDef td, HsqlName otherName) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                if (isTriggerInsertable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerInsertable = true;
                break;

            case StatementTypes.DELETE_WHERE :
                if (isTriggerDeletable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerDeletable = true;
                break;

            case StatementTypes.UPDATE_WHERE :
                if (isTriggerUpdatable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerUpdatable = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.addTrigger(td, otherName);
    }

    void removeTrigger(TriggerDef td) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                isTriggerInsertable = false;
                break;

            case StatementTypes.DELETE_WHERE :
                isTriggerDeletable = false;
                break;

            case StatementTypes.UPDATE_WHERE :
                isTriggerUpdatable = false;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.removeTrigger(td);
    }

    public int getCheckOption() {
        return check;
    }

    /**
     * Returns the query expression for the view.
     */
    public String getStatement() {
        return statement;
    }

    public void setStatement(String sql) {
        statement = sql;
    }

    /**
     * Overridden to disable SET TABLE READONLY DDL for View objects.
     */
    public void setDataReadOnly(boolean value) {
        throw Error.error(ErrorCode.X_28000);
    }

    public void collectAllFunctionExpressions(OrderedHashSet collector) {

        // filter schemaObjectNames
    }

    public Table getSubqueryTable() {
        return viewSubQuery.getTable();
    }

    public SubQuery[] getSubqueries() {
        return viewSubqueries;
    }
}
