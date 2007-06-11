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

import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.navigator.DataRowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.HsqlNameManager.HsqlName;

/**
 * Represents an SQL view or anonymous subquery (inline virtual table
 * descriptor) nested within an SQL statement. <p>
 *
 * Implements {@link org.hsqldb.lib.ObjectComparator ObjectComparator} to
 * provide the correct order of materialization for nested views / subqueries.
 *
 * @author boucherb@users
 * @author fredt@users
 */
class SubQuery implements ObjectComparator {

    int      level;
    boolean  isCorrelated;
    boolean  isExistsPredicate;
    boolean  uniqueRows;
    Select   select;
    Database database;
    Table    table;
    View     view;
    View     parentView;

    // IN condition optimisation
    Expression dataExpression;

    SubQuery() {}

    SubQuery(Database database, int level, boolean isCorrelated,
             boolean isExists, boolean uniqueRows, Select select,
             View view) throws HsqlException {

        this.level             = level;
        this.isCorrelated      = isCorrelated;
        this.isExistsPredicate = isExists;
        this.uniqueRows        = uniqueRows;
        this.select            = select;
        this.database          = database;
        this.view              = view;

        HsqlName name;

        if (view == null) {
            name = database.nameManager.newSubqueryTableName();
        } else {
            name = view.getName();
        }

        table = new Table(database, name, Table.SYSTEM_SUBQUERY);

        if (!isCorrelated) {
            resolveAndPrepare();
        }
    }

    SubQuery(Database database, int level,
             Expression dataExpression) throws HsqlException {

        this.level              = level;
        this.isCorrelated       = false;
        this.dataExpression     = dataExpression;
        dataExpression.subQuery = this;
        table                   = TableUtil.newSubqueryTable(database);

        TableUtil.setTableColumnsAsExpression(table, dataExpression,
                                              uniqueRows);
    }

    public void resolveAndPrepare() throws HsqlException {

        if (table == null) {
            return;
        }

        if (select != null) {
            select.resolveTypesAndPrepare();
        }

        if (table.columnCount == 0) {
            TableUtil.setTableColumns(table, select, uniqueRows);
        }
    }

    void setAsInSubquery(Expression e) throws HsqlException {

        dataExpression = e;
        table          = TableUtil.newSubqueryTable(database);

        TableUtil.setTableColumnsAsExpression(table, dataExpression,
                                              uniqueRows);

        isCorrelated = false;
    }

    /**
     * Fills the table with a result set
     */
    void materialise(Session session) throws HsqlException {

        //IN condition optimisation and table constructors
        if (dataExpression != null) {
            dataExpression.insertValuesIntoSubqueryTable(session);

            return;
        }

        Result r = select.getResult(session, isExistsPredicate ? 1
                                                               : 0);

        if (uniqueRows) {
            ((DataRowSetNavigator) r.getNavigator()).removeDuplicates();
        }

        table.insertResult(session, r);
    }

    boolean hasUniqueNotNullRows(Session session) throws HsqlException {

        Result r = select.getResult(session, 0);
        boolean result =
            ((DataRowSetNavigator) r.getNavigator()).hasUniqueNotNullRows();

        return result;
    }

    boolean hasRows(Session session) throws HsqlException {

        Result  r      = select.getResult(session, 1);
        boolean result = r.getNavigator().hasNext();

        return result;
    }

    Object getSingleObjectResult(Session session) throws HsqlException {
        return select.getValue(session);
    }

    void dematerialiseCorrelated(Session session) {

        if (isCorrelated && table != null) {
            table.clearAllData(session);
        }
    }

    void dematerialiseAll(Session session) {

        if (table != null) {
            table.clearAllData(session);
        }
    }

    /**
     * This results in the following sort order:
     *
     * view subqueries, then other subqueries
     *
     *    view subqueries:
     *        views sorted by creation order (earlier declaration first)
     *
     *    other subqueries:
     *        subqueries sorted by depth within select query (deep == higher level)
     *
     */
    public int compare(Object a, Object b) {

        SubQuery sqa = (SubQuery) a;
        SubQuery sqb = (SubQuery) b;

        if (sqa.parentView == null && sqb.parentView == null) {
            return sqb.level - sqa.level;
        } else if (sqa.parentView != null && sqb.parentView != null) {
            Database db = sqa.parentView.database;
            int      ia = db.schemaManager.getTableIndex(sqa.parentView);
            int      ib = db.schemaManager.getTableIndex(sqb.parentView);

            if (ia == -1) {
                ia = db.schemaManager.getTables(
                    sqa.parentView.getSchemaName().name).size();
            }

            if (ib == -1) {
                ib = db.schemaManager.getTables(
                    sqb.parentView.getSchemaName().name).size();
            }

            int diff = ia - ib;

            return diff == 0 ? sqb.level - sqa.level
                             : diff;
        } else {
            return sqa.parentView == null ? 1
                                          : -1;
        }
    }
}
