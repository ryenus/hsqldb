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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

/**
 * Table with data derived from a query expression.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.9.0
 */
public class TableDerived extends Table {

    QueryExpression queryExpression;
    View            view;
    SubQuery        subQuery;

    public TableDerived(Database database, HsqlName name, int type) {

        super(database, name, type);

        switch (type) {

            // for special use, not INFORMATION_SCHEMA views
            case TableBase.CHANGE_SET_TABLE :
            case TableBase.SYSTEM_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.VIEW_TABLE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }
    }

    public TableDerived(Database database, HsqlName name, int type,
                        QueryExpression queryExpression, SubQuery subQuery) {

        super(database, name, type);

        switch (type) {

            case TableBase.CHANGE_SET_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.RESULT_TABLE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        this.queryExpression = queryExpression;
        this.subQuery        = subQuery;
    }

    public TableDerived(Database database, HsqlName name, int type,
                        Type[] columnTypes, HashMappedList columnList) {
        this(database, name, type, columnTypes, columnList,
             ValuePool.emptyIntArray);
    }

    public TableDerived(Database database, HsqlName name, int type,
                        Type[] columnTypes, HashMappedList columnList,
                        int[] pkColumns) {

        this(database, name, type, (QueryExpression) null, (SubQuery) null);

        this.colTypes          = columnTypes;
        this.columnList        = columnList;
        columnCount            = columnList.size();

        createPrimaryKey(null, pkColumns, true);
    }

    public int getId() {
        return 0;
    }

    public boolean isWritable() {
        return true;
    }

    public boolean isInsertable() {
        return queryExpression == null ? false
                                       : queryExpression.isInsertable();
    }

    public boolean isUpdatable() {
        return queryExpression == null ? false
                                       : queryExpression.isUpdatable();
    }

    public int[] getUpdatableColumns() {
        return defaultColumnMap;
    }

    public Table getBaseTable() {
        return queryExpression == null ? this
                                       : queryExpression.getBaseTable();
    }

    public int[] getBaseTableColumnMap() {

        return queryExpression == null ? null
                                       : queryExpression
                                           .getBaseTableColumnMap();
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public QueryExpression getQueryExpression() {
        return queryExpression;
    }
}
