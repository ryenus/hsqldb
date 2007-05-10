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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;

/*
 * Utility functions to set up special tables.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class TableUtil {

    static Table newTable(Database database, int type,
                          HsqlName tableHsqlName) throws HsqlException {

        switch (type) {

            case Table.TEMP_TEXT_TABLE :
            case Table.TEXT_TABLE : {
                return new TextTable(database, tableHsqlName, type);
            }
            default : {
                return new Table(database, tableHsqlName, type);
            }
        }
    }

    static Table newSubqueryTable(Database database) throws HsqlException {

        HsqlName sqtablename = database.nameManager.newHsqlName(
            database.schemaManager.SYSTEM_SCHEMA_HSQLNAME, "SYSTEM_SUBQUERY",
            false, SchemaObject.TABLE);

        try {
            return new Table(database, sqtablename, Table.SYSTEM_SUBQUERY);
        } catch (Exception e) {
            return null;
        }
    }

    static Table newLookupTable(Database database) {

        try {
            Table table = TableUtil.newSubqueryTable(database);
            Column column =
                new Column(table.database.nameManager.getAutoColumnName(0),
                           Type.SQL_INTEGER, false, true, null);

            table.addColumn(column);
            table.createPrimaryKey(new int[]{ 0 });

            return table;
        } catch (HsqlException e) {
            return null;
        }
    }

    /**
     * Currently used only by org.hsqldb.SubQuery constructor
     */
    static void setTableColumns(Table table, Select select,
                                boolean uniqueRows) throws HsqlException {

        if (select == null) {
            return;
        }

        addColumns(table, select);

        int[] colIndexes = null;

        if (uniqueRows) {
            colIndexes = table.getNewColumnMap();

            ArrayUtil.fillSequence(colIndexes);
        }

        table.createPrimaryKey(colIndexes);

        // table doesn't need a Primary constraint object
    }

    // IN condition optimisation

    /**
     *
     */
    static void setTableColumnsAsExpression(Table table, Expression e,
            boolean uniqueRows) throws HsqlException {

        if (table.getColumnCount() != 0) {
            return;
        }

        for (int i = 0; i < e.argListDataType.length; i++) {
            Column column =
                new Column(table.database.nameManager.getAutoColumnName(i),
                           e.argListDataType[i], true, false, null);

            table.addColumn(column);
        }

        int[] colIndexes = null;

        if (uniqueRows) {
            colIndexes = table.getNewColumnMap();

            ArrayUtil.fillSequence(colIndexes);
        }

        table.createPrimaryKey(colIndexes);

        // table doesn't need a Primary constraint object
    }

    /**
     *  Add a set of columns based on a ResultMetaData
     */
    public static void addColumns(Table table, ResultMetaData metadata,
                                  int count) throws HsqlException {

        for (int i = 0; i < count; i++) {
            Column column =
                new Column(table.database.nameManager
                    .newHsqlName(table.getSchemaName(), metadata
                        .colLabels[i], metadata.isLabelQuoted[i], SchemaObject
                        .COLUMN), metadata.colTypes[i], true, false, null);

            table.addColumn(column);
        }
    }

    /**
     *  Add an array of columns
     */
    public static void addColumns(Table table,
                                  Column[] columns) throws HsqlException {

        for (int i = 0; i < columns.length; i++) {
            table.addColumn(columns[i]);
        }
    }

    /**
     *  Adds a set of columns based on a compiled Select
     */
    static void addColumns(Table table, Select select) throws HsqlException {

        for (int i = 0; i < select.visibleColumnCount; i++) {
            Expression e = select.exprColumns[i];
            Column column =
                new Column(table.database.nameManager
                    .newHsqlName(table.getSchemaName(), e.getAlias(), e
                        .isAliasQuoted(), SchemaObject.COLUMN), e
                            .dataType, true, false, null);

            table.addColumn(column);
        }
    }
}
