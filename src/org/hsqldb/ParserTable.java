/* Copyright (c) 2001-2025, The HSQL Development Group
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
import org.hsqldb.error.HsqlException;
import org.hsqldb.index.Index;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Parser for SQL table definition
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class ParserTable extends ParserDML {

    ParserTable(Session session, Scanner scanner) {
        super(session, scanner);
    }

    StatementSchema compileCreateTable(int type) {

        boolean  ifNot = readIfNotExists();
        HsqlName name  = readNewSchemaObjectName(SchemaObject.TABLE, false);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Table table;

        switch (type) {

            case TableBase.TEMP_TEXT_TABLE :
            case TableBase.TEXT_TABLE : {
                table = new TextTable(database, name, type);
                break;
            }

            default : {
                table = new Table(database, name, type);
            }
        }

        if (token.tokenType == Tokens.AS) {
            return compileCreateTableAsSubqueryDefinition(table, ifNot);
        }

        return compileCreateTableBody(table, ifNot);
    }

    StatementSchema compileCreateTableBody(Table table, boolean ifNot) {

        HsqlArrayList<Constraint> tempConstraints = new HsqlArrayList<>();
        HsqlArrayList<Constraint> tempIndexes     = new HsqlArrayList<>();
        Statement                 statement       = null;
        HsqlName[]                readLockNames   = null;
        boolean isTable = readTableContentsSource(
            table,
            tempConstraints,
            tempIndexes);

        if (!isTable) {
            return compileCreateTableAsSubqueryDefinition(table, ifNot);
        }

        readTableVersioningClause(table);

        if (token.tokenType == Tokens.AS) {
            read();

            QueryExpression queryExpression = null;

            queryExpression = readTableQuery();

            readThis(Tokens.WITH);
            readThis(Tokens.DATA);

            statement = new StatementQuery(
                session,
                queryExpression,
                compileContext);
            readLockNames = statement.getTableNamesForRead();

            Type[] dataTypes = queryExpression.getColumnTypes();

            if (table.getColumnCount() != dataTypes.length) {
                throw Error.error(ErrorCode.X_42593);
            }

            for (int i = 0; i < dataTypes.length; i++) {
                boolean b = table.getColumn(i)
                                 .getDataType()
                                 .canBeAssignedFrom(dataTypes[i]);

                if (!b) {
                    throw Error.error(ErrorCode.X_42561);
                }
            }
        }

        readTableOnCommitClause(table);

        if (database.sqlSyntaxMys) {
            OrderedHashMap<String, Token> list = super.readPropertyValuePairs(
                true,
                false);

            if (list != null) {
                Token value = list.get(Tokens.T_COMMENT);

                if (value != null) {
                    String comment = value.tokenString;

                    table.getName().comment = comment;
                }
            }
        }

        OrderedHashSet<HsqlName> names = new OrderedHashSet<>();

        names.add(database.getCatalogName());

        for (int i = 0; i < tempConstraints.size(); i++) {
            Constraint c    = tempConstraints.get(i);
            HsqlName   name = c.getMainTableName();

            if (name != null) {
                Table t = database.schemaManager.findUserTable(
                    name.name,
                    name.schema.name);

                if (t != null && !t.isTemp()) {
                    names.add(table.getName());
                }
            }
        }

        String     sql            = getLastPart();
        Object[] args = new Object[]{ table, tempConstraints, tempIndexes,
                                      statement, Boolean.valueOf(ifNot) };
        HsqlName[] writeLockNames = new HsqlName[names.size()];

        names.toArray(writeLockNames);

        return new StatementSchema(
            sql,
            StatementTypes.CREATE_TABLE,
            args,
            readLockNames,
            writeLockNames);
    }

    boolean readTableContentsSource(
            Table table,
            HsqlArrayList<Constraint> tempConstraints,
            HsqlArrayList<Constraint> tempIndexes) {

        int position = getPosition();

        readThis(Tokens.OPENBRACKET);

        Constraint c = new Constraint(
            null,
            null,
            SchemaObject.ConstraintTypes.TEMP);

        tempConstraints.add(c);

        boolean start       = true;
        boolean startPart   = true;
        boolean end         = false;
        boolean hasRowStart = false;
        boolean hasRowEnd   = false;

        while (!end) {
            switch (token.tokenType) {

                case Tokens.LIKE : {
                    ColumnSchema[] likeColumns = readLikeTable(table);

                    for (int i = 0; i < likeColumns.length; i++) {
                        table.addColumn(likeColumns[i]);
                    }

                    start     = false;
                    startPart = false;
                    continue;
                }

                case Tokens.CONSTRAINT :
                case Tokens.PRIMARY :
                case Tokens.FOREIGN :
                case Tokens.UNIQUE :
                case Tokens.CHECK :
                    if (!startPart) {
                        throw unexpectedToken();
                    }

                    readConstraint(table, tempConstraints);

                    start     = false;
                    startPart = false;
                    continue;
                case Tokens.PERIOD :
                    if (!startPart) {
                        throw unexpectedToken();
                    }

                    if (table.isTemp() || table.isText()) {
                        throw unexpectedToken();
                    }

                    PeriodDefinition period = readAndAddPeriod(table);

                    if (period == null) {
                        break;
                    }

                    start     = false;
                    startPart = false;
                    continue;
                case Tokens.COMMA :
                    if (startPart) {
                        throw unexpectedToken();
                    }

                    read();

                    startPart = true;
                    continue;
                case Tokens.CLOSEBRACKET :
                    read();

                    end = true;
                    continue;
                case Tokens.KEY :
                case Tokens.INDEX :
                    if (database.sqlSyntaxMys) {
                        readIndex(table, tempIndexes);

                        start     = false;
                        startPart = false;
                        continue;
                    }
            }

            if (!startPart) {
                throw unexpectedToken();
            }

            checkIsSchemaObjectName();

            HsqlName hsqlName = database.nameManager.newColumnHsqlName(
                table.getName(),
                token.tokenString,
                isDelimitedIdentifier());

            read();

            ColumnSchema newcolumn = readColumnDefinitionOrNull(
                table,
                hsqlName,
                tempConstraints);

            if (newcolumn == null) {
                if (start) {
                    rewind(position);

                    return false;
                } else {
                    throw Error.error(ErrorCode.X_42000);
                }
            }

            table.addColumn(newcolumn);

            start     = false;
            startPart = false;

            if (newcolumn.getSystemPeriodType()
                    == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START) {
                if (hasRowStart) {
                    throw Error.error(ErrorCode.X_42591);
                }

                hasRowStart = true;
            } else if (newcolumn.getSystemPeriodType()
                       == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END) {
                if (hasRowEnd) {
                    throw Error.error(ErrorCode.X_42591);
                }

                hasRowEnd = true;
            }
        }

        if (table.getColumnCount() == 0) {
            throw Error.error(ErrorCode.X_42591);
        }

        if (hasRowStart ^ hasRowEnd) {
            throw Error.error(ErrorCode.X_42516);
        }

        if (hasRowStart && table.systemPeriod == null) {
            throw Error.error(ErrorCode.X_42516);
        }

        setPeriodColumns(table, table.systemPeriod);
        setPeriodColumns(table, table.applicationPeriod);

        // not supported
        if (table.applicationPeriod != null) {
            throw Error.error(
                ErrorCode.X_0A501,
                table.applicationPeriod.getName().name);
        }

        return true;
    }

    /**
     * check period columns have already been defined in table and have the
     * supported data types
     */
    void setPeriodColumns(Table table, PeriodDefinition period) {

        if (period == null) {
            return;
        }

        OrderedHashSet<String> set         = period.columnNames;
        ColumnSchema           startColumn = null;
        ColumnSchema           endColumn   = null;
        HsqlName               name        = period.getName();
        int                    index       = table.findColumn(name.name);

        if (index >= 0) {
            throw Error.error(ErrorCode.X_42516, name.name);
        }

        for (int i = 0; i < 2; i++) {
            String columnName = set.get(i);

            index = table.findColumn(columnName);

            if (index < 0) {
                throw Error.error(ErrorCode.X_42501, columnName);
            }

            ColumnSchema column = table.getColumn(index);
            Type         type   = column.getDataType();

            switch (period.periodType) {

                case SchemaObject.PeriodType.PERIOD_SYSTEM :
                    if (!type.isTimestampType()) {
                        throw Error.error(ErrorCode.X_42516, columnName);
                    }

                    if (i == 0) {
                        if (column.getSystemPeriodType()
                                != SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START) {
                            throw Error.error(ErrorCode.X_42516, columnName);
                        }
                    } else {
                        if (column.getSystemPeriodType()
                                != SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END) {
                            throw Error.error(ErrorCode.X_42516, columnName);
                        }
                    }

                    break;

                case SchemaObject.PeriodType.PERIOD_APPLICATION :
                    if (!type.isDateOrTimestampType()) {
                        throw Error.error(ErrorCode.X_42516, columnName);
                    }

                    break;
            }

            if (i == 0) {
                startColumn = column;
            } else {
                endColumn = column;

                if (!startColumn.getDataType().equals(
                        endColumn.getDataType())) {
                    throw Error.error(ErrorCode.X_42516, columnName);
                }
            }
        }

        period.startColumn = startColumn;
        period.endColumn   = endColumn;
    }

    /**
     * check the system period name and column names do not exist
     */
    void checkPeriodColumnsAdd(Table table, PeriodDefinition period) {

        if (period == null) {
            return;
        }

        OrderedHashSet<String> set   = period.columnNames;
        HsqlName               name  = period.getName();
        int                    index = table.findColumn(name.name);

        if (index >= 0) {
            throw Error.error(ErrorCode.X_42516, name.name);
        }

        for (int i = 0; i < 2; i++) {
            String columnName = set.get(i);

            index = table.findColumn(columnName);

            if (index >= 0) {
                throw Error.error(ErrorCode.X_42504, columnName);
            }
        }
    }

    void readTableVersioningClause(Table table) {

        if (table.systemPeriod != null) {
            if (readIfThis(Tokens.WITH)) {
                readThis(Tokens.SYSTEM);
                readThis(Tokens.VERSIONING);

                table.isSystemVersioned = true;
            }
        }
    }

    void readTableOnCommitClause(Table table) {

        if (token.tokenType == Tokens.ON) {
            if (!table.isTemp()) {
                throw unexpectedToken();
            }

            read();
            readThis(Tokens.COMMIT);

            if (token.tokenType == Tokens.DELETE) {}
            else if (token.tokenType == Tokens.PRESERVE) {
                table.persistenceScope = TableBase.SCOPE_SESSION;
            }

            read();
            readThis(Tokens.ROWS);
        }
    }

    private ColumnSchema[] readLikeTable(Table table) {

        read();

        boolean           generated = false;
        boolean           identity  = false;
        boolean           defaults  = false;
        Table             likeTable = readTableName();
        OrderedIntHashSet set       = new OrderedIntHashSet();

        while (true) {
            boolean including = token.tokenType == Tokens.INCLUDING;

            if (!including && token.tokenType != Tokens.EXCLUDING) {
                break;
            }

            read();

            switch (token.tokenType) {

                case Tokens.GENERATED :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    generated = including;
                    break;

                case Tokens.IDENTITY :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    identity = including;
                    break;

                case Tokens.DEFAULTS :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    defaults = including;
                    break;

                default :
                    throw unexpectedToken();
            }

            read();
        }

        ColumnSchema[] columnList =
            new ColumnSchema[likeTable.getColumnCount()];

        for (int i = 0; i < columnList.length; i++) {
            ColumnSchema column = likeTable.getColumn(i).duplicate();
            HsqlName name = database.nameManager.newColumnSchemaHsqlName(
                table.getName(),
                column.getName());

            column.setName(name);
            column.setPrimaryKey(false);

            if (identity) {
                if (column.isIdentity()) {
                    column.setIdentity(
                        column.getIdentitySequence().duplicate());
                }
            } else {
                column.setIdentity(null);
            }

            if (!defaults) {
                column.setDefaultExpression(null);
            }

            if (!generated) {
                column.setGeneratingExpression(null);
            }

            columnList[i] = column;
        }

        return columnList;
    }

    StatementSchema compileCreateTableAsSubqueryDefinition(
            Table table,
            boolean ifNotExists) {

        HsqlName[]      readName        = null;
        boolean         withData        = true;
        HsqlName[]      columnNames     = null;
        Statement       statement       = null;
        QueryExpression queryExpression = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            columnNames = readColumnNames(table.getName());
        }

        readThis(Tokens.AS);

        queryExpression = readTableQuery();

        readThis(Tokens.WITH);

        if (token.tokenType == Tokens.NO) {
            read();

            withData = false;
        } else if (table.getTableType() == TableBase.TEXT_TABLE) {
            throw unexpectedTokenRequire(Tokens.T_NO);
        }

        readThis(Tokens.DATA);

        if (token.tokenType == Tokens.ON) {
            if (!table.isTemp()) {
                throw unexpectedToken();
            }

            read();
            readThis(Tokens.COMMIT);

            if (token.tokenType == Tokens.DELETE) {}
            else if (token.tokenType == Tokens.PRESERVE) {
                table.persistenceScope = TableBase.SCOPE_SESSION;
            }

            read();
            readThis(Tokens.ROWS);
        }

        if (columnNames == null) {
            columnNames = queryExpression.getResultColumnNames();
        } else {
            if (columnNames.length != queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593);
            }
        }

        TableUtil.setColumnsInSchemaTable(
            table,
            columnNames,
            queryExpression.getColumnTypes());
        table.createPrimaryKey();

        if (table.isTemp() && table.hasLobColumn()) {
            throw Error.error(ErrorCode.X_42534);
        }

        if (withData) {
            statement = new StatementQuery(
                session,
                queryExpression,
                compileContext);
            readName = statement.getTableNamesForRead();
        }

        Object[] args = new Object[]{ table, new HsqlArrayList<Constraint>(),
                                      new HsqlArrayList<Constraint>(),
                                      statement, Boolean.valueOf(ifNotExists) };
        String     sql            = getLastPart();
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;
        StatementSchema st = new StatementSchema(
            sql,
            StatementTypes.CREATE_TABLE,
            args,
            readName,
            writeLockNames);

        return st;
    }

    private QueryExpression readTableQuery() {

        readThis(Tokens.OPENBRACKET);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setReturningResult();
        queryExpression.resolve(session);
        readThis(Tokens.CLOSEBRACKET);

        return queryExpression;
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    static Table addTableConstraintDefinitions(
            Session session,
            Table table,
            HsqlArrayList<Constraint> tempConstraints,
            HsqlArrayList<Constraint> constraintList,
            boolean addToSchema) {

        Constraint c = tempConstraints.get(0);
        HsqlName indexName =
            session.database.nameManager.newConstraintIndexName(
                table.getName(),
                c.getName(),
                session.database.sqlSysIndexNames);

        c.setColumnsIndexes(table);
        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(
                c.getName(),
                table,
                table.getPrimaryIndex(),
                SchemaObject.ConstraintTypes.PRIMARY_KEY);

            table.addConstraint(newconstraint);

            if (addToSchema) {
                session.database.schemaManager.addSchemaObject(newconstraint);
            }
        }

        for (int i = 1; i < tempConstraints.size(); i++) {
            c = tempConstraints.get(i);

            switch (c.getConstraintType()) {

                case SchemaObject.ConstraintTypes.UNIQUE : {
                    c.setColumnsIndexes(table);

                    if (table.getUniqueConstraintForColumns(c.core.mainCols)
                            != null) {
                        throw Error.error(ErrorCode.X_42522);
                    }

                    // create an autonamed index
                    indexName =
                        session.database.nameManager.newConstraintIndexName(
                            table.getName(),
                            c.getName(),
                            session.database.sqlSysIndexNames);

                    Index index = table.createAndAddIndexStructure(
                        session,
                        indexName,
                        c.core.mainCols,
                        null,
                        null,
                        true,
                        true,
                        false);
                    Constraint newconstraint = new Constraint(
                        c.getName(),
                        table,
                        index,
                        SchemaObject.ConstraintTypes.UNIQUE);

                    table.addConstraint(newconstraint);

                    if (addToSchema) {
                        session.database.schemaManager.addSchemaObject(
                            newconstraint);
                    }

                    break;
                }

                case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                    addForeignKey(session, table, c, constraintList);
                    break;
                }

                case SchemaObject.ConstraintTypes.CHECK : {
                    try {
                        c.prepareCheckConstraint(session, table);
                    } catch (HsqlException e) {
                        if (session.isProcessingScript()) {
                            break;
                        }

                        throw e;
                    }

                    table.addConstraint(c);

                    if (c.isNotNull()) {
                        ColumnSchema column = table.getColumn(
                            c.notNullColumnIndex);

                        column.setNullable(false);
                        table.setColumnTypeVars(c.notNullColumnIndex);
                    }

                    if (addToSchema) {
                        session.database.schemaManager.addSchemaObject(c);
                    }

                    break;
                }
            }
        }

        return table;
    }

    static void addForeignKey(
            Session session,
            Table table,
            Constraint c,
            HsqlArrayList<Constraint> constraintList) {

        HsqlName mainTableName = c.getMainTableName();

        if (mainTableName == table.getName()) {
            c.core.mainTable = table;
        } else {
            Table mainTable = session.database.schemaManager.findUserTable(
                mainTableName.name,
                mainTableName.schema.name);

            if (mainTable == null) {
                if (constraintList == null) {
                    throw Error.error(ErrorCode.X_42501, mainTableName.name);
                }

                constraintList.add(c);

                return;
            }

            c.core.mainTable = mainTable;
        }

        c.setColumnsIndexes(table);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.checkCreateForeignKey(table, c);

        Constraint uniqueConstraint =
            c.core.mainTable.getUniqueConstraintForColumns(
                c.core.mainCols);

        if (uniqueConstraint == null) {
            throw Error.error(ErrorCode.X_42523);
        }

        Index mainIndex = uniqueConstraint.getMainIndex();
        boolean isForward = c.core.mainTable.getSchemaName()
                            != table.getSchemaName();
        int   offset    = session.database.schemaManager.getTableIndex(table);

        if (offset != -1
                && offset
                   < session.database.schemaManager.getTableIndex(
                       c.core.mainTable)) {
            isForward = true;
        }

        HsqlName refIndexName =
            session.database.nameManager.newConstraintIndexName(
                table.getName(),
                c.getName(),
                session.database.sqlSysIndexNames);
        Index index = table.createAndAddIndexStructure(
            session,
            refIndexName,
            c.core.refCols,
            null,
            null,
            false,
            true,
            isForward);
        HsqlName mainName = session.database.nameManager.newAutoName(
            "REF",
            c.getName().name,
            table.getSchemaName(),
            table.getName(),
            SchemaObject.INDEX);

        c.core.uniqueName = uniqueConstraint.getName();
        c.core.mainName   = mainName;
        c.core.mainIndex  = mainIndex;
        c.core.refTable   = table;
        c.core.refName    = c.getName();
        c.core.refIndex   = index;
        c.isForward       = isForward;

        table.addConstraint(c);
        c.core.mainTable.addConstraint(new Constraint(mainName, c));
        session.database.schemaManager.addSchemaObject(c);
    }

    Constraint readFKReferences(
            Table refTable,
            HsqlName constraintName,
            OrderedHashSet<String> refColSet) {

        HsqlName               mainTableName;
        OrderedHashSet<String> mainColSet = null;

        readThis(Tokens.REFERENCES);

        HsqlName schema;

        if (token.namePrefix == null) {
            schema = refTable.getSchemaName();
        } else {
            schema = database.schemaManager.getSchemaHsqlName(token.namePrefix);
        }

        if (refTable.getSchemaName() == schema
                && refTable.getName().name.equals(token.tokenString)) {
            mainTableName = refTable.getName();

            read();
        } else {
            mainTableName = readFKTableName(schema);
        }

        if (token.tokenType == Tokens.OPENBRACKET) {
            mainColSet = readColumnNames(false);
        }

        int matchType = OpTypes.MATCH_SIMPLE;

        if (token.tokenType == Tokens.MATCH) {
            read();

            switch (token.tokenType) {

                case Tokens.SIMPLE :
                    read();
                    break;

                case Tokens.PARTIAL :
                    throw unsupportedFeature();

                case Tokens.FULL :
                    read();

                    matchType = OpTypes.MATCH_FULL;
                    break;

                default :
                    throw unexpectedToken();
            }
        }

        // -- In a while loop we parse a maximium of two
        // -- "ON" statements following the foreign key
        // -- definition this can be
        // -- ON [UPDATE|DELETE] [NO ACTION|RESTRICT|CASCADE|SET [NULL|DEFAULT]]
        int deleteAction      = SchemaObject.ReferentialAction.NO_ACTION;
        int updateAction      = SchemaObject.ReferentialAction.NO_ACTION;
        OrderedIntHashSet set = new OrderedIntHashSet();

        while (token.tokenType == Tokens.ON) {
            read();

            if (!set.add(token.tokenType)) {
                throw unexpectedToken();
            }

            if (token.tokenType == Tokens.DELETE) {
                read();

                if (token.tokenType == Tokens.SET) {
                    read();

                    switch (token.tokenType) {

                        case Tokens.DEFAULT : {
                            read();

                            deleteAction =
                                SchemaObject.ReferentialAction.SET_DEFAULT;
                            break;
                        }

                        case Tokens.NULL :
                            read();

                            deleteAction =
                                SchemaObject.ReferentialAction.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    deleteAction = SchemaObject.ReferentialAction.CASCADE;
                } else if (token.tokenType == Tokens.RESTRICT) {
                    read();
                } else {
                    readThis(Tokens.NO);
                    readThis(Tokens.ACTION);
                }
            } else if (token.tokenType == Tokens.UPDATE) {
                read();

                if (token.tokenType == Tokens.SET) {
                    read();

                    switch (token.tokenType) {

                        case Tokens.DEFAULT : {
                            read();

                            updateAction =
                                SchemaObject.ReferentialAction.SET_DEFAULT;
                            break;
                        }

                        case Tokens.NULL :
                            read();

                            updateAction =
                                SchemaObject.ReferentialAction.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    updateAction = SchemaObject.ReferentialAction.CASCADE;
                } else if (token.tokenType == Tokens.RESTRICT) {
                    read();
                } else {
                    readThis(Tokens.NO);
                    readThis(Tokens.ACTION);
                }
            } else {
                throw unexpectedToken();
            }
        }

        if (readIfThis(Tokens.NOT)) {
            readThis(Tokens.DEFERRABLE);
        }

        if (constraintName == null) {
            constraintName = database.nameManager.newAutoName(
                "FK",
                refTable.getSchemaName(),
                refTable.getName(),
                SchemaObject.CONSTRAINT);
        }

        return new Constraint(
            constraintName,
            refTable.getName(),
            refColSet,
            mainTableName,
            mainColSet,
            SchemaObject.ConstraintTypes.FOREIGN_KEY,
            deleteAction,
            updateAction,
            matchType);
    }

    HsqlName readFKTableName(HsqlName schema) {

        HsqlName name;

        checkIsSchemaObjectName();

        Table table = database.schemaManager.findUserTable(
            token.tokenString,
            schema.name);

        if (table == null) {
            name = database.nameManager.newHsqlName(
                schema,
                token.tokenString,
                isDelimitedIdentifier(),
                SchemaObject.TABLE);
        } else {
            name = table.getName();
        }

        read();

        return name;
    }

    /**
     * Responsible for handling the creation of table columns during the process
     * of executing CREATE TABLE or ADD COLUMN etc. statements.
     *
     * @param table this table
     * @param hsqlName column name
     * @param constraintList list of constraints
     * @return a Column object with indicated attributes
     */
    ColumnSchema readColumnDefinitionOrNull(
            Table table,
            HsqlName hsqlName,
            HsqlArrayList<Constraint> constraintList) {

        boolean           isGenerated     = false;
        boolean           isIdentity      = false;
        boolean           isPKIdentity    = false;
        boolean           generatedAlways = false;
        Expression        generateExpr    = null;
        int sysPeriodType = SchemaObject.PeriodSystemColumnType.PERIOD_ROW_NONE;
        boolean           isNullable      = true;
        Expression        defaultExpr     = null;
        ExpressionLogical colConstraint   = null;
        Type              typeObject      = null;
        NumberSequence    sequence        = null;

        switch (token.tokenType) {

            case Tokens.GENERATED : {
                read();
                readThis(Tokens.ALWAYS);

                isGenerated     = true;
                generatedAlways = true;

                // not yet supported - type determination required
                throw unexpectedToken(Tokens.T_GENERATED);
            }

            case Tokens.IDENTITY : {
                read();

                isIdentity   = true;
                isPKIdentity = true;
                typeObject   = Type.SQL_INTEGER;
                sequence     = new NumberSequence(null, 0, 1, typeObject);
                break;
            }

            case Tokens.COMMA : {
                return null;
            }

            case Tokens.CLOSEBRACKET : {
                return null;
            }

            default : {
                if (token.isUndelimitedIdentifier) {
                    if (Tokens.T_SERIAL.equals(token.tokenString)) {
                        if (database.sqlSyntaxMys) {
                            read();

                            isIdentity   = true;
                            isPKIdentity = true;
                            typeObject   = Type.SQL_BIGINT;
                            sequence = new NumberSequence(
                                null,
                                1,
                                1,
                                typeObject);
                            break;
                        } else if (database.sqlSyntaxPgs) {
                            read();

                            isIdentity = true;
                            typeObject = Type.SQL_INTEGER;
                            sequence = new NumberSequence(
                                null,
                                1,
                                1,
                                typeObject);
                            break;
                        }
                    } else if (Tokens.T_BIGSERIAL.equals(token.tokenString)) {
                        if (database.sqlSyntaxPgs) {
                            read();

                            isIdentity   = true;
                            isPKIdentity = true;
                            typeObject   = Type.SQL_BIGINT;
                            sequence = new NumberSequence(
                                null,
                                1,
                                1,
                                typeObject);
                            break;
                        }
                    }
                }

                typeObject = readTypeDefinition(true, true);

                if (database.sqlSyntaxMys
                        && typeObject.isDomainType()
                        && typeObject.getName().name.equals(Tokens.T_ENUM)) {
                    typeObject.userTypeModifier = null;

                    HsqlName constName = database.nameManager.newAutoName(
                        "CT",
                        table.getSchemaName(),
                        table.getName(),
                        SchemaObject.CONSTRAINT);
                    Constraint c = new Constraint(
                        constName,
                        null,
                        SchemaObject.ConstraintTypes.CHECK);

                    constraintList.add(c);
                    readThis(Tokens.OPENBRACKET);

                    Expression left  = new ExpressionColumn(hsqlName.name);
                    Expression right = super.XreadInValueListConstructor(1);

                    readThis(Tokens.CLOSEBRACKET);

                    colConstraint = new ExpressionLogical(
                        OpTypes.IN,
                        left,
                        right);

                    colConstraint.setNoOptimisation();

                    c.check = colConstraint;
                }
            }
        }

        if (!isGenerated && !isIdentity) {
            if (database.sqlSyntaxMys) {
                switch (token.tokenType) {

                    case Tokens.NULL :
                        read();
                        break;

                    case Tokens.NOT :
                        read();
                        readThis(Tokens.NULL);

                        isNullable = false;
                        break;

                    default :
                }
            }

            switch (token.tokenType) {

                case Tokens.WITH : {
                    if (database.sqlSyntaxDb2) {
                        read();
                    } else {
                        throw unexpectedToken();
                    }
                }

                // fall through
                case Tokens.DEFAULT : {
                    read();

                    defaultExpr = readDefaultClause(typeObject);

                    if (defaultExpr.opType == OpTypes.SEQUENCE) {
                        if (database.sqlSyntaxPgs) {
                            sequence =
                                ((ExpressionColumn) defaultExpr).sequence;
                            defaultExpr = null;
                            isIdentity  = true;
                        }
                    }

                    break;
                }

                case Tokens.GENERATED : {
                    read();

                    if (token.tokenType == Tokens.BY) {
                        read();
                        readThis(Tokens.DEFAULT);
                    } else {
                        readThis(Tokens.ALWAYS);

                        generatedAlways = true;
                    }

                    readThis(Tokens.AS);

                    switch (token.tokenType) {

                        case Tokens.IDENTITY : {
                            read();

                            sequence = new NumberSequence(null, typeObject);

                            sequence.setAlways(generatedAlways);

                            if (token.tokenType == Tokens.OPENBRACKET) {
                                read();
                                readSequenceOptions(
                                    sequence,
                                    false,
                                    false,
                                    true);
                                readThis(Tokens.CLOSEBRACKET);
                            }

                            isIdentity = true;
                            break;
                        }

                        case Tokens.OPENBRACKET : {
                            if (!generatedAlways) {
                                throw unexpectedToken(Tokens.GENERATED);
                            }

                            isGenerated = true;

                            read();

                            generateExpr = XreadValueExpression();

                            readThis(Tokens.CLOSEBRACKET);
                            break;
                        }

                        case Tokens.SEQUENCE : {
                            if (generatedAlways) {
                                throw unexpectedToken();
                            }

                            read();

                            if (token.namePrefix != null) {
                                if (!token.namePrefix.equals(
                                        table.getSchemaName().name)) {
                                    throw unexpectedToken(token.namePrefix);
                                }
                            }

                            sequence = database.schemaManager.getSequence(
                                token.tokenString,
                                table.getSchemaName().name,
                                true);
                            isIdentity = true;

                            read();
                            break;
                        }

                        case Tokens.ROW : {
                            if (!typeObject.isTimestampType()) {
                                throw unexpectedToken();
                            }

                            read();

                            if (readIfThis(Tokens.START)) {
                                sysPeriodType =
                                    SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START;
                            } else {
                                readThis(Tokens.END);

                                sysPeriodType =
                                    SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END;
                            }

                            // always with TIME_ZONE and microsecond precision
                            if (typeObject.typeCode == Types.SQL_TIMESTAMP) {
                                typeObject = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                            }

                            break;
                        }
                    }

                    break;
                }

                case Tokens.IDENTITY : {
                    read();

                    isIdentity   = true;
                    isPKIdentity = true;
                    sequence     = new NumberSequence(null, 0, 1, typeObject);
                }
                break;
            }
        }

        if (!isGenerated && !isIdentity) {
            if (database.sqlSyntaxMys) {
                if (token.isUndelimitedIdentifier
                        && Tokens.T_AUTO_INCREMENT.equals(token.tokenString)) {
                    read();

                    isIdentity = true;
                    sequence   = new NumberSequence(null, 1, 1, typeObject);
                }
            }
        }

        ColumnSchema column = new ColumnSchema(
            hsqlName,
            typeObject,
            isNullable,
            false,
            defaultExpr);

        if (colConstraint != null) {
            colConstraint.setLeftNode(new ExpressionColumn(column));
        }

        column.setGeneratingExpression(generateExpr);
        column.setSystemPeriodType(sysPeriodType);
        readColumnConstraints(table, column, constraintList);

        if (token.tokenType == Tokens.IDENTITY && !isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        }

        // non-standard GENERATED after constraints
        if (token.tokenType == Tokens.GENERATED
                && !isIdentity
                && !isGenerated) {
            read();

            if (token.tokenType == Tokens.BY) {
                read();
                readThis(Tokens.DEFAULT);
            } else {
                readThis(Tokens.ALWAYS);

                generatedAlways = true;
            }

            readThis(Tokens.AS);
            readThis(Tokens.IDENTITY);

            sequence = new NumberSequence(null, typeObject);

            sequence.setAlways(generatedAlways);

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();
                readSequenceOptions(sequence, false, false, true);
                readThis(Tokens.CLOSEBRACKET);
            }

            isIdentity = true;
        }

        if (isIdentity) {
            column.setIdentity(sequence);
        }

        if (isPKIdentity && !column.isPrimaryKey()) {
            OrderedHashSet<String> set = new OrderedHashSet<>();

            set.add(column.getName().name);

            HsqlName constName = database.nameManager.newAutoName(
                "PK",
                table.getSchemaName(),
                table.getName(),
                SchemaObject.CONSTRAINT);
            Constraint c = new Constraint(
                constName,
                set,
                SchemaObject.ConstraintTypes.PRIMARY_KEY);

            c.setSimpleIdentityPK();
            constraintList.set(0, c);
            column.setPrimaryKey(true);
        }

        if (database.sqlSyntaxPgs
                && token.tokenType == Tokens.DEFAULT
                && column.getDefaultExpression() == null
                && column.getIdentitySequence() == null) {
            read();

            defaultExpr = readDefaultClause(typeObject);

            if (defaultExpr.opType == OpTypes.SEQUENCE) {
                sequence    = ((ExpressionColumn) defaultExpr).sequence;
                defaultExpr = null;
            }

            column.setDefaultExpression(defaultExpr);
            column.setIdentity(sequence);
        }

        return column;
    }

    /**
     * Reads and adds a table period definition to the table
     *
     * @param table a table
     */
    PeriodDefinition readAndAddPeriod(Table table) {

        PeriodDefinition period = readPeriod(table);

        if (period == null) {
            return null;
        }

        if (period.getPeriodType() == SchemaObject.PeriodType.PERIOD_SYSTEM) {
            table.systemPeriod = period;
        } else {
            table.applicationPeriod = period;
        }

        return period;
    }

    PeriodDefinition readPeriod(Table table) {

        int      periodType = SchemaObject.PeriodType.PERIOD_NONE;
        HsqlName periodName = null;
        int      position   = getPosition();

        readThis(Tokens.PERIOD);

        if (token.tokenType != Tokens.FOR) {
            rewind(position);

            return null;
        }

        readThis(Tokens.FOR);

        if (token.tokenType == Tokens.SYSTEM_TIME) {
            periodType = SchemaObject.PeriodType.PERIOD_SYSTEM;
            periodName = database.nameManager.newHsqlName(
                table.getName().schema,
                token.tokenString,
                false,
                SchemaObject.PERIOD);
        } else {
            periodType = SchemaObject.PeriodType.PERIOD_APPLICATION;

            // always use strict naming
            checkIsNonReservedIdentifier();
            checkIsIrregularCharInIdentifier();
            checkIsSimpleName();

            periodName = database.nameManager.newHsqlName(
                table.getName().schema,
                token.tokenString,
                isDelimitedIdentifier(),
                SchemaObject.PERIOD);
        }

        read();

        periodName.parent = table.getName();

        OrderedHashSet<String> set = readColumnNames(false);

        if (set.size() != 2) {
            throw Error.error(ErrorCode.X_42593);
        }

        PeriodDefinition period = new PeriodDefinition(
            periodName,
            periodType,
            set);

        if (period.getPeriodType() == SchemaObject.PeriodType.PERIOD_SYSTEM) {
            if (table.systemPeriod != null) {
                throw Error.error(ErrorCode.X_42581);    // unexpected token (for now)
            }
        } else {
            if (table.applicationPeriod != null) {
                throw Error.error(ErrorCode.X_42581);    // unexpected token (for now)
            }
        }

        return period;
    }

    /**
     * Reads and adds a table constraint definition to the list
     *
     * @param schemaObject table or domain
     * @param constraintList list of constraints
     */
    void readConstraint(
            SchemaObject schemaObject,
            HsqlArrayList<Constraint> constraintList) {

        HsqlName constName = null;

        if (token.tokenType == Tokens.CONSTRAINT) {
            read();

            constName = readNewDependentSchemaObjectName(
                schemaObject.getName(),
                SchemaObject.CONSTRAINT);
        }

        switch (token.tokenType) {

            case Tokens.PRIMARY : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                Constraint mainConst;

                mainConst = constraintList.get(0);

                if (mainConst.getConstraintType()
                        == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                    if (!mainConst.isSimpleIdentityPK) {
                        throw Error.error(ErrorCode.X_42532);
                    }
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName(
                        "PK",
                        schemaObject.getSchemaName(),
                        schemaObject.getName(),
                        SchemaObject.CONSTRAINT);
                }

                OrderedHashSet<String> set = readColumnNames(false);
                Constraint c = new Constraint(
                    constName,
                    set,
                    SchemaObject.ConstraintTypes.PRIMARY_KEY);

                constraintList.set(0, c);
                break;
            }

            case Tokens.UNIQUE : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();

                if (database.sqlSyntaxMys) {
                    if (!readIfThis(Tokens.INDEX)) {
                        readIfThis(Tokens.KEY);
                    }
                }

                OrderedHashSet<String> set = readColumnNames(false);

                if (constName == null) {
                    constName = database.nameManager.newAutoName(
                        "CT",
                        schemaObject.getSchemaName(),
                        schemaObject.getName(),
                        SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(
                    constName,
                    set,
                    SchemaObject.ConstraintTypes.UNIQUE);

                constraintList.add(c);
                break;
            }

            case Tokens.FOREIGN : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                OrderedHashSet<String> set = readColumnNames(false);
                Constraint c = readFKReferences(
                    (Table) schemaObject,
                    constName,
                    set);

                constraintList.add(c);
                break;
            }

            case Tokens.CHECK : {
                read();

                if (constName == null) {
                    constName = database.nameManager.newAutoName(
                        "CT",
                        schemaObject.getSchemaName(),
                        schemaObject.getName(),
                        SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(
                    constName,
                    null,
                    SchemaObject.ConstraintTypes.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);
                break;
            }

            default : {
                if (constName != null) {
                    throw unexpectedToken();
                }
            }
        }
    }

    /**
     * Reads column constraints
     */
    void readColumnConstraints(
            Table table,
            ColumnSchema column,
            HsqlArrayList<Constraint> constraintList) {

        boolean end                  = false;
        boolean hasNotNullConstraint = false;
        boolean hasNullNoiseWord     = false;
        boolean hasPrimaryKey        = false;

        if (column.getDataType().isTimestampType()) {
            if (token.tokenType == Tokens.ON) {
                int         position = getPosition();
                FunctionSQL function;

                try {
                    read();
                    readThis(Tokens.UPDATE);

                    if (readIfThis(Tokens.CURRENT_TIMESTAMP)) {
                        function = FunctionSQL.newSQLFunction(
                            Tokens.T_CURRENT_TIMESTAMP,
                            compileContext);
                    } else {
                        readThis(Tokens.LOCALTIMESTAMP);

                        function = FunctionSQL.newSQLFunction(
                            Tokens.T_LOCALTIMESTAMP,
                            compileContext);
                    }

                    function.resolveTypes(session, null);
                    column.setUpdateExpression(function);
                } catch (Exception e) {
                    rewind(position);
                }
            }
        }

        while (true) {
            HsqlName constName = null;

            if (token.tokenType == Tokens.CONSTRAINT) {
                read();

                constName = readNewDependentSchemaObjectName(
                    table.getName(),
                    SchemaObject.CONSTRAINT);
            }

            switch (token.tokenType) {

                case Tokens.PRIMARY : {
                    if (hasNullNoiseWord || hasPrimaryKey) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.KEY);

                    Constraint existingConst = constraintList.get(0);

                    if (existingConst.getConstraintType()
                            == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                        throw Error.error(ErrorCode.X_42532);
                    }

                    OrderedHashSet<String> set = new OrderedHashSet<>();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName(
                            "PK",
                            table.getSchemaName(),
                            table.getName(),
                            SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName,
                        set,
                        SchemaObject.ConstraintTypes.PRIMARY_KEY);

                    constraintList.set(0, c);
                    column.setPrimaryKey(true);

                    hasPrimaryKey = true;
                    break;
                }

                case Tokens.UNIQUE : {
                    read();

                    OrderedHashSet<String> set = new OrderedHashSet<>();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName(
                            "CT",
                            table.getSchemaName(),
                            table.getName(),
                            SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName,
                        set,
                        SchemaObject.ConstraintTypes.UNIQUE);

                    constraintList.add(c);
                    break;
                }

                case Tokens.FOREIGN : {
                    read();
                    readThis(Tokens.KEY);
                }

                // fall through
                case Tokens.REFERENCES : {
                    OrderedHashSet<String> set = new OrderedHashSet<>();

                    set.add(column.getName().name);

                    Constraint c = readFKReferences(table, constName, set);

                    constraintList.add(c);
                    break;
                }

                case Tokens.CHECK : {
                    read();

                    if (constName == null) {
                        constName = database.nameManager.newAutoName(
                            "CT",
                            table.getSchemaName(),
                            table.getName(),
                            SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName,
                        null,
                        SchemaObject.ConstraintTypes.CHECK);

                    readCheckConstraintCondition(c);

                    OrderedHashSet<Expression> set =
                        c.getCheckColumnExpressions();

                    for (int i = 0; i < set.size(); i++) {
                        ExpressionColumn e = (ExpressionColumn) set.get(i);

                        if (column.getName().name.equals(e.getColumnName())) {
                            if (e.getSchemaName() != null
                                    && !e.getSchemaName().equals(
                                        table.getSchemaName().name)) {
                                throw Error.error(ErrorCode.X_42505);
                            }
                        } else {
                            throw Error.error(ErrorCode.X_42501);
                        }
                    }

                    constraintList.add(c);
                    break;
                }

                case Tokens.NOT : {
                    if (hasNotNullConstraint || hasNullNoiseWord) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.NULL);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName(
                            "CT",
                            table.getSchemaName(),
                            table.getName(),
                            SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName,
                        null,
                        SchemaObject.ConstraintTypes.CHECK);

                    c.check = new ExpressionLogical(column);

                    constraintList.add(c);

                    hasNotNullConstraint = true;
                    break;
                }

                case Tokens.NULL : {
                    if (hasNotNullConstraint
                            || hasNullNoiseWord
                            || hasPrimaryKey) {
                        throw unexpectedToken();
                    }

                    if (constName != null) {
                        throw unexpectedToken();
                    }

                    read();

                    hasNullNoiseWord = true;
                    break;
                }

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }
    }

    /**
     * Responsible for handling check constraints section of CREATE TABLE ...
     *
     * @param c check constraint
     */
    void readCheckConstraintCondition(Constraint c) {

        readThis(Tokens.OPENBRACKET);

        Expression condition = XreadBooleanValueExpression();

        condition.setNoOptimisation();
        readThis(Tokens.CLOSEBRACKET);

        c.check = condition;
    }

    /**
     *  Reads a DEFAULT clause expression.
     */

    /*
     for datetime, the default must have the same fields
     */
    Expression readDefaultClause(Type dataType) {

        Expression e     = null;
        boolean    minus = false;

        if (token.tokenType == Tokens.NULL) {
            read();

            return new ExpressionValue(null, dataType);
        }

        if (dataType.isDateTimeType() || dataType.isIntervalType()) {
            switch (token.tokenType) {

                case Tokens.DATE :
                case Tokens.TIME :
                case Tokens.TIMESTAMP :
                case Tokens.INTERVAL : {
                    e = readDateTimeIntervalLiteral(session);

                    if (e.dataType.typeCode != dataType.typeCode) {

                        // error message
                        throw unexpectedToken();
                    }

                    Object defaultValue = e.getValue(session, dataType);

                    return new ExpressionValue(defaultValue, dataType);
                }

                case Tokens.X_VALUE :
                    break;

                default :
                    e = XreadDateTimeValueFunctionOrNull();

                    if (e == null) {
                        break;
                    }

                    e = XreadModifier(e);
                    break;
            }
        } else if (dataType.isNumberType()) {
            if (database.sqlSyntaxPgs && token.tokenType == Tokens.NEXTVAL) {
                return readNextvalFunction();
            }

            if (database.sqlDoubleNaN
                    && dataType.typeCode == Types.SQL_DOUBLE) {

                // special for NaN
                e = XreadNumericValueExpression();
            } else {
                if (token.tokenType == Tokens.MINUS_OP) {
                    read();

                    minus = true;
                }
            }
        } else if (dataType.isCharacterType()) {
            switch (token.tokenType) {

                case Tokens.USER :
                case Tokens.CURRENT_USER :
                case Tokens.CURRENT_ROLE :
                case Tokens.SESSION_USER :
                case Tokens.SYSTEM_USER :
                case Tokens.CURRENT_CATALOG :
                case Tokens.CURRENT_SCHEMA :
                case Tokens.CURRENT_PATH :
                    FunctionSQL function = FunctionSQL.newSQLFunction(
                        token.tokenString,
                        compileContext);

                    e = readSQLFunction(function);
                    break;

                default :
            }
        } else if (dataType.isBooleanType()) {
            switch (token.tokenType) {

                case Tokens.TRUE :
                    read();

                    return new ExpressionBoolean(true);

                case Tokens.FALSE :
                    read();

                    return new ExpressionBoolean(false);
            }
        } else if (dataType.isBitType()) {
            switch (token.tokenType) {

                case Tokens.TRUE :
                    read();

                    return new ExpressionValue(
                        BinaryData.singleBitOne,
                        dataType);

                case Tokens.FALSE :
                    read();

                    return new ExpressionValue(
                        BinaryData.singleBitZero,
                        dataType);
            }
        } else if (dataType.isArrayType()) {
            e = readCollection(OpTypes.ARRAY);

            if (e.nodes.length > 0) {
                throw Error.parseError(
                    ErrorCode.X_42562,
                    null,
                    scanner.getLineNumber());
            }

            e.dataType = dataType;

            return e;
        }

        if (e != null) {
            e.resolveTypes(session, null);

            if (!dataType.canBeAssignedFrom(e.getDataType())) {
                throw Error.parseError(
                    ErrorCode.X_42562,
                    null,
                    scanner.getLineNumber());
            }

            return e;
        }

        boolean inParens = false;

        if ((database.sqlSyntaxMss || database.sqlSyntaxPgs)
                && token.tokenType == Tokens.OPENBRACKET) {
            read();

            inParens = true;
        }

        if (token.tokenType == Tokens.X_VALUE) {
            Object value       = token.tokenValue;
            Type   valueType   = token.dataType;
            Type   convertType = dataType;

            if (dataType.typeCode == Types.SQL_CLOB) {
                convertType = Type.getType(
                    Types.SQL_VARCHAR,
                    null,
                    database.collation,
                    dataType.precision,
                    0);
            } else if (dataType.typeCode == Types.SQL_BLOB) {
                convertType = Type.getType(
                    Types.SQL_VARBINARY,
                    null,
                    null,
                    dataType.precision,
                    0);
            }

            if (minus) {
                value = valueType.negate(value);
            }

            value = convertType.convertToType(session, value, valueType);

            read();

            if (inParens) {
                readThis(Tokens.CLOSEBRACKET);
            }

            return new ExpressionValue(value, convertType);
        }

        if (database.sqlSyntaxOra || database.sqlSyntaxPgs) {
            e = XreadAllTypesCommonValueExpression(false);

            if (e != null) {
                if (e.getType() == OpTypes.ROW_SUBQUERY) {
                    TableDerived t = (TableDerived) e.getTable();
                    QuerySpecification qs =
                        (QuerySpecification) t.getQueryExpression();

                    qs.setReturningResult();
                }

                e.resolveColumnReferences(
                    session,
                    RangeGroup.emptyGroup,
                    0,
                    RangeGroup.emptyArray,
                    null,
                    true);
                e.resolveTypes(session, null);

                if (e.getType() == OpTypes.ROW_SUBQUERY) {
                    TableDerived t = (TableDerived) e.getTable();
                    QuerySpecification qs =
                        (QuerySpecification) t.getQueryExpression();
                    Table        d = qs.getRangeVariables()[0].getTable();

                    if (d != session.database.schemaManager.dualTable
                            || qs.exprColumns.length != 1) {
                        throw Error.error(ErrorCode.X_42565);
                    }

                    e = qs.exprColumns[0];
                }

                if (inParens) {
                    readThis(Tokens.CLOSEBRACKET);
                }

                return e;
            }
        }

        if (database.sqlSyntaxDb2) {
            Object value = null;

            switch (dataType.typeComparisonGroup) {

                case Types.SQL_VARCHAR :
                    value = "";
                    break;

                case Types.SQL_VARBINARY :
                    value = BinaryData.zeroLengthBinary;
                    break;

                case Types.SQL_NUMERIC :
                    value = Integer.valueOf(0);
                    break;

                case Types.SQL_BOOLEAN :
                    value = Boolean.FALSE;
                    break;

                case Types.SQL_CLOB :
                    value = "";

                    return new ExpressionValue(value, Type.SQL_VARCHAR_DEFAULT);

                case Types.SQL_BLOB :
                    value = BinaryData.zeroLengthBinary;

                    return new ExpressionValue(
                        value,
                        Type.SQL_VARBINARY_DEFAULT);

                case Types.TIME : {
                    FunctionSQL function = FunctionSQL.newSQLFunction(
                        Tokens.T_CURRENT_TIME,
                        compileContext);

                    function.resolveTypes(session, null);

                    return function;
                }

                case Types.DATE : {
                    FunctionSQL function = FunctionSQL.newSQLFunction(
                        Tokens.T_CURRENT_DATE,
                        compileContext);

                    function.resolveTypes(session, null);

                    return function;
                }

                case Types.TIMESTAMP : {
                    FunctionSQL function = FunctionSQL.newSQLFunction(
                        Tokens.T_CURRENT_TIMESTAMP,
                        compileContext);

                    function.resolveTypes(session, null);

                    return function;
                }
            }

            value = dataType.convertToDefaultType(session, value);

            return new ExpressionValue(value, dataType);
        }

        if (inParens) {
            readThis(Tokens.CLOSEBRACKET);
        }

        throw unexpectedToken();
    }

    /**
     * A comma after START WITH is accepted for 1.8.x compatibility
     */
    void readSequenceOptions(
            NumberSequence sequence,
            boolean withType,
            boolean isAlter,
            boolean allowComma) {

        OrderedIntHashSet set = new OrderedIntHashSet();

        while (true) {
            boolean end = false;

            if (set.contains(token.tokenType)) {
                throw unexpectedToken();
            }

            switch (token.tokenType) {

                case Tokens.AS : {
                    if (withType) {
                        set.add(token.tokenType);
                        read();

                        Type type = readTypeDefinition(false, true);

                        sequence.setDefaults(sequence.getName(), type);
                        break;
                    }

                    throw unexpectedToken();
                }

                case Tokens.START : {
                    set.add(token.tokenType);
                    read();
                    readThis(Tokens.WITH);

                    long value = readBigint();

                    sequence.setStartValueNoCheck(value);

                    if (allowComma) {
                        readIfThis(Tokens.COMMA);
                    }

                    break;
                }

                case Tokens.RESTART : {
                    if (!isAlter) {
                        end = true;
                        break;
                    }

                    set.add(token.tokenType);
                    read();

                    if (readIfThis(Tokens.WITH)) {
                        long value = readBigint();

                        sequence.setCurrentValueNoCheck(value);
                    } else {
                        sequence.setStartValueDefault();
                    }

                    break;
                }

                case Tokens.INCREMENT : {
                    set.add(token.tokenType);
                    read();
                    readThis(Tokens.BY);

                    long value = readBigint();

                    sequence.setIncrement(value);
                    break;
                }

                case Tokens.NO : {
                    read();

                    if (set.contains(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    if (token.tokenType == Tokens.MAXVALUE) {
                        sequence.setDefaultMaxValue();
                    } else if (token.tokenType == Tokens.MINVALUE) {
                        sequence.setDefaultMinValue();
                    } else if (token.tokenType == Tokens.CYCLE) {
                        sequence.setCycle(false);
                    } else {
                        throw unexpectedToken();
                    }

                    set.add(token.tokenType);
                    read();
                    break;
                }

                case Tokens.MAXVALUE : {
                    set.add(token.tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMaxValueNoCheck(value);
                    break;
                }

                case Tokens.MINVALUE : {
                    set.add(token.tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMinValueNoCheck(value);
                    break;
                }

                case Tokens.CYCLE : {
                    set.add(token.tokenType);
                    read();
                    sequence.setCycle(true);
                    break;
                }

                default :
                    if ((database.sqlSyntaxOra || database.sqlSyntaxDb2)
                            && isSimpleName()) {
                        if (token.tokenString.equals("NOCACHE")
                                || token.tokenString.equals("NOCYCLE")
                                || token.tokenString.equals("NOMAXVALUE")
                                || token.tokenString.equals("NOMINVALUE")
                                || token.tokenString.equals("NOORDER")
                                || token.tokenString.equals("ORDER")) {
                            read();
                            break;
                        }

                        if (token.tokenString.equals("CACHE")) {
                            read();
                            readBigint();
                            break;
                        }
                    }

                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        sequence.checkValues();
    }

    private void readIndex(Table table, HsqlArrayList<Constraint> indexList) {

        HsqlName indexHsqlName;

        read();

        indexHsqlName        = readNewSchemaObjectName(
            SchemaObject.INDEX,
            true);
        indexHsqlName.schema = table.getSchemaName();
        indexHsqlName.parent = table.getName();
        indexHsqlName.schema = table.getSchemaName();

        if (readIfThis(Tokens.USING)) {
            if ("BTREE".equals(token.tokenString)
                    || "HASH".equals(token.tokenString)) {
                read();
            }
        }

        readThis(Tokens.ON);

        int[] indexColumns = readColumnList(table, true);
        Constraint c = new Constraint(
            indexHsqlName,
            table,
            indexColumns,
            SchemaObject.INDEX);

        indexList.add(c);
    }

    Boolean readIfNotExists() {

        Boolean ifNot = Boolean.FALSE;

        if (token.tokenType == Tokens.IF) {
            int position = getPosition();

            read();

            if (token.tokenType == Tokens.NOT) {
                read();
                readThis(Tokens.EXISTS);

                ifNot = Boolean.TRUE;
            } else {
                rewind(position);

                ifNot = Boolean.FALSE;
            }
        }

        return ifNot;
    }

    StatementSchema compileAlterTableAddPeriod(Table table) {

        PeriodDefinition period = readPeriod(table);

        if (period.getPeriodType() == SchemaObject.PeriodType.PERIOD_SYSTEM) {
            checkPeriodColumnsAdd(table, period);
        } else {

            // application period not supported
            setPeriodColumns(table, period);

            throw Error.error(ErrorCode.X_0A501);
        }

        readThis(Tokens.ADD);
        readIfThis(Tokens.COLUMN);

        String nameString = period.columnNames.get(0);

        if (!token.tokenString.equals(nameString)) {
            throw unexpectedToken();
        }

        HsqlArrayList<Constraint> list = new HsqlArrayList<>();
        HsqlName hsqlName = database.nameManager.newColumnHsqlName(
            table.getName(),
            token.tokenString,
            isDelimitedIdentifier());

        read();

        ColumnSchema columnStart = readColumnDefinitionOrNull(
            table,
            hsqlName,
            list);

        if (columnStart == null) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (columnStart.getSystemPeriodType()
                != SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START) {
            throw Error.error(ErrorCode.X_42516, columnStart.getNameString());
        }

        readThis(Tokens.ADD);
        readIfThis(Tokens.COLUMN);
        checkIsSimpleName();

        nameString = period.columnNames.get(1);

        if (!token.tokenString.equals(nameString)) {
            throw unexpectedToken();
        }

        hsqlName = database.nameManager.newColumnHsqlName(
            table.getName(),
            token.tokenString,
            isDelimitedIdentifier());

        read();

        ColumnSchema columnEnd = readColumnDefinitionOrNull(
            table,
            hsqlName,
            list);

        if (columnEnd == null) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (columnEnd.getSystemPeriodType()
                != SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END) {
            throw Error.error(ErrorCode.X_42516, columnEnd.getNameString());
        }

        period.startColumn = columnStart;
        period.endColumn   = columnEnd;

        String   sql  = getLastPart();
        Object[] args = new Object[]{ table, period };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(
            sql,
            StatementTypes.ADD_TABLE_PERIOD,
            args,
            null,
            writeLockNames);
    }

    StatementSchema compileAlterTableDropPeriod(Table table) {

        if (readIfThis(Tokens.SYSTEM_TIME)) {

            //
        } else {

            // application period not supported
            throw Error.error(ErrorCode.X_0A501);
        }

        PeriodDefinition period = table.systemPeriod;

        if (period == null) {
            throw Error.error(ErrorCode.X_42517);
        }

        if (table.isSystemVersioned) {
            throw Error.error(ErrorCode.X_42518);
        }

        boolean  cascade = readIfThis(Tokens.CASCADE);
        String   sql     = getLastPart();
        Object[] args    = new Object[]{ table, period, cascade };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(
            sql,
            StatementTypes.DROP_TABLE_PERIOD,
            args,
            null,
            writeLockNames);
    }

    StatementSchema compileAlterTableAddVersioning(Table table) {

        PeriodDefinition period = table.systemPeriod;

        if (period == null) {
            throw Error.error(ErrorCode.X_42518);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[]{ table };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(
            sql,
            StatementTypes.ADD_TABLE_SYSTEM_VERSIONING,
            args,
            null,
            writeLockNames);
    }

    StatementSchema compileAlterTableDropVersioning(Table table) {

        if (!table.isSystemVersioned) {
            throw Error.error(ErrorCode.X_42518);
        }

        boolean  cascade = readIfThis(Tokens.CASCADE);
        String   sql     = getLastPart();
        Object[] args    = new Object[]{ table, cascade };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(
            sql,
            StatementTypes.DROP_TABLE_SYSTEM_VERSIONING,
            args,
            null,
            writeLockNames);
    }
}
