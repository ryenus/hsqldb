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
import org.hsqldb.index.Index;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.Right;
import org.hsqldb.rights.User;
import org.hsqldb.types.Type;

//4-8-2005 MarcH and HuugO ALTER TABLE <tablename> ALTER COLUMN <column name> SET [NOT] NULL support added
public class DDLParser extends Parser {

    DDLParser(Session session, Database db, Tokenizer t) {
        super(session, db, t);
    }

    void processCreate() throws HsqlException {

        int     tableType   = Table.MEMORY_TABLE;
        boolean isTempTable = false;
        boolean isTable     = false;

        /* N.b.  Admin priv is not required for all CREATE
         * actions any more.  Therefore, check for required
         * authorization in the target-object-specific code below.
         * session.checkAdmin();
         */
        session.checkDDLWrite();
        session.setScripting(true);

        if (tokenType == Token.GLOBAL) {
            read();
            readThis(Token.TEMPORARY);

            isTempTable = true;
        } else if (tokenType == Token.TEMP) {
            read();

            isTempTable = true;
        } else if (tokenType == Token.TEMPORARY) {
            read();

            isTempTable = true;
        }

        switch (tokenType) {

            // table
            case Token.MEMORY :
                read();
                readThis(Token.TABLE);

                if (isTempTable) {
                    tableType = Table.TEMP_TABLE;
                } else {
                    tableType = Table.MEMORY_TABLE;
                }

                isTable = true;
                break;

            case Token.TABLE :
                read();

                if (isTempTable) {
                    tableType = Table.TEMP_TABLE;
                } else {
                    tableType = database.getDefaultTableType();
                }

                isTable = true;
                break;

            case Token.CACHED :
                if (isTempTable) {
                    throw unexpectedToken();
                }

                read();
                readThis(Token.TABLE);

                tableType = Table.CACHED_TABLE;
                isTable   = true;
                break;

            case Token.TEXT :
                if (isTempTable) {
                    throw unexpectedToken();
                }

                read();
                readThis(Token.TABLE);

                tableType = Table.TEXT_TABLE;
                isTable   = true;
                break;

            default :
                if (isTempTable) {
                    throw unexpectedToken();
                }
        }

        if (isTable) {
            processCreateTable(tableType);

            return;
        }

        switch (tokenType) {

            // other objects
            case Token.ALIAS :
                session.checkAdmin();
                read();
                processCreateAlias();
                break;

            case Token.SEQUENCE :
                read();
                processCreateSequence();
                break;

            case Token.SCHEMA :
                session.checkAdmin();
                session.setScripting(false);
                read();
                processCreateSchema();
                break;

            case Token.TRIGGER :
                session.checkAdmin();
                read();
                processCreateTrigger();
                break;

            case Token.USER :
                session.checkAdmin();
                read();
                processCreateUser();
                break;

            case Token.ROLE :
                session.checkAdmin();
                read();

                HsqlName name = readUserIdentifier();

                database.getGranteeManager().addRole(name);
                break;

            case Token.VIEW :
                read();
                processCreateView();
                break;

            // index
            case Token.UNIQUE :
                read();
                readThis(Token.INDEX);
                processCreateIndex(true);
                break;

            case Token.INDEX :
                read();
                processCreateIndex(false);
                break;

            default : {
                throw unexpectedToken();
            }
        }
    }

    void processAlterTable() throws HsqlException {

        String tableName = tokenString;
        String schema    = session.getSchemaNameForWrite(namePrefix);

        checkSchemaUpdateAuthorization(schema);

        Table t = database.schemaManager.getUserTable(session, tableName,
            schema);

        if (t.isView()) {
            throw Trace.error(Trace.NOT_A_TABLE);
        }

        session.setScripting(true);
        read();

        switch (tokenType) {

            case Token.RENAME : {
                read();
                readThis(Token.TO);
                processAlterTableRename(t);

                return;
            }
            case Token.ADD : {
                read();

                HsqlName cname = null;

                if (tokenType == Token.CONSTRAINT) {
                    read();

                    cname =
                        readNewDependentSchemaObjectName(t.getSchemaName());
                }

                switch (tokenType) {

                    case Token.FOREIGN :
                        read();
                        readThis(Token.KEY);
                        processAlterTableAddForeignKeyConstraint(t, cname);

                        return;

                    case Token.UNIQUE :
                        read();
                        processAlterTableAddUniqueConstraint(t, cname);

                        return;

                    case Token.CHECK :
                        read();
                        processAlterTableAddCheckConstraint(t, cname);

                        return;

                    case Token.PRIMARY :
                        read();
                        readThis(Token.KEY);
                        processAlterTableAddPrimaryKey(t, cname);

                        return;

                    case Token.COLUMN :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        read();
                        checkIsSimpleName();
                        processAlterTableAddColumn(t);

                        return;

                    default :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        checkIsSimpleName();
                        processAlterTableAddColumn(t);

                        return;
                }
            }
            case Token.DROP : {
                read();

                switch (tokenType) {

                    case Token.PRIMARY : {
                        boolean cascade = false;

                        read();
                        readThis(Token.KEY);

                        if (tokenType == Token.CASCADE) {
                            read();

                            cascade = true;
                        }

                        if (t.hasPrimaryKey()) {
                            processAlterTableDropConstraint(
                                t, t.getPrimaryConstraint().getName().name,
                                cascade);
                        } else {
                            throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                                              Trace.TABLE_HAS_NO_PRIMARY_KEY,
                                              new Object[] {
                                "PRIMARY KEY", t.getName().name
                            });
                        }

                        return;
                    }
                    case Token.CONSTRAINT : {
                        boolean cascade = false;

                        read();
                        checkIsName();

                        String name = tokenString;

                        if (tokenType == Token.CASCADE) {
                            read();

                            cascade = true;
                        }

                        processAlterTableDropConstraint(t, tokenString,
                                                        cascade);
                        read();

                        return;
                    }
                    case Token.COLUMN :
                        read();
                    default : {
                        checkIsSimpleName();

                        String  name    = tokenString;
                        boolean cascade = false;

                        read();

                        if (tokenType == Token.RESTRICT) {
                            read();
                        } else if (tokenType == Token.CASCADE) {
                            read();

                            cascade = true;
                        }

                        processAlterTableDropColumn(t, name, cascade);

                        return;
                    }
                }
            }
            case Token.ALTER : {
                read();

                if (tokenType == Token.COLUMN) {
                    read();
                }

                int    columnIndex = t.getColumnIndex(tokenString);
                Column column      = t.getColumn(columnIndex);

                read();
                processAlterColumn(t, column, columnIndex);

                return;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    /**
     * Responsible for handling the execution CREATE TABLE SQL statements.
     *
     * @param type Description of the Parameter
     * @throws HsqlException
     */
    void processCreateTable(int type) throws HsqlException {

        HsqlName name = readNewSchemaObjectName();

        database.schemaManager.checkUserTableNotExists(session, name.name,
                name.schema.name);

        Table table = TableUtil.newTable(database, type, name);

        readThis(Token.OPENBRACKET);

        HsqlArrayList tempConstraints = new HsqlArrayList();

        {
            Constraint c = new Constraint(null, null, Constraint.TEMP);

            tempConstraints.add(c);
        }

        for (int colIndex = 0; ; ) {
            boolean end = false;

            switch (tokenType) {

                case Token.CONSTRAINT :
                case Token.PRIMARY :
                case Token.FOREIGN :
                case Token.UNIQUE :
                case Token.CHECK :
                    readTableConstraint(table, tempConstraints);
                    break;

                case Token.COMMA :
                    read();
                    break;

                case Token.CLOSEBRACKET :
                    read();

                    end = true;
                    break;

                default :
                    checkIsName();

                    HsqlName hsqlName =
                        database.nameManager.newHsqlName(tokenString,
                                                         isQuoted);

                    read();

                    Column newcolumn = readColumnDefinition(table, hsqlName,
                        tempConstraints);

                    table.addColumn(newcolumn);

                    colIndex++;
            }

            if (end) {
                break;
            }
        }

        if (tokenType == Token.ON) {
            if (!table.isTemp) {
                throw unexpectedToken();
            }

            read();
            readThis(Token.COMMIT);

            if (tokenType == Token.DELETE) {}
            else if (tokenType == Token.PRESERVE) {
                table.onCommitPreserve = true;
            }

            read();
            readThis(Token.ROWS);
        }

        // set up constraints
        try {
            table = addTableConstraintDefinitions(table, tempConstraints);

            database.schemaManager.addTable(table);
        } catch (HsqlException e) {
            database.schemaManager.removeExportedKeys(table);
            database.schemaManager.removeIndexNames(table.tableName);
            database.schemaManager.removeConstraintNames(table.tableName);

            throw e;
        }
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    Table addTableConstraintDefinitions(Table table,
                                        HsqlArrayList tempConstraints)
                                        throws HsqlException {

        Constraint c = (Constraint) tempConstraints.get(0);

        if (c.mainColSet != null) {
            c.core.mainCols = table.getColumnIndexes(c.mainColSet);
        }

        HsqlName indexName = database.nameManager.newAutoName("IDX",
            c.getName() == null ? null
                                : c.getName().name);

        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(c.getName(), table,
                table.getPrimaryIndex(), Constraint.PRIMARY_KEY);

            table.addConstraint(newconstraint);
            database.schemaManager.addConstraint(newconstraint,
                                                 table.getName());
        }

        for (int i = 1; i < tempConstraints.size(); i++) {
            c = (Constraint) tempConstraints.get(i);

            switch (c.constType) {

                case Constraint.UNIQUE : {
                    c.setColumnsIndexes(table);

                    if (table.getUniqueConstraintForColumns(c.core.mainCols)
                            != null) {
                        throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
                    }

                    // create an autonamed index
                    indexName = table.database.nameManager.newAutoName("IDX",
                            c.getName().name);

                    Index index = table.createIndex(session, c.core.mainCols,
                                                    null, indexName, true,
                                                    true, false);
                    Constraint newconstraint = new Constraint(c.getName(),
                        table, index, Constraint.UNIQUE);

                    table.addConstraint(newconstraint);
                    table.database.schemaManager.addConstraint(newconstraint,
                            table.getName());

                    break;
                }
                case Constraint.FOREIGN_KEY : {
                    c.setColumnsIndexes(table);

                    Constraint uniqueConstraint =
                        c.core.mainTable.getUniqueConstraintForColumns(
                            c.core.mainCols, c.core.refCols);
                    Index      mainIndex  = uniqueConstraint.getMainIndex();
                    TableWorks tableWorks = new TableWorks(session, table);

                    tableWorks.checkCreateForeignKey(c);

                    int offset = database.schemaManager.getTableIndex(table);
                    boolean isForward =
                        offset != -1
                        && offset
                           < database.schemaManager.getTableIndex(
                               c.core.mainTable);
                    HsqlName refIndexName =
                        database.nameManager.newAutoName("IDX",
                                                         (HsqlName) null);
                    Index index = table.createIndex(session, c.core.refCols,
                                                    null, refIndexName,
                                                    false, true, isForward);
                    HsqlName mainName =
                        database.nameManager.newAutoName("REF",
                                                         c.getName().name);

                    c.core.uniqueName = uniqueConstraint.getName();
                    c.core.mainName   = mainName;
                    c.core.mainIndex  = mainIndex;
                    c.core.refTable   = table;
                    c.core.refName    = c.getName();
                    c.core.refIndex   = index;

                    table.addConstraint(c);
                    c.core.mainTable.addConstraint(new Constraint(mainName,
                            c));
                    database.schemaManager.addConstraint(c, table.getName());

                    break;
                }
                case Constraint.CHECK : {
                    c.prepareCheckConstraint(session, table);
                    table.addConstraint(c);

                    if (c.isNotNull) {
                        Column column = table.getColumn(c.notNullColumnIndex);

                        column.setNullable(false);
                        table.setColumnTypeVars(c.notNullColumnIndex);
                    }

                    database.schemaManager.addConstraint(c, table.getName());

                    break;
                }
            }
        }

        return table;
    }

    private Constraint readFKReferences(Table refTable,
                                        HsqlName constraintName,
                                        OrderedHashSet refColSet)
                                        throws HsqlException {

        Table          mainTable;
        OrderedHashSet mainColSet = null;

        readThis(Token.REFERENCES);

        if (refTable.getName().name.equals(tokenString)) {
            mainTable = refTable;

            if (namePrefix != null
                    &&!refTable.getName().schema.name.equals(namePrefix)) {
                throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                                  namePrefix);
            }

            read();
        } else {
            mainTable = readTableName(refTable.getName().schema.name);
        }

        if (tokenType == Token.OPENBRACKET) {
            mainColSet = readColumnNames(false);
        } else {

            // columns are resolved in the calling method
            if (mainTable == refTable) {

                // fredt - FK statement is part of CREATE TABLE and is self-referencing
                // reference must be to same table being created
            } else if (!mainTable.hasPrimaryKey()) {
                throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                                  Trace.TABLE_HAS_NO_PRIMARY_KEY);
            }
        }

        // -- In a while loop we parse a maximium of two
        // -- "ON" statements following the foreign key
        // -- definition this can be
        // -- ON [UPDATE|DELETE] [NO ACTION|RESTRICT|CASCADE|SET [NULL|DEFAULT]]
        int               deleteAction = Constraint.NO_ACTION;
        int               updateAction = Constraint.NO_ACTION;
        OrderedIntHashSet set          = new OrderedIntHashSet();

        while (tokenType == Token.ON) {
            read();

            if (!set.add(tokenType)) {
                throw unexpectedToken();
            }

            if (tokenType == Token.DELETE) {
                read();

                if (tokenType == Token.SET) {
                    read();

                    if (tokenType == Token.DEFAULT) {
                        read();

                        deleteAction = Constraint.SET_DEFAULT;
                    } else if (Token.T_NULL.equals(tokenString)
                               && tokenType == Token.X_VALUE) {
                        read();

                        deleteAction = Constraint.SET_NULL;
                    } else {
                        throw unexpectedToken();
                    }
                } else if (tokenType == Token.CASCADE) {
                    read();

                    deleteAction = Constraint.CASCADE;
                } else if (tokenType == Token.RESTRICT) {
                    read();
                } else {
                    readThis(Token.NO);
                    readThis(Token.ACTION);
                }
            } else if (tokenType == Token.UPDATE) {
                read();

                if (tokenType == Token.SET) {
                    read();

                    if (tokenType == Token.DEFAULT) {
                        read();

                        updateAction = Constraint.SET_DEFAULT;
                    } else if (Token.T_NULL.equals(tokenString)
                               && tokenType == Token.X_VALUE) {
                        updateAction = Constraint.SET_NULL;
                    } else {
                        throw unexpectedToken();
                    }
                } else if (tokenType == Token.CASCADE) {
                    read();

                    updateAction = Constraint.CASCADE;
                } else if (tokenType == Token.RESTRICT) {
                    read();
                } else {
                    readThis(Token.NO);
                    readThis(Token.ACTION);
                }
            } else {
                throw unexpectedToken();
            }
        }

        if (constraintName == null) {
            constraintName = database.nameManager.newAutoName("FK",
                    refTable.getSchemaName());
        }

        return new Constraint(constraintName, refColSet, mainTable,
                              mainColSet, Constraint.FOREIGN_KEY,
                              deleteAction, updateAction);
    }

    /**
     * Responsible for handling the execution CREATE VIEW SQL statements.
     *
     * @throws HsqlException
     */
    void processCreateView() throws HsqlException {

        HsqlName name = readNewSchemaObjectName();

        database.schemaManager.checkUserViewNotExists(session, name.name,
                name.schema.name);

        HsqlName[] colList = null;

        if (tokenType == Token.OPENBRACKET) {
            colList = readColumnNames();
        }

        readThis(Token.AS);

        int logPosition = getPosition();
        int brackets    = readOpenBrackets();
        Select select = readQueryExpression(brackets, true, false, true,
                                            true);

        if (select.intoTableName != null) {
            throw (Trace.error(Trace.INVALID_IDENTIFIER, Token.INTO));
        }

        String sql  = getLastPart(logPosition);
        View   view = new View(session, database, name, sql, colList);

        session.commit();
        database.schemaManager.addTable(view);
    }

    void processCreateSequence() throws HsqlException {

/*
        CREATE SEQUENCE <name>
        [AS {INTEGER | BIGINT}]
        [START WITH <value>]
        [INCREMENT BY <value>]
*/
        HsqlName hsqlName = readNewSchemaObjectName();
        NumberSequence sequence = new NumberSequence(hsqlName,
            Type.SQL_INTEGER);

        readSequenceOptions(sequence, true, false);
        database.schemaManager.addSequence(sequence);
    }

    /**
     * If an invalid alias is encountered while processing an old script,
     * simply discard it.
     * convert org.hsql.Library aliases from versions < 1.60 to org.hsqldb.
     * Discard aliases for ABS
     */
    void processCreateAlias() throws HsqlException {

        String alias;
        String methodFQN;

        try {
            checkIsSimpleName();
            checkIsNotQuoted();

            if (isReservedKey) {
                throw Trace.error(Trace.INVALID_IDENTIFIER);
            }

            alias = tokenString;
        } catch (HsqlException e) {
            if (session.isProcessingScript()) {
                alias = null;
            } else {
                throw e;
            }
        }

        read();
        readThis(Token.FOR);
        checkIsSimpleName();

        methodFQN = tokenString;

        String oldLib = "org.hsql.Library.";
        String newLib = "org.hsqldb.Library.";

        if (methodFQN.startsWith(oldLib)) {
            methodFQN = newLib + methodFQN.substring(oldLib.length());
        }

        if (alias != null) {
            database.aliasManager.addAlias(alias, methodFQN);
        }
    }

    /**
     *  Responsible for handling the execution of CREATE TRIGGER SQL
     *  statements. <p>
     *
     *  typical sql is: CREATE TRIGGER tr1 AFTER INSERT ON tab1 CALL "pkg.cls"
     *
     * @throws HsqlException
     */
    void processCreateTrigger() throws HsqlException {

        Table          table;
        boolean        isForEachRow = false;
        boolean        isNowait     = false;
        int            queueSize;
        String         beforeOrAfter;
        String         operation;
        String         className;
        TriggerDef     td;
        HsqlName       name    = readNewDependentSchemaObjectName();
        OrderedHashSet columns = null;

        queueSize     = TriggerDef.getDefaultQueueSize();
        beforeOrAfter = tokenString;

        switch (tokenType) {

            case Token.BEFORE :
            case Token.AFTER :
                read();
                break;

            default :
                throw unexpectedToken();
        }

        operation = tokenString;

        switch (tokenType) {

            case Token.INSERT :
            case Token.DELETE :
                read();
                break;

            case Token.UPDATE :
                read();

                if (tokenType == Token.OF) {
                    read();

                    columns = readColumnNames(false);
                }
                break;

            default :
                throw unexpectedToken();
        }

        readThis(Token.ON);

        table = readTableName();

        checkSchemaUpdateAuthorization(table.getSchemaName().name);

        if (table.isView()) {
            throw Trace.error(Trace.NOT_A_TABLE);
        }

        name.setAndCheckSchema(table.getSchemaName());
        database.schemaManager.checkTriggerExists(name.name,
                name.schema.name, false);

        if (columns != null) {
            int[] cols = table.getColumnIndexes(columns);

            // do this inside trigger class
            table.getColumnCheckList(cols);
        }

        Expression      condition       = null;
        String          oldTableName    = null;
        String          newTableName    = null;
        String          oldRowName      = null;
        String          newRowName      = null;
        RangeVariable[] rangeVars       = new RangeVariable[4];
        HsqlArrayList   sqlStatements   = new HsqlArrayList();
        HsqlArrayList   sqlReplacements = new HsqlArrayList();

        if (tokenType == Token.REFERENCING) {
            read();

            if (tokenType != Token.OLD && tokenType != Token.NEW) {
                throw unexpectedToken();
            }

            while (true) {
                if (tokenType == Token.OLD) {
                    if (operation.equals(Token.T_INSERT)) {
                        throw unexpectedToken();
                    }

                    read();

                    if (tokenType == Token.TABLE) {
                        if (oldTableName != null
                                || operation.equals(Token.BEFORE)) {
                            throw unexpectedToken();
                        }

                        read();
                        readNoiseWord(Token.AS);
                        checkIsSimpleName();

                        oldTableName = tokenString;

                        String n = oldTableName;

                        if (n.equals(newTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        RangeVariable range = new RangeVariable(table, n,
                            null, compileContext);

                        rangeVars[TriggerDef.OLD_TABLE] = range;
                    } else if (tokenType == Token.ROW) {
                        if (oldRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readNoiseWord(Token.AS);
                        checkIsSimpleName();

                        oldRowName = tokenString;

                        String n = oldRowName;

                        if (n.equals(newTableName) || n.equals(oldTableName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = true;

                        RangeVariable range = new RangeVariable(table, n,
                            null, compileContext);

                        rangeVars[TriggerDef.OLD_ROWS] = range;
                    } else {
                        throw unexpectedToken();
                    }
                } else if (tokenType == Token.NEW) {
                    if (operation.equals(Token.T_DELETE)) {
                        throw unexpectedToken();
                    }

                    read();

                    if (tokenType == Token.TABLE) {
                        if (newTableName != null
                                || operation.equals(Token.BEFORE)) {
                            throw unexpectedToken();
                        }

                        read();
                        readNoiseWord(Token.AS);
                        checkIsSimpleName();

                        newTableName = tokenString;

                        String n = newTableName;

                        if (n.equals(oldTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        RangeVariable range = new RangeVariable(table, n,
                            null, compileContext);

                        rangeVars[TriggerDef.NEW_TABLE] = range;
                    } else if (tokenType == Token.ROW) {
                        if (newRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readNoiseWord(Token.AS);
                        checkIsSimpleName();

                        newRowName   = tokenString;
                        isForEachRow = true;

                        String n = newRowName;

                        if (n.equals(oldTableName) || n.equals(newTableName)
                                || n.equals(oldRowName)) {
                            throw unexpectedToken();
                        }

                        RangeVariable range = new RangeVariable(table, n,
                            null, compileContext);

                        rangeVars[TriggerDef.NEW_ROWS] = range;
                    } else {
                        throw unexpectedToken();
                    }
                } else {
                    break;
                }

                read();
            }
        }

        if (isForEachRow && tokenType != Token.FOR) {
            throw unexpectedToken();
        }

        // "FOR EACH ROW" or "CALL"
        if (tokenType == Token.FOR) {
            read();
            readThis(Token.EACH);

            if (tokenType == Token.ROW) {
                isForEachRow = true;
            } else if (tokenType == Token.STATEMENT) {
                if (isForEachRow) {
                    throw unexpectedToken();
                }
            } else {
                throw unexpectedToken();
            }

            read();
        }

        if (Token.T_NOWAIT.equals(tokenString)) {
            read();

            isNowait = true;
        } else if (Token.T_QUEUE.equals(tokenString)) {
            read();

            queueSize = readInteger();
        }

        if (tokenType == Token.WHEN) {
            read();
            readThis(Token.OPENBRACKET);

            condition = readCondition();

            readThis(Token.CLOSEBRACKET);

            OrderedHashSet unresolved =
                condition.resolveColumnReferences(rangeVars, null);

            if (unresolved != null) {
                Expression col = (Expression) unresolved.get(0);

                throw Trace.error(Trace.COLUMN_NOT_FOUND, col.getAlias());
            }
        }

        if (tokenType == Token.CALL) {
            read();
            checkIsSimpleName();
            checkIsQuoted();

            className = tokenString;

            read();

            td = new TriggerDef(name, beforeOrAfter, operation, isForEachRow,
                                table, className, isNowait, queueSize);

            table.addTrigger(td);

            if (td.isValid()) {
                try {

                    // start the trigger thread
                    td.start();
                } catch (Exception e) {
                    throw Trace.error(Trace.UNKNOWN_FUNCTION, e.toString());
                }
            }

            database.schemaManager.addTrigger(td, table.getName());
            session.setScripting(true);

            return;
        }

        throw unexpectedToken();
    }

    //read

    /**
     * Responsible for handling the creation of table columns during the process
     * of executing CREATE TABLE or ADD COLUMN etc. statements.
     *
     * @param table this table
     * @param columnIndex this column's index
     * @param hsqlName column name
     * @param constraintList list of constraints
     * @return a Column object with indicated attributes
     * @throws HsqlException
     */
    Column readColumnDefinition(Table table, HsqlName hsqlName,
                                HsqlArrayList constraintList)
                                throws HsqlException {

        boolean        isIdentity     = false;
        boolean        isPKIdentity   = false;
        boolean        identityAlways = false;
        Expression     generateExpr   = null;
        boolean        isNullable     = true;
        Expression     defaultExpr    = null;
        Type           typeObject;
        NumberSequence sequence = null;

        if (tokenType == Token.IDENTITY) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            typeObject   = Type.SQL_INTEGER;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        } else {
            typeObject = readTypeDefinition();
        }

        if (isIdentity) {}
        else if (tokenType == Token.DEFAULT) {
            read();

            defaultExpr = readAndCheckDefaultClause(typeObject);
        } else if (tokenType == Token.GENERATED &&!isIdentity) {
            read();

            if (tokenType == Token.BY) {
                read();
                readThis(Token.DEFAULT);
            } else {
                readThis(Token.ALWAYS);

                identityAlways = true;
            }

            readThis(Token.AS);

            if (tokenType == Token.IDENTITY) {
                read();

                sequence = new NumberSequence(null, typeObject);

                sequence.setAlways(identityAlways);

                if (tokenType == Token.OPENBRACKET) {
                    read();
                    readSequenceOptions(sequence, false, false);
                    readThis(Token.CLOSEBRACKET);
                }

                isIdentity = true;
            } else if (tokenType == Token.OPENBRACKET) {
                read();

                generateExpr = readOr();

                readThis(Token.CLOSEBRACKET);
            }
        }

        Column column = new Column(hsqlName, typeObject, isNullable, false,
                                   defaultExpr);

        readColumnConstraints(table, column, constraintList);

        if (tokenType == Token.IDENTITY &&!isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        }

        if (isIdentity) {
            column.setIdentity(sequence);
        }

        if (isPKIdentity &&!column.isPrimaryKey()) {
            OrderedHashSet set = new OrderedHashSet();

            set.add(column.getName().name);

            HsqlName constName = database.nameManager.newAutoName("PK",
                table.getSchemaName());
            Constraint c = new Constraint(constName, set,
                                          Constraint.PRIMARY_KEY);

            constraintList.set(0, c);
            column.setPrimaryKey(true);
        }

        return column;
    }

    private void readSequenceOptions(NumberSequence sequence,
                                     boolean withType,
                                     boolean isAlter) throws HsqlException {

        OrderedIntHashSet set = new OrderedIntHashSet();

        while (true) {
            boolean end = false;

            if (set.contains(tokenType)) {
                throw unexpectedToken();
            }

            switch (tokenType) {

                case Token.AS : {
                    if (withType) {
                        read();

                        Type type = readTypeDefinition();

                        sequence.setDefaults(sequence.name, type);

                        break;
                    }

                    throw unexpectedToken();
                }
                case Token.START : {
                    if (isAlter) {
                        end = true;

                        break;
                    }

                    set.add(tokenType);
                    read();
                    readThis(Token.WITH);

                    long value = readBigint();

                    sequence.setStartValueNoCheck(value);

                    break;
                }
                case Token.RESTART : {
                    if (!isAlter) {
                        end = true;

                        break;
                    }

                    set.add(tokenType);
                    read();
                    readThis(Token.WITH);

                    long value = readBigint();

                    sequence.setStartValueNoCheck(value);

                    break;
                }
                case Token.INCREMENT : {
                    set.add(tokenType);
                    read();
                    readThis(Token.BY);

                    long value = readBigint();

                    sequence.setIncrement(value);

                    break;
                }
                case Token.NO :
                    read();

                    if (tokenType == Token.MAXVALUE) {
                        sequence.setDefaultMaxValue();
                    } else if (tokenType == Token.MINVALUE) {
                        sequence.setDefaultMinValue();
                    } else if (tokenType == Token.CYCLE) {
                        sequence.setCycle(false);
                    } else {
                        throw unexpectedToken();
                    }

                    set.add(tokenType);
                    read();
                    break;

                case Token.MAXVALUE : {
                    set.add(tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMaxValueNoCheck(value);

                    break;
                }
                case Token.MINVALUE : {
                    set.add(tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMinValueNoCheck(value);

                    break;
                }
                case Token.CYCLE :
                    set.add(tokenType);
                    read();
                    sequence.setCycle(true);
                    break;

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        sequence.checkValues();
    }

    /**
     * Reads and adds a table constraint definition to the list
     *
     * @param table this table
     * @param constraintList list of constraints
     * @throws HsqlException
     */
    private void readTableConstraint(Table table,
                                     HsqlArrayList constraintList)
                                     throws HsqlException {

        HsqlName constName = null;

        if (tokenType == Token.CONSTRAINT) {
            read();

            constName =
                readNewDependentSchemaObjectName(table.getSchemaName());
        }

        switch (tokenType) {

            case Token.PRIMARY : {
                read();
                readThis(Token.KEY);

                Constraint mainConst;

                mainConst = (Constraint) constraintList.get(0);

                if (mainConst.constType == Constraint.PRIMARY_KEY) {
                    throw Trace.error(Trace.SECOND_PRIMARY_KEY);
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("PK",
                            table.getSchemaName());
                }

                OrderedHashSet set = readColumnNames(false);
                Constraint c = new Constraint(constName, set,
                                              Constraint.PRIMARY_KEY);

                constraintList.set(0, c);

                break;
            }
            case Token.UNIQUE : {
                read();

                OrderedHashSet set = readColumnNames(false);

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            table.getSchemaName());
                }

                Constraint c = new Constraint(constName, set,
                                              Constraint.UNIQUE);

                constraintList.add(c);

                break;
            }
            case Token.FOREIGN : {
                read();
                readThis(Token.KEY);

                OrderedHashSet set = readColumnNames(false);
                Constraint     c   = readFKReferences(table, constName, set);

                constraintList.add(c);

                break;
            }
            case Token.CHECK : {
                read();

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            table.getSchemaName());
                }

                Constraint c = new Constraint(constName, null,
                                              Constraint.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
        }
    }

    /**
     * Reads column constraints
     */
    void readColumnConstraints(Table table, Column column,
                               HsqlArrayList constraintList)
                               throws HsqlException {

        boolean end = false;

        while (true) {
            HsqlName constName = null;

            if (tokenType == Token.CONSTRAINT) {
                read();

                constName =
                    readNewDependentSchemaObjectName(table.getSchemaName());
            }

            switch (tokenType) {

                case Token.PRIMARY : {
                    read();
                    readThis(Token.KEY);

                    Constraint existingConst =
                        (Constraint) constraintList.get(0);

                    if (existingConst.constType == Constraint.PRIMARY_KEY) {
                        throw Trace.error(Trace.SECOND_PRIMARY_KEY);
                    }

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("PK",
                                table.getSchemaName());
                    }

                    Constraint c = new Constraint(constName, set,
                                                  Constraint.PRIMARY_KEY);

                    constraintList.set(0, c);
                    column.setPrimaryKey(true);

                    break;
                }
                case Token.UNIQUE : {
                    read();

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName());
                    }

                    Constraint c = new Constraint(constName, set,
                                                  Constraint.UNIQUE);

                    constraintList.add(c);

                    break;
                }
                case Token.FOREIGN : {
                    read();
                    readThis(Token.KEY);

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    Constraint c = readFKReferences(table, constName, set);

                    constraintList.add(c);

                    break;
                }
                case Token.CHECK : {
                    read();

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName());
                    }

                    Constraint c = new Constraint(constName, null,
                                                  Constraint.CHECK);

                    readCheckConstraintCondition(c);

                    OrderedHashSet set = c.getCheckColumnExpressions();

                    for (int i = 0; i < set.size(); i++) {
                        Expression e = (Expression) set.get(i);

                        if (column.getName().name.equals(e.getColumnName())) {
                            if (e.getSchemaName() != null
                                    && e.getSchemaName()
                                       != table.getSchemaName().name) {
                                throw Trace.error(
                                    Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
                            }
                        } else {
                            throw Trace.error(Trace.COLUMN_NOT_FOUND);
                        }
                    }

                    constraintList.add(c);

                    break;
                }
                case Token.NOT : {
                    read();

                    if (!(Token.T_NULL.equals(tokenString)
                            && tokenType == Token.X_VALUE)) {
                        throw unexpectedToken();
                    }

                    read();

                    constName = database.nameManager.newAutoName("CT",
                            table.getSchemaName());

                    Constraint c = new Constraint(constName, null,
                                                  Constraint.CHECK);

                    c.check = new Expression(column);

                    constraintList.add(c);

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
     * @throws HsqlException
     */
    void readCheckConstraintCondition(Constraint c) throws HsqlException {

        readThis(Token.OPENBRACKET);

        Expression condition = readOr();

        readThis(Token.CLOSEBRACKET);

        c.check = condition;
    }

    /**
     * Process a bracketed column list as used in the declaration of SQL
     * CONSTRAINTS and return an array containing the indexes of the columns
     * within the table.
     *
     * @param table table that contains the columns
     * @param acceptAscDesc boolean
     * @return array of column indexes
     * @throws HsqlException if a column is not found or is duplicate
     */
    private int[] readColumnList(Table table,
                                 boolean ascOrDesc) throws HsqlException {

        OrderedHashSet set = readColumnNames(ascOrDesc);

        return table.getColumnIndexes(set);
    }

    /**
     *
     * @param type data type of column
     * @throws HsqlException
     * @return new Expression
     */
    private Expression readAndCheckDefaultClause(Type type)
    throws HsqlException {

        if (type.type == Types.OTHER) {
            throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
        }

        Expression expr = readDefaultClause(type);

        expr.resolveTypes(session, null);

        if (expr.isValidColumnDefaultExpression()) {
            Object defValTemp;

            try {
                defValTemp = expr.getValue(session, type);
            } catch (HsqlException e) {
                throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
            }

            if (defValTemp != null && database.sqlEnforceStrictSize) {
                try {
                    type.convertToTypeLimits(defValTemp);
                } catch (HsqlException e) {

                    // default value is too long for fixed size column
                    throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
                }
            }

            return expr;
        }

        throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
    }

    /**
     *  Reads a DEFAULT clause expression.
     */
    Expression readDefaultClause(Type dataType) throws HsqlException {

        SQLFunction function = LegacyFunction.newLegacyFunction(tokenString);

        try {
            if (function != null) {
                return readSQLFunction(function);
            }
        } catch (HsqlException e) {}

        function = SQLFunction.newSQLFunction(tokenString);

        if (function != null) {
            return readSQLFunction(function);
        }

        switch (tokenType) {

            case Token.MINUS : {
                read();

                if (tokenType == Token.X_VALUE) {
                    value = dataType.convertToType(session, value, valueType);

                    Expression e = new Expression(Expression.NEGATE,
                                                  new Expression(value,
                                                      dataType));

                    read();

                    return e;
                }

                break;
            }
            case Token.X_VALUE : {
                value = dataType.convertToType(session, value, valueType);

                Expression e = new Expression(value, dataType);

                read();

                return e;
            }
        }

        throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE, tokenString);
    }

    void processCreateIndex(boolean unique) throws HsqlException {

        Table  table;
        String schema = namePrefix;
        HsqlName indexHsqlName = database.nameManager.newHsqlName(tokenString,
            isQuoted);

        read();
        readThis(Token.ON);

        table = readTableName();

        String tableSchemaName = table.getSchemaName().name;

        checkSchemaUpdateAuthorization(tableSchemaName);

        if (schema != null &&!tableSchemaName.equals(schema)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        indexHsqlName.schema = table.getSchemaName();

        database.schemaManager.checkIndexExists(indexHsqlName.name,
                tableSchemaName, false);

        int[] indexColumns = readColumnList(table, true);

        session.commit();
        session.setScripting(true);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addIndex(indexColumns, indexHsqlName, unique);
    }

    /**
     * CREATE SCHEMA PUBLIC in scripts should pass this, so we do not throw
     * if this schema is created a second time
     */
    void processCreateSchema() throws HsqlException {

        if (session.isSchemaDefintion()) {
            throw Trace.error(Trace.INVALID_IDENTIFIER);
        }

        HsqlName schemaName = readNewSchemaName();

        readThis(Token.AUTHORIZATION);
        checkIsSimpleName();

        String ownername = tokenString;

        read();

        if (database.schemaManager.schemaExists(schemaName.name)) {
            if (!session.isProcessingScript) {
                throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
            }
        } else {
            Grantee owner = database.getGranteeManager().get(ownername);

            if (owner == null) {
                throw Trace.error(Trace.NO_SUCH_GRANTEE, ownername);
            }

            database.schemaManager.createSchema(schemaName, owner);
        }

        database.logger.writeToLog(session,
                                   DatabaseScript.getSchemaCreateDDL(database,
                                       schemaName));
        database.logger.writeToLog(session,
                                   "SET SCHEMA " + schemaName.statementName);
        session.startSchemaDefinition(schemaName.name);

        session.loggedSchema = session.currentSchema;
    }

    void processCreateUser() throws HsqlException {

        HsqlName name;
        String   password;
        boolean  admin   = false;
        String   grantor = session.getUsername();

        name = readUserIdentifier();

        readThis(Token.PASSWORD);

        password = readPassword();

        if (tokenType == Token.ADMIN) {
            read();

            admin = true;
        }

        database.getUserManager().createUser(name, password);

        if (admin) {
            database.getGranteeManager().grant(
                name.name, GranteeManager.DBA_ADMIN_ROLE_NAME, grantor);
        }
    }

    HsqlName readUserIdentifier() throws HsqlException {

        checkIsSimpleName();

        String  token    = tokenString;
        boolean isQuoted = this.isQuoted;

        if (token.equalsIgnoreCase("SA")) {
            token    = "SA";
            isQuoted = false;
        }

        HsqlName name = database.nameManager.newHsqlName(token, isQuoted);

        read();

        return name;
    }

    String readPassword() throws HsqlException {

        String token = tokenString;

        read();

        return token;
    }

    void processAlter() throws HsqlException {

        session.checkDDLWrite();
        session.setScripting(true);
        read();

        switch (tokenType) {

            case Token.INDEX : {
                read();
                processAlterIndexRename();

                break;
            }
            case Token.SCHEMA : {
                read();
                session.checkAdmin();
                processAlterSchemaRename();

                break;
            }
            case Token.SEQUENCE : {
                read();
                processAlterSequence();

                break;
            }
            case Token.TABLE : {
                read();
                processAlterTable();

                break;
            }
            case Token.USER : {
                read();
                session.checkAdmin();
                processAlterUser();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... RENAME ...
     * @param table table
     * @throws HsqlException
     */
    void processAlterTableRename(Table table) throws HsqlException {

        String schema = table.getSchemaName().name;

        checkIsName();

        String newSchema = namePrefix;

        newSchema = newSchema == null ? schema
                                      : session.getSchemaNameForWrite(
                                          newSchema);

        if (!schema.equals(newSchema)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        database.schemaManager.checkUserTableNotExists(session, tokenString,
                schema);
        session.commit();
        session.setScripting(true);
        database.schemaManager.renameTable(session, table, tokenString,
                                           isQuoted);
        read();
    }

    void processAlterTableAddUniqueConstraint(Table table,
            HsqlName name) throws HsqlException {

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName());
        }

        int[] cols = this.readColumnList(table, false);

        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addUniqueConstraint(cols, name);
    }

    void processAlterTableAddForeignKeyConstraint(Table table,
            HsqlName name) throws HsqlException {

        if (name == null) {
            name = database.nameManager.newAutoName("FK",
                    table.getSchemaName());
        }

        OrderedHashSet set = readColumnNames(false);
        Constraint     c   = readFKReferences(table, name, set);

        c.setColumnsIndexes(table);
        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addForeignKey(c);
    }

    void processAlterTableAddCheckConstraint(Table table,
            HsqlName name) throws HsqlException {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName());
        }

        check = new Constraint(name, null, Constraint.CHECK);

        readCheckConstraintCondition(check);
        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addCheckConstraint(check);
    }

    void processAlterTableAddColumn(Table table) throws HsqlException {

        int           colIndex = table.getColumnCount();
        HsqlArrayList list     = new HsqlArrayList();
        Constraint constraint  = new Constraint(null, null, Constraint.TEMP);

        list.add(constraint);
        checkIsName();

        HsqlName hsqlName = database.nameManager.newHsqlName(tokenString,
            isQuoted);

        read();

        Column column = readColumnDefinition(table, hsqlName, list);

        if (tokenType == Token.BEFORE) {
            read();

            colIndex = table.getColumnIndex(tokenString);

            read();
        }

        TableWorks tableWorks = new TableWorks(session, table);

        session.commit();
        tableWorks.addColumn(column, colIndex, list);

        return;
    }

    void processAlterTableAddPrimaryKey(Table table,
                                        HsqlName name) throws HsqlException {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName());
        }

        int[] cols = readColumnList(table, false);
        Constraint constraint = new Constraint(name, null,
                                               Constraint.PRIMARY_KEY);

        constraint.core.mainCols = cols;

        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addPrimaryKey(constraint, name);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP COLUMN ...
     *
     * @param t table
     * @throws HsqlException
     */
    void processAlterTableDropColumn(Table table, String colName,
                                     boolean cascade) throws HsqlException {

        int colindex = table.getColumnIndex(colName);

        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropColumn(colindex, cascade);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP CONSTRAINT ...
     *
     * @param t table
     * @param name
     * @throws HsqlException
     */
    void processAlterTableDropConstraint(Table table, String name,
                                         boolean cascade)
                                         throws HsqlException {

        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropConstraint(name, cascade);

        return;
    }

    void processAlterColumn(Table table, Column column,
                            int columnIndex) throws HsqlException {

        int position = getPosition();

        switch (tokenType) {

            case Token.RENAME : {
                read();
                readThis(Token.TO);
                processAlterColumnRename(table, column);

                return;
            }
            case Token.DROP : {
                read();

                if (tokenType == Token.DEFAULT) {
                    read();

                    TableWorks tw = new TableWorks(session, table);

                    tw.setColDefaultExpression(columnIndex, null);

                    return;
                } else if (tokenType == Token.GENERATED) {
                    read();
                    column.setIdentity(null);
                    table.setColumnTypeVars(columnIndex);

                    return;
                }
            }
            case Token.SET : {
                read();

                switch (tokenType) {

                    case Token.DEFAULT : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET DEFAULT
                        TableWorks tw   = new TableWorks(session, table);
                        Type       type = column.getType();
                        Expression expr =
                            this.readAndCheckDefaultClause(type);

                        tw.setColDefaultExpression(columnIndex, expr);

                        return;
                    }
                    case Token.NOT : {

                        //ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
                        read();
                        checkIsValue();

                        if (value != null) {
                            throw unexpectedToken();
                        }

                        read();

                        TableWorks tw = new TableWorks(session, table);

                        tw.setColNullability(column, false);

                        return;
                    }
                    case Token.X_VALUE : {
                        if (value == null) {
                            read();

                            //ALTER TABLE .. ALTER COLUMN .. SET NULL
                            TableWorks tw = new TableWorks(session, table);

                            tw.setColNullability(column, true);

                            return;
                        }

                        throw unexpectedToken();
                    }
                    default :
                        rewind(position);
                        read();
                        break;
                }
            }
            default :
        }

        if (tokenType == Token.SET || tokenType == Token.RESTART) {
            if (!column.isIdentity()) {
                throw Trace.error(Trace.SQL_IDENTITY_DEFINITION_NOT_EXISTS);
            }

            processAlterColumnSequenceOptions(column);

            return;
        } else {
            processAlterColumnType(table, column);

            return;
        }
    }

    void processAlterSequence() throws HsqlException {

        String schema = session.getSchemaNameForWrite(namePrefix);

        checkSchemaUpdateAuthorization(schema);

        NumberSequence sequence =
            database.schemaManager.getSequence(tokenString, schema);

        read();

        if (tokenType == Token.RENAME) {
            read();
            readThis(Token.TO);
            checkIsName();
            database.schemaManager.renameSequence(session, sequence,
                                                  tokenString, isQuoted);
            read();

            return;
        }

        NumberSequence copy = sequence.duplicate();

        readSequenceOptions(copy, false, true);
        sequence.reset(copy);
    }

    void processAlterColumnSequenceOptions(Column column)
    throws HsqlException {

        OrderedIntHashSet set = new OrderedIntHashSet();
        NumberSequence sequence = sequence =
            column.getIdentitySequence().duplicate();

        while (true) {
            boolean end = false;

            switch (tokenType) {

                case Token.RESTART : {
                    if (!set.add(tokenType)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Token.WITH);

                    long value = readBigint();

                    sequence.setStartValue(value);

                    break;
                }
                case Token.SET :
                    read();

                    switch (tokenType) {

                        case Token.INCREMENT : {
                            if (!set.add(tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            readThis(Token.BY);

                            long value = readBigint();

                            sequence.setIncrement(value);

                            break;
                        }
                        case Token.NO :
                            read();

                            if (tokenType == Token.MAXVALUE) {
                                sequence.setDefaultMaxValue();
                            } else if (tokenType == Token.MINVALUE) {
                                sequence.setDefaultMinValue();
                            } else if (tokenType == Token.CYCLE) {
                                sequence.setCycle(false);
                            } else {
                                throw unexpectedToken();
                            }

                            if (!set.add(tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            break;

                        case Token.MAXVALUE : {
                            if (!set.add(tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMaxValueNoCheck(value);

                            break;
                        }
                        case Token.MINVALUE : {
                            if (!set.add(tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMinValueNoCheck(value);

                            break;
                        }
                        case Token.CYCLE :
                            if (!set.add(tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            sequence.setCycle(true);
                            break;

                        default :
                            throw Trace.error(Trace.INVALID_IDENTIFIER,
                                              tokenString);
                    }
                    break;

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        sequence.checkValues();
        column.getIdentitySequence().reset(sequence);
    }

    /**
     * Allow changes to type of column or addition of an IDENTITY generator
     * Constraint definitions other than NOT NULL are not allowed
     */
    private void processAlterColumnType(Table table,
                                        Column oldCol) throws HsqlException {

        HsqlArrayList list = new HsqlArrayList();
        Constraint    c    = table.getPrimaryConstraint();

        if (c == null) {
            c = new Constraint(null, null, Constraint.TEMP);
        }

        list.add(c);

        Column newCol = readColumnDefinition(table, oldCol.columnName, list);

        if (oldCol.isIdentity() && newCol.isIdentity()) {
            throw Trace.error(Trace.SQL_SECOND_IDENTITY_COLUMN);
        }

        if (list.size() > 1) {
            throw Trace.error(Trace.SQL_CONSTRAINT_NOT_ALLOWED);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.retypeColumn(oldCol, newCol);
    }

    /**
     * Responsible for handling tail of ALTER COLUMN ... RENAME ...
     * @param table table
     * @param column column
     * @throws HsqlException
     */
    private void processAlterColumnRename(Table table,
                                          Column column)
                                          throws HsqlException {

        checkIsSimpleName();

        if (table.findColumn(tokenString) > -1) {
            throw Trace.error(Trace.COLUMN_ALREADY_EXISTS, tokenString);
        }

        table.database.schemaManager.checkColumnIsInView(table,
                column.columnName.name);
        session.commit();
        session.setScripting(true);
        table.renameColumn(column, tokenString, isQuoted);
        read();
    }

    /**
     * Handles ALTER SCHEMA ... RENAME TO ...
     *
     * @throws HsqlException
     */
    void processAlterSchemaRename() throws HsqlException {

        HsqlName name = readSchemaName();

        checkSchemaUpdateAuthorization(name.name);
        readThis(Token.RENAME);
        readThis(Token.TO);
        checkIsSimpleName();

        String  newName         = tokenString;
        boolean newNameIsQuoted = isQuoted;

        read();
        database.schemaManager.renameSchema(name.name, newName,
                                            newNameIsQuoted);
    }

    /**
     * Handles ALTER INDEX ... RENAME TO ...
     *
     * @throws HsqlException
     */
    void processAlterIndexRename() throws HsqlException {

        checkIsNameOrKeyword();

        String name   = tokenString;
        String schema = session.getSchemaNameForWrite(namePrefix);

        read();
        readThis(Token.RENAME);
        readThis(Token.TO);
        checkIsName();

        String  newName         = tokenString;
        String  newSchema       = namePrefix;
        boolean newNameIsQuoted = isQuoted;

        read();

        newSchema = newSchema == null ? schema
                                      : session.getSchemaNameForWrite(
                                          newSchema);

        if (!schema.equals(newSchema)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        Table t = database.schemaManager.findUserTableForIndex(session, name,
            schema);

        checkSchemaUpdateAuthorization(schema);

        if (t == null) {
            throw Trace.error(Trace.INDEX_NOT_FOUND, name);
        }

        database.schemaManager.checkIndexExists(name, t.getSchemaName().name,
                true);

        if (HsqlName.isReservedName(name)) {
            throw Trace.error(Trace.SYSTEM_INDEX, name);
        }

        if (HsqlName.isReservedName(newName)) {
            throw Trace.error(Trace.BAD_INDEX_CONSTRAINT_NAME, newName);
        }

        session.setScripting(true);
        session.commit();
        t.getIndex(name).setName(newName, newNameIsQuoted);
        database.schemaManager.renameIndex(name, newName, t.getName());
    }

    /**
     * Handles ALTER INDEX ... RENAME TO ...
     *
     * @throws HsqlException
     */
    void processAlterUser() throws HsqlException {

        String   password;
        User     userObject;
        HsqlName userName = readUserIdentifier();

        userObject = database.getUserManager().get(userName.name);

        Trace.check(!userName.name.equals(Token.T_PUBLIC),
                    Trace.NONMOD_ACCOUNT, Token.T_PUBLIC);
        readThis(Token.SET);

        if (tokenType == Token.PASSWORD) {
            read();

            password = readPassword();

            userObject.setPassword(password);
            database.logger.writeToLog(session, userObject.getAlterUserDDL());
            session.setScripting(false);
        } else if (tokenType == Token.INITIAL) {
            read();
            readThis(Token.SCHEMA);

            HsqlName schemaName;

            if (tokenType == Token.DEFAULT) {
                schemaName = null;
            } else {
                schemaName =
                    database.schemaManager.getSchemaHsqlName(tokenString);
            }

            userObject.setInitialSchema(schemaName);
            database.setMetaDirty(true);
            read();
        } else {
            throw unexpectedToken();
        }
    }

    /**
     * Responsible for handling parse and execute of SQL DROP DDL
     *
     * @throws  HsqlException
     */
    void processDrop() throws HsqlException {

        boolean isview;

        session.checkReadWrite();

        //session.checkAdmin();
        session.setScripting(true);
        read();

        isview = false;

        switch (tokenType) {

            case Token.INDEX : {
                processDropIndex();

                break;
            }
            case Token.SCHEMA : {
                session.checkAdmin();
                processDropSchema();

                break;
            }
            case Token.SEQUENCE : {
                processDropSequence();

                break;
            }
            case Token.TRIGGER : {
                session.checkAdmin();
                processDropTrigger();

                break;
            }
            case Token.USER : {
                session.checkAdmin();
                processDropUser();

                break;
            }
            case Token.ROLE : {
                session.checkAdmin();
                processDropRole();

                break;
            }
            case Token.VIEW : {
                isview = true;
            }    //fall thru
            case Token.TABLE : {
                processDropTable(isview);

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    void processDropTable(boolean isView) throws HsqlException {

        boolean ifexists = false;
        boolean cascade  = false;

        read();
        checkIsNameOrKeyword();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        String name   = tokenString;
        String schema = namePrefix;

        read();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        if (ifexists && schema != null
                &&!database.schemaManager.schemaExists(schema)) {
            return;
        }

        schema = session.getSchemaNameForWrite(schema);

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.dropTable(session, name, schema, ifexists,
                                         isView, cascade);
    }

    /**
     * Unless CASCADE option is given, we reject if target user is
     * the Direct authorization for any schema.
     */
    void processDropUser() throws HsqlException {

        boolean cascade = false;

        session.checkDDLWrite();
        read();
        checkIsSimpleName();

        String userName = tokenString;

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        Grantee grantee =
            database.getUserManager().get(userName).getGrantee();

        if (database.getSessionManager().isUserActive(userName)) {

            // todo - new error message "cannot drop a user that is currently connected."    // NOI18N
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        if (!cascade && database.schemaManager.hasSchemas(grantee)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        database.schemaManager.dropSchemas(grantee, cascade);
        database.getUserManager().dropUser(userName);
    }

    /**
     * Unless CASCADE option is given, we reject if target role is
     * the Direct authorization for any schema.
     */
    void processDropRole() throws HsqlException {

        boolean cascade = false;

        session.checkDDLWrite();

        // Unless CASCADE option is given, we reject if target role is
        // the Direct authorization for any schema.
        read();
        checkIsSimpleName();

        String roleToDrop = tokenString;

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        Grantee role = database.getGranteeManager().getRole(roleToDrop);

        if (!cascade && database.schemaManager.hasSchemas(role)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        database.schemaManager.dropSchemas(role, cascade);
        database.getGranteeManager().dropRole(roleToDrop);
    }

    void processDropSequence() throws HsqlException {

        boolean cascade  = false;
        boolean ifexists = false;

        session.checkDDLWrite();
        read();
        checkIsName();

        String name   = tokenString;
        String schema = namePrefix;

        read();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        if (ifexists && schema != null
                &&!database.schemaManager.schemaExists(schema)) {
            return;
        }

        schema = session.getSchemaNameForWrite(schema);

        NumberSequence sequence = database.schemaManager.findSequence(name,
            schema);

        if (sequence == null) {
            if (ifexists) {
                return;
            } else {
                throw Trace.error(Trace.SEQUENCE_NOT_FOUND);
            }
        }

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.checkCascadeDropViews(sequence, cascade);
        database.schemaManager.dropSequence(sequence);
    }

    void processDropTrigger() throws HsqlException {

        session.checkAdmin();
        session.checkDDLWrite();
        read();
        checkIsName();

        String triggername = tokenString;
        String schema      = session.getSchemaNameForWrite(namePrefix);

        read();
        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.dropTrigger(session, triggername, schema);
    }

    void processDropIndex() throws HsqlException {

        boolean ifexists = false;

        session.checkDDLWrite();
        read();
        checkIsName();

        String name   = tokenString;
        String schema = session.getSchemaNameForWrite(namePrefix);

        read();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        if (ifexists && schema != null
                &&!database.schemaManager.schemaExists(schema)) {
            return;
        }

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.dropIndex(session, name, schema, ifexists);
    }

    void processDropSchema() throws HsqlException {

        boolean ifexists = false;
        boolean cascade  = false;

        read();

        HsqlName name = readNewSchemaName();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        if (!database.schemaManager.schemaExists(name.name)) {
            if (ifexists) {
                return;
            }

            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        database.schemaManager.dropSchema(name.name, cascade);

        if (name == session.getCurrentSchemaHsqlName()) {
            session.setSchema(database.schemaManager.getDefaultSchemaName());
        }
    }

    boolean isGrantToken() {

        switch (tokenType) {

            case Token.ALL :
            case Token.INSERT :
            case Token.UPDATE :
            case Token.SELECT :
            case Token.DELETE :
            case Token.USAGE :
            case Token.EXECUTE :
            case Token.REFERENCES :
                return true;

            default :
                return false;
        }
    }

    /**
     *  Responsible for handling the execution of GRANT and REVOKE SQL
     *  statements.
     *
     * @param grant true if grant, false if revoke
     * @throws HsqlException
     */
    void processGrantOrRevoke(boolean grant) throws HsqlException {

        /* At some point, this should also permit holders of
         * "WITH GRANT OPTION" to do grants and revokes. */
        HsqlArrayList granteeList = new HsqlArrayList();
        String        grantor     = session.getUsername();
        Right         right       = null;
        SchemaObject  accessKey;
        boolean       isTable = false;
        boolean       isUsage = false;
        boolean       isExec  = false;
        boolean       isAll   = false;

        session.checkDDLWrite();
        session.setScripting(true);
        read();

        // this will catch all the keywords
        if (isQuoted ||!isGrantToken()) {
            session.checkAdmin();
            processRoleGrantOrRevoke(grant);

            return;
        }

        // ALL means all the rights the grantor can grant
        if (tokenType == Token.ALL) {
            read();

            if (tokenType == Token.PRIVILEGES) {
                read();
            }

            right = Right.fullRights;
            isAll = true;
        } else {
            right = new Right();

            boolean loop = true;

            while (loop) {
                checkIsNotQuoted();

                int rightType =
                    GranteeManager.getCheckSingleRight(tokenString);
                int            grantType = tokenType;
                OrderedHashSet columnSet = null;

                read();

                switch (grantType) {

                    case Token.SELECT :
                    case Token.INSERT :
                    case Token.UPDATE :
                        if (tokenType == Token.OPENBRACKET) {
                            columnSet = readColumnNames(false);
                        }
                    case Token.DELETE :
                        if (right == null) {
                            right = new Right();
                        }

                        right.set(rightType, columnSet);

                        isTable = true;
                        break;

                    case Token.USAGE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right   = Right.fullRights;
                        isUsage = true;
                        loop    = false;

                        continue;
                    case Token.EXECUTE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right  = Right.fullRights;
                        isExec = true;
                        loop   = false;

                        continue;
                    case Token.REFERENCES :
                        throw unexpectedToken();
                }

                if (tokenType == Token.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        readThis(Token.ON);

        accessKey = null;

        if (tokenString.equals(Token.T_CLASS)) {
            if (!isExec &&!isAll) {
                throw unexpectedToken();
            }

            read();
            session.checkAdmin();

            if (!isQuoted) {
                throw Trace.error(Trace.QUOTED_IDENTIFIER_REQUIRED);
            }

            // grant
            read();
        } else if (tokenType == Token.SEQUENCE) {
            if (!isUsage &&!isAll) {
                throw unexpectedToken();
            }

            read();
            checkIsName();

            String schema = session.getSchemaName(namePrefix);

            checkSchemaGrantAuthorization(schema);

            accessKey = database.schemaManager.getSequence(tokenString,
                    schema);

            read();
        } else {
            if (!isTable &&!isAll) {
                throw unexpectedToken();
            }

            readNoiseWord(Token.TABLE);

            Table  t      = readTableName();
            String schema = t.getSchemaName().name;

            checkSchemaGrantAuthorization(schema);

            accessKey = t;

            right.setColumns(t);

            if (t.getTableType() == Table.TEMP_TABLE &&!isAll) {
                throw unexpectedToken();
            }
        }

        if (grant) {
            readThis(Token.TO);
        } else {
            readThis(Token.FROM);
        }

        while (true) {
            checkIsSimpleName();
            granteeList.add(tokenString);
            read();

            if (tokenType != Token.COMMA) {
                break;
            }
        }

        GranteeManager gm = database.getGranteeManager();

        if (grant) {
            boolean withGrant = false;

/*
            if (tokenType == Token.WITH) {
                read();

                if (tokenType == Token.GRANT) {
                    read();
                    readThis(Token.OPTION);

                    withGrant = true;
                } else {
                    throw Trace.error(Trace.UNEXPECTED_TOKEN, tokenString);
                }
            }

            if (tokenType == Token.GRANTED) {
                read();
                readThis(Token.BY);
                checkIsSimpleName();

                grantor = tokenString;
                read();
            }
*/
            gm.grant(granteeList, accessKey, right, grantor, withGrant);
        } else {
            gm.revoke(granteeList, accessKey, right, grantor);
        }

        read();
    }

    /**
     *  Responsible for handling the execution of GRANT/REVOKE role...
     *  statements.
     *
     * @throws HsqlException
     */
    void processRoleGrantOrRevoke(boolean grant) throws HsqlException {

        String         grantor        = session.getUsername();
        HsqlArrayList  list           = new HsqlArrayList();
        GranteeManager granteeManager = database.getGranteeManager();

        while (true) {
            if (isGrantToken()) {
                throw Trace.error(Trace.ILLEGAL_ROLE_NAME, tokenString);
            }

            if (!granteeManager.isRole(tokenString)) {
                throw Trace.error(grant ? Trace.NO_SUCH_ROLE_GRANT
                                        : Trace.NO_SUCH_ROLE_REVOKE);
            }

            list.add(tokenString);
            read();

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            break;
        }

        if (grant) {
            readThis(Token.TO);
        } else {
            readThis(Token.FROM);
        }

        checkIsSimpleName();

        String grantee = tokenString;

        read();

        GranteeManager gm = database.getGranteeManager();

        for (int i = 0; i < list.size(); i++) {
            if (grant) {
                gm.grant(grantee, (String) list.get(i), grantor);
            } else {
                gm.revoke(grantee, (String) list.get(i), grantor);
            }
        }
    }
}
