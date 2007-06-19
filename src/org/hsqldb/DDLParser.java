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
import org.hsqldb.types.DomainType;
import org.hsqldb.types.DistinctType;

public class DDLParser extends Parser {

    DDLParser(Session session, Tokenizer t) {
        super(session, t);
    }

    void processCreate() throws HsqlException {

        int     tableType   = Table.MEMORY_TABLE;
        boolean isTempTable = false;
        boolean isTable     = false;

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
                read();
                processCreateAlias();
                break;

            case Token.SEQUENCE :
                read();
                processCreateSequence();
                break;

            case Token.SCHEMA :
                read();
                session.setScripting(false);
                processCreateSchema();
                break;

            case Token.TRIGGER :
                read();
                processCreateTrigger();
                break;

            case Token.USER :
                read();
                processCreateUser();
                break;

            case Token.ROLE :
                read();
                processCreateRole();
                break;

            case Token.VIEW :
                read();
                processCreateView();
                break;

            case Token.DOMAIN :
                read();
                processCreateDomain();
                break;

            case Token.TYPE :
                read();
                processCreateType();
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

    void processAlter() throws HsqlException {

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
                processAlterUser();

                break;
            }
            case Token.DOMAIN : {
                read();
                processAlterDomain();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    /**
     * Responsible for handling parse and execute of SQL DROP DDL
     *
     * @throws  HsqlException
     */
    void processDrop() throws HsqlException {

        boolean isview;

        session.setScripting(true);
        read();

        isview = false;

        switch (tokenType) {

            case Token.INDEX : {
                read();
                processDropIndex();

                break;
            }
            case Token.SCHEMA : {
                read();
                processDropSchema();

                break;
            }
            case Token.SEQUENCE : {
                read();
                processDropSequence();

                break;
            }
            case Token.TRIGGER : {
                read();
                processDropTrigger();

                break;
            }
            case Token.USER : {
                read();
                processDropUser();

                break;
            }
            case Token.ROLE : {
                read();
                processDropRole();

                break;
            }
            case Token.DOMAIN :
                read();
                processDropDomain();
                break;

            case Token.TYPE :
                read();
                processDropType();
                break;

            case Token.VIEW : {
                isview = true;
            }    //fall thru
            case Token.TABLE : {
                read();
                processDropTable(isview);

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    void processAlterTable() throws HsqlException {

        String   tableName = tokenString;
        HsqlName schema    = session.getSchemaHsqlName(namePrefix);

        checkSchemaUpdateAuthorization(schema);

        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        if (t.isView()) {
            throw Trace.error(Trace.NOT_A_TABLE);
        }

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

                    cname = readNewDependentSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);
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

                        processAlterTableDropConstraint(t, name, cascade);
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

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorization(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);

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
                    readConstraint(table, tempConstraints);
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
                        database.nameManager.newHsqlName(table.getSchemaName(),
                                                         tokenString,
                                                         isQuoted,
                                                         SchemaObject.COLUMN,
                                                         name);

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

            database.schemaManager.addSchemaObject(table);
        } catch (HsqlException e) {
            database.schemaManager.removeExportedKeys(table);
            database.schemaManager.removeDependentObjects(table.getName());

            throw e;
        }
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    Table addTableConstraintDefinitions(Table table,
                                        HsqlArrayList tempConstraints)
                                        throws HsqlException {

        Constraint c        = (Constraint) tempConstraints.get(0);
        String     namePart = c.getName() == null ? null
                                                  : c.getName().name;
        HsqlName indexName = database.nameManager.newAutoName("IDX", namePart,
            table.getSchemaName(), table.getName(), SchemaObject.INDEX);

        if (c.mainColSet != null) {
            c.core.mainCols = table.getColumnIndexes(c.mainColSet);
        }

        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(c.getName(), table,
                table.getPrimaryIndex(), Constraint.PRIMARY_KEY);

            table.addConstraint(newconstraint);
            database.schemaManager.addSchemaObject(newconstraint);
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
                            c.getName().name, table.getSchemaName(),
                            table.getName(), SchemaObject.INDEX);

                    Index index =
                        table.createAndAddIndexStructure(c.core.mainCols,
                                                         indexName, true,
                                                         true, false);
                    Constraint newconstraint = new Constraint(c.getName(),
                        table, index, Constraint.UNIQUE);

                    table.addConstraint(newconstraint);
                    database.schemaManager.addSchemaObject(newconstraint);

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

                    boolean isForward = c.core.mainTable.getSchemaName()
                                        != table.getSchemaName();
                    int offset = database.schemaManager.getTableIndex(table);

                    if (offset != -1
                            && offset
                               < database.schemaManager.getTableIndex(
                                   c.core.mainTable)) {
                        isForward = true;
                    }

                    HsqlName refIndexName =
                        database.nameManager.newAutoName("IDX",
                                                         table.getSchemaName(),
                                                         table.getName(),
                                                         SchemaObject.INDEX);
                    Index index =
                        table.createAndAddIndexStructure(c.core.refCols,
                                                         refIndexName, false,
                                                         true, isForward);
                    HsqlName mainName = database.nameManager.newAutoName("REF",
                        c.getName().name, table.getSchemaName(),
                        table.getName(), SchemaObject.INDEX);

                    c.core.uniqueName = uniqueConstraint.getName();
                    c.core.mainName   = mainName;
                    c.core.mainIndex  = mainIndex;
                    c.core.refTable   = table;
                    c.core.refName    = c.getName();
                    c.core.refIndex   = index;
                    c.isForward       = isForward;

                    table.addConstraint(c);
                    c.core.mainTable.addConstraint(new Constraint(mainName,
                            c));
                    database.schemaManager.addSchemaObject(c);

                    break;
                }
                case Constraint.CHECK : {
                    c.prepareCheckConstraint(session, table, false);
                    table.addConstraint(c);

                    if (c.isNotNull()) {
                        Column column = table.getColumn(c.notNullColumnIndex);

                        column.setNullable(false);
                        table.setColumnTypeVars(c.notNullColumnIndex);
                    }

                    database.schemaManager.addSchemaObject(c);

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

        String schema = namePrefix;

        if (namePrefix == null) {
            schema = refTable.getSchemaName().name;
        }

        if (refTable.getSchemaName().name.equals(schema)
                && refTable.getName().name.equals(tokenString)) {
            mainTable = refTable;

            read();
        } else {
            mainTable = readTableName(schema);
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
                    refTable.getSchemaName(), refTable.getName(),
                    SchemaObject.CONSTRAINT);
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

        HsqlName name = readNewSchemaObjectName(SchemaObject.VIEW);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        HsqlName[] colList = null;

        if (tokenType == Token.OPENBRACKET) {
            colList = readColumnNames(name);
        }

        readThis(Token.AS);

        int    position = getPosition();
        Select select   = readQueryExpression(0, true, false, true, true);

        if (select.intoTableName != null) {
            throw (Trace.error(Trace.INVALID_IDENTIFIER, Token.INTO));
        }

        String sql = getLastPart(position);

        checkSchemaUpdateAuthorization(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);

        View view = new View(session, database, name, sql, colList);

        database.schemaManager.addSchemaObject(view);
    }

    void processCreateSequence() throws HsqlException {

/*
        CREATE SEQUENCE <name>
        [AS {INTEGER | BIGINT}]
        [START WITH <value>]
        [INCREMENT BY <value>]
*/
        HsqlName name = readNewSchemaObjectName(SchemaObject.SEQUENCE);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorization(name.schema);

        NumberSequence sequence = new NumberSequence(name, Type.SQL_INTEGER);

        readSequenceOptions(sequence, true, false);
        database.schemaManager.addSchemaObject(sequence);
    }

    void processCreateDomain() throws HsqlException {

        DomainType domain = null;
        HsqlName   name   = readNewSchemaObjectName(SchemaObject.DOMAIN);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorization(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);
        readIfThis(Token.AS);

        Type       predefinedType = readTypeDefinition(false);
        Expression defaultClause  = null;

        if (readIfThis(Token.DEFAULT)) {
            defaultClause = readDefaultClause(predefinedType);
        }

        domain = new DomainType(name, predefinedType, defaultClause);

        HsqlArrayList tempConstraints = new HsqlArrayList();

        compileContext.currentDomain = domain;

        while (true) {
            boolean end = false;

            switch (tokenType) {

                case Token.CONSTRAINT :
                case Token.CHECK :
                    readConstraint(domain, tempConstraints);
                    break;

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        compileContext.currentDomain = null;

        for (int i = 0; i < tempConstraints.size(); i++) {
            Constraint c = (Constraint) tempConstraints.get(i);

            c.prepareCheckConstraint(session, null, false);
            domain.addConstraint(c);
            database.schemaManager.addSchemaObject(c);
        }

        database.schemaManager.addSchemaObject(domain);
    }

    void processCreateType() throws HsqlException {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TYPE);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorization(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);
        readThis(Token.AS);

        Type predefinedType = readTypeDefinition(false);

        readIfThis(Token.FINAL);

        DistinctType userType = new DistinctType(name, predefinedType);

        // create the type
        database.schemaManager.addSchemaObject(userType);
    }

    /**
     * If an invalid alias is encountered while processing an old script,
     * simply discard it.
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

        checkDatabaseUpdateAuthorization();

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
        int            beforeOrAfterType;
        String         operation;
        int            operationType;
        String         className;
        TriggerDef     td;
        HsqlName       name    = readNewSchemaObjectName(SchemaObject.TRIGGER);
        OrderedHashSet columns = null;

        queueSize         = TriggerDef.defaultQueueSize;
        beforeOrAfter     = tokenString;
        beforeOrAfterType = tokenType;

        switch (tokenType) {

            case Token.BEFORE :
            case Token.AFTER :
                read();
                break;

            default :
                throw unexpectedToken();
        }

        operation     = tokenString;
        operationType = tokenType;

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

        name.setSchemaIfNull(table.getSchemaName());
        checkSchemaUpdateAuthorization(name.schema);

        if (table.isView()) {
            throw Trace.error(Trace.NOT_A_TABLE);
        }

        if (name.schema != table.getSchemaName()) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              name.schema.name);
        }

        name.parent = table.getName();

        database.schemaManager.checkSchemaObjectNotExists(name);

        if (columns != null) {
            int[] cols = table.getColumnIndexes(columns);

            // do this inside trigger class
            table.getColumnCheckList(cols);
        }

        Expression      condition          = null;
        String          oldTableName       = null;
        String          newTableName       = null;
        String          oldRowName         = null;
        String          newRowName         = null;
        Table[]         transitions        = new Table[4];
        RangeVariable[] rangeVars          = new RangeVariable[4];
        HsqlArrayList   compiledStatements = new HsqlArrayList();
        String          conditionSQL       = null;
        String          procedureSQL       = null;

        if (tokenType == Token.REFERENCING) {
            read();

            if (tokenType != Token.OLD && tokenType != Token.NEW) {
                throw unexpectedToken();
            }

            while (true) {
                if (tokenType == Token.OLD) {
                    if (operationType == Token.INSERT) {
                        throw unexpectedToken();
                    }

                    read();

                    if (tokenType == Token.TABLE) {
                        if (oldTableName != null
                                || beforeOrAfterType == Token.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Token.AS);
                        checkIsSimpleName();

                        oldTableName = tokenString;

                        String n = oldTableName;

                        if (n.equals(newTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isQuoted,
                            SchemaObject.TRANSITION);
                        Table transition = table.newTransitionTable(hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, compileContext);

                        transitions[TriggerDef.OLD_TABLE] = transition;
                        rangeVars[TriggerDef.OLD_TABLE]   = range;
                    } else if (tokenType == Token.ROW) {
                        if (oldRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Token.AS);
                        checkIsSimpleName();

                        oldRowName = tokenString;

                        String n = oldRowName;

                        if (n.equals(newTableName) || n.equals(oldTableName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = true;

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isQuoted,
                            SchemaObject.TRANSITION);
                        Table transition = table.newTransitionTable(hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, compileContext);

                        transitions[TriggerDef.OLD_ROW] = transition;
                        rangeVars[TriggerDef.OLD_ROW]   = range;
                    } else {
                        throw unexpectedToken();
                    }
                } else if (tokenType == Token.NEW) {
                    if (operationType == Token.DELETE) {
                        throw unexpectedToken();
                    }

                    read();

                    if (tokenType == Token.TABLE) {
                        if (newTableName != null
                                || beforeOrAfterType == Token.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Token.AS);
                        checkIsSimpleName();

                        newTableName = tokenString;

                        String n = newTableName;

                        if (n.equals(oldTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isQuoted,
                            SchemaObject.TRANSITION);
                        Table transition = table.newTransitionTable(hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, compileContext);

                        transitions[TriggerDef.NEW_TABLE] = transition;
                        rangeVars[TriggerDef.NEW_TABLE]   = range;
                    } else if (tokenType == Token.ROW) {
                        if (newRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Token.AS);
                        checkIsSimpleName();

                        newRowName   = tokenString;
                        isForEachRow = true;

                        String n = newRowName;

                        if (n.equals(oldTableName) || n.equals(newTableName)
                                || n.equals(oldRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isQuoted,
                            SchemaObject.TRANSITION);
                        Table transition = table.newTransitionTable(hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, compileContext);

                        transitions[TriggerDef.NEW_ROW] = transition;
                        rangeVars[TriggerDef.NEW_ROW]   = range;
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

        //
        if (rangeVars[TriggerDef.OLD_TABLE] != null) {}

        if (rangeVars[TriggerDef.NEW_TABLE] != null) {}

        //
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

            int position = getPosition();

            condition    = readOr();
            conditionSQL = getLastPart(position);

            readThis(Token.CLOSEBRACKET);

            if (condition.getDataType().type != Types.SQL_BOOLEAN) {
                throw Trace.error(Trace.NOT_A_CONDITION);
            }

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

            database.schemaManager.addSchemaObject(td);
            session.setScripting(true);

            return;
        }

        boolean isBlock = false;

        if (readIfThis(Token.BEGIN)) {
            readThis(Token.ATOMIC);

            isBlock = true;
        }

        int position = getPosition();

        while (true) {
            CompiledStatement cs = null;

            switch (tokenType) {

                case Token.INSERT :
                    if (beforeOrAfterType == Token.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileInsertStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Token.SEMICOLON);
                    }
                    break;

                case Token.UPDATE :
                    if (beforeOrAfterType == Token.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileUpdateStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Token.SEMICOLON);
                    }
                    break;

                case Token.DELETE :
                    if (beforeOrAfterType == Token.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileDeleteStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Token.SEMICOLON);
                    }
                    break;

                case Token.MERGE :
                    if (beforeOrAfterType == Token.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileMergeStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Token.SEMICOLON);
                    }
                    break;

                case Token.SET :
                    if (beforeOrAfterType != Token.BEFORE
                            || operationType == Token.DELETE) {
                        throw unexpectedToken();
                    }

                    cs = compileSetStatement(table, rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Token.SEMICOLON);
                    }
                    break;

                case Token.END :
                    break;

                default :
                    throw unexpectedToken();
            }

            if (!isBlock || tokenType == Token.END) {
                break;
            }
        }

        procedureSQL = getLastPart(position);

        if (isBlock) {
            readThis(Token.END);
        }

        CompiledStatement[] csArray =
            new CompiledStatement[compiledStatements.size()];

        compiledStatements.toArray(csArray);

        OrderedHashSet references = compileContext.getSchemaObjectNames();

        for (int i = 0; i < csArray.length; i++) {
            boolean[] check = csArray[i].getInsertOrUpdateColumnCheckList();

            if (check != null) {
                table.getColumnNames(check, references);
            }
        }

        references.remove(table.getName());

        td = new TriggerDefSQL(name, beforeOrAfter, operation, isForEachRow,
                               table, transitions, rangeVars, condition,
                               csArray, conditionSQL, procedureSQL,
                               references);

        table.addTrigger(td);
        database.schemaManager.addSchemaObject(td);
        session.setScripting(true);
    }

    /**
     * Retrieves an SET CompiledStatement from this parse context.
     */
    CompiledStatement compileSetStatement(Table table,
                                          RangeVariable[] rangeVars)
                                          throws HsqlException {

        read();

        Expression[]   updateExpressions;
        int[]          columnMap;
        OrderedHashSet colNames = new OrderedHashSet();
        HsqlArrayList  exprList = new HsqlArrayList();

        readSetClauseList(rangeVars[TriggerDef.NEW_ROW], colNames, exprList);

        columnMap         = table.getColumnIndexes(colNames);
        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);
        resolveUpdateExpressions(table, rangeVars, columnMap,
                                 updateExpressions, emptyRangeVariables);

        CompiledStatement cs = new CompiledStatement(session, table,
            rangeVars, columnMap, updateExpressions, compileContext);

        return cs;
    }

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
            typeObject = readTypeDefinition(true);
        }

        if (isIdentity) {}
        else if (tokenType == Token.DEFAULT) {
            read();

            defaultExpr = readDefaultClause(typeObject);
        } else if (tokenType == Token.GENERATED && !isIdentity) {
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

        if (tokenType == Token.IDENTITY && !isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        }

        if (isIdentity) {
            column.setIdentity(sequence);
        }

        if (isPKIdentity && !column.isPrimaryKey()) {
            OrderedHashSet set = new OrderedHashSet();

            set.add(column.getName().name);

            HsqlName constName = database.nameManager.newAutoName("PK",
                table.getSchemaName(), table.getName(),
                SchemaObject.CONSTRAINT);
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

                        Type type = readTypeDefinition(false);

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
    private void readConstraint(SchemaObject schemaObject,
                                HsqlArrayList constraintList)
                                throws HsqlException {

        HsqlName constName = null;

        if (tokenType == Token.CONSTRAINT) {
            read();

            constName =
                readNewDependentSchemaObjectName(schemaObject.getName(),
                                                 SchemaObject.CONSTRAINT);
        }

        switch (tokenType) {

            case Token.PRIMARY : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedToken(Token.T_CHECK);
                }

                read();
                readThis(Token.KEY);

                Constraint mainConst;

                mainConst = (Constraint) constraintList.get(0);

                if (mainConst.constType == Constraint.PRIMARY_KEY) {
                    throw Trace.error(Trace.SECOND_PRIMARY_KEY);
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("PK",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                OrderedHashSet set = readColumnNames(false);
                Constraint c = new Constraint(constName, set,
                                              Constraint.PRIMARY_KEY);

                constraintList.set(0, c);

                break;
            }
            case Token.UNIQUE : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedToken(Token.T_CHECK);
                }

                read();

                OrderedHashSet set = readColumnNames(false);

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(constName, set,
                                              Constraint.UNIQUE);

                constraintList.add(c);

                break;
            }
            case Token.FOREIGN : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedToken(Token.T_CHECK);
                }

                read();
                readThis(Token.KEY);

                OrderedHashSet set = readColumnNames(false);
                Constraint c = readFKReferences((Table) schemaObject,
                                                constName, set);

                constraintList.add(c);

                break;
            }
            case Token.CHECK : {
                read();

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(constName, null,
                                              Constraint.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
            default : {
                if (constName != null) {
                    throw Trace.error(Trace.UNEXPECTED_TOKEN);
                }
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

                constName = readNewDependentSchemaObjectName(table.getName(),
                        SchemaObject.CONSTRAINT);
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
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
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
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
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
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
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
                            table.getSchemaName(), table.getName(),
                            SchemaObject.CONSTRAINT);

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
     *  Reads a DEFAULT clause expression.
     */
    Expression readDefaultClause(Type dataType) throws HsqlException {

        Expression e = readTerm();

        switch (e.getType()) {

            case Expression.VALUE :
                break;

            case Expression.NEGATE :
                if (e.getArg().getType() == Expression.VALUE) {
                    break;
                }

                throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE, tokenString);
            case Expression.SQL_FUNCTION :
                if (((SQLFunction) e).isValueFunction()) {
                    break;
                }

                throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE, tokenString);
        }

        e.resolveTypes(null);
        e.getValue(session, dataType);

        return e;
    }

    void processCreateIndex(boolean unique) throws HsqlException {

        Table    table;
        HsqlName indexHsqlName = readNewSchemaObjectName(SchemaObject.INDEX);

        readThis(Token.ON);

        table = readTableName();

        HsqlName tableSchema = table.getSchemaName();

        indexHsqlName.setSchemaIfNull(tableSchema);

        indexHsqlName.parent = table.getName();

        checkSchemaUpdateAuthorization(tableSchema);

        if (indexHsqlName.schema != tableSchema) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        indexHsqlName.schema = table.getSchemaName();

        database.schemaManager.checkSchemaObjectNotExists(indexHsqlName);

        int[] indexColumns = readColumnList(table, true);

        session.commit();

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
        checkDatabaseUpdateAuthorization();

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

    void processCreateRole() throws HsqlException {

        HsqlName name = readUserIdentifier();

        checkDatabaseUpdateAuthorization();
        database.getGranteeManager().addRole(name);
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

        checkDatabaseUpdateAuthorization();
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

        HsqlName name = database.nameManager.newHsqlName(token, isQuoted,
            SchemaObject.GRANTEE);

        read();

        return name;
    }

    String readPassword() throws HsqlException {

        String token = tokenString;

        read();

        return token;
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... RENAME ...
     * @param table table
     * @throws HsqlException
     */
    void processAlterTableRename(Table table) throws HsqlException {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE);

        name.setSchemaIfNull(table.getSchemaName());

        if (table.getSchemaName() != name.schema) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        database.schemaManager.renameSchemaObject(table.getName(), name);
    }

    void processAlterTableAddUniqueConstraint(Table table,
            HsqlName name) throws HsqlException {

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
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
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
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
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, Constraint.CHECK);

        readCheckConstraintCondition(check);
        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addCheckConstraint(check);
    }

    void processAlterTableAddColumn(Table table) throws HsqlException {

        int           colIndex   = table.getColumnCount();
        HsqlArrayList list       = new HsqlArrayList();
        Constraint    constraint = new Constraint(null, null, Constraint.TEMP);

        list.add(constraint);
        checkIsName();

        HsqlName hsqlName =
            database.nameManager.newHsqlName(table.getSchemaName(),
                                             tokenString, isQuoted,
                                             SchemaObject.COLUMN,
                                             table.getName());

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
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
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
                        Expression expr = this.readDefaultClause(type);

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

        HsqlName schema = session.getSchemaHsqlName(namePrefix);

        checkSchemaUpdateAuthorization(schema);

        NumberSequence sequence =
            database.schemaManager.getSequence(tokenString, schema.name, true);

        read();

        if (tokenType == Token.RENAME) {
            read();
            readThis(Token.TO);

            HsqlName name = readNewSchemaObjectName(SchemaObject.SEQUENCE);

            name.setSchemaIfNull(sequence.getSchemaName());

            if (sequence.getSchemaName() != name.schema) {
                throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
            }

            session.commit();
            database.schemaManager.renameSchemaObject(sequence.getName(),
                    name);

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
                                          Column column) throws HsqlException {

        checkIsSimpleName();

        if (table.findColumn(tokenString) > -1) {
            throw Trace.error(Trace.COLUMN_ALREADY_EXISTS, tokenString);
        }

        table.database.schemaManager.checkColumnIsReferenced(column.getName());
        session.commit();
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

        checkSchemaUpdateAuthorization(name);
        readThis(Token.RENAME);
        readThis(Token.TO);

        HsqlName newName = readNewSchemaName();

        checkDatabaseUpdateAuthorization();
        database.schemaManager.renameSchema(name, newName);
    }

    /**
     * Handles ALTER INDEX ... RENAME TO ...
     *
     * @throws HsqlException
     */
    void processAlterIndexRename() throws HsqlException {

        checkIsName();

        HsqlName schema = session.getSchemaHsqlName(namePrefix);
        HsqlName name = database.schemaManager.getSchemaObjectName(schema,
            tokenString, SchemaObject.INDEX);

        read();
        readThis(Token.RENAME);
        readThis(Token.TO);

        HsqlName newName = readNewSchemaObjectName(SchemaObject.INDEX);

        newName.setSchemaIfNull(schema);

        if (name.schema != newName.schema) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.renameSchemaObject(name, newName);
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

        checkDatabaseUpdateAuthorization();

        userObject = database.getUserManager().get(userName.name);

        Trace.check(!userName.name.equals(Token.T_PUBLIC),
                    Trace.NONMOD_ACCOUNT, Token.T_PUBLIC);
        readThis(Token.SET);

        if (tokenType == Token.PASSWORD) {
            read();

            password = readPassword();

            userObject.setPassword(password);
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

    void processAlterDomain() throws HsqlException {

        HsqlName schema = session.getSchemaHsqlName(namePrefix);

        checkSchemaUpdateAuthorization(schema);

        DomainType domain = database.schemaManager.getDomain(tokenString,
            schema.name, true);

        read();

        switch (tokenType) {

            case Token.RENAME : {
                read();
                readThis(Token.TO);

                HsqlName newName =
                    readNewSchemaObjectName(SchemaObject.DOMAIN);

                newName.setSchemaIfNull(schema);

                if (domain.getSchemaName() != newName.schema) {
                    throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
                }

                checkSchemaUpdateAuthorization(schema);
                database.schemaManager.renameSchemaObject(domain.getName(),
                        newName);

                return;
            }
            case Token.DROP : {
                read();

                if (tokenType == Token.DEFAULT) {
                    read();
                    domain.removeDefaultClause();

                    return;
                } else if (tokenType == Token.CONSTRAINT) {
                    read();
                    checkIsName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), tokenString,
                        SchemaObject.CONSTRAINT);

                    read();

//                    domain.removeConstraint(tokenString);
                    database.schemaManager.removeSchemaObject(name);

                    return;
                }
            }
            case Token.SET : {
                read();
                readThis(Token.DEFAULT);

                Expression e = readDefaultClause(domain);

                domain.setDefaultClause(e);

                return;
            }
            case Token.ADD : {
                read();

                if (tokenType == Token.CONSTRAINT
                        || tokenType == Token.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    readConstraint(domain, tempConstraints);

                    Constraint c = (Constraint) tempConstraints.get(0);

                    domain.addConstraint(c);
                    database.schemaManager.addSchemaObject(c);
                }

                return;
            }
        }
    }

    void processDropTable(boolean isView) throws HsqlException {

        boolean ifExists = false;
        boolean cascade  = false;

        checkIsNameOrKeyword();

        String name   = tokenString;
        String schema = namePrefix;

        read();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifExists = true;
        }

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        if (ifExists && schema != null
                && !database.schemaManager.schemaExists(schema)) {
            return;
        }

        HsqlName schemaHsqlName = session.getSchemaHsqlName(schema);

        checkSchemaUpdateAuthorization(schemaHsqlName);

        Table table = database.schemaManager.findUserTable(session, name,
            schemaHsqlName.name);

        if (table == null || table.isView() != isView) {
            if (ifExists) {
                return;
            } else {
                throw Trace.error(isView ? Trace.VIEW_NOT_FOUND
                                         : Trace.TABLE_NOT_FOUND, name);
            }
        }

        database.schemaManager.dropTableOrView(session, table, cascade);
    }

    /**
     * Unless CASCADE option is given, we reject if target user is
     * the Direct authorization for any schema.
     */
    void processDropUser() throws HsqlException {

        boolean cascade = false;

        checkIsSimpleName();

        String userName = tokenString;

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        checkDatabaseUpdateAuthorization();

        Grantee grantee = database.getUserManager().get(userName).getGrantee();

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

        // Unless CASCADE option is given, we reject if target role is
        // the Direct authorization for any schema.
        checkIsSimpleName();

        String roleToDrop = tokenString;

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        checkDatabaseUpdateAuthorization();

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
                && !database.schemaManager.schemaExists(schema)) {
            return;
        }

        HsqlName schemaHsqlName = session.getSchemaHsqlName(schema);
        NumberSequence sequence = database.schemaManager.getSequence(name,
            schemaHsqlName.name, !ifexists);

        if (sequence == null) {
            return;
        }

        checkSchemaUpdateAuthorization(schemaHsqlName);
        database.schemaManager.removeSchemaObject(sequence.getName(), cascade);
    }

    void processDropTrigger() throws HsqlException {

        checkIsName();

        String   name   = tokenString;
        HsqlName schema = session.getSchemaHsqlName(namePrefix);

        read();
        checkSchemaUpdateAuthorization(schema);

        HsqlName triggerName =
            database.schemaManager.getSchemaObjectName(schema, name,
                SchemaObject.TRIGGER);

        database.schemaManager.removeSchemaObject(triggerName);
    }

    void processDropIndex() throws HsqlException {

        boolean ifexists = false;

        checkIsName();

        String   name   = tokenString;
        HsqlName schema = session.getSchemaHsqlName(namePrefix);

        read();

        if (tokenType == Token.IF) {
            read();
            readThis(Token.EXISTS);

            ifexists = true;
        }

        if (ifexists && schema != null
                && !database.schemaManager.schemaExists(schema.name)) {
            return;
        }

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.dropIndex(session, name, schema.name, ifexists);
    }

    void processDropSchema() throws HsqlException {

        boolean  ifexists = false;
        boolean  cascade  = false;
        HsqlName name     = readNewSchemaName();

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
                session.setScripting(false);

                return;
            }

            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        checkDatabaseUpdateAuthorization();
        database.schemaManager.dropSchema(name.name, cascade);
    }

    void processDropDomain() throws HsqlException {

        boolean cascade = false;

        checkIsNameOrKeyword();

        HsqlName schema = session.getSchemaHsqlName(namePrefix);
        HsqlName name = database.schemaManager.getSchemaObjectName(schema,
            tokenString, SchemaObject.DOMAIN);
        DomainType domain =
            (DomainType) database.schemaManager.findSchemaObject(schema.name,
                tokenString, SchemaObject.DOMAIN);

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        checkSchemaUpdateAuthorization(schema);

        if (!cascade) {
            database.schemaManager.checkObjectIsReferenced(name);
        }

        OrderedHashSet set =
            database.schemaManager.getReferencingObjects(name);

        for (int i = 0; i < set.size(); i++) {
            HsqlName n     = (HsqlName) set.get(i);
            Table    table = (Table) database.schemaManager.getSchemaObject(n);

            table.removeDomainOrType(name);
        }

        Constraint[] constraints = domain.getConstraints();

        set.clear();
        set.addAll(constraints);
        database.schemaManager.removeSchemaObjects(set);
        database.schemaManager.removeSchemaObject(name, cascade);
    }

    void processDropType() throws HsqlException {

        boolean cascade = false;

        checkIsNameOrKeyword();

        HsqlName schema = session.getSchemaHsqlName(namePrefix);
        HsqlName name = database.schemaManager.getSchemaObjectName(schema,
            tokenString, SchemaObject.TYPE);

        read();

        if (tokenType == Token.CASCADE) {
            cascade = true;

            read();
        } else if (tokenType == Token.RESTRICT) {
            read();
        }

        checkSchemaUpdateAuthorization(schema);
        database.schemaManager.removeSchemaObject(name, cascade);
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

        session.setScripting(true);

        // this will catch all the keywords
        if (isQuoted || !isGrantToken()) {
            checkDatabaseUpdateAuthorization();
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
            if (!isExec && !isAll) {
                throw unexpectedToken();
            }

            read();
            checkDatabaseUpdateAuthorization();

            if (!isQuoted) {
                throw Trace.error(Trace.QUOTED_IDENTIFIER_REQUIRED);
            }

            // grant
            read();
        } else if (tokenType == Token.SEQUENCE) {
            if (!isUsage && !isAll) {
                throw unexpectedToken();
            }

            read();
            checkIsName();

            HsqlName schema = session.getSchemaHsqlName(namePrefix);

            checkSchemaGrantAuthorization(schema);

            accessKey = database.schemaManager.getSequence(tokenString,
                    schema.name, true);

            read();
        } else {
            if (!isTable && !isAll) {
                throw unexpectedToken();
            }

            readIfThis(Token.TABLE);

            Table    t      = readTableName();
            HsqlName schema = t.getSchemaName();

            checkSchemaGrantAuthorization(schema);

            accessKey = t;

            right.setColumns(t);

            if (t.getTableType() == Table.TEMP_TABLE && !isAll) {
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
                readThis(Token.GRANT);
                readThis(Token.OPTION);

                withGrant = true;
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

    void checkSchemaUpdateAuthorization(HsqlName schema) throws HsqlException {

        if (database.schemaManager.isSystemSchema(schema.name)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        session.getUser().checkSchemaUpdateOrGrantRights(schema.name);
        session.checkDDLWrite();
    }

    void checkSchemaGrantAuthorization(HsqlName schema) throws HsqlException {
        session.getUser().checkSchemaUpdateOrGrantRights(schema.name);
        session.checkDDLWrite();
    }

    void checkDatabaseUpdateAuthorization() throws HsqlException {
        session.checkAdmin();
        session.checkDDLWrite();
    }
}
