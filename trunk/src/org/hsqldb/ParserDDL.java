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

import java.lang.reflect.Method;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.Right;
import org.hsqldb.rights.User;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Type;
import org.hsqldb.types.UserTypeModifier;

/**
 * Parser for DDL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ParserDDL extends ParserRoutine {

    final static int[]   schemaCommands             = new int[] {
        Tokens.CREATE, Tokens.GRANT
    };
    final static short[] startStatementTokens       = new short[] {
        Tokens.CREATE, Tokens.GRANT, Tokens.ALTER, Tokens.DROP
    };
    final static short[] startStatementTokensSchema = new short[] {
        Tokens.CREATE, Tokens.GRANT,
    };

    ParserDDL(Session session, Scanner scanner) {
        super(session, scanner);
    }

    void reset(String sql) {
        super.reset(sql);
    }

    StatementSchema compileCreate() {

        int     tableType = TableBase.MEMORY_TABLE;
        boolean isTable   = false;

        read();

        switch (token.tokenType) {

            case Tokens.GLOBAL :
                read();
                readThis(Tokens.TEMPORARY);
                readIfThis(Tokens.MEMORY);
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.TEMP :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.TEMPORARY :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.MEMORY :
                read();
                readThis(Tokens.TABLE);

                isTable = true;
                break;

            case Tokens.CACHED :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.CACHED_TABLE;
                break;

            case Tokens.TEXT :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEXT_TABLE;
                break;

            case Tokens.TABLE :
                read();

                isTable   = true;
                tableType = database.schemaManager.getDefaultTableType();
                break;

            default :
        }

        if (isTable) {
            return compileCreateTable(tableType);
        }

        switch (token.tokenType) {

            // other objects
            case Tokens.ALIAS :
                return compileCreateAlias();

            case Tokens.SEQUENCE :
                return compileCreateSequence();

            case Tokens.SCHEMA :
                return compileCreateSchema();

            case Tokens.TRIGGER :
                return compileCreateTrigger();

            case Tokens.USER :
                return compileCreateUser();

            case Tokens.ROLE :
                return compileCreateRole();

            case Tokens.VIEW :
                return compileCreateView(false);

            case Tokens.DOMAIN :
                return compileCreateDomain();

            case Tokens.TYPE :
                return compileCreateType();

            case Tokens.CHARACTER :
                return compileCreateCharacterSet();

            // index
            case Tokens.UNIQUE :
                read();
                checkIsThis(Tokens.INDEX);

                return compileCreateIndex(true);

            case Tokens.INDEX :
                return compileCreateIndex(false);

            case Tokens.AGGREGATE :
            case Tokens.FUNCTION :
            case Tokens.PROCEDURE :
                return compileCreateProcedureOrFunction();

            default : {
                throw unexpectedToken();
            }
        }
    }

    void processAlter() {

        session.setScripting(true);
        readThis(Tokens.ALTER);

        switch (token.tokenType) {

            case Tokens.TABLE : {
                read();
                processAlterTable();

                break;
            }
            case Tokens.DOMAIN : {
                read();
                processAlterDomain();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlter() {

        read();

        switch (token.tokenType) {

            case Tokens.INDEX : {
                read();

                HsqlName name = readNewSchemaObjectName(SchemaObject.INDEX,
                    true);

                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(name, SchemaObject.INDEX);
            }
            case Tokens.SCHEMA : {
                read();

                HsqlName name = readSchemaName();

                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(name, SchemaObject.SCHEMA);
            }
            case Tokens.CATALOG : {
                read();
                checkIsSimpleName();

                String name = token.tokenString;

                checkValidCatalogName(name);
                read();
                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(database.getCatalogName(),
                                           SchemaObject.CATALOG);
            }
            case Tokens.SEQUENCE : {
                return compileAlterSequence();
            }
            case Tokens.TABLE : {
                return compileAlterTable();
            }
            case Tokens.USER : {
                return compileAlterUser();
            }
            case Tokens.DOMAIN : {
                return compileAlterDomain();
            }
            case Tokens.VIEW : {
                return compileCreateView(true);
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

/*
    CompiledStatementInterface compileAlter() {

        CompiledStatementInterface cs = null;
        read();
        String sql = getStatement(getParsePosition(), endStatementTokensAlter);

        cs = new CompiledStatementSchema(sql, StatementCodes.ALTER_TYPE, null);

        return cs;
    }
*/
    Statement compileDrop() {

        int      objectTokenType;
        int      objectType;
        int      statementType;
        boolean  canCascade  = false;
        boolean  cascade     = false;
        boolean  useIfExists = false;
        boolean  ifExists    = false;
        HsqlName writeName   = null;
        HsqlName catalogName = database.getCatalogName();

        read();

        objectTokenType = this.token.tokenType;

        switch (objectTokenType) {

            case Tokens.INDEX : {
                read();

                statementType = StatementTypes.DROP_INDEX;
                objectType    = SchemaObject.INDEX;
                useIfExists   = true;
                writeName     = catalogName;

                break;
            }
            case Tokens.ASSERTION : {
                read();

                statementType = StatementTypes.DROP_ASSERTION;
                objectType    = SchemaObject.ASSERTION;
                canCascade    = true;

                break;
            }
            case Tokens.SPECIFIC : {
                read();

                switch (token.tokenType) {

                    case Tokens.ROUTINE :
                    case Tokens.PROCEDURE :
                    case Tokens.FUNCTION :
                        read();
                        break;

                    default :
                        throw unexpectedToken();
                }

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.SPECIFIC_ROUTINE;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.PROCEDURE : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.PROCEDURE;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.FUNCTION : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.FUNCTION;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.SCHEMA : {
                read();

                statementType = StatementTypes.DROP_SCHEMA;
                objectType    = SchemaObject.SCHEMA;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.SEQUENCE : {
                read();

                statementType = StatementTypes.DROP_SEQUENCE;
                objectType    = SchemaObject.SEQUENCE;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.TRIGGER : {
                read();

                statementType = StatementTypes.DROP_TRIGGER;
                objectType    = SchemaObject.TRIGGER;
                writeName     = catalogName;
                canCascade    = false;
                useIfExists   = true;

                break;
            }
            case Tokens.USER : {
                read();

                statementType = StatementTypes.DROP_USER;
                objectType    = SchemaObject.GRANTEE;
                writeName     = catalogName;
                canCascade    = true;

                break;
            }
            case Tokens.ROLE : {
                read();

                statementType = StatementTypes.DROP_ROLE;
                objectType    = SchemaObject.GRANTEE;
                writeName     = catalogName;
                canCascade    = true;

                break;
            }
            case Tokens.DOMAIN :
                read();

                statementType = StatementTypes.DROP_DOMAIN;
                objectType    = SchemaObject.DOMAIN;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.TYPE :
                read();

                statementType = StatementTypes.DROP_TYPE;
                objectType    = SchemaObject.TYPE;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.CHARACTER :
                read();
                readThis(Tokens.SET);

                statementType = StatementTypes.DROP_CHARACTER_SET;
                objectType    = SchemaObject.CHARSET;
                writeName     = catalogName;
                canCascade    = false;
                useIfExists   = true;
                break;

            case Tokens.VIEW :
                read();

                statementType = StatementTypes.DROP_VIEW;
                objectType    = SchemaObject.VIEW;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.TABLE :
                read();

                statementType = StatementTypes.DROP_TABLE;
                objectType    = SchemaObject.TABLE;
                writeName     = catalogName;
                canCascade    = true;
                useIfExists   = true;
                break;

            default :
                throw unexpectedToken();
        }

        if (useIfExists && token.tokenType == Tokens.IF) {
            int position = getPosition();

            read();

            if (token.tokenType == Tokens.EXISTS) {
                read();

                ifExists = true;
            } else {
                rewind(position);
            }
        }

        checkIsIdentifier();

        HsqlName name = null;

        switch (objectTokenType) {

            case Tokens.USER : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                Grantee grantee =
                    database.getUserManager().get(token.tokenString);

                name = grantee.getName();

                read();

                break;
            }
            case Tokens.ROLE : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                Grantee role =
                    database.getGranteeManager().getRole(token.tokenString);

                name = role.getName();

                read();

                break;
            }
            case Tokens.SCHEMA : {
                name      = readNewSchemaName();
                writeName = catalogName;

                break;
            }
            case Tokens.TABLE : {
                boolean isModule = token.namePrePrefix == null
                                   && Tokens.T_MODULE.equals(token.namePrefix);

                name = readNewSchemaObjectName(objectType, false);

                if (isModule) {
                    Object[] args = new Object[] {
                        name, Boolean.valueOf(ifExists)
                    };

                    return new StatementSession(StatementTypes.DROP_TABLE,
                                                args);
                }

                break;
            }
            default :
                name = readNewSchemaObjectName(objectType, false);
        }

        if (!ifExists && useIfExists && token.tokenType == Tokens.IF) {
            read();
            readThis(Tokens.EXISTS);

            ifExists = true;
        }

        if (canCascade) {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else if (token.tokenType == Tokens.RESTRICT) {
                read();
            }
        }

        Object[] args = new Object[] {
            name, new Integer(objectType), Boolean.valueOf(cascade),
            Boolean.valueOf(ifExists)
        };
        String sql = getLastPart();
        Statement cs = new StatementSchema(sql, statementType, args, null,
                                           writeName);

        return cs;
    }

    private void processAlterTable() {

        String   tableName = token.tokenString;
        HsqlName schema    = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        if (t.isView()) {
            throw Error.error(ErrorCode.X_42501, tableName);
        }

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);
                processAlterTableRename(t);

                return;
            }
            case Tokens.ADD : {
                read();

                HsqlName cname = null;

                if (token.tokenType == Tokens.CONSTRAINT) {
                    read();

                    cname = readNewDependentSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);

                    database.schemaManager.checkSchemaObjectNotExists(cname);
                }

                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);
                        processAlterTableAddForeignKeyConstraint(t, cname);

                        return;

                    case Tokens.UNIQUE :
                        read();
                        processAlterTableAddUniqueConstraint(t, cname);

                        return;

                    case Tokens.CHECK :
                        read();
                        processAlterTableAddCheckConstraint(t, cname);

                        return;

                    case Tokens.PRIMARY :
                        read();
                        readThis(Tokens.KEY);
                        processAlterTableAddPrimaryKey(t, cname);

                        return;

                    case Tokens.COLUMN :
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
            case Tokens.DROP : {
                read();

                switch (token.tokenType) {

                    case Tokens.PRIMARY : {
                        boolean cascade = false;

                        read();
                        readThis(Tokens.KEY);

                        if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        if (t.hasPrimaryKey()) {
                            processAlterTableDropConstraint(
                                t, t.getPrimaryConstraint().getName().name,
                                cascade);
                        } else {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        return;
                    }
                    case Tokens.CONSTRAINT : {
                        boolean cascade = false;

                        read();

                        SchemaObject object = readSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);

                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        } else if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        processAlterTableDropConstraint(
                            t, object.getName().name, cascade);

//                        read();
                        return;
                    }
                    case Tokens.COLUMN :
                        read();

                    // fall through
                    default : {
                        checkIsSimpleName();

                        String  name    = token.tokenString;
                        boolean cascade = false;

                        read();

                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        } else if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        processAlterTableDropColumn(t, name, cascade);

                        return;
                    }
                }
            }
            case Tokens.ALTER : {
                read();

                if (token.tokenType == Tokens.COLUMN) {
                    read();
                }

                int          columnIndex = t.getColumnIndex(token.tokenString);
                ColumnSchema column      = t.getColumn(columnIndex);

                read();
                processAlterColumn(t, column, columnIndex);

                return;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlterTable() {

        read();

        String   tableName = token.tokenString;
        HsqlName schema    = session.getSchemaHsqlName(token.namePrefix);
        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileRenameObject(t.getName(), SchemaObject.TABLE);
            }
            case Tokens.ADD : {
                read();

                HsqlName cname = null;

                if (token.tokenType == Tokens.CONSTRAINT) {
                    read();

                    cname = readNewDependentSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);
                }

                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableAddForeignKeyConstraint(t,
                                cname);

                    case Tokens.UNIQUE :
                        read();

                        return compileAlterTableAddUniqueConstraint(t, cname);

                    case Tokens.CHECK :
                        read();

                        return compileAlterTableAddCheckConstraint(t, cname);

                    case Tokens.PRIMARY :
                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableAddPrimaryKey(t, cname);

                    case Tokens.COLUMN :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        read();
                        checkIsSimpleName();

                        return compileAlterTableAddColumn(t);

                    default :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        checkIsSimpleName();

                        return compileAlterTableAddColumn(t);
                }
            }
            case Tokens.DROP : {
                read();

                switch (token.tokenType) {

                    case Tokens.PRIMARY : {
                        boolean cascade = false;

                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableDropPrimaryKey(t);
                    }
                    case Tokens.CONSTRAINT : {
                        read();

                        return compileAlterTableDropConstraint(t);
                    }
                    case Tokens.COLUMN :
                        read();

                    // fall through
                    default : {
                        checkIsSimpleName();

                        String  name    = token.tokenString;
                        boolean cascade = false;

                        read();

                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        } else if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        return compileAlterTableDropColumn(t, name, cascade);
                    }
                }
            }
            case Tokens.ALTER : {
                read();

                if (token.tokenType == Tokens.COLUMN) {
                    read();
                }

                int          columnIndex = t.getColumnIndex(token.tokenString);
                ColumnSchema column      = t.getColumn(columnIndex);

                read();

                return compileAlterColumn(t, column, columnIndex);
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    private Statement compileAlterTableDropConstraint(Table t) {

        boolean cascade = false;
        SchemaObject object = readSchemaObjectName(t.getSchemaName(),
            SchemaObject.CONSTRAINT);

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        Object[] args = new Object[] {
            object.getName(), ValuePool.getInt(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };
        String sql = getLastPart();
        Statement cs = new StatementSchema(sql,
                                           StatementTypes.DROP_CONSTRAINT,
                                           args);

        cs.writeTableNames = getReferenceArray(t.getName(), cascade);

        return cs;
    }

    private Statement compileAlterTableDropPrimaryKey(Table t) {

        boolean cascade = false;

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        if (!t.hasPrimaryKey()) {
            throw Error.error(ErrorCode.X_42501);
        }

        SchemaObject object = t.getPrimaryConstraint();
        Object[]     args   = new Object[] {
            object.getName(), ValuePool.getInt(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };
        String sql = getLastPart();
        Statement cs = new StatementSchema(sql,
                                           StatementTypes.DROP_CONSTRAINT,
                                           args);

        cs.writeTableNames = getReferenceArray(t.getName(), cascade);

        return cs;
    }

    HsqlName[] getReferenceArray(HsqlName objectName, boolean cascade) {

        if (cascade) {
            OrderedHashSet names = new OrderedHashSet();

            database.schemaManager.getCascadingReferencingObjectNames(
                objectName, names);

            Iterator it = names.iterator();

            while (it.hasNext()) {
                HsqlName name = (HsqlName) it.next();

                if (name.type != SchemaObject.TABLE) {
                    it.remove();
                }
            }

            names.add(objectName);

            HsqlName[] array = new HsqlName[names.size()];

            names.toArray(array);

            return array;
        } else {
            return new HsqlName[]{ objectName };
        }
    }

    StatementSession compileDeclareLocalTableOrNull() {

        int position = super.getPosition();

        try {
            readThis(Tokens.DECLARE);
            readThis(Tokens.LOCAL);
            readThis(Tokens.TEMPORARY);
            readThis(Tokens.TABLE);
        } catch (Exception e) {

            // may be cursor
            rewind(position);

            return null;
        }

        if (token.namePrePrefix != null) {
            throw unexpectedToken();
        }

        if (token.namePrePrefix == null
                && (token.namePrefix == null
                    || Tokens.T_MODULE.equals(token.namePrefix))) {

            // valid name
        } else {
            throw unexpectedToken();
        }

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE, false);

        name.schema = SqlInvariants.MODULE_HSQLNAME;

        Table table = TableUtil.newTable(database, TableBase.TEMP_TABLE, name);
        StatementSchema cs          = compileCreateTableBody(table);
        HsqlArrayList   constraints = (HsqlArrayList) cs.arguments[1];

        for (int i = 0; i < constraints.size(); i++) {
            Constraint c = (Constraint) constraints.get(i);

            if (c.getConstraintType()
                    == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                throw unexpectedToken(Tokens.T_FOREIGN);
            }
        }

        StatementSession ss =
            new StatementSession(StatementTypes.DECLARE_SESSION_TABLE,
                                 cs.arguments);

        return ss;
    }

    StatementSchema compileCreateTable(int type) {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE, false);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Table table = TableUtil.newTable(database, type, name);

        return compileCreateTableBody(table);
    }

    StatementSchema compileCreateTableBody(Table table) {

        HsqlArrayList tempConstraints = new HsqlArrayList();

        if (token.tokenType == Tokens.AS) {
            return readTableAsSubqueryDefinition(table);
        }

        int position = getPosition();

        readThis(Tokens.OPENBRACKET);

        {
            Constraint c = new Constraint(null, null,
                                          SchemaObject.ConstraintTypes.TEMP);

            tempConstraints.add(c);
        }

        boolean start     = true;
        boolean startPart = true;
        boolean end       = false;

        while (!end) {
            switch (token.tokenType) {

                case Tokens.LIKE : {
                    ColumnSchema[] likeColumns = readLikeTable(table);

                    for (int i = 0; i < likeColumns.length; i++) {
                        table.addColumn(likeColumns[i]);
                    }

                    start     = false;
                    startPart = false;

                    break;
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
                    break;

                case Tokens.COMMA :
                    if (startPart) {
                        throw unexpectedToken();
                    }

                    read();

                    startPart = true;
                    break;

                case Tokens.CLOSEBRACKET :
                    read();

                    end = true;
                    break;

                default :
                    if (!startPart) {
                        throw unexpectedToken();
                    }

                    checkIsSchemaObjectName();

                    HsqlName hsqlName =
                        database.nameManager.newColumnHsqlName(table.getName(),
                            token.tokenString, isDelimitedIdentifier());

                    read();

                    ColumnSchema newcolumn = readColumnDefinitionOrNull(table,
                        hsqlName, tempConstraints);

                    if (newcolumn == null) {
                        if (start) {
                            rewind(position);

                            return readTableAsSubqueryDefinition(table);
                        } else {
                            throw Error.error(ErrorCode.X_42000);
                        }
                    }

                    table.addColumn(newcolumn);

                    start     = false;
                    startPart = false;
            }
        }

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

        Object[] args = new Object[] {
            table, tempConstraints, null
        };
        String   sql  = getLastPart();

        return new StatementSchema(sql, StatementTypes.CREATE_TABLE, args);
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
            HsqlName name =
                database.nameManager.newColumnSchemaHsqlName(table.getName(),
                    column.getName());

            column.setName(name);
            column.setNullable(true);
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

    StatementSchema readTableAsSubqueryDefinition(Table table) {

        HsqlName   readName    = null;
        boolean    withData    = true;
        HsqlName[] columnNames = null;
        Statement  statement   = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            columnNames = readColumnNames(table.getName());
        }

        readThis(Tokens.AS);
        readThis(Tokens.OPENBRACKET);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setReturningResult();
        queryExpression.resolve(session);
        readThis(Tokens.CLOSEBRACKET);
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

        TableUtil.setColumnsInSchemaTable(table, columnNames,
                                          queryExpression.getColumnTypes());
        table.createPrimaryKey();

        if (withData) {
            statement = new StatementQuery(session, queryExpression,
                                           compileContext);
            readName = statement.getTableNamesForRead()[0];
        }

        Object[] args = new Object[] {
            table, null, statement
        };
        String   sql  = getLastPart();
        StatementSchema st = new StatementSchema(sql,
            StatementTypes.CREATE_TABLE, args, readName, null);

        return st;
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    static Table addTableConstraintDefinitions(Session session, Table table,
            HsqlArrayList tempConstraints, HsqlArrayList constraintList,
            boolean addToSchema) {

        Constraint c        = (Constraint) tempConstraints.get(0);
        String     namePart = c.getName() == null ? null
                                                  : c.getName().name;
        HsqlName indexName = session.database.nameManager.newAutoName("IDX",
            namePart, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);

        c.setColumnsIndexes(table);
        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(c.getName(), table,
                table.getPrimaryIndex(),
                SchemaObject.ConstraintTypes.PRIMARY_KEY);

            table.addConstraint(newconstraint);

            if (addToSchema) {
                session.database.schemaManager.addSchemaObject(newconstraint);
            }
        }

        for (int i = 1; i < tempConstraints.size(); i++) {
            c = (Constraint) tempConstraints.get(i);

            switch (c.constType) {

                case SchemaObject.ConstraintTypes.UNIQUE : {
                    c.setColumnsIndexes(table);

                    if (table.getUniqueConstraintForColumns(c.core.mainCols)
                            != null) {
                        throw Error.error(ErrorCode.X_42522);
                    }

                    // create an autonamed index
                    indexName = session.database.nameManager.newAutoName("IDX",
                            c.getName().name, table.getSchemaName(),
                            table.getName(), SchemaObject.INDEX);

                    Index index = table.createAndAddIndexStructure(session,
                        indexName, c.core.mainCols, null, null, true, true,
                        false);
                    Constraint newconstraint = new Constraint(c.getName(),
                        table, index, SchemaObject.ConstraintTypes.UNIQUE);

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
                        c.prepareCheckConstraint(session, table, false);
                    } catch (HsqlException e) {
                        if (session.isProcessingScript()) {
                            break;
                        }

                        throw e;
                    }

                    table.addConstraint(c);

                    if (c.isNotNull()) {
                        ColumnSchema column =
                            table.getColumn(c.notNullColumnIndex);

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

    static void addForeignKey(Session session, Table table, Constraint c,
                              HsqlArrayList constraintList) {

        HsqlName mainTableName = c.getMainTableName();

        if (mainTableName == table.getName()) {
            c.core.mainTable = table;
        } else {
            Table mainTable =
                session.database.schemaManager.findUserTable(session,
                    mainTableName.name, mainTableName.schema.name);

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

        Constraint uniqueConstraint =
            c.core.mainTable.getUniqueConstraintForColumns(c.core.mainCols,
                c.core.refCols);

        if (uniqueConstraint == null) {
            throw Error.error(ErrorCode.X_42523);
        }

        Index      mainIndex  = uniqueConstraint.getMainIndex();
        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.checkCreateForeignKey(c);

        boolean isForward = c.core.mainTable.getSchemaName()
                            != table.getSchemaName();
        int offset = session.database.schemaManager.getTableIndex(table);

        if (offset != -1
                && offset
                   < session.database.schemaManager.getTableIndex(
                       c.core.mainTable)) {
            isForward = true;
        }

        HsqlName refIndexName = session.database.nameManager.newAutoName("IDX",
            table.getSchemaName(), table.getName(), SchemaObject.INDEX);
        Index index = table.createAndAddIndexStructure(session, refIndexName,
            c.core.refCols, null, null, false, true, isForward);
        HsqlName mainName = session.database.nameManager.newAutoName("REF",
            c.getName().name, table.getSchemaName(), table.getName(),
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

    private Constraint readFKReferences(Table refTable,
                                        HsqlName constraintName,
                                        OrderedHashSet refColSet) {

        HsqlName       mainTableName;
        OrderedHashSet mainColSet = null;

        readThis(Tokens.REFERENCES);

        HsqlName schema;

        if (token.namePrefix == null) {
            schema = refTable.getSchemaName();
        } else {
            schema =
                database.schemaManager.getSchemaHsqlName(token.namePrefix);
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
        } else {

            // columns are resolved in the calling method
            if (mainTableName == refTable.getName()) {

                // fredt - FK statement is part of CREATE TABLE and is self-referencing
                // reference must be to same table being created
            } else {
/*
                if (!mainTable.hasPrimaryKey()) {
                    throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                                      Trace.TABLE_HAS_NO_PRIMARY_KEY);

                }
*/
            }
        }

        int matchType = OpTypes.MATCH_SIMPLE;

        if (token.tokenType == Tokens.MATCH) {
            read();

            switch (token.tokenType) {

                case Tokens.SIMPLE :
                    read();
                    break;

                case Tokens.PARTIAL :
                    throw super.unsupportedFeature();
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

        if (constraintName == null) {
            constraintName = database.nameManager.newAutoName("FK",
                    refTable.getSchemaName(), refTable.getName(),
                    SchemaObject.CONSTRAINT);
        }

        return new Constraint(constraintName, refTable.getName(), refColSet,
                              mainTableName, mainColSet,
                              SchemaObject.ConstraintTypes.FOREIGN_KEY,
                              deleteAction, updateAction, matchType);
    }

    private HsqlName readFKTableName(HsqlName schema) {

        HsqlName name;

        checkIsSchemaObjectName();

        Table table = database.schemaManager.findUserTable(session,
            token.tokenString, schema.name);

        if (table == null) {
            name = database.nameManager.newHsqlName(schema, token.tokenString,
                    isDelimitedIdentifier(), SchemaObject.TABLE);
        } else {
            name = table.getName();
        }

        read();

        return name;
    }

    StatementSchema compileCreateView(boolean alter) {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.VIEW, true);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorisation(name.schema);

        HsqlName[] colList = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            colList = readColumnNames(name);
        }

        readThis(Tokens.AS);
        startRecording();

        int             position = getPosition();
        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTable();
        }

        Token[] tokenisedStatement = getRecordedStatement();
        int     check              = SchemaObject.ViewCheckModes.CHECK_NONE;

        if (token.tokenType == Tokens.WITH) {
            read();

            check = SchemaObject.ViewCheckModes.CHECK_CASCADE;

            if (readIfThis(Tokens.LOCAL)) {
                check = SchemaObject.ViewCheckModes.CHECK_LOCAL;
            } else {
                readIfThis(Tokens.CASCADED);
            }

            readThis(Tokens.CHECK);
            readThis(Tokens.OPTION);
        }

        View view = new View(database, name, colList, check);

        queryExpression.setView(view);
        queryExpression.resolve(session);
        view.setStatement(Token.getSQL(tokenisedStatement));

        String          fullSQL = getLastPart();
        Object[]        args    = new Object[]{ view };
        int             type    = alter ? StatementTypes.ALTER_VIEW
                                        : StatementTypes.CREATE_VIEW;
        StatementSchema cs      = new StatementSchema(fullSQL, type, args);
        StatementQuery s = new StatementQuery(session, queryExpression,
                                              compileContext);

        cs.readTableNames = s.readTableNames;

        return cs;
    }

    StatementSchema compileCreateSequence() {

        read();

        /*
                CREATE SEQUENCE <name>
                [AS {INTEGER | BIGINT}]
                [START WITH <value>]
                [INCREMENT BY <value>]
        */
        HsqlName name = readNewSchemaObjectName(SchemaObject.SEQUENCE, false);
        NumberSequence sequence = new NumberSequence(name, Type.SQL_INTEGER);

        readSequenceOptions(sequence, true, false);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ sequence };

        return new StatementSchema(sql, StatementTypes.CREATE_SEQUENCE, args);
    }

    StatementSchema compileCreateDomain() {

        UserTypeModifier userTypeModifier = null;
        HsqlName         name;

        read();

        name = readNewSchemaObjectName(SchemaObject.DOMAIN, false);

        readIfThis(Tokens.AS);

        Type       type          = readTypeDefinition(false).duplicate();
        Expression defaultClause = null;

        if (readIfThis(Tokens.DEFAULT)) {
            defaultClause = readDefaultClause(type);
        }

        userTypeModifier = new UserTypeModifier(name, SchemaObject.DOMAIN,
                type);

        userTypeModifier.setDefaultClause(defaultClause);

        type.userTypeModifier = userTypeModifier;

        HsqlArrayList tempConstraints = new HsqlArrayList();

        compileContext.currentDomain = type;

        while (true) {
            boolean end = false;

            switch (token.tokenType) {

                case Tokens.CONSTRAINT :
                case Tokens.CHECK :
                    readConstraint(type, tempConstraints);
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
            userTypeModifier.addConstraint(c);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[]{ type };

        return new StatementSchema(sql, StatementTypes.CREATE_DOMAIN, args);
    }

    StatementSchema compileCreateType() {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.TYPE, false);

        readThis(Tokens.AS);

        Type type = readTypeDefinition(false).duplicate();

        readIfThis(Tokens.FINAL);

        UserTypeModifier userTypeModifier = new UserTypeModifier(name,
            SchemaObject.TYPE, type);

        type.userTypeModifier = userTypeModifier;

        String   sql  = getLastPart();
        Object[] args = new Object[]{ type };

        return new StatementSchema(sql, StatementTypes.CREATE_TYPE, args);
    }

    StatementSchema compileCreateCharacterSet() {

        read();
        readThis(Tokens.SET);

        HsqlName name = readNewSchemaObjectName(SchemaObject.CHARSET, false);

        readIfThis(Tokens.AS);
        readThis(Tokens.GET);

        String schema = session.getSchemaName(token.namePrefix);
        Charset source =
            (Charset) database.schemaManager.getSchemaObject(token.tokenString,
                schema, SchemaObject.CHARSET);

        read();

        if (token.tokenType == Tokens.COLLATION) {
            read();
            readThis(Tokens.FROM);
            readThis(Tokens.DEFAULT);
        }

        Charset charset = new Charset(name);

        charset.base = source.getName();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ charset };

        return new StatementSchema(sql, StatementTypes.CREATE_CHARACTER_SET,
                                   args);
    }

    StatementSchema compileCreateAlias() {

        HsqlName  name     = null;
        Routine[] routines = null;
        String    alias;
        String    methodFQN = null;

        if (!session.isProcessingScript()) {
            throw super.unsupportedFeature();
        }

        read();

        try {
            alias = token.tokenString;

            read();
            readThis(Tokens.FOR);

            methodFQN = token.tokenString;

            read();
        } catch (HsqlException e) {
            alias = null;
        }

        if (alias != null) {
            HsqlName schema =
                database.schemaManager.getDefaultSchemaHsqlName();

            name = database.nameManager.newHsqlName(schema, alias,
                    SchemaObject.FUNCTION);

            Method[] methods = Routine.getMethods(methodFQN);

            routines = Routine.newRoutines(session, methods);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            name, routines
        };

        return new StatementSchema(sql, StatementTypes.CREATE_ALIAS, args);
    }

    StatementSchema compileCreateTrigger() {

        Table          table;
        Boolean        isForEachRow = null;
        boolean        isNowait     = false;
        boolean        hasQueueSize = false;
        int            queueSize    = 0;
        int            beforeOrAfterType;
        int            operationType;
        String         className;
        TriggerDef     td;
        HsqlName       name;
        HsqlName       otherName           = null;
        OrderedHashSet columns             = null;
        int[]          updateColumnIndexes = null;

        read();

        name = readNewSchemaObjectName(SchemaObject.TRIGGER, true);

        switch (token.tokenType) {

            case Tokens.INSTEAD :
                beforeOrAfterType = TriggerDef.getTiming(Tokens.INSTEAD);

                read();
                readThis(Tokens.OF);
                break;

            case Tokens.BEFORE :
            case Tokens.AFTER :
                beforeOrAfterType = TriggerDef.getTiming(token.tokenType);

                read();
                break;

            default :
                throw unexpectedToken();
        }

        switch (token.tokenType) {

            case Tokens.INSERT :
            case Tokens.DELETE :
                operationType = TriggerDef.getOperationType(token.tokenType);

                read();
                break;

            case Tokens.UPDATE :
                operationType = TriggerDef.getOperationType(token.tokenType);

                read();

                if (token.tokenType == Tokens.OF
                        && beforeOrAfterType != TriggerDef.INSTEAD) {
                    read();

                    columns = new OrderedHashSet();

                    readColumnNameList(columns, null, false);
                }
                break;

            default :
                throw unexpectedToken();
        }

        readThis(Tokens.ON);

        table = readTableName();

        if (token.tokenType == Tokens.BEFORE) {
            read();
            checkIsSimpleName();

            otherName = readNewSchemaObjectName(SchemaObject.TRIGGER, true);
        }

        name.setSchemaIfNull(table.getSchemaName());
        checkSchemaUpdateAuthorisation(name.schema);

        if (beforeOrAfterType == TriggerDef.INSTEAD) {
            if (!table.isView()
                    || ((View) table).getCheckOption()
                       == SchemaObject.ViewCheckModes.CHECK_CASCADE) {
                throw Error.error(ErrorCode.X_42538, name.schema.name);
            }
        } else {
            if (table.isView()) {
                throw Error.error(ErrorCode.X_42538, name.schema.name);
            }
        }

        if (name.schema != table.getSchemaName()) {
            throw Error.error(ErrorCode.X_42505, name.schema.name);
        }

        name.parent = table.getName();

        database.schemaManager.checkSchemaObjectNotExists(name);

        if (columns != null) {
            updateColumnIndexes = table.getColumnIndexes(columns);

            for (int i = 0; i < updateColumnIndexes.length; i++) {
                if (updateColumnIndexes[i] == -1) {
                    throw Error.error(ErrorCode.X_42544,
                                      (String) columns.get(i));
                }
            }
        }

        Expression      condition    = null;
        String          oldTableName = null;
        String          newTableName = null;
        String          oldRowName   = null;
        String          newRowName   = null;
        Table[]         transitions  = new Table[4];
        RangeVariable[] rangeVars    = new RangeVariable[4];
        String          conditionSQL = null;

        if (token.tokenType == Tokens.REFERENCING) {
            read();

            if (token.tokenType != Tokens.OLD
                    && token.tokenType != Tokens.NEW) {
                throw unexpectedToken();
            }

            while (true) {
                if (token.tokenType == Tokens.OLD) {
                    if (operationType == StatementTypes.INSERT) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (Boolean.TRUE.equals(isForEachRow)
                                || oldTableName != null
                                || beforeOrAfterType == TriggerDef.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();
                        read();

                        oldTableName = token.tokenString;

                        String n = oldTableName;

                        if (n.equals(newTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = Boolean.FALSE;

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.OLD_TABLE] = transition;
                        rangeVars[TriggerDef.OLD_TABLE]   = range;
                    } else {
                        if (Boolean.FALSE.equals(isForEachRow)
                                || oldRowName != null) {
                            throw unexpectedToken();
                        }

                        readIfThis(Tokens.ROW);
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        oldRowName = token.tokenString;

                        read();

                        String n = oldRowName;

                        if (n.equals(newTableName) || n.equals(oldTableName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = Boolean.TRUE;

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.OLD_ROW] = transition;
                        rangeVars[TriggerDef.OLD_ROW]   = range;
                    }
                } else if (token.tokenType == Tokens.NEW) {
                    if (operationType == StatementTypes.DELETE_WHERE) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (Boolean.TRUE.equals(isForEachRow)
                                || newTableName != null
                                || beforeOrAfterType == TriggerDef.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newTableName = token.tokenString;

                        read();

                        isForEachRow = Boolean.FALSE;

                        String n = newTableName;

                        if (n.equals(oldTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.NEW_TABLE] = transition;
                        rangeVars[TriggerDef.NEW_TABLE]   = range;
                    } else {
                        if (Boolean.FALSE.equals(isForEachRow)
                                || newRowName != null) {
                            throw unexpectedToken();
                        }

                        readIfThis(Tokens.ROW);
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newRowName = token.tokenString;

                        read();

                        isForEachRow = Boolean.TRUE;

                        String n = newRowName;

                        if (n.equals(oldTableName) || n.equals(newTableName)
                                || n.equals(oldRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.NEW_ROW] = transition;
                        rangeVars[TriggerDef.NEW_ROW]   = range;
                    }
                } else {
                    break;
                }
            }
        }

        if (Boolean.TRUE.equals(isForEachRow)
                && token.tokenType != Tokens.FOR) {
            throw unexpectedTokenRequire(Tokens.T_FOR);
        }

        if (token.tokenType == Tokens.FOR) {
            read();
            readThis(Tokens.EACH);

            if (token.tokenType == Tokens.ROW) {
                if (Boolean.FALSE.equals(isForEachRow)) {
                    throw unexpectedToken();
                }

                isForEachRow = Boolean.TRUE;
            } else if (token.tokenType == Tokens.STATEMENT) {
                if (Boolean.TRUE.equals(isForEachRow)
                        || beforeOrAfterType == TriggerDef.BEFORE) {
                    throw unexpectedToken();
                }

                isForEachRow = Boolean.FALSE;
            } else {
                throw unexpectedToken();
            }

            read();
        }

        //
        if (rangeVars[TriggerDef.OLD_TABLE] != null) {}

        if (rangeVars[TriggerDef.NEW_TABLE] != null) {}

        //
        if (Tokens.T_QUEUE.equals(token.tokenString)) {
            read();

            queueSize    = readInteger();
            hasQueueSize = true;
        }

        if (Tokens.T_NOWAIT.equals(token.tokenString)) {
            read();

            isNowait = true;
        }

        if (token.tokenType == Tokens.WHEN
                && beforeOrAfterType != TriggerDef.INSTEAD) {
            read();
            readThis(Tokens.OPENBRACKET);

            int position = getPosition();

            isCheckOrTriggerCondition = true;
            condition                 = XreadBooleanValueExpression();
            conditionSQL              = getLastPart(position);
            isCheckOrTriggerCondition = false;

            readThis(Tokens.CLOSEBRACKET);

            HsqlList unresolved = condition.resolveColumnReferences(rangeVars,
                null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (isForEachRow == null) {
            isForEachRow = Boolean.FALSE;
        }

        if (token.tokenType == Tokens.CALL) {
            int position = getPosition();

            try {
                read();
                checkIsSimpleName();
                checkIsDelimitedIdentifier();

                className = token.tokenString;

                read();

                td = new TriggerDef(name, beforeOrAfterType, operationType,
                                    isForEachRow.booleanValue(), table,
                                    transitions, rangeVars, condition,
                                    conditionSQL, updateColumnIndexes,
                                    className, isNowait, queueSize);

                String   sql  = getLastPart();
                Object[] args = new Object[] {
                    td, otherName
                };

                return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER,
                                           args, null, table.getName());
            } catch (HsqlException e) {
                rewind(position);
            }
        }

        //
        if (hasQueueSize) {
            throw unexpectedToken(Tokens.T_QUEUE);
        }

        if (isNowait) {
            throw unexpectedToken(Tokens.T_NOWAIT);
        }

        Routine routine = compileTriggerRoutine(table, rangeVars,
            beforeOrAfterType, operationType);

        td = new TriggerDefSQL(name, beforeOrAfterType, operationType,
                               isForEachRow.booleanValue(), table,
                               transitions, rangeVars, condition,
                               conditionSQL, updateColumnIndexes, routine);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            td, otherName
        };

        return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER, args,
                                   null, table.getName());
    }

    Routine compileTriggerRoutine(Table table, RangeVariable[] ranges,
                                  int beforeOrAfter, int operation) {

        int impact = (beforeOrAfter == TriggerDef.BEFORE) ? Routine.READS_SQL
                                                          : Routine
                                                              .MODIFIES_SQL;
        Routine routine = new Routine(table, ranges, impact, beforeOrAfter,
                                      operation);

        startRecording();

        Statement statement = compileSQLProcedureStatementOrNull(routine,
            null);

        if (statement == null) {
            throw unexpectedToken();
        }

        Token[] tokenisedStatement = getRecordedStatement();
        String  sql                = Token.getSQL(tokenisedStatement);

        statement.setSQL(sql);
        routine.setProcedure(statement);
        routine.resolve(session);

        return routine;
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
    ColumnSchema readColumnDefinitionOrNull(Table table, HsqlName hsqlName,
            HsqlArrayList constraintList) {

        boolean        isGenerated     = false;
        boolean        isIdentity      = false;
        boolean        isPKIdentity    = false;
        boolean        generatedAlways = false;
        Expression     generateExpr    = null;
        boolean        isNullable      = true;
        Expression     defaultExpr     = null;
        Type           typeObject      = null;
        NumberSequence sequence        = null;

        if (token.tokenType == Tokens.GENERATED) {
            read();
            readThis(Tokens.ALWAYS);

            isGenerated     = true;
            generatedAlways = true;

            // not yet
            throw unexpectedToken(Tokens.T_GENERATED);
        } else if (token.tokenType == Tokens.IDENTITY) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            typeObject   = Type.SQL_INTEGER;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        } else if (token.tokenType == Tokens.COMMA) {
            return null;
        } else if (token.tokenType == Tokens.CLOSEBRACKET) {
            return null;
        } else {
            typeObject = readTypeDefinition(true);
        }

        if (isGenerated || isIdentity) {}
        else if (token.tokenType == Tokens.DEFAULT) {
            read();

            defaultExpr = readDefaultClause(typeObject);
        } else if (token.tokenType == Tokens.GENERATED && !isIdentity) {
            read();

            if (token.tokenType == Tokens.BY) {
                read();
                readThis(Tokens.DEFAULT);
            } else {
                readThis(Tokens.ALWAYS);

                generatedAlways = true;
            }

            readThis(Tokens.AS);

            if (token.tokenType == Tokens.IDENTITY) {
                read();

                sequence = new NumberSequence(null, typeObject);

                sequence.setAlways(generatedAlways);

                if (token.tokenType == Tokens.OPENBRACKET) {
                    read();
                    readSequenceOptions(sequence, false, false);
                    readThis(Tokens.CLOSEBRACKET);
                }

                isIdentity = true;
            } else if (token.tokenType == Tokens.OPENBRACKET) {
                if (!generatedAlways) {
                    throw super.unexpectedTokenRequire(Tokens.T_ALWAYS);
                }

                isGenerated = true;
            }
        } else if (token.tokenType == Tokens.IDENTITY && !isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        }

        if (isGenerated) {
            readThis(Tokens.OPENBRACKET);

            generateExpr = XreadValueExpression();

            readThis(Tokens.CLOSEBRACKET);
        }

        ColumnSchema column = new ColumnSchema(hsqlName, typeObject,
                                               isNullable, false, defaultExpr);

        column.setGeneratingExpression(generateExpr);
        readColumnConstraints(table, column, constraintList);

        if (token.tokenType == Tokens.IDENTITY && !isIdentity) {
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
            Constraint c =
                new Constraint(constName, set,
                               SchemaObject.ConstraintTypes.PRIMARY_KEY);

            constraintList.set(0, c);
            column.setPrimaryKey(true);
        }

        return column;
    }

    private void readSequenceOptions(NumberSequence sequence,
                                     boolean withType, boolean isAlter) {

        OrderedIntHashSet set = new OrderedIntHashSet();

        while (true) {
            boolean end = false;

            if (set.contains(token.tokenType)) {
                throw unexpectedToken();
            }

            switch (token.tokenType) {

                case Tokens.AS : {
                    if (withType) {
                        read();

                        Type type = readTypeDefinition(true);

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
                case Tokens.NO :
                    read();

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
                case Tokens.CYCLE :
                    set.add(token.tokenType);
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
     * @param schemaObject table or domain
     * @param constraintList list of constraints
     */
    private void readConstraint(SchemaObject schemaObject,
                                HsqlArrayList constraintList) {

        HsqlName constName = null;

        if (token.tokenType == Tokens.CONSTRAINT) {
            read();

            constName =
                readNewDependentSchemaObjectName(schemaObject.getName(),
                                                 SchemaObject.CONSTRAINT);
        }

        switch (token.tokenType) {

            case Tokens.PRIMARY : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                Constraint mainConst;

                mainConst = (Constraint) constraintList.get(0);

                if (mainConst.constType
                        == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                    throw Error.error(ErrorCode.X_42532);
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("PK",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                OrderedHashSet set = readColumnNames(false);
                Constraint c =
                    new Constraint(constName, set,
                                   SchemaObject.ConstraintTypes.PRIMARY_KEY);

                constraintList.set(0, c);

                break;
            }
            case Tokens.UNIQUE : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();

                OrderedHashSet set = readColumnNames(false);

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c =
                    new Constraint(constName, set,
                                   SchemaObject.ConstraintTypes.UNIQUE);

                constraintList.add(c);

                break;
            }
            case Tokens.FOREIGN : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                OrderedHashSet set = readColumnNames(false);
                Constraint c = readFKReferences((Table) schemaObject,
                                                constName, set);

                constraintList.add(c);

                break;
            }
            case Tokens.CHECK : {
                read();

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c =
                    new Constraint(constName, null,
                                   SchemaObject.ConstraintTypes.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
            default : {
                if (constName != null) {
                    throw super.unexpectedToken();
                }
            }
        }
    }

    /**
     * Reads column constraints
     */
    void readColumnConstraints(Table table, ColumnSchema column,
                               HsqlArrayList constraintList) {

        boolean end                  = false;
        boolean hasNotNullConstraint = false;
        boolean hasNullNoiseWord     = false;
        boolean hasPrimaryKey        = false;

        while (true) {
            HsqlName constName = null;

            if (token.tokenType == Tokens.CONSTRAINT) {
                read();

                constName = readNewDependentSchemaObjectName(table.getName(),
                        SchemaObject.CONSTRAINT);
            }

            switch (token.tokenType) {

                case Tokens.PRIMARY : {
                    if (hasNullNoiseWord || hasPrimaryKey) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.KEY);

                    Constraint existingConst =
                        (Constraint) constraintList.get(0);

                    if (existingConst.constType
                            == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                        throw Error.error(ErrorCode.X_42532);
                    }

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("PK",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName, set,
                        SchemaObject.ConstraintTypes.PRIMARY_KEY);

                    constraintList.set(0, c);
                    column.setPrimaryKey(true);

                    hasPrimaryKey = true;

                    break;
                }
                case Tokens.UNIQUE : {
                    read();

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, set,
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
                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    Constraint c = readFKReferences(table, constName, set);

                    constraintList.add(c);

                    break;
                }
                case Tokens.CHECK : {
                    read();

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, null,
                                       SchemaObject.ConstraintTypes.CHECK);

                    readCheckConstraintCondition(c);

                    OrderedHashSet set = c.getCheckColumnExpressions();

                    for (int i = 0; i < set.size(); i++) {
                        ExpressionColumn e = (ExpressionColumn) set.get(i);

                        if (column.getName().name.equals(e.getColumnName())) {
                            if (e.getSchemaName() != null
                                    && e.getSchemaName()
                                       != table.getSchemaName().name) {
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
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, null,
                                       SchemaObject.ConstraintTypes.CHECK);

                    c.check = new ExpressionLogical(column);

                    constraintList.add(c);

                    hasNotNullConstraint = true;

                    break;
                }
                case Tokens.NULL : {
                    if (hasNotNullConstraint || hasNullNoiseWord
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
        startRecording();

        isCheckOrTriggerCondition = true;

        Expression condition = XreadBooleanValueExpression();

        isCheckOrTriggerCondition = false;

        Token[] tokens = getRecordedStatement();

        readThis(Tokens.CLOSEBRACKET);

        c.check = condition;
    }

    /**
     * Process a bracketed column list as used in the declaration of SQL
     * CONSTRAINTS and return an array containing the indexes of the columns
     * within the table.
     *
     * @param table table that contains the columns
     * @param ascOrDesc boolean
     * @return array of column indexes
     */
    private int[] readColumnList(Table table, boolean ascOrDesc) {

        OrderedHashSet set = readColumnNames(ascOrDesc);

        return table.getColumnIndexes(set);
    }

    StatementSchema compileCreateIndex(boolean unique) {

        Table    table;
        HsqlName indexHsqlName;

        read();

        indexHsqlName = readNewSchemaObjectName(SchemaObject.INDEX, true);

        readThis(Tokens.ON);

        table = readTableName();

        HsqlName tableSchema = table.getSchemaName();

        indexHsqlName.setSchemaIfNull(tableSchema);

        indexHsqlName.parent = table.getName();

        if (indexHsqlName.schema != tableSchema) {
            throw Error.error(ErrorCode.X_42505);
        }

        indexHsqlName.schema = table.getSchemaName();

        int[]    indexColumns = readColumnList(table, true);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, Boolean.valueOf(unique)
        };

        return new StatementSchema(sql, StatementTypes.CREATE_INDEX, args,
                                   null, table.getName());
    }

    StatementSchema compileCreateSchema() {

        HsqlName schemaName    = null;
        String   authorisation = null;

        read();

        if (token.tokenType != Tokens.AUTHORIZATION) {
            schemaName = readNewSchemaName();
        }

        if (token.tokenType == Tokens.AUTHORIZATION) {
            read();
            checkIsSimpleName();

            authorisation = token.tokenString;

            read();

            if (schemaName == null) {
                Grantee owner =
                    database.getGranteeManager().get(authorisation);

                if (owner == null) {
                    throw Error.error(ErrorCode.X_28501, authorisation);
                }

                schemaName =
                    database.nameManager.newHsqlName(owner.getName().name,
                                                     isDelimitedIdentifier(),
                                                     SchemaObject.SCHEMA);

                SqlInvariants.checkSchemaNameNotSystem(token.tokenString);
            }
        }

        if (SqlInvariants.PUBLIC_ROLE_NAME.equals(authorisation)) {
            throw Error.error(ErrorCode.X_28502, authorisation);
        }

        Grantee owner = authorisation == null ? session.getGrantee()
                                              : database.getGranteeManager()
                                                  .get(authorisation);

        if (owner == null) {
            throw Error.error(ErrorCode.X_28501, authorisation);
        }

        if (!session.getGrantee().isSchemaCreator()) {
            throw Error.error(ErrorCode.X_0L000,
                              session.getGrantee().getNameString());
        }

        if (database.schemaManager.schemaExists(schemaName.name)) {
            throw Error.error(ErrorCode.X_42504, schemaName.name);
        }

        if (schemaName.name.equals(SqlInvariants.LOBS_SCHEMA)) {
            schemaName = SqlInvariants.LOBS_SCHEMA_HSQLNAME;
            owner      = schemaName.owner;
        }

        String        sql  = getLastPart();
        Object[]      args = new Object[] {
            schemaName, owner
        };
        HsqlArrayList list = new HsqlArrayList();
        StatementSchema cs = new StatementSchema(sql,
            StatementTypes.CREATE_SCHEMA, args, null, null);

        cs.setSchemaHsqlName(schemaName);
        list.add(cs);
        getCompiledStatementBody(list);

        StatementSchema[] array = new StatementSchema[list.size()];

        list.toArray(array);

        boolean swapped;

        do {
            swapped = false;

            for (int i = 0; i < array.length - 1; i++) {
                if (array[i].order > array[i + 1].order) {
                    StatementSchema temp = array[i + 1];

                    array[i + 1] = array[i];
                    array[i]     = temp;
                    swapped      = true;
                }
            }
        } while (swapped);

        return new StatementSchemaDefinition(array);
    }

    void getCompiledStatementBody(HsqlList list) {

        int    position;
        String sql;
        int    statementType;

        for (boolean end = false; !end; ) {
            StatementSchema cs = null;

            position = getPosition();

            switch (token.tokenType) {

                case Tokens.CREATE :
                    read();

                    switch (token.tokenType) {

                        // not in schema definition
                        case Tokens.SCHEMA :
                        case Tokens.USER :
                        case Tokens.UNIQUE :
                            throw unexpectedToken();
                        case Tokens.INDEX :
                            statementType = StatementTypes.CREATE_INDEX;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.SEQUENCE :
                            cs     = compileCreateSequence();
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.ROLE :
                            cs     = compileCreateRole();
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.DOMAIN :
                            statementType = StatementTypes.CREATE_DOMAIN;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.TYPE :
                            cs     = compileCreateType();
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.CHARACTER :
                            cs     = compileCreateCharacterSet();
                            cs.sql = getLastPart(position);
                            break;

                        // no supported
                        case Tokens.ASSERTION :
                            throw unexpectedToken();
                        case Tokens.TABLE :
                        case Tokens.MEMORY :
                        case Tokens.CACHED :
                        case Tokens.TEMP :
                        case Tokens.GLOBAL :
                        case Tokens.TEMPORARY :
                        case Tokens.TEXT :
                            statementType = StatementTypes.CREATE_TABLE;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.TRIGGER :
                            statementType = StatementTypes.CREATE_TRIGGER;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.VIEW :
                            statementType = StatementTypes.CREATE_VIEW;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.FUNCTION :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatementForRoutine(
                                position, startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        case Tokens.PROCEDURE :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatementForRoutine(
                                position, startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null);
                            break;

                        default :
                            throw unexpectedToken();
                    }
                    break;

                case Tokens.GRANT :
                    cs     = compileGrantOrRevoke();
                    cs.sql = getLastPart(position);
                    break;

                case Tokens.SEMICOLON :
                    read();

                    end = true;
                    break;

                case Tokens.X_ENDPARSE :
                    end = true;
                    break;

                default :
                    throw unexpectedToken();
            }

            if (cs != null) {
                cs.isSchemaDefinition = true;

                list.add(cs);
            }
        }
    }

    StatementSchema compileCreateRole() {

        read();

        HsqlName name = readNewUserIdentifier();
        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementSchema(sql, StatementTypes.CREATE_ROLE, args);
    }

    StatementSchema compileCreateUser() {

        HsqlName name;
        String   password;
        boolean  admin   = false;
        Grantee  grantor = session.getGrantee();

        read();

        name = readNewUserIdentifier();

        readThis(Tokens.PASSWORD);

        password = readPassword();

        if (token.tokenType == Tokens.ADMIN) {
            read();

            admin = true;
        }

        checkDatabaseUpdateAuthorisation();

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            name, password, grantor, Boolean.valueOf(admin)
        };

        return new StatementSchema(sql, StatementTypes.CREATE_USER, args);
    }

    HsqlName readNewUserIdentifier() {

        checkIsSimpleName();

        String  tokenS   = token.tokenString;
        boolean isQuoted = isDelimitedIdentifier();

        if (tokenS.equalsIgnoreCase("SA")) {
            tokenS   = "SA";
            isQuoted = false;
        }

        HsqlName name = database.nameManager.newHsqlName(tokenS, isQuoted,
            SchemaObject.GRANTEE);

        read();

        return name;
    }

    String readPassword() {

        String tokenS = token.tokenString;

        read();

        return tokenS;
    }

    Statement compileRenameObject(HsqlName name, int objectType) {

        HsqlName newName = readNewSchemaObjectName(objectType, true);
        String   sql     = getLastPart();
        Object[] args    = new Object[] {
            name, newName
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... RENAME ...
     * @param table table
     */
    void processAlterTableRename(Table table) {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE, true);

        name.setSchemaIfNull(table.getSchemaName());

        if (table.getSchemaName() != name.schema) {
            throw Error.error(ErrorCode.X_42505);
        }

        database.schemaManager.renameSchemaObject(table.getName(), name);
    }

    void processAlterTableAddUniqueConstraint(Table table, HsqlName name) {

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        int[] cols = this.readColumnList(table, false);

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addUniqueConstraint(cols, name);
    }

    Statement compileAlterTableAddUniqueConstraint(Table table,
            HsqlName name) {

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        int[]    cols = this.readColumnList(table, false);
        String   sql  = getLastPart();
        Object[] args = new Object[] {
            cols, name
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddForeignKeyConstraint(Table table, HsqlName name) {

        if (name == null) {
            name = database.nameManager.newAutoName("FK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set           = readColumnNames(false);
        Constraint     c             = readFKReferences(table, name, set);
        HsqlName       mainTableName = c.getMainTableName();

        c.core.mainTable = database.schemaManager.getTable(session,
                mainTableName.name, mainTableName.schema.name);

        c.setColumnsIndexes(table);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addForeignKey(c);
    }

    Statement compileAlterTableAddForeignKeyConstraint(Table table,
            HsqlName name) {

        if (name == null) {
            name = database.nameManager.newAutoName("FK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set           = readColumnNames(false);
        Constraint     c             = readFKReferences(table, name, set);
        HsqlName       mainTableName = c.getMainTableName();

        c.core.mainTable = database.schemaManager.getTable(session,
                mainTableName.name, mainTableName.schema.name);

        c.setColumnsIndexes(table);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ c };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   c.core.mainTableName, table.getName());
    }

    void processAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, SchemaObject.ConstraintTypes.CHECK);

        readCheckConstraintCondition(check);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addCheckConstraint(check);
    }

    Statement compileAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, SchemaObject.ConstraintTypes.CHECK);

        readCheckConstraintCondition(check);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ check };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddColumn(Table table) {

        int           colIndex = table.getColumnCount();
        HsqlArrayList list     = new HsqlArrayList();
        Constraint constraint =
            new Constraint(null, null, SchemaObject.ConstraintTypes.TEMP);

        list.add(constraint);
        checkIsSchemaObjectName();

        HsqlName hsqlName =
            database.nameManager.newColumnHsqlName(table.getName(),
                token.tokenString, isDelimitedIdentifier());

        read();

        ColumnSchema column = readColumnDefinitionOrNull(table, hsqlName,
            list);

        if (column == null) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (token.tokenType == Tokens.BEFORE) {
            read();

            colIndex = table.getColumnIndex(token.tokenString);

            read();
        }

        TableWorks tableWorks = new TableWorks(session, table);

        session.commit(false);
        tableWorks.addColumn(column, colIndex, list);

        return;
    }

    Statement compileAlterTableAddColumn(Table table) {

        int           colIndex = table.getColumnCount();
        HsqlArrayList list     = new HsqlArrayList();
        Constraint constraint =
            new Constraint(null, null, SchemaObject.ConstraintTypes.TEMP);

        list.add(constraint);
        checkIsSchemaObjectName();

        HsqlName hsqlName =
            database.nameManager.newColumnHsqlName(table.getName(),
                token.tokenString, isDelimitedIdentifier());

        read();

        ColumnSchema column = readColumnDefinitionOrNull(table, hsqlName,
            list);

        if (column == null) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (token.tokenType == Tokens.BEFORE) {
            read();

            colIndex = table.getColumnIndex(token.tokenString);

            read();
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            column, new Integer(colIndex), list
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddPrimaryKey(Table table, HsqlName name) {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set = readColumnNames(false);
        Constraint constraint =
            new Constraint(name, set,
                           SchemaObject.ConstraintTypes.PRIMARY_KEY);

        constraint.setColumnsIndexes(table);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addPrimaryKey(constraint, name);
    }

    Statement compileAlterTableAddPrimaryKey(Table table, HsqlName name) {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set = readColumnNames(false);
        Constraint constraint =
            new Constraint(name, set,
                           SchemaObject.ConstraintTypes.PRIMARY_KEY);

        constraint.setColumnsIndexes(table);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ constraint };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP COLUMN ...
     */
    void processAlterTableDropColumn(Table table, String colName,
                                     boolean cascade) {

        int colindex = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropColumn(colindex, cascade);
    }

    Statement compileAlterTableDropColumn(Table table, String colName,
                                          boolean cascade) {

        HsqlName writeName = null;
        int      colindex  = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        Object[] args = new Object[] {
            table.getColumn(colindex).getName(),
            ValuePool.getInt(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };

        if (!table.isTemp()) {
            writeName = table.getName();
        }

        return new StatementSchema(null, StatementTypes.DROP_COLUMN, args,
                                   null, writeName);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP CONSTRAINT ...
     */
    void processAlterTableDropConstraint(Table table, String name,
                                         boolean cascade) {

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropConstraint(name, cascade);

        return;
    }

    void processAlterColumn(Table table, ColumnSchema column,
                            int columnIndex) {

        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);
                processAlterColumnRename(table, column);

                return;
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    TableWorks tw = new TableWorks(session, table);

                    tw.setColDefaultExpression(columnIndex, null);

                    return;
                } else if (token.tokenType == Tokens.GENERATED) {
                    read();
                    column.setIdentity(null);
                    table.setColumnTypeVars(columnIndex);

                    return;
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();

                switch (token.tokenType) {

                    case Tokens.DATA : {
                        read();
                        readThis(Tokens.TYPE);
                        processAlterColumnDataType(table, column);

                        return;
                    }
                    case Tokens.DEFAULT : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET DEFAULT
                        TableWorks tw   = new TableWorks(session, table);
                        Type       type = column.getDataType();
                        Expression expr = this.readDefaultClause(type);

                        tw.setColDefaultExpression(columnIndex, expr);

                        return;
                    }
                    case Tokens.NOT : {

                        //ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
                        read();
                        readThis(Tokens.NULL);
                        session.commit(false);

                        TableWorks tw = new TableWorks(session, table);

                        tw.setColNullability(column, false);

                        return;
                    }
                    case Tokens.NULL : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET NULL
                        session.commit(false);

                        TableWorks tw = new TableWorks(session, table);

                        tw.setColNullability(column, true);

                        return;
                    }
                    default :
                        rewind(position);
                        read();
                        break;
                }
            }
            default :
        }

        if (token.tokenType == Tokens.SET
                || token.tokenType == Tokens.RESTART) {
            if (!column.isIdentity()) {
                throw Error.error(ErrorCode.X_42535);
            }

            processAlterColumnSequenceOptions(column);

            return;
        } else {
            processAlterColumnType(table, column, true);

            return;
        }
    }

    Statement compileAlterColumn(Table table, ColumnSchema column,
                                 int columnIndex) {

        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileAlterColumnRename(table, column);
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    return compileAlterColumnDropDefault(table, column,
                                                         columnIndex);
                } else if (token.tokenType == Tokens.GENERATED) {
                    read();

                    return compileAlterColumnDropGenerated(table, column,
                                                           columnIndex);
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();

                switch (token.tokenType) {

                    case Tokens.DATA : {
                        read();
                        readThis(Tokens.TYPE);

                        return compileAlterColumnDataType(table, column);
                    }
                    case Tokens.DEFAULT : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET DEFAULT
                        Type       type = column.getDataType();
                        Expression expr = this.readDefaultClause(type);

                        return compileAlterColumnSetDefault(table, column,
                                                            expr);
                    }
                    case Tokens.NOT : {

                        //ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
                        read();
                        readThis(Tokens.NULL);

                        return compileAlterColumnSetNullability(table, column,
                                false);
                    }
                    case Tokens.NULL : {
                        read();

                        return compileAlterColumnSetNullability(table, column,
                                true);
                    }
                    default :
                        rewind(position);
                        read();
                        break;
                }
            }

            // fall through
            default :
        }

        if (token.tokenType == Tokens.SET
                || token.tokenType == Tokens.RESTART) {
            if (!column.isIdentity()) {
                throw Error.error(ErrorCode.X_42535);
            }

            return compileAlterColumnSequenceOptions(table, column);
        } else {
            return compileAlterColumnType(table, column);
        }
    }

    private Statement compileAlterColumnDataType(Table table,
            ColumnSchema column) {

        HsqlName writeName  = null;
        Type     typeObject = readTypeDefinition(false);
        String   sql        = getLastPart();
        Object[] args       = new Object[] {
            table, column, typeObject
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   null, table.getName());
    }

    private Statement compileAlterColumnType(Table table,
            ColumnSchema column) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    private Statement compileAlterColumnSequenceOptions(Table table,
            ColumnSchema column) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    private Statement compileAlterColumnSetNullability(Table table,
            ColumnSchema column, boolean b) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    private Statement compileAlterColumnSetDefault(Table table,
            ColumnSchema column, Expression expr) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    private Statement compileAlterColumnDropGenerated(Table table,
            ColumnSchema column, int columnIndex) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    private Statement compileAlterColumnDropDefault(Table table,
            ColumnSchema column, int columnIndex) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   table.getName());
    }

    Statement compileAlterSequence() {

        read();

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        NumberSequence sequence =
            database.schemaManager.getSequence(token.tokenString, schema.name,
                                               true);

        read();

        if (token.tokenType == Tokens.RENAME) {
            read();
            readThis(Tokens.TO);

            return compileRenameObject(sequence.getName(),
                                       SchemaObject.SEQUENCE);
        }

        NumberSequence copy = sequence.duplicate();

        readSequenceOptions(copy, false, true);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            sequence, copy
        };

        return new StatementSchema(sql, StatementTypes.ALTER_SEQUENCE, args);
    }

    void processAlterColumnSequenceOptions(ColumnSchema column) {

        OrderedIntHashSet set      = new OrderedIntHashSet();
        NumberSequence    sequence = column.getIdentitySequence().duplicate();

        while (true) {
            boolean end = false;

            switch (token.tokenType) {

                case Tokens.RESTART : {
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.WITH);

                    long value = readBigint();

                    sequence.setStartValue(value);

                    break;
                }
                case Tokens.SET :
                    read();

                    switch (token.tokenType) {

                        case Tokens.INCREMENT : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            readThis(Tokens.BY);

                            long value = readBigint();

                            sequence.setIncrement(value);

                            break;
                        }
                        case Tokens.NO :
                            read();

                            if (token.tokenType == Tokens.MAXVALUE) {
                                sequence.setDefaultMaxValue();
                            } else if (token.tokenType == Tokens.MINVALUE) {
                                sequence.setDefaultMinValue();
                            } else if (token.tokenType == Tokens.CYCLE) {
                                sequence.setCycle(false);
                            } else {
                                throw unexpectedToken();
                            }

                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            break;

                        case Tokens.MAXVALUE : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMaxValueNoCheck(value);

                            break;
                        }
                        case Tokens.MINVALUE : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMinValueNoCheck(value);

                            break;
                        }
                        case Tokens.CYCLE :
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            sequence.setCycle(true);
                            break;

                        default :
                            throw super.unexpectedToken();
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
     * Should allow only limited changes to column type
     */
    private void processAlterColumnDataType(Table table, ColumnSchema oldCol) {
        processAlterColumnType(table, oldCol, false);
    }

    /**
     * Allows changes to type of column or addition of an IDENTITY generator.
     * IDENTITY is not removed if it does not appear in new column definition
     * Constraint definitions are not allowed
     */
    private void processAlterColumnType(Table table, ColumnSchema oldCol,
                                        boolean fullDefinition) {

        ColumnSchema newCol;

        if (oldCol.isGenerated()) {
            throw Error.error(ErrorCode.X_42561);
        }

        if (fullDefinition) {
            HsqlArrayList list = new HsqlArrayList();
            Constraint    c    = table.getPrimaryConstraint();

            if (c == null) {
                c = new Constraint(null, null,
                                   SchemaObject.ConstraintTypes.TEMP);
            }

            list.add(c);

            newCol = readColumnDefinitionOrNull(table, oldCol.getName(), list);

            if (newCol == null) {
                throw Error.error(ErrorCode.X_42000);
            }

            if (oldCol.isIdentity() && newCol.isIdentity()) {
                throw Error.error(ErrorCode.X_42525);
            }

            if (list.size() > 1) {
                throw Error.error(ErrorCode.X_42524);
            }
        } else {
            Type type = readTypeDefinition(true);

            if (oldCol.isIdentity()) {
                if (!type.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42561);
                }
            }

            newCol = oldCol.duplicate();

            newCol.setType(type);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.retypeColumn(oldCol, newCol);
    }

    /**
     * Responsible for handling tail of ALTER COLUMN ... RENAME ...
     */
    private void processAlterColumnRename(Table table, ColumnSchema column) {

        checkIsSimpleName();

        if (table.findColumn(token.tokenString) > -1) {
            throw Error.error(ErrorCode.X_42504, token.tokenString);
        }

        database.schemaManager.checkColumnIsReferenced(table.getName(),
                column.getName());
        session.commit(false);
        table.renameColumn(column, token.tokenString, isDelimitedIdentifier());
        read();
    }

    private Statement compileAlterColumnRename(Table table,
            ColumnSchema column) {

        checkIsSimpleName();

        HsqlName name = readNewSchemaObjectName(SchemaObject.COLUMN, true);

        if (table.findColumn(name.name) > -1) {
            throw Error.error(ErrorCode.X_42504, name.name);
        }

        database.schemaManager.checkColumnIsReferenced(table.getName(),
                column.getName());

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            column.getName(), name
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args);
    }

    Statement compileAlterSchemaRename() {

        HsqlName name = readSchemaName();

        checkSchemaUpdateAuthorisation(name);
        readThis(Tokens.RENAME);
        readThis(Tokens.TO);

        HsqlName newName = readNewSchemaName();
        String   sql     = getLastPart();
        Object[] args    = new Object[] {
            name, newName
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args);
    }

    Statement compileAlterUser() {

        read();

        String   password;
        User     userObject;
        HsqlName userName = readNewUserIdentifier();

        userObject = database.getUserManager().get(userName.name);

        if (userName.name.equals(Tokens.T_PUBLIC)) {
            throw Error.error(ErrorCode.X_42503);
        }

        readThis(Tokens.SET);

        if (token.tokenType == Tokens.PASSWORD) {
            read();

            password = readPassword();

            Object[] args = new Object[] {
                userObject, password
            };

            return new StatementCommand(StatementTypes.SET_USER_PASSWORD,
                                        args);
        } else if (token.tokenType == Tokens.INITIAL) {
            read();
            readThis(Tokens.SCHEMA);

            HsqlName schemaName;

            if (token.tokenType == Tokens.DEFAULT) {
                schemaName = null;
            } else {
                schemaName = database.schemaManager.getSchemaHsqlName(
                    token.tokenString);
            }

            read();

            Object[] args = new Object[] {
                userObject, schemaName
            };

            return new StatementCommand(StatementTypes.SET_USER_INITIAL_SCHEMA,
                                        args);
        } else {
            throw unexpectedToken();
        }
    }

    void processAlterDomain() {

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Type domain = database.schemaManager.getDomain(token.tokenString,
            schema.name, true);

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                HsqlName newName = readNewSchemaObjectName(SchemaObject.DOMAIN,
                    true);

                newName.setSchemaIfNull(schema);

                if (domain.getSchemaName() != newName.schema) {
                    throw Error.error(ErrorCode.X_42505, newName.schema.name);
                }

                checkSchemaUpdateAuthorisation(schema);
                database.schemaManager.renameSchemaObject(domain.getName(),
                        newName);

                return;
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();
                    domain.userTypeModifier.removeDefaultClause();

                    return;
                } else if (token.tokenType == Tokens.CONSTRAINT) {
                    read();
                    checkIsSchemaObjectName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), token.tokenString,
                        SchemaObject.CONSTRAINT, true);

                    read();
                    database.schemaManager.removeSchemaObject(name);

                    return;
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();
                readThis(Tokens.DEFAULT);

                Expression e = readDefaultClause(domain);

                domain.userTypeModifier.setDefaultClause(e);

                return;
            }
            case Tokens.ADD : {
                read();

                if (token.tokenType == Tokens.CONSTRAINT
                        || token.tokenType == Tokens.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    readConstraint(domain, tempConstraints);

                    Constraint c = (Constraint) tempConstraints.get(0);

                    domain.userTypeModifier.addConstraint(c);
                    database.schemaManager.addSchemaObject(c);

                    return;
                }
            }
        }

        throw unexpectedToken();
    }

    Statement compileAlterDomain() {

        read();

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        Type domain = database.schemaManager.getDomain(token.tokenString,
            schema.name, true);

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileRenameObject(domain.getName(),
                                           SchemaObject.DOMAIN);
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    return compileAlterDomainDropDefault(domain);
                } else if (token.tokenType == Tokens.CONSTRAINT) {
                    read();
                    checkIsSchemaObjectName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), token.tokenString,
                        SchemaObject.CONSTRAINT, true);

                    read();

                    return compileAlterDomainDropConstraint(domain, name);
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();
                readThis(Tokens.DEFAULT);

                Expression e = readDefaultClause(domain);

                return compileAlterDomainSetDefault(domain, e);
            }
            case Tokens.ADD : {
                read();

                if (token.tokenType == Tokens.CONSTRAINT
                        || token.tokenType == Tokens.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    readConstraint(domain, tempConstraints);

                    Constraint c = (Constraint) tempConstraints.get(0);

                    return compileAlterDomainAddConstraint(domain, c);
                }
            }
        }

        throw unexpectedToken();
    }

    private Statement compileAlterDomainAddConstraint(Type domain,
            Constraint c) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainSetDefault(Type domain, Expression e) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainDropConstraint(Type domain,
            HsqlName name) {

        String sql = super.getStatement(getParsePosition(),
                                        startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainDropDefault(Type domain) {

        String sql = getStatement(getParsePosition(), startStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private boolean isGrantToken() {

        switch (token.tokenType) {

            case Tokens.ALL :
            case Tokens.INSERT :
            case Tokens.UPDATE :
            case Tokens.SELECT :
            case Tokens.DELETE :
            case Tokens.USAGE :
            case Tokens.EXECUTE :
            case Tokens.REFERENCES :
                return true;

            default :
                return false;
        }
    }

    StatementSchema compileGrantOrRevoke() {

        boolean grant = token.tokenType == Tokens.GRANT;

        read();

        if (isGrantToken()
                || (!grant
                    && (token.tokenType == Tokens.GRANT
                        || token.tokenType == Tokens.HIERARCHY))) {
            return compileRightGrantOrRevoke(grant);
        } else {
            return compileRoleGrantOrRevoke(grant);
        }
    }

    private StatementSchema compileRightGrantOrRevoke(boolean grant) {

        OrderedHashSet granteeList = new OrderedHashSet();
        Grantee        grantor     = null;
        Right          right       = null;

//        SchemaObject   schemaObject;
        HsqlName objectName    = null;
        boolean  isTable       = false;
        boolean  isUsage       = false;
        boolean  isExec        = false;
        boolean  isAll         = false;
        boolean  isGrantOption = false;
        boolean  cascade       = false;

        if (!grant) {
            if (token.tokenType == Tokens.GRANT) {
                read();
                readThis(Tokens.OPTION);
                readThis(Tokens.FOR);

                isGrantOption = true;

                // throw not suppoerted
            } else if (token.tokenType == Tokens.HIERARCHY) {
                throw unsupportedFeature();
/*
                read();
                readThis(Token.OPTION);
                readThis(Token.FOR);
*/
            }
        }

        // ALL means all the rights the grantor can grant
        if (token.tokenType == Tokens.ALL) {
            read();

            if (token.tokenType == Tokens.PRIVILEGES) {
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
                    GranteeManager.getCheckSingleRight(token.tokenString);
                int            grantType = token.tokenType;
                OrderedHashSet columnSet = null;

                read();

                switch (grantType) {

                    case Tokens.REFERENCES :
                    case Tokens.SELECT :
                    case Tokens.INSERT :
                    case Tokens.UPDATE :
                        if (token.tokenType == Tokens.OPENBRACKET) {
                            columnSet = readColumnNames(false);
                        }

                    // fall through
                    case Tokens.DELETE :
                    case Tokens.TRIGGER :
                        if (right == null) {
                            right = new Right();
                        }

                        right.set(rightType, columnSet);

                        isTable = true;
                        break;

                    case Tokens.USAGE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right   = Right.fullRights;
                        isUsage = true;
                        loop    = false;

                        continue;
                    case Tokens.EXECUTE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right  = Right.fullRights;
                        isExec = true;
                        loop   = false;

                        continue;
                }

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        readThis(Tokens.ON);

        int objectType = 0;

        switch (token.tokenType) {

            case Tokens.CLASS :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                if (!isSimpleName() || !isDelimitedIdentifier()) {
                    throw Error.error(ErrorCode.X_42569);
                }

                objectType = SchemaObject.FUNCTION;
                objectName = readNewSchemaObjectName(SchemaObject.FUNCTION,
                                                     false);
                break;

            case Tokens.SPECIFIC : {
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                switch (token.tokenType) {

                    case Tokens.ROUTINE :
                    case Tokens.PROCEDURE :
                    case Tokens.FUNCTION :
                        read();
                        break;

                    default :
                        throw unexpectedToken();
                }

                objectType = SchemaObject.SPECIFIC_ROUTINE;

                break;
            }
            case Tokens.FUNCTION :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.FUNCTION;
                break;

            case Tokens.PROCEDURE :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.PROCEDURE;
                break;

            case Tokens.ROUTINE :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.ROUTINE;
                break;

            case Tokens.TYPE :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.TYPE;
                break;

            case Tokens.DOMAIN :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.DOMAIN;
                break;

            case Tokens.SEQUENCE :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.SEQUENCE;
                break;

            case Tokens.CHARACTER :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();
                readThis(Tokens.SET);

                objectType = SchemaObject.CHARSET;
                break;

            case Tokens.TABLE :
            default :
                if (!isTable && !isAll) {
                    throw unexpectedToken();
                }

                readIfThis(Tokens.TABLE);

                objectType = SchemaObject.TABLE;
        }

        objectName = readNewSchemaObjectName(objectType, false);

        if (grant) {
            readThis(Tokens.TO);
        } else {
            readThis(Tokens.FROM);
        }

        while (true) {
            checkIsSimpleName();
            granteeList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                break;
            }
        }

        if (grant) {
            if (token.tokenType == Tokens.WITH) {
                read();
                readThis(Tokens.GRANT);
                readThis(Tokens.OPTION);

                isGrantOption = true;
            }

            /** @todo - implement */
            if (token.tokenType == Tokens.GRANTED) {
                read();
                readThis(Tokens.BY);

                if (token.tokenType == Tokens.CURRENT_USER) {
                    read();

                    //
                } else {
                    readThis(Tokens.CURRENT_ROLE);

                    if (session.getRole() == null) {
                        throw Error.error(ErrorCode.X_0P000);
                    }

                    grantor = session.getRole();
                }
            }
        } else {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else {
                readThis(Tokens.RESTRICT);
            }
        }

        int      typee = grant ? StatementTypes.GRANT
                               : StatementTypes.REVOKE;
        Object[] args  = new Object[] {
            granteeList, objectName, right, grantor, Boolean.valueOf(cascade),
            Boolean.valueOf(isGrantOption)
        };
        String          sql = getLastPart();
        StatementSchema cs  = new StatementSchema(sql, typee, args);

        return cs;
    }

    private StatementSchema compileRoleGrantOrRevoke(boolean grant) {

        Grantee        grantor     = session.getGrantee();
        OrderedHashSet roleList    = new OrderedHashSet();
        OrderedHashSet granteeList = new OrderedHashSet();
        boolean        cascade     = false;

        if (!grant && token.tokenType == Tokens.ADMIN) {
            throw unsupportedFeature();
/*
            read();
            readThis(Token.OPTION);
            readThis(Token.FOR);
*/
        }

        while (true) {
            checkIsSimpleName();
            roleList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }

        if (grant) {
            readThis(Tokens.TO);
        } else {
            readThis(Tokens.FROM);
        }

        while (true) {
            checkIsSimpleName();
            granteeList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                break;
            }
        }

        if (grant) {
            if (token.tokenType == Tokens.WITH) {
                throw unsupportedFeature();
/*
                read();
                readThis(Token.ADMIN);
                readThis(Token.OPTION);
*/
            }
        }

        if (token.tokenType == Tokens.GRANTED) {
            read();
            readThis(Tokens.BY);

            if (token.tokenType == Tokens.CURRENT_USER) {
                read();

                //
            } else {
                readThis(Tokens.CURRENT_ROLE);

                if (session.getRole() == null) {
                    throw Error.error(ErrorCode.X_0P000);
                }

                grantor = session.getRole();
            }
        }

        if (!grant) {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else {
                readThis(Tokens.RESTRICT);
            }
        }

        int             type = grant ? StatementTypes.GRANT_ROLE
                                     : StatementTypes.REVOKE_ROLE;
        Object[]        args = new Object[] {
            granteeList, roleList, grantor, Boolean.valueOf(cascade)
        };
        String          sql  = getLastPart();
        StatementSchema cs   = new StatementSchema(sql, type, args);

        return cs;
    }

    void checkSchemaUpdateAuthorisation(HsqlName schema) {

        if (session.isProcessingLog) {
            return;
        }

        SqlInvariants.checkSchemaNameNotSystem(schema.name);

        if (isSchemaDefinition) {
            if (schema != session.getCurrentSchemaHsqlName()) {
                throw Error.error(ErrorCode.X_42505);
            }
        } else {
            session.getGrantee().checkSchemaUpdateOrGrantRights(schema.name);
        }

        session.checkDDLWrite();
    }

    void checkDatabaseUpdateAuthorisation() {
        session.checkAdmin();
        session.checkDDLWrite();
    }

    StatementSchema compileComment() {

        HsqlName name;
        int      type;

        readThis(Tokens.COMMENT);
        readThis(Tokens.ON);

        switch (token.tokenType) {

            case Tokens.ROUTINE :
            case Tokens.TABLE : {
                type = token.tokenType == Tokens.ROUTINE ? SchemaObject.ROUTINE
                                                         : SchemaObject.TABLE;

                read();
                checkIsSchemaObjectName();

                name = database.nameManager.newHsqlName(token.tokenString,
                        token.isDelimitedIdentifier, type);

                if (token.namePrefix == null) {
                    name.schema = session.getCurrentSchemaHsqlName();
                } else {
                    name.schema = database.nameManager.newHsqlName(
                        token.namePrefix, token.isDelimitedPrefix,
                        SchemaObject.SCHEMA);
                }

                read();

                break;
            }
            case Tokens.COLUMN : {
                read();
                checkIsSchemaObjectName();

                name = database.nameManager.newHsqlName(token.tokenString,
                        token.isDelimitedIdentifier, SchemaObject.COLUMN);

                if (token.namePrefix == null) {
                    throw Error.error(ErrorCode.X_42501);
                }

                name.parent =
                    database.nameManager.newHsqlName(token.namePrefix,
                                                     token.isDelimitedPrefix,
                                                     SchemaObject.TABLE);

                if (token.namePrePrefix == null) {
                    name.parent.schema = session.getCurrentSchemaHsqlName();
                } else {
                    name.parent.schema = database.nameManager.newHsqlName(
                        token.namePrePrefix, token.isDelimitedPrePrefix,
                        SchemaObject.TABLE);
                }

                read();

                break;
            }
            default :
                throw unexpectedToken();
        }

        readThis(Tokens.IS);

        String   comment   = readQuotedString();
        Object[] arguments = new Object[] {
            name, comment
        };

        return new StatementSchema(null, StatementTypes.COMMENT, arguments);
    }
}
