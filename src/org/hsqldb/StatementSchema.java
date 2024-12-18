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
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.map.ValuePool;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.Right;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;

/**
 * Implementation of Statement for DDL statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class StatementSchema extends Statement {

    int      order;
    Object[] arguments = ValuePool.emptyObjectArray;
    boolean  isSchemaDefinition;
    Token[]  statementTokens;

    StatementSchema(int type, int group) {
        super(type, group);

        isTransactionStatement = true;
    }

    StatementSchema(String sql, int type) {
        this(sql, type, null, null, null);
    }

    StatementSchema(
            String sql,
            int type,
            Object[] args,
            HsqlName[] readName,
            HsqlName[] writeName) {

        super(type);

        isTransactionStatement = true;
        this.sql               = sql;

        if (args != null) {
            arguments = args;
        }

        if (readName != null) {
            readTableNames = readName;
        }

        if (writeName != null) {
            writeTableNames = writeName;
        }

        switch (type) {

            case StatementTypes.RENAME_OBJECT :
            case StatementTypes.RENAME_SCHEMA :
            case StatementTypes.ALTER_DOMAIN :
            case StatementTypes.ALTER_CONSTRAINT :
            case StatementTypes.ALTER_INDEX :
            case StatementTypes.ALTER_ROUTINE :
            case StatementTypes.ALTER_SEQUENCE :
            case StatementTypes.ALTER_TYPE :
            case StatementTypes.ALTER_TABLE :
            case StatementTypes.ALTER_TRANSFORM :
            case StatementTypes.ALTER_VIEW :
            case StatementTypes.ADD_TABLE_PERIOD :
            case StatementTypes.DROP_TABLE_PERIOD :
            case StatementTypes.ADD_TABLE_SYSTEM_VERSIONING :
            case StatementTypes.DROP_TABLE_SYSTEM_VERSIONING :
            case StatementTypes.DROP_ASSERTION :
            case StatementTypes.DROP_CHARACTER_SET :
            case StatementTypes.DROP_COLLATION :
            case StatementTypes.DROP_TYPE :
            case StatementTypes.DROP_DOMAIN :
            case StatementTypes.DROP_ROLE :
            case StatementTypes.DROP_USER :
            case StatementTypes.DROP_ROUTINE :
            case StatementTypes.DROP_SCHEMA :
            case StatementTypes.DROP_SEQUENCE :
            case StatementTypes.DROP_TABLE :
            case StatementTypes.DROP_TRANSFORM :
            case StatementTypes.DROP_TRANSLATION :
            case StatementTypes.DROP_TRIGGER :
            case StatementTypes.DROP_CAST :
            case StatementTypes.DROP_ORDERING :
            case StatementTypes.DROP_VIEW :
            case StatementTypes.DROP_INDEX :
            case StatementTypes.DROP_CONSTRAINT :
            case StatementTypes.DROP_COLUMN :
            case StatementTypes.DROP_REFERENCE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.GRANT :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 10;
                break;

            case StatementTypes.GRANT_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 10;
                break;

            case StatementTypes.REVOKE :
            case StatementTypes.REVOKE_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.CREATE_SCHEMA :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                break;

            case StatementTypes.CREATE_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ROUTINE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 7;
                break;

            case StatementTypes.CREATE_SEQUENCE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TABLE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 2;
                break;

            case StatementTypes.CREATE_TRANSFORM :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TRANSLATION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TRIGGER :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 7;
                break;

            case StatementTypes.CREATE_CAST :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 2;
                break;

            case StatementTypes.CREATE_TYPE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ORDERING :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_VIEW :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 5;
                break;

            case StatementTypes.CREATE_USER :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ASSERTION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 9;
                break;

            case StatementTypes.CREATE_CHARACTER_SET :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_COLLATION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_DOMAIN :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ALIAS :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 8;
                break;

            case StatementTypes.CREATE_INDEX :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 4;
                break;

            case StatementTypes.CREATE_REFERENCE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 12;
                break;

            case StatementTypes.COMMENT :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 11;
                break;

            case StatementTypes.CHECK :
                group           = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                statementTokens = (Token[]) args[0];
                break;

            case StatementTypes.LOG_SCHEMA_STATEMENT :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSchema");
        }
    }

    public Result execute(Session session) {

        Result result;

        try {
            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, getSQL());
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);

            return result;
        }

        session.database.schemaManager.setSchemaChangeTimestamp();

        HsqlName sessionSchema = session.currentSchema;

        try {
            if (type == StatementTypes.RENAME_SCHEMA) {
                session.currentSchema =
                    SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
            }

            if (isLogged) {
                session.database.logger.writeOtherStatement(session, sql);
            }
        } catch (Throwable e) {
            return Result.newErrorResult(e, sql);
        } finally {
            if (type == StatementTypes.RENAME_SCHEMA) {
                session.currentSchema = sessionSchema;
            }
        }

        return result;
    }

    Result getResult(Session session) {

        SchemaManager schemaManager = session.database.schemaManager;

        if (this.isExplain) {
            return Result.newSingleColumnStringResult(
                "OPERATION",
                describe(session));
        }

        mainSwitch:
        switch (type) {

            case StatementTypes.RENAME_OBJECT :
            case StatementTypes.RENAME_SCHEMA : {
                HsqlName     name     = (HsqlName) arguments[0];
                HsqlName     newName  = (HsqlName) arguments[1];
                boolean      ifExists = ((Boolean) arguments[2]).booleanValue();
                SchemaObject object;

                switch (name.type) {

                    case SchemaObject.CATALOG : {
                        try {
                            session.checkAdmin();
                            session.checkDDLWrite();
                            name.rename(newName);
                            break mainSwitch;
                        } catch (HsqlException e) {
                            return Result.newErrorResult(e, sql);
                        }
                    }

                    case SchemaObject.SCHEMA : {
                        checkSchemaUpdateAuthorisation(session, name);
                        schemaManager.checkSchemaNameCanChange(name);
                        schemaManager.renameSchema(name, newName);
                        break mainSwitch;
                    }
                }

                try {
                    name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

                    if (ifExists) {
                        object = schemaManager.findUserTable(
                            name.name,
                            name.schema.name);

                        if (object == null) {
                            return Result.updateZeroResult;
                        }
                    }

                    if (name.type == SchemaObject.COLUMN) {
                        Table table = schemaManager.getUserTable(name.parent);
                        int   index = table.getColumnIndex(name.name);

                        object = table.getColumn(index);
                    } else {
                        object = schemaManager.findSchemaObject(name);

                        if (object == null) {
                            throw Error.error(ErrorCode.X_42501, name.name);
                        }

                        if (name.type == SchemaObject.SPECIFIC_ROUTINE) {
                            name = ((Routine) object).getSpecificName();
                        } else {
                            name = object.getName();
                        }
                    }

                    checkSchemaUpdateAuthorisation(session, name.schema);
                    newName.setSchemaIfNull(name.schema);

                    if (name.schema != newName.schema) {
                        HsqlException e = Error.error(ErrorCode.X_42505);

                        return Result.newErrorResult(e, sql);
                    }

                    newName.parent = name.parent;

                    switch (object.getType()) {

                        case SchemaObject.COLUMN :
                            HsqlName parent = object.getName().parent;

                            schemaManager.checkObjectIsReferenced(parent);

                            Table table = schemaManager.getUserTable(parent);
                            TriggerDef[] triggers = table.getTriggers();

                            for (int i = 0; i < triggers.length; i++) {
                                if (triggers[i] instanceof TriggerDefSQL) {
                                    throw Error.error(
                                        ErrorCode.X_42502,
                                        triggers[i].getName()
                                                   .getSchemaQualifiedStatementName());
                                }
                            }

                            table.renameColumn((ColumnSchema) object, newName);
                            break;

                        default :
                            schemaManager.renameSchemaObject(name, newName);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_CONSTRAINT : {
                Table      table      = (Table) arguments[0];
                int[]      newColumns = (int[]) arguments[1];
                HsqlName   name       = (HsqlName) arguments[2];
                Constraint constraint;

                try {
                    constraint =
                        (Constraint) session.database.schemaManager.findSchemaObject(
                            name);

                    constraint.extendFKIndexColumns(session, newColumns);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_INDEX : {
                Table    table        = (Table) arguments[0];
                int[]    indexColumns = (int[]) arguments[1];
                HsqlName name         = (HsqlName) arguments[2];
                Index    index;

                try {
                    index =
                        (Index) session.database.schemaManager.findSchemaObject(
                            name);

                    TableWorks tableWorks = new TableWorks(session, table);

                    tableWorks.alterIndex(index, indexColumns);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_SEQUENCE : {
                try {
                    NumberSequence sequence = (NumberSequence) arguments[0];
                    NumberSequence settings = (NumberSequence) arguments[1];

                    checkSchemaUpdateAuthorisation(
                        session,
                        sequence.getSchemaName());
                    sequence.reset(settings);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_DOMAIN :
                try {
                    int subType = ((Integer) arguments[0]).intValue();
                    Type domain = (Type) arguments[1];
                    Expression domainDefault =
                        domain.userTypeModifier.getDefaultClause();
                    OrderedHashSet<HsqlName> refSet =
                        session.database.schemaManager.getReferencesTo(
                            domain.getName());
                    OrderedHashSet<SchemaObject> tableSet =
                        new OrderedHashSet<>();

                    for (int i = 0; i < refSet.size(); i++) {
                        HsqlName objectName = refSet.get(i);
                        HsqlName tableName  = objectName.parent;

                        if (tableName.type == SchemaObject.TABLE) {
                            SchemaObject table =
                                session.database.schemaManager.findSchemaObject(
                                    tableName);

                            tableSet.add(table);
                        }
                    }

                    if (subType == StatementTypes.ADD_CONSTRAINT) {
                        Constraint c = (Constraint) arguments[2];

                        for (int i = 0; i < tableSet.size(); i++) {
                            Table      table = (Table) tableSet.get(i);
                            TableWorks tw    = new TableWorks(session, table);

                            tw.checkAddDomainConstraint(domain, c);
                        }
                    }

                    switch (subType) {

                        case StatementTypes.ADD_CONSTRAINT : {
                            Constraint c = (Constraint) arguments[2];

                            setOrCheckObjectName(
                                session,
                                domain.getName(),
                                c.getName(),
                                true);
                            domain.userTypeModifier.addConstraint(c);
                            session.database.schemaManager.addSchemaObject(c);
                            break;
                        }

                        case StatementTypes.ADD_DEFAULT : {
                            Expression e = (Expression) arguments[2];

                            domain.userTypeModifier.setDefaultClause(e);
                            break;
                        }

                        case StatementTypes.DROP_CONSTRAINT : {
                            HsqlName name = (HsqlName) arguments[2];

                            session.database.schemaManager.removeSchemaObject(
                                name);
                            break;
                        }

                        case StatementTypes.DROP_DEFAULT : {
                            domain.userTypeModifier.removeDefaultClause();
                            break;
                        }
                    }

                    for (int i = 0; i < tableSet.size(); i++) {
                        Table table = (Table) tableSet.get(i);

                        if (subType == StatementTypes.DROP_DEFAULT) {
                            for (int j = 0; j < table.getColumnCount(); j++) {
                                ColumnSchema column = table.getColumn(j);

                                if (column.dataType == domain) {
                                    if (column.getDefaultExpression() == null) {
                                        column.setDefaultExpression(
                                            domainDefault);
                                    }
                                }
                            }
                        }

                        table.resetDefaultFlags();
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            case StatementTypes.ALTER_TABLE :
                try {
                    int   subType = ((Integer) arguments[0]).intValue();
                    Table table   = (Table) arguments[1];

                    switch (subType) {

                        case StatementTypes.ADD_CONSTRAINT : {
                            Constraint c           = (Constraint) arguments[2];
                            Boolean    ifNotExists = (Boolean) arguments[3];

                            if (ifNotExists.booleanValue()) {
                                SchemaObject object =
                                    session.database.schemaManager.findSchemaObject(
                                        c.getName().name,
                                        c.getName().schema.name,
                                        c.getName().type);

                                if (object != null) {
                                    return Result.updateZeroResult;
                                }
                            }

                            switch (c.getConstraintType()) {

                                case SchemaObject.ConstraintTypes.PRIMARY_KEY : {
                                    TableWorks tableWorks = new TableWorks(
                                        session,
                                        table);

                                    tableWorks.addPrimaryKey(c);
                                    break;
                                }

                                case SchemaObject.ConstraintTypes.UNIQUE : {
                                    TableWorks tableWorks = new TableWorks(
                                        session,
                                        table);

                                    tableWorks.addUniqueConstraint(c);
                                    break;
                                }

                                case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                                    TableWorks tableWorks = new TableWorks(
                                        session,
                                        table);

                                    tableWorks.addForeignKey(c);
                                    break;
                                }

                                case SchemaObject.ConstraintTypes.CHECK : {
                                    TableWorks tableWorks = new TableWorks(
                                        session,
                                        table);

                                    tableWorks.addCheckConstraint(c);
                                    break;
                                }
                            }

                            break;
                        }

                        case StatementTypes.ADD_COLUMN : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int colIndex = ((Integer) arguments[3]).intValue();
                            HsqlArrayList<Constraint> list =
                                (HsqlArrayList<Constraint>) arguments[4];
                            Boolean ifNotExists = (Boolean) arguments[5];
                            TableWorks tableWorks = new TableWorks(
                                session,
                                table);

                            if (ifNotExists.booleanValue()) {
                                if (table.findColumn(column.getName().name)
                                        != -1) {
                                    break;
                                }
                            }

                            tableWorks.addColumn(column, colIndex, list);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_TYPE : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            Type         type   = (Type) arguments[3];
                            ColumnSchema newCol = column.duplicate();

                            newCol.setType(type);

                            TableWorks tw = new TableWorks(session, table);

                            tw.retypeColumn(column, newCol);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_TYPE_IDENTITY : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            Type         type   = (Type) arguments[3];
                            NumberSequence sequence =
                                (NumberSequence) arguments[4];
                            ColumnSchema newCol = column.duplicate();

                            newCol.setType(type);
                            newCol.setIdentity(sequence);

                            TableWorks tw = new TableWorks(session, table);

                            tw.retypeColumn(column, newCol);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_SEQUENCE : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();
                            NumberSequence sequence =
                                (NumberSequence) arguments[4];

                            if (column.isIdentity()) {
                                column.getIdentitySequence().reset(sequence);
                            } else {
                                column.setIdentity(sequence);
                                table.setColumnTypeVars(columnIndex);
                            }

                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_NULL : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            boolean nullable =
                                ((Boolean) arguments[3]).booleanValue();
                            TableWorks   tw     = new TableWorks(
                                session,
                                table);

                            tw.setColNullability(column, nullable);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_DEFAULT : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();
                            Expression   e      = (Expression) arguments[4];
                            TableWorks   tw     = new TableWorks(
                                session,
                                table);

                            tw.setColDefaultExpression(columnIndex, e);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_DROP_DEFAULT : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();
                            TableWorks   tw     = new TableWorks(
                                session,
                                table);

                            tw.setColDefaultExpression(columnIndex, null);
                            table.setColumnTypeVars(columnIndex);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_DROP_EXPRESSION : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();

                            column.setGeneratingExpression(null);
                            table.setColumnTypeVars(columnIndex);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_DROP_GENERATED : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();

                            column.setIdentity(null);
                            table.setColumnTypeVars(columnIndex);
                            break;
                        }

                        case StatementTypes.ALTER_COLUMN_PROPERTIES : {
                            ColumnSchema column = (ColumnSchema) arguments[2];
                            int columnIndex =
                                ((Integer) arguments[3]).intValue();
                            Statement[]  statements =
                                (Statement[]) arguments[4];

                            for (int i = 0; i < statements.length; i++) {
                                if (statements[i] != null) {
                                    Result result = statements[i].execute(
                                        session);

                                    if (result.isError()) {
                                        return result;
                                    }
                                }
                            }

                            break;
                        }
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            case StatementTypes.ADD_TABLE_PERIOD : {
                Table            table  = (Table) arguments[0];
                PeriodDefinition period = (PeriodDefinition) arguments[1];
                TablePeriodWorks works  = new TablePeriodWorks(session, table);

                try {
                    if (period.getPeriodType()
                            == SchemaObject.PeriodType.PERIOD_SYSTEM) {
                        works.addSystemPeriod(period);
                    } else {
                        works.addApplicationPeriod(period);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DROP_TABLE_PERIOD : {
                Table            table   = (Table) arguments[0];
                PeriodDefinition period  = (PeriodDefinition) arguments[1];
                Boolean          cascade = (Boolean) arguments[2];
                TablePeriodWorks works   = new TablePeriodWorks(session, table);

                try {
                    if (period.getPeriodType()
                            == SchemaObject.PeriodType.PERIOD_SYSTEM) {
                        works.dropSystemPeriod(cascade.booleanValue());
                    } else {
                        works.dropApplicationPeriod(cascade.booleanValue());
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ADD_TABLE_SYSTEM_VERSIONING : {
                Table            table = (Table) arguments[0];
                TablePeriodWorks works = new TablePeriodWorks(session, table);

                try {
                    works.addSystemVersioning();
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DROP_TABLE_SYSTEM_VERSIONING : {
                Table            table   = (Table) arguments[0];
                Boolean          cascade = (Boolean) arguments[1];
                TablePeriodWorks works   = new TablePeriodWorks(session, table);

                try {
                    works.dropSystemVersioning(cascade);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_ROUTINE : {
                Routine routine = (Routine) arguments[0];

                try {
                    Routine oldRoutine =
                        (Routine) schemaManager.findSchemaObject(
                            routine.getSpecificName());

                    schemaManager.replaceReferences(oldRoutine, routine);
                    oldRoutine.setAsAlteredRoutine(routine);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ALTER_TYPE :
            case StatementTypes.ALTER_TRANSFORM : {
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSchema");
            }

            case StatementTypes.ALTER_VIEW : {
                View view = (View) arguments[0];

                try {
                    checkSchemaUpdateAuthorisation(
                        session,
                        view.getSchemaName());

                    View oldView = (View) schemaManager.findSchemaObject(
                        view.getName());

                    if (oldView == null) {
                        throw Error.error(
                            ErrorCode.X_42501,
                            view.getName().name);
                    }

                    view.setName(oldView.getName());
                    view.compile(session, null);

                    OrderedHashSet<HsqlName> dependents =
                        schemaManager.getReferencesTo(
                            oldView.getName());

                    if (dependents.getCommonElementCount(view.getReferences())
                            > 0) {
                        throw Error.error(ErrorCode.X_42502);
                    }

                    int i = schemaManager.getTableIndex(oldView);

                    schemaManager.setTable(i, view);

                    OrderedHashSet<Table> set = new OrderedHashSet<>();

                    set.add(view);

                    try {
                        schemaManager.recompileDependentObjects(set);
                        schemaManager.replaceReferences(oldView, view);
                    } catch (HsqlException e) {
                        schemaManager.setTable(i, oldView);
                        schemaManager.recompileDependentObjects(set);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DROP_COLUMN : {
                try {
                    HsqlName name       = (HsqlName) arguments[0];
                    int      objectType = ((Integer) arguments[1]).intValue();
                    boolean  cascade = ((Boolean) arguments[2]).booleanValue();
                    boolean  ifExists = ((Boolean) arguments[3]).booleanValue();
                    Table    table = schemaManager.getUserTable(name.parent);
                    int      colindex   = table.getColumnIndex(name.name);

                    if (table.getColumnCount() == 1) {
                        throw Error.error(ErrorCode.X_42591);
                    }

                    checkSchemaUpdateAuthorisation(
                        session,
                        table.getSchemaName());

                    TableWorks tableWorks = new TableWorks(session, table);

                    tableWorks.dropColumn(colindex, cascade);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DROP_ASSERTION :
            case StatementTypes.DROP_CHARACTER_SET :
            case StatementTypes.DROP_COLLATION :
            case StatementTypes.DROP_TYPE :
            case StatementTypes.DROP_DOMAIN :
            case StatementTypes.DROP_ROLE :
            case StatementTypes.DROP_USER :
            case StatementTypes.DROP_ROUTINE :
            case StatementTypes.DROP_SCHEMA :
            case StatementTypes.DROP_SEQUENCE :
            case StatementTypes.DROP_TABLE :
            case StatementTypes.DROP_TRANSFORM :
            case StatementTypes.DROP_TRANSLATION :
            case StatementTypes.DROP_TRIGGER :
            case StatementTypes.DROP_CAST :
            case StatementTypes.DROP_ORDERING :
            case StatementTypes.DROP_VIEW :
            case StatementTypes.DROP_INDEX :
            case StatementTypes.DROP_CONSTRAINT :
            case StatementTypes.DROP_REFERENCE : {
                try {
                    HsqlName name       = (HsqlName) arguments[0];
                    int      objectType = ((Integer) arguments[1]).intValue();
                    boolean  cascade = ((Boolean) arguments[2]).booleanValue();
                    boolean  ifExists = ((Boolean) arguments[3]).booleanValue();

                    switch (type) {

                        case StatementTypes.DROP_ROLE :
                        case StatementTypes.DROP_USER :
                            session.checkAdmin();
                            session.checkDDLWrite();
                            break;

                        case StatementTypes.DROP_SCHEMA :
                            checkSchemaUpdateAuthorisation(session, name);

                            if (!schemaManager.schemaExists(name.name)) {
                                if (ifExists) {
                                    return Result.updateZeroResult;
                                }
                            }

                            break;

                        default :
                            if (name.schema == null) {
                                name.schema =
                                    session.getCurrentSchemaHsqlName();
                            } else {
                                if (!schemaManager.schemaExists(
                                        name.schema.name)) {
                                    if (ifExists) {
                                        return Result.updateZeroResult;
                                    }
                                }
                            }

                            name.schema = schemaManager.getUserSchemaHsqlName(
                                name.schema.name);

                            checkSchemaUpdateAuthorisation(
                                session,
                                name.schema);

                            SchemaObject object =
                                schemaManager.findSchemaObject(
                                    name);

                            if (object == null) {
                                if (ifExists) {
                                    return Result.updateZeroResult;
                                }

                                throw Error.error(ErrorCode.X_42501, name.name);
                            }

                            if (name.type == SchemaObject.SPECIFIC_ROUTINE) {
                                name = ((Routine) object).getSpecificName();
                            } else {
                                name = object.getName();
                            }
                    }

                    if (!cascade) {
                        schemaManager.checkObjectIsReferenced(name);
                    }

                    switch (type) {

                        case StatementTypes.DROP_ROLE :
                            dropRole(session, name, cascade);
                            break;

                        case StatementTypes.DROP_USER :
                            dropUser(session, name, cascade);
                            break;

                        case StatementTypes.DROP_SCHEMA :
                            dropSchema(session, name, cascade);
                            break;

                        case StatementTypes.DROP_ASSERTION :
                            break;

                        case StatementTypes.DROP_CHARACTER_SET :
                        case StatementTypes.DROP_COLLATION :
                        case StatementTypes.DROP_SEQUENCE :
                        case StatementTypes.DROP_TRIGGER :
                        case StatementTypes.DROP_REFERENCE :
                            dropObject(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TYPE :
                            dropType(session, name, cascade);
                            break;

                        case StatementTypes.DROP_DOMAIN :
                            dropDomain(session, name, cascade);
                            break;

                        case StatementTypes.DROP_ROUTINE :
                            dropRoutine(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TABLE :
                        case StatementTypes.DROP_VIEW :
                            dropTable(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TRANSFORM :
                        case StatementTypes.DROP_TRANSLATION :
                        case StatementTypes.DROP_CAST :
                        case StatementTypes.DROP_ORDERING :
                            break;

                        case StatementTypes.DROP_INDEX :
                            checkSchemaUpdateAuthorisation(
                                session,
                                name.schema);
                            schemaManager.dropIndex(session, name);
                            break;

                        case StatementTypes.DROP_CONSTRAINT :
                            checkSchemaUpdateAuthorisation(
                                session,
                                name.schema);
                            schemaManager.dropConstraint(
                                session,
                                name,
                                cascade);
                            break;
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.GRANT :
            case StatementTypes.REVOKE : {
                try {
                    boolean  grant = type == StatementTypes.GRANT;
                    OrderedHashSet<String> granteeList =
                        (OrderedHashSet<String>) arguments[0];
                    HsqlName name = (HsqlName) arguments[1];

                    setSchemaName(session, null, name);

                    name = schemaManager.getSchemaObjectName(
                        name.schema,
                        name.name,
                        name.type,
                        true);

                    SchemaObject schemaObject = schemaManager.findSchemaObject(
                        name);
                    Right   right   = (Right) arguments[2];
                    Grantee grantor = (Grantee) arguments[3];
                    boolean cascade = ((Boolean) arguments[4]).booleanValue();
                    boolean isGrantOption =
                        ((Boolean) arguments[5]).booleanValue();

                    if (grantor == null) {
                        grantor = isSchemaDefinition
                                  ? schemaName.owner
                                  : session.getGrantee();
                    }

                    GranteeManager gm = session.database.granteeManager;

                    switch (schemaObject.getType()) {

                        case SchemaObject.CHARSET :
                            break;

                        case SchemaObject.VIEW :
                        case SchemaObject.TABLE : {
                            Table t = (Table) schemaObject;

                            right.setColumns(t);

                            if (t.isTemp && !right.isFull()) {
                                return Result.newErrorResult(
                                    Error.error(ErrorCode.X_42595),
                                    sql);
                            }

                            Expression[] filters = right.getFiltersArray();

                            // only check the expression resolves
                            for (int i = 0; i < filters.length; i++) {
                                if (filters[i] != null) {
                                    Expression expr = filters[i].duplicate();

                                    expr.resolveGrantFilter(session, t);
                                }
                            }
                        }
                    }

                    if (grant) {
                        gm.grant(
                            session,
                            granteeList,
                            schemaObject,
                            right,
                            grantor,
                            isGrantOption);
                    } else {
                        gm.revoke(
                            granteeList,
                            schemaObject,
                            right,
                            grantor,
                            isGrantOption,
                            cascade);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.GRANT_ROLE :
            case StatementTypes.REVOKE_ROLE : {
                try {
                    boolean grant = type == StatementTypes.GRANT_ROLE;
                    OrderedHashSet<String> granteeList =
                        (OrderedHashSet<String>) arguments[0];
                    OrderedHashSet<String> roleList =
                        (OrderedHashSet<String>) arguments[1];
                    Grantee        grantor = (Grantee) arguments[2];
                    boolean cascade = ((Boolean) arguments[3]).booleanValue();
                    GranteeManager gm      = session.database.granteeManager;

                    gm.checkGranteeList(granteeList);

                    for (int i = 0; i < granteeList.size(); i++) {
                        String grantee = granteeList.get(i);

                        gm.checkRoleList(grantee, roleList, grantor, grant);
                    }

                    if (grant) {
                        for (int i = 0; i < granteeList.size(); i++) {
                            String grantee = granteeList.get(i);

                            for (int j = 0; j < roleList.size(); j++) {
                                String roleName = roleList.get(j);

                                gm.grant(grantee, roleName, grantor);
                            }
                        }
                    } else {
                        for (int i = 0; i < granteeList.size(); i++) {
                            String grantee = granteeList.get(i);

                            for (int j = 0; j < roleList.size(); j++) {
                                gm.revoke(grantee, roleList.get(j), grantor);
                            }
                        }
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_ASSERTION : {
                return Result.updateZeroResult;
            }

            case StatementTypes.CREATE_CHARACTER_SET : {
                Charset charset = (Charset) arguments[0];

                try {
                    setOrCheckObjectName(
                        session,
                        null,
                        charset.getName(),
                        true);
                    schemaManager.addSchemaObject(charset);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_COLLATION : {
                Collation collation = (Collation) arguments[0];

                try {
                    setOrCheckObjectName(
                        session,
                        null,
                        collation.getName(),
                        true);
                    schemaManager.addSchemaObject(collation);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_ROLE : {
                try {
                    session.checkAdmin();
                    session.checkDDLWrite();

                    HsqlName name = (HsqlName) arguments[0];

                    session.database.getGranteeManager().addRole(name);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_USER : {
                HsqlName name     = (HsqlName) arguments[0];
                String   password = (String) arguments[1];
                Grantee  grantor  = (Grantee) arguments[2];
                boolean  admin    = ((Boolean) arguments[3]).booleanValue();
                boolean  isDigest = ((Boolean) arguments[4]).booleanValue();

                try {
                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.getUserManager()
                                    .createUser(
                                        session,
                                        name,
                                        password,
                                        isDigest);

                    if (admin) {
                        session.database.getGranteeManager()
                                        .grant(
                                            name.name,
                                            SqlInvariants.DBA_ADMIN_ROLE_NAME,
                                            grantor);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_SCHEMA : {
                HsqlName name        = (HsqlName) arguments[0];
                Grantee  owner       = (Grantee) arguments[1];
                Boolean  ifNotExists = (Boolean) arguments[2];

                try {
                    session.checkDDLWrite();

                    if (schemaManager.schemaExists(name.name)) {
                        if (session.isProcessingScript()
                                && SqlInvariants.PUBLIC_SCHEMA.equals(
                                    name.name)) {}
                        else {
                            if (ifNotExists != null
                                    && ifNotExists.booleanValue()) {
                                return Result.updateZeroResult;
                            }

                            throw Error.error(ErrorCode.X_42504, name.name);
                        }
                    } else {
                        schemaManager.createSchema(name, owner);

                        // always include authorization
                        Schema schema = schemaManager.findSchema(name.name);

                        this.sql = schema.getSQL();
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_ROUTINE : {
                Routine routine = (Routine) arguments[0];

                try {
                    setOrCheckObjectName(
                        session,
                        null,
                        routine.getName(),
                        false);
                    schemaManager.addSchemaObject(routine);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_ALIAS : {
                HsqlName  name     = (HsqlName) arguments[0];
                Routine[] routines = (Routine[]) arguments[1];

                try {
                    session.checkAdmin();
                    session.checkDDLWrite();

                    if (name != null) {
                        for (int i = 0; i < routines.length; i++) {
                            routines[i].setName(name);
                            schemaManager.addSchemaObject(routines[i]);
                        }
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_SEQUENCE : {
                NumberSequence sequence    = (NumberSequence) arguments[0];
                Boolean        ifNotExists = (Boolean) arguments[1];

                try {
                    setOrCheckObjectName(
                        session,
                        null,
                        sequence.getName(),
                        true);
                    schemaManager.addSchemaObject(sequence);
                    break;
                } catch (HsqlException e) {
                    if (ifNotExists != null && ifNotExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }
            }

            case StatementTypes.CREATE_DOMAIN : {
                Type type = (Type) arguments[0];
                Constraint[] constraints =
                    type.userTypeModifier.getConstraints();

                try {
                    setOrCheckObjectName(session, null, type.getName(), true);

                    for (int i = 0; i < constraints.length; i++) {
                        Constraint c = constraints[i];

                        setOrCheckObjectName(
                            session,
                            type.getName(),
                            c.getName(),
                            true);
                        schemaManager.addSchemaObject(c);
                    }

                    schemaManager.addSchemaObject(type);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_TABLE : {
                Table                     table = (Table) arguments[0];
                HsqlArrayList<Constraint> tempConstraints =
                    (HsqlArrayList<Constraint>) arguments[1];
                HsqlArrayList<Constraint> tempIndexes =
                    (HsqlArrayList<Constraint>) arguments[2];
                StatementDMQL statement = (StatementDMQL) arguments[3];
                Boolean                   ifNotExists = (Boolean) arguments[4];
                HsqlArrayList<Constraint> foreignConstraints = null;

                try {
                    setOrCheckObjectName(session, null, table.getName(), true);
                } catch (HsqlException e) {
                    if (ifNotExists != null && ifNotExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }

                try {
                    if (isSchemaDefinition) {
                        foreignConstraints = new HsqlArrayList<>();
                    }

                    if (tempConstraints.size() > 0) {
                        table = ParserDDL.addTableConstraintDefinitions(
                            session,
                            table,
                            tempConstraints,
                            foreignConstraints,
                            true);
                        arguments[1] = foreignConstraints;
                    }

                    table.compile(session, null);
                    schemaManager.addSchemaObject(table);

                    if (!tempIndexes.isEmpty()) {
                        TableWorks tableWorks = new TableWorks(session, table);

                        for (int i = 0; i < tempIndexes.size(); i++) {
                            Constraint c = tempIndexes.get(i);

                            tableWorks.addIndex(
                                c.getMainColumns(),
                                c.getName(),
                                false);
                        }

                        table = tableWorks.getTable();
                    }

                    if (statement != null) {
                        Result result = statement.execute(session);

                        if (result.isError()) {
                            return result;
                        }

                        table.insertIntoTable(session, result);

                        if (table.hasLobColumn) {
                            RowIterator it = table.rowIterator(session);

                            while (it.next()) {
                                Row      row  = it.getCurrentRow();
                                Object[] data = row.getData();

                                session.sessionData.adjustLobUsageCount(
                                    table,
                                    data,
                                    1);
                            }
                        }
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    schemaManager.removeExportedKeys(table);
                    schemaManager.removeDependentObjects(table.getName());
                    schemaManager.removeSchemaObject(table.getName());

                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_TRANSFORM :
            case StatementTypes.CREATE_TRANSLATION :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_TRIGGER : {
                TriggerDef trigger     = (TriggerDef) arguments[0];
                HsqlName   otherName   = (HsqlName) arguments[1];
                Boolean    ifNotExists = (Boolean) arguments[2];

                try {
                    setOrCheckObjectName(
                        session,
                        null,
                        trigger.getName(),
                        true);

                    if (otherName != null) {
                        setOrCheckObjectName(session, null, otherName, false);

                        if (schemaManager.findSchemaObject(otherName) == null) {
                            throw Error.error(
                                ErrorCode.X_42501,
                                otherName.name);
                        }
                    }

                    trigger.table.addTrigger(trigger, otherName);
                    schemaManager.addSchemaObject(trigger);
                    trigger.start();
                    break;
                } catch (HsqlException e) {
                    if (ifNotExists != null && ifNotExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }
            }

            case StatementTypes.CREATE_CAST :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_TYPE : {
                Type type = (Type) arguments[0];

                try {
                    setOrCheckObjectName(session, null, type.getName(), true);
                    schemaManager.addSchemaObject(type);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_ORDERING :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_VIEW : {
                View    view        = (View) arguments[0];
                Boolean ifNotExists = (Boolean) arguments[1];

                try {
                    setOrCheckObjectName(session, null, view.getName(), true);
                } catch (HsqlException e) {
                    if (ifNotExists != null && ifNotExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }

                try {
                    view.compile(session, null);
                    schemaManager.addSchemaObject(view);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_INDEX : {
                Table         table;
                HsqlName      name;
                int[]         indexColumns;
                boolean       unique;
                RoutineSchema routineSchema;
                Boolean       ifNotExists;

                table         = (Table) arguments[0];
                indexColumns  = (int[]) arguments[1];
                name          = (HsqlName) arguments[2];
                unique        = ((Boolean) arguments[3]).booleanValue();
                routineSchema = (RoutineSchema) arguments[4];
                ifNotExists   = (Boolean) arguments[5];

                /*
                        Index index        = table.getIndexForColumns(indexColumns);

                        if (index != null
                                && ArrayUtil.areEqual(indexColumns, index.getColumns(),
                                                      indexColumns.length, unique)) {
                            if (index.isUnique() || !unique) {
                                return;
                            }
                        }
                */
                try {
                    setOrCheckObjectName(session, table.getName(), name, true);
                } catch (HsqlException e) {
                    if (ifNotExists != null && ifNotExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }

                try {
                    TableWorks tableWorks = new TableWorks(session, table);

                    tableWorks.addIndex(indexColumns, name, unique);
                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.CREATE_REFERENCE : {
                HsqlName name;
                HsqlName targetName;

                name       = (HsqlName) arguments[0];
                targetName = (HsqlName) arguments[1];

                setSchemaName(session, null, name);
                setSchemaName(session, null, targetName);
                setOrCheckObjectName(session, null, name, true);

                // find the new target
                SchemaObject object =
                    session.database.schemaManager.findAnySchemaObjectForSynonym(
                        name.name,
                        name.schema.name);

                if (object != null) {
                    throw Error.error(ErrorCode.X_42504);
                }

                object =
                    session.database.schemaManager.findAnySchemaObjectForSynonym(
                        targetName.name,
                        targetName.schema.name);

                if (object == null) {
                    throw Error.error(ErrorCode.X_42501);
                }

                if (!session.getGrantee()
                            .isFullyAccessibleByRole(object.getName())) {
                    throw Error.error(ErrorCode.X_42501);
                }

                targetName = object.getName();

                SchemaObject reference = new ReferenceObject(name, targetName);

                schemaManager.addSchemaObject(reference);
                break;
            }

            case StatementTypes.COMMENT : {
                HsqlName name    = (HsqlName) arguments[0];
                String   comment = (String) arguments[1];

                switch (name.type) {

                    case SchemaObject.COLUMN : {
                        Table table = (Table) schemaManager.getSchemaObject(
                            name.parent.name,
                            name.parent.schema.name,
                            SchemaObject.TABLE);

                        if (!session.getGrantee()
                                    .isFullyAccessibleByRole(table.getName())) {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        int index = table.getColumnIndex(name.name);

                        if (index < 0) {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        ColumnSchema column = table.getColumn(index);

                        column.getName().comment = comment;
                        break;
                    }

                    case SchemaObject.TRIGGER :
                    case SchemaObject.SEQUENCE :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.TABLE :
                    case SchemaObject.VIEW : {
                        SchemaObject object = schemaManager.getSchemaObject(
                            name.name,
                            name.schema.name,
                            name.type);

                        if (!session.getGrantee()
                                    .isFullyAccessibleByRole(
                                        object.getName())) {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        object.getName().comment = comment;
                        break;
                    }
                }

                break;
            }

            // for logging only
            case StatementTypes.LOG_SCHEMA_STATEMENT :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSchema");
        }

        return Result.updateZeroResult;
    }

    private void dropType(Session session, HsqlName name, boolean cascade) {

        checkSchemaUpdateAuthorisation(session, name.schema);

        Type distinct = (Type) session.database.schemaManager.findSchemaObject(
            name);

        session.database.schemaManager.removeSchemaObject(name, cascade);

        distinct.userTypeModifier = null;
    }

    private static void dropDomain(
            Session session,
            HsqlName name,
            boolean cascade) {

        Type domain = (Type) session.database.schemaManager.findSchemaObject(
            name);
        OrderedHashSet<HsqlName> set =
            session.database.schemaManager.getReferencesTo(
                domain.getName());

        if (!cascade && set.size() > 0) {
            HsqlName objectName = set.get(0);

            throw Error.error(
                ErrorCode.X_42502,
                objectName.getSchemaQualifiedStatementName());
        }

        Constraint[] constraints = domain.userTypeModifier.getConstraints();
        OrderedHashSet<HsqlName> constraintNames = new OrderedHashSet<>();

        for (int i = 0; i < constraints.length; i++) {
            constraintNames.add(constraints[i].getName());
        }

        session.database.schemaManager.removeSchemaObjects(constraintNames);
        session.database.schemaManager.removeSchemaObject(
            domain.getName(),
            cascade);

        domain.userTypeModifier = null;
    }

    private static void dropRole(
            Session session,
            HsqlName name,
            boolean cascade) {

        Grantee role = session.database.getGranteeManager().getRole(name.name);

        if (!cascade && session.database.schemaManager.hasSchemas(role)) {
            HsqlArrayList<Schema> list =
                session.database.schemaManager.getSchemas(
                    role);
            Schema schema = list.get(0);

            throw Error.error(
                ErrorCode.X_42502,
                schema.getName().statementName);
        }

        session.database.schemaManager.dropSchemas(session, role, cascade);
        session.database.getGranteeManager().dropRole(name.name);
    }

    private static void dropUser(
            Session session,
            HsqlName name,
            boolean cascade) {

        Grantee grantee = session.database.getUserManager().get(name.name);

        if (session.database.getSessionManager().isUserActive(name.name)) {
            throw Error.error(ErrorCode.X_42539);
        }

        if (!cascade && session.database.schemaManager.hasSchemas(grantee)) {
            HsqlArrayList<Schema> list =
                session.database.schemaManager.getSchemas(
                    grantee);
            Schema schema = list.get(0);

            throw Error.error(
                ErrorCode.X_42502,
                schema.getName().statementName);
        }

        session.database.schemaManager.dropSchemas(session, grantee, cascade);
        session.database.getUserManager().dropUser(name.name);
    }

    private void dropSchema(Session session, HsqlName name, boolean cascade) {

        HsqlName schema = session.database.schemaManager.getUserSchemaHsqlName(
            name.name);

        checkSchemaUpdateAuthorisation(session, schema);
        session.database.schemaManager.dropSchema(session, name.name, cascade);
    }

    private void dropRoutine(Session session, HsqlName name, boolean cascade) {
        checkSchemaUpdateAuthorisation(session, name.schema);
        session.database.schemaManager.removeSchemaObject(name, cascade);
    }

    private void dropObject(Session session, HsqlName name, boolean cascade) {

        checkSchemaUpdateAuthorisation(session, name.schema);

        name = session.database.schemaManager.getSchemaObjectName(
            name.schema,
            name.name,
            name.type,
            true);

        session.database.schemaManager.removeSchemaObject(name, cascade);
    }

    private void dropTable(Session session, HsqlName name, boolean cascade) {

        Table table = session.database.schemaManager.findUserTable(
            name.name,
            name.schema.name);

        session.database.schemaManager.dropTableOrView(session, table, cascade);
    }

    static void checkSchemaUpdateAuthorisation(
            Session session,
            HsqlName schema) {

        if (session.isProcessingLog()) {
            return;
        }

        if (SqlInvariants.isSystemSchemaName(schema.name)) {
            throw Error.error(ErrorCode.X_42503);
        }

        if (session.parser.isSchemaDefinition) {
            if (schema == session.getCurrentSchemaHsqlName()) {
                return;
            }

            throw Error.error(ErrorCode.X_42505, schema.name);
        }

        session.getGrantee().checkSchemaUpdateOrGrantRights(schema);
        session.checkDDLWrite();
    }

    void setOrCheckObjectName(
            Session session,
            HsqlName parent,
            HsqlName name,
            boolean checkNotExists) {

        if (name.schema == null) {
            name.schema = schemaName == null
                          ? session.getCurrentSchemaHsqlName()
                          : schemaName;
        } else {
            name.schema = session.getSchemaHsqlName(name.schema.name);

            if (name.schema == null) {
                throw Error.error(ErrorCode.X_42505);
            }

            if (isSchemaDefinition && schemaName != name.schema) {
                throw Error.error(ErrorCode.X_42505);
            }
        }

        if (name.parent == null) {
            name.parent = parent;
        }

        if (!isSchemaDefinition) {
            checkSchemaUpdateAuthorisation(session, name.schema);
        }

        if (checkNotExists) {
            session.database.schemaManager.checkSchemaObjectNotExists(name);
        }
    }

    void setSchemaName(Session session, HsqlName parent, HsqlName name) {

        if (name.type == SchemaObject.SCHEMA) {
            return;
        }

        if (name.schema == null) {
            name.schema = schemaName == null
                          ? session.getCurrentSchemaHsqlName()
                          : schemaName;
        } else {
            name.schema = session.getSchemaHsqlName(name.schema.name);

            if (name.schema == null) {
                throw Error.error(ErrorCode.X_42505);
            }

            if (isSchemaDefinition && schemaName != name.schema) {
                throw Error.error(ErrorCode.X_42505);
            }
        }
    }

    public boolean isAutoCommitStatement() {
        return true;
    }

    public String describe(Session session) {
        return sql;
    }

    public Object[] getArguments() {
        return arguments;
    }
}
