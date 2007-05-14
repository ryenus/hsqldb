/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2007, The HSQL Development Group
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
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.User;
import org.hsqldb.types.Type;

/**
 * Script generation.
 *
 * The core functionality of this class was inherited from Hypersonic and
 * extensively rewritten and extended in successive versions of HSQLDB.<p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author fredt@users
 * @version 1.8.0
 * @since 1.7.0
 */
public class DatabaseScript {

    /**
     * Returns the DDL and all other statements for the database excluding
     * INSERT and SET <tablename> READONLY statements.
     * cachedData == true indicates that SET <tablename> INDEX statements should
     * also be included.
     *
     * This class should not have any dependencies on metadata reporting.
     */
    public static Result getScript(Database database, boolean indexRoots) {

        Iterator it;
        Result   r = Result.newSingleColumnResult("COMMAND", Type.SQL_VARCHAR);

        r.metaData.tableNames[0] = "SYSTEM_SCRIPT";

        // collation for database
        if (database.collation.name != null) {
            String name =
                StringConverter.toQuotedString(database.collation.name, '"',
                                               true);

            addRow(r, "SET DATABASE COLLATION " + name);
        }

        // aliases
        HashMap map     = database.aliasManager.getAliasMap();
        HashMap builtin = Library.getAliasMap();

        it = map.keySet().iterator();

        while (it.hasNext()) {
            String alias  = (String) it.next();
            String java   = (String) map.get(alias);
            String biJava = (String) builtin.get(alias);

            if (biJava != null && biJava.equals(java)) {
                continue;
            }

            StringBuffer buffer = new StringBuffer(64);

            buffer.append(Token.T_CREATE).append(' ').append(
                Token.T_ALIAS).append(' ');
            buffer.append(alias);
            buffer.append(" FOR \"");
            buffer.append(java);
            buffer.append('"');
            addRow(r, buffer.toString());
        }

        // Role definitions
        it = database.getGranteeManager().getRoleNames().iterator();

        String role;

        while (it.hasNext()) {
            role = (String) it.next();

            // ADMIN_ROLE_NAME is not persisted
            if (!GranteeManager.isReserved(role)) {
                addRow(r, "CREATE ROLE " + role);
            }
        }

        // Create Users
        Iterator users =
            database.getUserManager().getUsers().values().iterator();

        for (; users.hasNext(); ) {
            User   u    = (User) users.next();
            String name = u.getName();

            addRow(r, u.getCreateUserDDL());
        }

        // Create schemas and schema objects such as tables, sequences, etc.
        addSchemaStatements(database, r, indexRoots);
        addCrossSchemaStatements(database, r, indexRoots);

        // Set User Session Start Schemas
        users = database.getUserManager().getUsers().values().iterator();

        HsqlName initialSchema;

        for (; users.hasNext(); ) {
            User   u    = (User) users.next();
            String name = u.getName();

            initialSchema = u.getInitialSchema();

            if (initialSchema != null) {
                addRow(r, u.getInitialSchemaDDL());
            }
        }

        // rights for classes, tables and views
        addRightsStatements(database, r);

        if (database.logger.hasLog()) {
            int     delay  = database.logger.getWriteDelay();
            boolean millis = delay < 1000;

            if (millis) {
                if (delay != 0 && delay < 20) {
                    delay = 20;
                }
            } else {
                delay /= 1000;
            }

            String statement = "SET WRITE_DELAY " + delay + (millis ? " MILLIS"
                                                                    : "");

            addRow(r, statement);
        }

        return r;
    }

    static void addSchemaStatements(Database database, Result r,
                                    boolean indexRoots) {

        Iterator schemas = database.schemaManager.userSchemaNameIterator();

        while (schemas.hasNext()) {
            String schemaKey = (String) schemas.next();
            HsqlName schema =
                database.schemaManager.toSchemaHsqlName(schemaKey);
            HashMappedList tableList =
                database.schemaManager.getTables(schema.name);

            // schema creation
            {
                String ddl = getSchemaCreateDDL(database, schema);

                addRow(r, ddl);

                ddl = getSetSchemaDDL(database, schema);

                addRow(r, ddl);
            }

            // sequences
            /*
                     CREATE SEQUENCE <name>
                     [AS {INTEGER | BIGINT}]
                     [START WITH <value>]
                     [INCREMENT BY <value>]
             */
            Iterator it = database.schemaManager.sequenceIterator(schema.name);

            while (it.hasNext()) {
                NumberSequence seq = (NumberSequence) it.next();

                addRow(r, getSequenceDDL(seq));
            }

            // tables
            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table t = (Table) tableList.get(i);

                if (t.isView()) {
                    continue;
                }

                StringBuffer a = new StringBuffer(128);

                getTableDDL(t, false, a);
                addRow(r, a.toString());

                // indexes for table
                for (int j = 1; j < t.getIndexCount(); j++) {
                    Index index = t.getIndex(j);

                    if (index.isConstraint()) {

                        // the following are autocreated with the table
                        // indexes for primary keys
                        // indexes for unique constraints
                        // own table indexes for foreign keys
                        continue;
                    }

                    a = new StringBuffer(64);

                    a.append(Token.T_CREATE).append(' ');

                    if (index.isUnique()) {
                        a.append(Token.T_UNIQUE).append(' ');
                    }

                    a.append(Token.T_INDEX).append(' ');
                    a.append(index.getName().statementName);
                    a.append(' ').append(Token.T_ON).append(' ');
                    a.append(t.getName().statementName);

                    int[] col = index.getColumns();
                    int   len = index.getVisibleColumns();

                    getColumnList(t, col, len, a);
                    addRow(r, a.toString());
                }

                // readonly for TEXT tables only
                if (t.isText() && t.isConnected() && t.isDataReadOnly()) {
                    a = new StringBuffer(64);

                    a.append(Token.T_SET).append(' ').append(
                        Token.T_TABLE).append(' ');
                    a.append(t.getName().statementName);
                    a.append(' ').append(Token.T_READONLY).append(' ').append(
                        Token.T_TRUE);
                    addRow(r, a.toString());
                }

                // data source
                String dataSource = getDataSourceDDL(t);

                if (dataSource != null) {
                    addRow(r, dataSource);
                }

                // header
                String header = getDataSourceHeader(t);

                if (!indexRoots && header != null) {
                    addRow(r, header);
                }
            }

            // RESTART WITH <value> statements
            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table t = (Table) tableList.get(i);

                if (!t.isTemp()) {
                    String ddl = getIdentityUpdateDDL(t);

                    addRow(r, ddl);
                }
            }
        }
    }

    static void addCrossSchemaStatements(Database database, Result r,
                                         boolean indexRoots) {

        Iterator schemas = database.schemaManager.userSchemaNameIterator();

        while (schemas.hasNext()) {
            String schemaKey = (String) schemas.next();
            HsqlName schema =
                database.schemaManager.toSchemaHsqlName(schemaKey);
            HashMappedList tableList =
                database.schemaManager.getTables(schema.name);
            String setSchemaStatement = getSetSchemaDDL(database, schema);

            addRow(r, setSchemaStatement);

            // forward referencing foreign keys
            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table        t              = (Table) tableList.get(i);
                Constraint[] constraintList = t.getConstraints();

                for (int j = 0, vSize = constraintList.length; j < vSize;
                        j++) {
                    Constraint c = constraintList[j];

                    if (c.isForward) {
                        StringBuffer a = new StringBuffer(128);

                        a.append(Token.T_ALTER).append(' ').append(
                            Token.T_TABLE).append(' ');
                        a.append(c.getRef().getName().statementName);
                        a.append(' ').append(Token.T_ADD).append(' ');
                        getFKStatement(c, a);
                        addRow(r, a.toString());
                    }
                }
            }

            // views
            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table t = (Table) tableList.get(i);

                if (t.isView()) {
                    View         v = (View) tableList.get(i);
                    StringBuffer a = new StringBuffer(128);

                    a.append(Token.T_CREATE).append(' ').append(
                        Token.T_VIEW).append(' ');
                    a.append(v.getName().statementName).append(' ').append(
                        '(');

                    int count = v.getColumnCount();

                    for (int j = 0; j < count; j++) {
                        a.append(v.getColumn(j).columnName.statementName);

                        if (j < count - 1) {
                            a.append(',');
                        }
                    }

                    a.append(')').append(' ').append(Token.T_AS).append(' ');
                    a.append(v.getStatement());
                    addRow(r, a.toString());
                }
            }

            // triggers
            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table t = (Table) tableList.get(i);

                for (int tv = 0; tv < t.triggerLists.length; tv++) {
                    HsqlArrayList trigVec = t.triggerLists[tv];

                    if (trigVec == null) {
                        continue;
                    }

                    int trCount = trigVec.size();

                    for (int k = 0; k < trCount; k++) {
                        String a = ((TriggerDef) trigVec.get(k)).getDDL();

                        addRow(r, a);
                    }
                }
            }

            // SET <tablename> INDEX statements
            Session sysSession = database.sessionManager.getSysSession();

            for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
                Table t = (Table) tableList.get(i);

                if (indexRoots && t.isIndexCached()
                        && !t.isEmpty(sysSession)) {
                    addRow(r, getIndexRootsDDL((Table) tableList.get(i)));
                }
            }
        }
    }

    // GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY
    // (START WITH n INCREMENT BY m MINVALUE o MAXVALUE p)
    static String getIdentityDDL(NumberSequence seq) {

        StringBuffer a = new StringBuffer(128);

        a.append(Token.T_GENERATED).append(' ');

        if (seq.isAlways()) {
            a.append(Token.T_ALWAYS);
        } else {
            a.append(Token.T_BY).append(' ').append(Token.T_DEFAULT);
        }

        a.append(' ').append(Token.T_AS).append(' ').append(
            Token.T_IDENTITY).append(Token.T_OPENBRACKET).append(
            Token.T_START).append(' ').append(Token.T_WITH).append(' ');
        a.append(seq.getStartValue());

        if (seq.getIncrement() != 1) {
            a.append(' ').append(Token.T_INCREMENT).append(' ').append(
                Token.T_BY).append(' ');
            a.append(seq.getIncrement());
        }

        if (!seq.hasDefaultMinMax()) {
            a.append(' ').append(Token.T_MINVALUE).append(' ');
            a.append(seq.getMinValue());
            a.append(' ').append(Token.T_MAXVALUE).append(' ');
            a.append(seq.getMaxValue());
        }

        if (seq.isCycle()) {
            a.append(' ').append(Token.T_CYCLE);
        }

        a.append(Token.T_CLOSEBRACKET);

        return a.toString();
    }

    static String getSequenceDDL(NumberSequence seq) {

        StringBuffer a = new StringBuffer(128);

        a.append(Token.T_CREATE).append(' ');
        a.append(Token.T_SEQUENCE).append(' ');
        a.append(seq.getName().statementName).append(' ');
        a.append(Token.T_AS).append(' ');
        a.append(seq.getType().getName()).append(' ');
        a.append(Token.T_START).append(' ');
        a.append(Token.T_WITH).append(' ');
        a.append(seq.peek()).append(' ');

        if (seq.getIncrement() != 1) {
            a.append(' ').append(Token.T_INCREMENT).append(' ');
            a.append(Token.T_BY).append(' ');
            a.append(seq.getIncrement());
        }

        if (!seq.hasDefaultMinMax()) {
            a.append(' ').append(Token.T_MINVALUE).append(' ');
            a.append(seq.getMinValue());
            a.append(' ').append(Token.T_MAXVALUE).append(' ');
            a.append(seq.getMaxValue());
        }

        if (seq.isCycle()) {
            a.append(' ').append(Token.T_CYCLE);
        }

        return a.toString();
    }

    static String getIdentityUpdateDDL(Table t) {

        if (t.identityColumn == -1) {
            return "";
        } else {
            String tablename = t.getName().statementName;
            String colname =
                t.getColumn(t.identityColumn).columnName.statementName;
            NumberSequence seq = t.identitySequence;
            StringBuffer   a   = new StringBuffer(128);

            a.append(Token.T_ALTER).append(' ').append(Token.T_TABLE).append(
                ' ').append(tablename).append(' ').append(
                Token.T_ALTER).append(' ').append(Token.T_COLUMN).append(
                ' ').append(colname).append(' ').append(
                Token.T_RESTART).append(' ').append(Token.T_WITH).append(
                ' ').append(seq.peek());

            return a.toString();
        }
    }

    static String getIndexRootsDDL(Table t) {

        StringBuffer a = new StringBuffer(128);

        a.append(Token.T_SET).append(' ').append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append(' ').append(Token.T_INDEX).append('\'');
        a.append(t.getIndexRoots());
        a.append('\'');

        return a.toString();
    }

    static String getSetSchemaDDL(Database database, HsqlName schemaName) {

        StringBuffer ab = new StringBuffer(128);

        ab.append(Token.T_SET).append(' ');
        ab.append(Token.T_SCHEMA).append(' ');
        ab.append(schemaName.statementName);

        return ab.toString();
    }

    static String getSchemaCreateDDL(Database database, HsqlName schemaName) {

        StringBuffer ab = new StringBuffer(128);

        ab.append(Token.T_CREATE).append(' ');
        ab.append(Token.T_SCHEMA).append(' ');
        ab.append(schemaName.statementName).append(' ');
        ab.append(Token.T_AUTHORIZATION).append(' ');

        Grantee owner = database.schemaManager.toSchemaOwner(schemaName.name);

        ab.append(owner.getStatementName());

        return ab.toString();
    }

    static void getTableDDL(Table t, boolean useSchema, StringBuffer a) {

        a.append(Token.T_CREATE).append(' ');

        if (t.isTemp) {
            a.append(Token.T_GLOBAL).append(' ');
            a.append(Token.T_TEMPORARY).append(' ');
        }

        if (t.isText()) {
            a.append(Token.T_TEXT).append(' ');
        } else if (t.isCached()) {
            a.append(Token.T_CACHED).append(' ');
        } else {
            a.append(Token.T_MEMORY).append(' ');
        }

        a.append(Token.T_TABLE).append(' ');

        if (useSchema) {
            a.append(t.getName().schema.statementName).append('.');
        }

        a.append(t.getName().statementName);
        a.append('(');

        int        columns = t.getColumnCount();
        int[]      pk      = t.getPrimaryKey();
        Constraint pkConst = t.getPrimaryConstraint();

        for (int j = 0; j < columns; j++) {
            Column column  = t.getColumn(j);
            String colname = column.columnName.statementName;
            Type   type    = column.getType();

            a.append(colname);
            a.append(' ');
            a.append(type.getDefinition());

            String defaultString = column.getDefaultDDL();

            if (defaultString != null) {
                a.append(' ').append(Token.T_DEFAULT).append(' ');
                a.append(defaultString);
            }

            if (column.isIdentity()) {
                a.append(' ').append(
                    getIdentityDDL(column.getIdentitySequence()));
            }

            if (!column.isNullable()) {
                Constraint c = t.getNotNullConstraintForColumn(j);

                if (!c.getName().isReservedName()) {
                    a.append(' ').append(Token.T_CONSTRAINT).append(
                        ' ').append(c.getName().statementName);
                }

                a.append(' ').append(Token.T_NOT).append(' ').append(
                    Token.T_NULL);
            }

            if (pk.length == 1 && j == pk[0]
                    && pkConst.getName().isReservedName()) {
                a.append(' ').append(Token.T_PRIMARY).append(' ').append(
                    Token.T_KEY);
            }

            if (j < columns - 1) {
                a.append(',');
            }
        }

        Constraint[] constraintList = t.getConstraints();

        for (int j = 0, vSize = constraintList.length; j < vSize; j++) {
            Constraint c = constraintList[j];

            switch (c.getType()) {

                case Constraint.PRIMARY_KEY :
                    if (pk.length > 1
                            || (pk.length == 1
                                && !c.getName().isReservedName())) {
                        a.append(',');

                        if (!c.getName().isReservedName()) {
                            a.append(Token.T_CONSTRAINT).append(' ');
                            a.append(c.getName().statementName).append(' ');
                        }

                        a.append(Token.T_PRIMARY).append(' ').append(
                            Token.T_KEY);
                        getColumnList(t, pk, pk.length, a);
                    }
                    break;

                case Constraint.UNIQUE :
                    a.append(',');

                    if (!c.getName().isReservedName()) {
                        a.append(Token.T_CONSTRAINT).append(' ');
                        a.append(c.getName().statementName);
                        a.append(' ');
                    }

                    a.append(Token.T_UNIQUE);

                    int[] col = c.getMainColumns();

                    getColumnList(t, col, col.length, a);
                    break;

                case Constraint.FOREIGN_KEY :

                    // forward referencing FK
                    if (!c.isForward) {
                        a.append(',');
                        getFKStatement(c, a);
                    }
                    break;

                case Constraint.CHECK :
                    if (c.isNotNull) {
                        break;
                    }

                    a.append(',');

                    if (!c.getName().isReservedName()) {
                        a.append(Token.T_CONSTRAINT).append(' ');
                        a.append(c.getName().statementName).append(' ');
                    }

                    a.append(Token.T_CHECK).append('(');
                    a.append(c.check.getDDL());
                    a.append(')');

                    // should not throw as it is already tested OK
                    break;
            }
        }

        a.append(')');

        if (t.onCommitPreserve) {
            a.append(' ').append(Token.T_ON).append(' ');
            a.append(Token.T_COMMIT).append(' ').append(Token.T_PRESERVE);
            a.append(' ').append(Token.T_ROWS);
        }
    }

    /**
     * Generates the SET TABLE <tablename> SOURCE <string> statement for a
     * text table;
     */
    static String getDataSourceDDL(Table t) {

        String dataSource = t.getDataSource();

        if (dataSource == null) {
            return null;
        }

        boolean      isDesc = t.isDescDataSource();
        StringBuffer a      = new StringBuffer(128);

        a.append(Token.T_SET).append(' ').append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append(' ').append(Token.T_SOURCE).append(' ').append('"');
        a.append(dataSource);
        a.append('"');

        if (isDesc) {
            a.append(' ').append(Token.T_DESC);
        }

        return a.toString();
    }

    /**
     * Generates the SET TABLE <tablename> SOURCE HEADER <string> statement for a
     * text table;
     */
    private static String getDataSourceHeader(Table t) {

        String header = t.getHeader();

        if (header == null) {
            return null;
        }

        StringBuffer a = new StringBuffer(128);

        a.append(Token.T_SET).append(' ').append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append(' ').append(Token.T_SOURCE).append(' ');
        a.append(Token.T_HEADER).append(' ');
        a.append(header);

        return a.toString();
    }

    /**
     * Generates the column definitions for a table.
     */
    private static void getColumnList(Table t, int[] col, int len,
                                      StringBuffer a) {

        a.append('(');

        for (int i = 0; i < len; i++) {
            a.append(t.getColumn(col[i]).columnName.statementName);

            if (i < len - 1) {
                a.append(',');
            }
        }

        a.append(')');
    }

    /**
     * Generates the foreign key declaration for a given Constraint object.
     */
    private static void getFKStatement(Constraint c, StringBuffer a) {

        if (!c.getName().isReservedName()) {
            a.append(Token.T_CONSTRAINT).append(' ');
            a.append(c.getName().statementName);
            a.append(' ');
        }

        a.append(Token.T_FOREIGN).append(' ').append(Token.T_KEY);

        int[] col = c.getRefColumns();

        getColumnList(c.getRef(), col, col.length, a);
        a.append(' ').append(Token.T_REFERENCES).append(' ');
        a.append(c.getMain().getSchemaName().statementName).append('.');
        a.append(c.getMain().getName().statementName);

        col = c.getMainColumns();

        getColumnList(c.getMain(), col, col.length, a);

        if (c.getDeleteAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Token.T_ON).append(' ').append(
                Token.T_DELETE).append(' ');
            a.append(getFKAction(c.getDeleteAction()));
        }

        if (c.getUpdateAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Token.T_ON).append(' ').append(
                Token.T_UPDATE).append(' ');
            a.append(getFKAction(c.getUpdateAction()));
        }
    }

    /**
     * Returns the foreign key action rule.
     */
    private static String getFKAction(int action) {

        switch (action) {

            case Constraint.CASCADE :
                return Token.T_CASCADE;

            case Constraint.SET_DEFAULT :
                return Token.T_SET + ' ' + Token.T_DEFAULT;

            case Constraint.SET_NULL :
                return Token.T_SET + ' ' + Token.T_NULL;

            default :
                return Token.T_NO + ' ' + Token.T_ACTION;
        }
    }

    /**
     * Adds a script line to the result.
     */
    private static void addRow(Result r, String sql) {

        if (sql == null || sql.length() == 0) {
            return;
        }

        String[] s = new String[1];

        s[0] = sql;

        r.initialiseNavigator().add(s);
    }

    /**
     * Generates the GRANT statements for grantees.
     *
     * When views is true, generates rights for views only. Otherwise
     * generates rights for tables, sequences, and classes.
     *
     * Does not generate script for:
     *
     * grant on builtin classes to public
     * grant select on system tables
     *
     */
    private static void addRightsStatements(Database dDatabase, Result r) {

        GranteeManager gm       = dDatabase.getGranteeManager();
        Iterator       grantees = gm.getGrantees().iterator();

        // grantees has ALL Users and Roles, incl. hidden and reserved ones.
        // Therefore, we filter out the non-persisting ones.
        while (grantees.hasNext()) {
            Grantee g    = (Grantee) grantees.next();
            String  name = g.getName();

            // _SYSTEM user, DBA Role grants/revokes not persisted
            if (name.equals("_SYSTEM") || name.equals("DBA")) {
                continue;
            }

            HsqlArrayList list = g.getRightsDDL();

            for (int i = 0; i < list.size(); i++) {
                addRow(r, (String) list.get(i));
            }
        }
    }

    static String getSavepointDDL(String name) {

        StringBuffer sb = new StringBuffer(Token.T_SAVEPOINT);

        sb.append(' ').append('"').append(name).append('"');

        return sb.toString();
    }

    static String getSavepointRollbackDDL(String name) {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_ROLLBACK).append(' ').append(Token.T_TO).append(' ');
        sb.append(Token.T_SAVEPOINT).append(' ');
        sb.append('"').append(name).append('"');

        return sb.toString();
    }
}
