/* Copyright (c) 2001-2024, The HSQL Development Group
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

import java.util.concurrent.atomic.AtomicLong;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.rights.Grantee;

/**
 * Provides Name Management for SQL objects. <p>
 *
 * This class now includes the HsqlName class introduced in 1.7.1 and improves
 * auto-naming with multiple databases in the engine.<p>
 *
 * Methods check user defined names and issue system generated names
 * for SQL objects.<p>
 *
 * This class does not deal with the type of the SQL object for which it
 * is used.<p>
 *
 * Some names beginning with SYS_ are reserved for system generated names.
 * These are defined in isReserveName(String name) and created by the
 * makeAutoName(String type) factory method<p>
 *
 * sysNumber is used to generate system-generated names. It is
 * set to the largest integer encountered in names that use the
 * SYS_xxxxxxx_INTEGER format. As the DDL is processed before any ALTER
 * command, any new system generated name will have a larger integer suffix
 * than all the existing names.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.7.2
 */
public final class HsqlNameManager {

    private static final HsqlNameManager staticManager = new HsqlNameManager();

    static {
        staticManager.serialNumber.set(Long.MIN_VALUE);
    }

    private static final HsqlName[] autoColumnNames       = new HsqlName[32];
    private static final String[]   autoNoNameColumnNames = new String[32];

    static {
        for (int i = 0; i < autoColumnNames.length; i++) {
            autoColumnNames[i] = new HsqlName(
                staticManager,
                "C" + (i + 1),
                0,
                false);
            autoNoNameColumnNames[i] = String.valueOf(i);
        }
    }

    private AtomicLong serialNumber = new AtomicLong(1);     // 0 is reserved in lookups
    private AtomicLong sysNumber = new AtomicLong(10000);    // avoid name clash in older scripts
    private HsqlName catalogName;
    private boolean  sqlRegularNames;
    HsqlName         subqueryTableName;

    public HsqlNameManager() {
        sqlRegularNames = true;
    }

    public HsqlNameManager(Database database) {

        catalogName = new HsqlName(
            this,
            SqlInvariants.DEFAULT_CATALOG_NAME,
            SchemaObject.CATALOG,
            false);
        sqlRegularNames          = database.sqlRegularNames;
        subqueryTableName = new HsqlName(
            this,
            SqlInvariants.SYSTEM_SUBQUERY,
            false,
            SchemaObject.TABLE);
        subqueryTableName.schema = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;
    }

    public HsqlName getCatalogName() {
        return catalogName;
    }

    public void setSqlRegularNames(boolean value) {
        sqlRegularNames = value;
    }

    public static HsqlName newSystemObjectName(String name, int type) {
        return new HsqlName(staticManager, name, type, false);
    }

    public static HsqlName newInfoSchemaColumnName(
            String name,
            HsqlName table) {

        HsqlName hsqlName = new HsqlName(
            staticManager,
            name,
            false,
            SchemaObject.COLUMN);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        hsqlName.parent = table;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaTableName(String name) {

        HsqlName hsqlName = new HsqlName(
            staticManager,
            name,
            SchemaObject.TABLE,
            false);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaObjectName(
            String name,
            boolean isQuoted,
            int type) {

        HsqlName hsqlName = new HsqlName(staticManager, name, type, isQuoted);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public HsqlName newHsqlName(HsqlName schema, String name, int type) {

        HsqlName hsqlName = new HsqlName(this, name, type, false);

        hsqlName.schema = schema;

        return hsqlName;
    }

    //
    public HsqlName newHsqlName(String name, boolean isquoted, int type) {
        return new HsqlName(this, name, isquoted, type);
    }

    public HsqlName newHsqlName(
            HsqlName schema,
            String name,
            boolean isquoted,
            int type) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;

        return hsqlName;
    }

    public HsqlName newHsqlName(
            HsqlName schema,
            String name,
            boolean isquoted,
            int type,
            HsqlName parent) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;
        hsqlName.parent = parent;

        return hsqlName;
    }

    public HsqlName newColumnSchemaHsqlName(HsqlName table, SimpleName name) {
        return newColumnHsqlName(table, name.name, name.isNameQuoted);
    }

    public HsqlName newColumnHsqlName(
            HsqlName table,
            String name,
            boolean isquoted) {

        HsqlName hsqlName = new HsqlName(
            this,
            name,
            isquoted,
            SchemaObject.COLUMN);

        hsqlName.schema = table.schema;
        hsqlName.parent = table;

        return hsqlName;
    }

    /**
     * Same name string but different objects and serial number
     */
    public HsqlName getSubqueryTableName() {
        return subqueryTableName;
    }

    /**
     * Auto names are used for autogenerated indexes or anonymous constraints.
     */
    public HsqlName newAutoName(
            String prefix,
            HsqlName schema,
            HsqlName parent,
            int type) {
        return newAutoName(prefix, null, schema, parent, type);
    }

    public HsqlName newConstraintIndexName(
            HsqlName tableName,
            HsqlName constName,
            boolean useConstraint) {

        if (constName == null) {
            useConstraint = false;
        }

        if (useConstraint) {
            HsqlName name = new HsqlName(
                this,
                constName.name,
                SchemaObject.INDEX,
                false);

            name.schema = tableName.schema;
            name.parent = tableName;

            return name;
        } else {
            String constNameString = constName == null
                                     ? null
                                     : constName.name;

            return newAutoName(
                "IDX",
                constNameString,
                tableName.schema,
                tableName,
                SchemaObject.INDEX);
        }
    }

    public HsqlName newSpecificRoutineName(HsqlName name) {

        StringBuilder sb = new StringBuilder();

        sb.append(name.name).append('_').append(sysNumber.incrementAndGet());

        HsqlName hsqlName = new HsqlName(
            this,
            sb.toString(),
            SchemaObject.SPECIFIC_ROUTINE,
            name.isNameQuoted);

        hsqlName.parent = name;
        hsqlName.schema = name.schema;

        return hsqlName;
    }

    /**
     * Column index i is 0 based, returns 1 based numbered column.
     */
    public static HsqlName getAutoColumnName(int i) {

        if (i < autoColumnNames.length) {
            return autoColumnNames[i];
        }

        return new HsqlName(
            staticManager,
            "C" + (i + 1),
            SchemaObject.COLUMN,
            false);
    }

    public static String getAutoNoNameColumnString(int i) {

        if (i < autoColumnNames.length) {
            return autoNoNameColumnNames[i];
        }

        return String.valueOf(i);
    }

    public static String getAutoSavepointNameString(long i, int j) {

        StringBuilder sb = new StringBuilder();

        sb.append('S').append(i).append('_').append(j);

        return sb.toString();
    }

    /**
     * Auto names are used for autogenerated indexes or anonymous constraints.
     */
    public HsqlName newAutoName(
            String prefix,
            String namepart,
            HsqlName schema,
            HsqlName parent,
            int type) {

        StringBuilder sb = new StringBuilder();

        if (prefix != null) {
            if (!prefix.isEmpty()) {
                sb.append("SYS_").append(prefix).append('_');

                if (namepart != null) {
                    sb.append(namepart).append('_');
                }

                sb.append(sysNumber.incrementAndGet());
            }
        } else {
            sb.append(namepart);
        }

        HsqlName name = new HsqlName(this, sb.toString(), type, false);

        name.schema = schema;
        name.parent = parent;

        return name;
    }

    public static SimpleName getSimpleName(String name, boolean isNameQuoted) {
        return new SimpleName(name, isNameQuoted);
    }

    public static class SimpleName {

        public String  name;
        public boolean isNameQuoted;

        private SimpleName() {}

        private SimpleName(String name, boolean isNameQuoted) {
            this.name         = name;
            this.isNameQuoted = isNameQuoted;
        }

        public int hashCode() {
            return name.hashCode();
        }

        public boolean equals(Object other) {

            if (other instanceof SimpleName) {
                return ((SimpleName) other).name.equals(name);
            }

            return false;
        }

        public String getStatementName() {
            return isNameQuoted
                   ? StringConverter.toQuotedString(name, '"', true)
                   : name;
        }

        public String getNameString() {
            return name;
        }
    }

    public static final class HsqlName extends SimpleName {

        static HsqlName[] emptyArray = new HsqlName[]{};

        //
        HsqlNameManager  manager;
        public String    statementName;
        public String    comment;
        public HsqlName  schema;
        public HsqlName  parent;
        public Grantee   owner;
        public final int type;

        //
        private final long serialNumber;

        private HsqlName(HsqlNameManager man, int type) {
            manager      = man;
            this.type    = type;
            serialNumber = manager.serialNumber.getAndIncrement();
        }

        private HsqlName(
                HsqlNameManager man,
                String name,
                boolean isquoted,
                int type) {
            this(man, type);

            rename(name, isquoted);
        }

        /** for auto names and system-defined names */
        private HsqlName(
                HsqlNameManager man,
                String name,
                int type,
                boolean isQuoted) {

            this(man, type);

            this.name          = name;
            this.statementName = name;
            this.isNameQuoted  = isQuoted;

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"', true);
            }
        }

        public String getStatementName() {
            return statementName;
        }

        public String getSchemaQualifiedStatementName() {

            switch (type) {

                case SchemaObject.PARAMETER :
                case SchemaObject.VARIABLE : {
                    return statementName;
                }

                case SchemaObject.COLUMN :
                case SchemaObject.PERIOD : {
                    if (parent == null
                            || SqlInvariants.SYSTEM_SUBQUERY.equals(
                                parent.name)) {
                        return statementName;
                    }

                    StringBuilder sb = new StringBuilder();

                    if (schema != null) {
                        sb.append(schema.getStatementName()).append('.');
                    }

                    sb.append(parent.getStatementName())
                      .append('.')
                      .append(statementName);

                    return sb.toString();
                }
            }

            if (schema == null
                    || SqlInvariants.SYSTEM_SCHEMA.equals(schema.name)) {
                return statementName;
            }

            StringBuilder sb = new StringBuilder(64);

            sb.append(schema.getStatementName())
              .append('.')
              .append(statementName);

            return sb.toString();
        }

        String getCommentSQL(String typeName) {

            if (comment == null) {
                return null;
            }

            StringBuilder sb = new StringBuilder(64);

            sb.append(Tokens.T_COMMENT)
              .append(' ')
              .append(Tokens.T_ON)
              .append(' ')
              .append(typeName)
              .append(' ')
              .append(getSchemaQualifiedStatementName())
              .append(' ')
              .append(Tokens.T_IS)
              .append(' ')
              .append(StringConverter.toQuotedString(comment, '\'', true));

            return sb.toString();
        }

        public void rename(HsqlName name) {
            rename(name.name, name.isNameQuoted);
        }

        public void rename(String name, boolean isquoted) {

            if (manager.sqlRegularNames && name.length() > 128) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            // get rid of the excess
            this.name          = name;
            this.statementName = this.name;
            this.isNameQuoted  = isquoted;

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"', true);
            }

            if (name.startsWith("SYS_")) {
                int length = name.lastIndexOf('_') + 1;

                try {
                    int temp = Integer.parseInt(name.substring(length));

                    if (temp > manager.sysNumber.get()) {
                        manager.sysNumber.set(temp);
                    }
                } catch (NumberFormatException e) {}
            }
        }

        public void setSchemaIfNull(HsqlName schema) {
            if (this.schema == null) {
                this.schema = schema;
            }
        }

        public boolean equals(Object other) {

            if (other instanceof HsqlName) {
                return serialNumber == ((HsqlName) other).serialNumber;
            } else if (other instanceof SimpleName) {
                return super.equals(other);
            }

            return false;
        }

        /**
         * hash code for this object is based on its unique serial number.
         */
        public int hashCode() {
            return (int) (serialNumber ^ (serialNumber >>> 32));
        }

        public long getSerialNumber() {
            return serialNumber;
        }

        /**
         * "SYS_IDX_" is used for auto-indexes on referring FK columns or
         * unique constraints.
         * "SYS_PK_" is for the primary key constraints
         * "SYS_CT_" is for unique and check constraints
         * "SYS_REF_" is for FK constraints in referenced tables
         * "SYS_FK_" is for FK constraints in referencing tables
         *
         */
        static final String[] sysPrefixes = new String[]{ "SYS_IDX_", "SYS_PK_",
                "SYS_REF_", "SYS_CT_", "SYS_FK_", };

        static int sysPrefixLength(String name) {

            for (int i = 0; i < sysPrefixes.length; i++) {
                if (name.startsWith(sysPrefixes[i])) {
                    return sysPrefixes[i].length();
                }
            }

            return 0;
        }

        static boolean isReservedName(String name) {
            return sysPrefixLength(name) > 0;
        }

        boolean isReservedName() {
            return isReservedName(name);
        }

        public String toString() {

            return getClass().getName() + super.hashCode() + "[serialNumber="
                   + this.serialNumber + ", name=" + name
                   + ", name.hashCode()=" + name.hashCode() + ", isNameQuoted="
                   + isNameQuoted + "]";
        }

        /**
         * Returns true if the identifier consists of all uppercase letters
         * digits and underscore, beginning with a letter and is not in the
         * keyword list.
         */
        static boolean isRegularIdentifier(String name) {

            for (int i = 0, length = name.length(); i < length; i++) {
                int c = name.charAt(i);

                if (c >= 'A' && c <= 'Z') {
                    continue;
                } else if (c == '_' && i > 0) {
                    continue;
                } else if (c >= '0' && c <= '9') {
                    continue;
                }

                return false;
            }

            return !Tokens.isKeyword(name);
        }
    }
}
