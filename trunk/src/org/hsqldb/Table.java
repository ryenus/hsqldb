/* Copyright (c) 2001-2021, The HSQL Development Group
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
import org.hsqldb.index.Index;
import org.hsqldb.index.Index.IndexUse;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.map.ValuePool;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorDataChange;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.DataSpaceManager;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.trigger.Trigger;
import org.hsqldb.types.Collation;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

/**
 * Holds the data structures and methods for creation of a named database table.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.6.1
 */
public class Table extends TableBase implements SchemaObject {

    public static final Table[] emptyArray = new Table[]{};

    // main properties
    protected HsqlName tableName;
    protected long     changeTimestamp;

    //
    public OrderedHashMap columnList;              // columns in table
    int                   identityColumn;          // -1 means no such column
    NumberSequence        identitySequence;        // next value of identity column

// -----------------------------------------------------------------------
    Constraint[]            constraintList;        // constraint for the table
    Constraint[]            fkConstraints;         //
    Constraint[]            fkMainConstraints;
    Constraint[]            checkConstraints;
    TriggerDef[]            triggerList;
    TriggerDef[][]          triggerLists;          // array of trigger lists
    Expression[]            colDefaults;           // expressions of DEFAULT values
    private boolean         hasDefaultValues;      // shortcut for above
    boolean[]               colGenerated;          // generated columns
    private boolean         hasGeneratedValues;    // shortcut for above
    boolean[]               colUpdated;            // auto update columns
    private boolean         hasUpdatedValues;      // shortcut for above
    boolean[]               colRefFK;              // foreign key columns
    boolean[]               colMainFK;             // columns referenced by foreign key
    int                     referentialActions;    // has set null, set default or cascade
    int                     cascadingDeletes;      // has on delete cascade
    boolean                 isDropped;             // has been dropped
    private boolean         hasDomainColumns;      // shortcut
    private boolean         hasNotNullColumns;     // shortcut
    protected int[]         defaultColumnMap;      // holding 0,1,2,3,...
    private RangeVariable[] defaultRanges;

    //
    PeriodDefinition systemPeriod;
    PeriodDefinition applicationPeriod;
    int              systemPeriodStartColumn;
    int              systemPeriodEndColumn;

    //
    public Table(Database database, HsqlName name, int type) {

        this.database      = database;
        this.tableName     = name;
        this.persistenceId = database.persistentStoreCollection.getNextId();

        switch (type) {

            case CHANGE_SET_TABLE :
                persistenceScope = SCOPE_STATEMENT;
                isSessionBased   = true;
                break;

            case SYSTEM_SUBQUERY :
                persistenceScope = SCOPE_STATEMENT;
                isSessionBased   = true;
                break;

            case INFO_SCHEMA_TABLE :
                persistenceScope = SCOPE_TRANSACTION;
                isSessionBased   = true;
                break;

            case SYSTEM_TABLE :
                persistenceScope = SCOPE_FULL;
                isSchemaBased    = true;
                break;

            case CACHED_TABLE :
                persistenceScope = SCOPE_FULL;
                isSchemaBased    = true;

                if (database.logger.isFileDatabase()) {
                    isCached = true;
                    isLogged = !database.isFilesReadOnly();
                } else {
                    type = MEMORY_TABLE;
                }
                break;

            case MEMORY_TABLE :
                persistenceScope = SCOPE_FULL;
                isSchemaBased    = true;
                isLogged         = !database.isFilesReadOnly();
                break;

            case TEMP_TABLE :
                persistenceScope = SCOPE_TRANSACTION;
                isTemp           = true;
                isSchemaBased    = true;
                isSessionBased   = true;
                break;

            case TEMP_TEXT_TABLE :
                persistenceScope = SCOPE_SESSION;

                if (!database.logger.isFileDatabase()) {
                    throw Error.error(ErrorCode.DATABASE_IS_MEMORY_ONLY);
                }

                isSchemaBased  = true;
                isSessionBased = true;
                isTemp         = true;
                isText         = true;
                isReadOnly     = true;
                break;

            case TEXT_TABLE :
                persistenceScope = SCOPE_FULL;

                if (!database.logger.isFileDatabase()) {
                    if (!database.logger.isAllowedFullPath()) {
                        throw Error.error(ErrorCode.DATABASE_IS_MEMORY_ONLY);
                    }

                    isReadOnly = true;
                }

                isSchemaBased = true;
                isText        = true;
                break;

            case VIEW_TABLE :
                persistenceScope = SCOPE_STATEMENT;
                isSchemaBased    = true;
                isSessionBased   = true;
                isView           = true;
                break;

            case MODULE_TABLE :
            case RESULT_TABLE :
                persistenceScope = SCOPE_SESSION;
                isSessionBased   = true;
                break;

            case TableBase.FUNCTION_TABLE :
                persistenceScope = SCOPE_STATEMENT;
                isSessionBased   = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        // type may have changed above for CACHED tables
        tableType         = type;
        identityColumn    = -1;
        columnList        = new OrderedHashMap();
        indexList         = Index.emptyArray;
        constraintList    = Constraint.emptyArray;
        fkConstraints     = Constraint.emptyArray;
        fkMainConstraints = Constraint.emptyArray;
        checkConstraints  = Constraint.emptyArray;
        triggerList       = TriggerDef.emptyArray;
        triggerLists      = new TriggerDef[TriggerDef.NUM_TRIGS][];

        for (int i = 0; i < TriggerDef.NUM_TRIGS; i++) {
            triggerLists[i] = TriggerDef.emptyArray;
        }

        if (database.isFilesReadOnly() && isFileBased()) {
            this.isReadOnly = true;
        }
    }

    /** trigger transition table */
    public Table(Table table, HsqlName name) {

        persistenceScope    = SCOPE_STATEMENT;
        name.schema         = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;
        this.tableName      = name;
        this.database       = table.database;
        this.tableType      = RESULT_TABLE;
        this.columnList     = table.columnList;
        this.columnCount    = table.columnCount;
        this.indexList      = Index.emptyArray;
        this.constraintList = Constraint.emptyArray;

        createPrimaryKey();
    }

    public int getType() {
        return SchemaObject.TABLE;
    }

    /**
     *  Returns the HsqlName object fo the table
     */
    public final HsqlName getName() {
        return tableName;
    }

    /**
     * Returns the schema name.
     */
    public HsqlName getSchemaName() {
        return tableName.schema;
    }

    /**
     * Returns the catalog name
     */
    public HsqlName getCatalogName() {
        return database.getCatalogName();
    }

    public Grantee getOwner() {
        return tableName.schema.owner;
    }

    public OrderedHashSet getReferences() {

        OrderedHashSet set = new OrderedHashSet();

        if (identitySequence != null && identitySequence.getName() != null) {
            set.add(identitySequence.getName());
        }

        return set;
    }

    public OrderedHashSet getReferencesForScript() {

        OrderedHashSet set = getReferences();

        for (int i = 0; i < colTypes.length; i++) {
            ColumnSchema   column = getColumn(i);
            OrderedHashSet refs   = column.getReferences();

            if (refs != null && !refs.isEmpty()) {
                set.addAll(refs);
            }
        }

        return set;
    }

    public OrderedHashSet getReferencesForDependents() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < colTypes.length; i++) {
            ColumnSchema   column = getColumn(i);
            OrderedHashSet refs   = column.getReferences();

            if (refs != null && !refs.isEmpty()) {
                set.add(column.getName());
            }
        }

        for (int i = 0; i < fkConstraints.length; i++) {
            if (fkConstraints[i].getMainTableName() != getName()) {
                set.add(fkConstraints[i].getName());
            }
        }

        for (int i = 0; i < triggerList.length; i++) {
            set.add(triggerList[i].getName());
        }

        return set;
    }

    public OrderedHashSet getComponents() {

        OrderedHashSet set = new OrderedHashSet();

        set.addAll(constraintList);
        set.addAll(triggerList);

        for (int i = 0; i < indexList.length; i++) {
            if (!indexList[i].isConstraint()) {
                set.add(indexList[i]);
            }
        }

        return set;
    }

    public void compile(Session session, SchemaObject parentObject) {

        for (int i = 0; i < columnCount; i++) {
            ColumnSchema column = getColumn(i);

            column.compile(session, this);
        }
    }

    public String getSQL() {

        StringBuilder sb = new StringBuilder();

        sb.append(Tokens.T_CREATE).append(' ');

        if (isTemp()) {
            sb.append(Tokens.T_GLOBAL).append(' ');
            sb.append(Tokens.T_TEMPORARY).append(' ');
        } else if (isText()) {
            sb.append(Tokens.T_TEXT).append(' ');
        } else if (isCached()) {
            sb.append(Tokens.T_CACHED).append(' ');
        } else {
            sb.append(Tokens.T_MEMORY).append(' ');
        }

        sb.append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append('(');

        int[]      pk      = getPrimaryKey();
        Constraint pkConst = getPrimaryConstraint();

        for (int j = 0; j < columnCount; j++) {
            ColumnSchema column  = getColumn(j);
            String       colname = column.getName().statementName;
            Type         type    = column.getDataType();

            if (j > 0) {
                sb.append(',');
            }

            sb.append(colname);
            sb.append(' ');
            sb.append(type.getTypeDefinition());

            if (!type.isDistinctType() && !type.isDomainType()) {
                if (type.isCharacterType()) {
                    Collation collation = type.getCollation();

                    if (collation.isObjectCollation()) {
                        sb.append(' ').append(collation.getCollateSQL());
                    }
                }
            }

            String defaultString = column.getDefaultSQL();

            if (defaultString != null) {
                sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                sb.append(defaultString);
            }

            if (column.isAutoUpdate()) {
                sb.append(' ').append(Tokens.T_ON).append(' ');
                sb.append(Tokens.T_UPDATE).append(' ');
                sb.append(column.getUpdateExpression().getSQL());
            }

            if (column.isIdentity()) {
                sb.append(' ').append(
                    column.getIdentitySequence().getSQLColumnDefinition());
            }

            if (column.isGenerated()) {
                sb.append(' ').append(Tokens.T_GENERATED).append(' ');
                sb.append(Tokens.T_ALWAYS).append(' ').append(
                    Tokens.T_AS).append(Tokens.T_OPENBRACKET);
                sb.append(column.getGeneratingExpression().getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);
            }

            if (column.getSystemPeriodType()
                    == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START) {
                sb.append(' ').append(Tokens.T_GENERATED).append(' ');
                sb.append(Tokens.T_ALWAYS).append(' ');
                sb.append(Tokens.T_AS).append(' ');
                sb.append(Tokens.T_ROW).append(' ').append(Tokens.T_START);
            } else if (column.getSystemPeriodType()
                       == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END) {
                sb.append(' ').append(Tokens.T_GENERATED).append(' ');
                sb.append(Tokens.T_ALWAYS).append(' ');
                sb.append(Tokens.T_AS).append(' ');
                sb.append(Tokens.T_ROW).append(' ').append(Tokens.T_END);
            }

            if (!column.isNullable()) {
                Constraint c = getNotNullConstraintForColumn(j);

                if (c != null && !c.getName().isReservedName()) {
                    sb.append(' ').append(Tokens.T_CONSTRAINT).append(
                        ' ').append(c.getName().statementName);
                }

                sb.append(' ').append(Tokens.T_NOT).append(' ').append(
                    Tokens.T_NULL);
            }

            if (pk.length == 1 && j == pk[0]
                    && pkConst.getName().isReservedName()) {
                sb.append(' ').append(Tokens.T_PRIMARY).append(' ').append(
                    Tokens.T_KEY);
            }
        }

        if (systemPeriod != null) {
            sb.append(',');
            sb.append(Tokens.T_PERIOD).append(' ');
            sb.append(Tokens.T_FOR).append(' ').append(Tokens.T_SYSTEM_TIME);
            sb.append('(');
            sb.append(systemPeriod.getStartColumn().getName().statementName);
            sb.append(',');
            sb.append(systemPeriod.getEndColumn().getName().statementName);
            sb.append(')');
        }

        if (applicationPeriod != null) {
            sb.append(',');
            sb.append(Tokens.T_PERIOD).append(' ');
            sb.append(Tokens.T_FOR).append(' ');
            sb.append(applicationPeriod.getName().statementName);
            sb.append('(');
            sb.append(
                applicationPeriod.getStartColumn().getName().statementName);
            sb.append(',');
            sb.append(
                applicationPeriod.getEndColumn().getName().statementName);
            sb.append(')');
        }

        Constraint[] constraintList = getConstraints();

        for (int j = 0, vSize = constraintList.length; j < vSize; j++) {
            Constraint c = constraintList[j];

            if (!c.isForward) {
                String d = c.getSQL();

                if (d.length() > 0) {
                    sb.append(',');
                    sb.append(d);
                }
            }
        }

        sb.append(')');

        if (onCommitPreserve()) {
            sb.append(' ').append(Tokens.T_ON).append(' ');
            sb.append(Tokens.T_COMMIT).append(' ').append(Tokens.T_PRESERVE);
            sb.append(' ').append(Tokens.T_ROWS);
        }

        if (isSystemVersioned) {
            sb.append(' ').append(Tokens.T_WITH).append(' ');
            sb.append(Tokens.T_SYSTEM).append(' ').append(Tokens.T_VERSIONING);
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public RangeVariable[] getDefaultRanges() {

        if (defaultRanges == null) {
            defaultRanges = new RangeVariable[]{ new RangeVariable(this, 0) };
        }

        return defaultRanges;
    }

    public final void setName(HsqlName name) {
        tableName = name;
    }

    String[] getSQL(OrderedHashSet resolved, OrderedHashSet unresolved) {

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (c.isForward) {
                unresolved.add(c);
            } else if (c.getConstraintType() == SchemaObject.ConstraintTypes
                    .UNIQUE || c.getConstraintType() == SchemaObject
                    .ConstraintTypes.PRIMARY_KEY) {
                resolved.add(c.getName());
            }
        }

        HsqlArrayList list = new HsqlArrayList();

        list.add(getSQL());

        if (!isTemp && !isText && identitySequence != null
                && identitySequence.getName() == null) {
            list.add(NumberSequence.getRestartSQL(this));
        }

        for (int i = 0; i < indexList.length; i++) {
            if (!indexList[i].isConstraint()
                    && indexList[i].getColumnCount() > 0) {
                list.add(indexList[i].getSQL());
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String getSQLForReadOnly() {

        if (isReadOnly) {
            StringBuilder sb = new StringBuilder(64);

            sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(
                ' ');
            sb.append(getName().getSchemaQualifiedStatementName());
            sb.append(' ').append(Tokens.T_READ).append(' ');
            sb.append(Tokens.T_ONLY);

            return sb.toString();
        } else {
            return null;
        }
    }

    public String[] getSQLForTextSource(boolean withHeader) {

        // readonly for TEXT tables only
        if (isText()) {
            HsqlArrayList list = new HsqlArrayList();

            if (isReadOnly) {
                list.add(getSQLForReadOnly());
            }

            // data source
            String dataSource = ((TextTable) this).getDataSourceDDL();

            if (dataSource != null) {
                list.add(dataSource);
            }

            // header
            String header = ((TextTable) this).getDataSourceHeader();

            if (withHeader && header != null && !isReadOnly) {
                list.add(header);
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } else {
            return null;
        }
    }

    public String getSQLForClustered() {

        if (!isCached() && !isText()) {
            return null;
        }

        Index index = getClusteredIndex();

        if (index == null) {
            return null;
        }

        String colList = getColumnListSQL(index.getColumns(),
                                          index.getColumnCount());
        StringBuilder sb = new StringBuilder(64);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_CLUSTERED).append(' ');
        sb.append(Tokens.T_ON).append(' ').append(colList);

        return sb.toString();
    }

    public String getSQLForTableSpace() {

        if (!isCached() || tableSpace == DataSpaceManager.tableIdDefault) {
            return null;
        }

        StringBuilder sb = new StringBuilder(64);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SPACE).append(' ').append(tableSpace);

        return sb.toString();
    }

    public String[] getTriggerSQL() {

        HsqlArrayList list = new HsqlArrayList();

        for (int i = 0; i < triggerList.length; i++) {
            if (!triggerList[i].isSystem()) {
                list.add(triggerList[i].getSQL());
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String getIndexRootsSQL(long[] roots) {

        StringBuilder sb = new StringBuilder(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
        sb.append(Tokens.T_INDEX).append(' ');
        sb.append('\'');
        sb.append(StringUtil.getList(roots, " ", ""));
        sb.append(' ');
        sb.append(StringUtil.getList(new long[indexList.length], " ", ""));
        sb.append(' ').append(store.elementCount());
        sb.append('\'');

        return sb.toString();
    }

    public String getColumnListSQL(int[] col, int len) {

        StringBuilder sb = new StringBuilder();

        sb.append('(');

        for (int i = 0; i < len; i++) {
            sb.append(getColumn(col[i]).getName().statementName);

            if (i < len - 1) {
                sb.append(',');
            }
        }

        sb.append(')');

        return sb.toString();
    }

    public String getColumnListWithTypeSQL() {

        StringBuilder sb = new StringBuilder();

        sb.append('(');

        for (int j = 0; j < columnCount; j++) {
            ColumnSchema column  = getColumn(j);
            String       colname = column.getName().statementName;
            Type         type    = column.getDataType();

            if (j > 0) {
                sb.append(',');
            }

            sb.append(colname);
            sb.append(' ');
            sb.append(type.getTypeDefinition());
        }

        sb.append(')');

        return sb.toString();
    }

    public void setForwardConstraints(OrderedHashSet resolved) {

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType()
                    == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                Table mainTable = c.getMain();

                if (mainTable != this
                        && !resolved.contains(mainTable.getName())) {
                    c.isForward = true;
                }
            }
        }
    }

    public boolean isConnected() {
        return true;
    }

    /**
     * compares two full table rows based on a set of columns
     *
     * @param a a full row
     * @param b a full row
     * @param cols array of column indexes to compare
     * @param coltypes array of column types for the full row
     *
     * @return comparison result, -1,0,+1
     */
    public static int compareRows(Session session, Object[] a, Object[] b,
                                  int[] cols, Type[] coltypes) {

        int fieldcount = cols.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = coltypes[cols[j]].compare(session, a[cols[j]], b[cols[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Used to create row id's
     */
    public int getId() {
        return tableName.hashCode();
    }

    public String getTableTypeString() {

        switch (tableType) {

            case TableBase.MEMORY_TABLE :
                return Tokens.T_MEMORY;

            case TableBase.CACHED_TABLE :
                return Tokens.T_CACHED;

            case TableBase.TEXT_TABLE :
                return Tokens.T_TEXT;

            case TableBase.MODULE_TABLE :
                return Tokens.T_MODULE;

            case TableBase.FUNCTION_TABLE :
                return Tokens.T_FUNCTION;

            case TableBase.INFO_SCHEMA_TABLE :
            case TableBase.VIEW_TABLE :
                return Tokens.T_VIEW;

            case TableBase.TEMP_TABLE :
                return Tokens.T_TEMP;

            case TableBase.SYSTEM_SUBQUERY :
            default :
                return "SUBQUERY";
        }
    }

    public final boolean isSchemaBaseTable() {

        switch (tableType) {

            case TableBase.MEMORY_TABLE :
            case TableBase.CACHED_TABLE :
            case TableBase.TEXT_TABLE :
                return true;

            default :
                return false;
        }
    }

    public final boolean isWithDataSource() {
        return isWithDataSource;
    }

    public final boolean isText() {
        return isText;
    }

    public final boolean isTemp() {
        return isTemp;
    }

    public final boolean isReadOnly() {
        return isReadOnly;
    }

    public final boolean isView() {
        return isView;
    }

    public boolean isQueryBased() {
        return false;
    }

    public boolean isCached() {
        return isCached;
    }

    public boolean isDataReadOnly() {
        return isReadOnly;
    }

    public boolean isDropped() {
        return isDropped;
    }

    /**
     * returns false if the table has to be recreated in order to add / drop
     * indexes. Only CACHED tables return false.
     */
    final boolean isIndexingMutable() {
        return !isCached;
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations
     */
    public void checkDataReadOnly() {

        if (isDataReadOnly()) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }
    }

// ----------------------------------------------------------------------------
// akede@users - 1.7.2 patch Files readonly
    public void setDataReadOnly(boolean value) {

        // Changing the Read-Only mode for the table is only allowed if the
        // the database can realize it.
        if (!value) {
            if (database.isFilesReadOnly() && isFileBased()) {
                throw Error.error(ErrorCode.DATA_IS_READONLY);
            } else if (database.getType() == DatabaseType.DB_MEM && isText) {
                throw Error.error(ErrorCode.DATA_IS_READONLY);
            }
        }

        isReadOnly = value;
    }

    /**
     * Text or Cached Tables are normally file based
     */
    public boolean isFileBased() {
        return isCached || isText;
    }

    /**
     *  Adds a constraint.
     */
    public void addConstraint(Constraint c) {

        int index = c.getConstraintType()
                    == SchemaObject.ConstraintTypes.PRIMARY_KEY ? 0
                                                                : constraintList
                                                                    .length;

        constraintList =
            (Constraint[]) ArrayUtil.toAdjustedArray(constraintList, c, index,
                1);

        updateConstraintLists();
    }

    void updateConstraintLists() {

        int fkCount    = 0;
        int mainCount  = 0;
        int checkCount = 0;

        referentialActions = 0;
        cascadingDeletes   = 0;

        for (int i = 0; i < constraintList.length; i++) {
            switch (constraintList[i].getConstraintType()) {

                case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                    fkCount++;
                    break;

                case SchemaObject.ConstraintTypes.MAIN :
                    mainCount++;
                    break;

                case SchemaObject.ConstraintTypes.CHECK :
                    if (constraintList[i].isNotNull()) {
                        break;
                    }

                    checkCount++;
                    break;
            }
        }

        fkConstraints     = fkCount == 0 ? Constraint.emptyArray
                                         : new Constraint[fkCount];
        fkCount           = 0;
        fkMainConstraints = mainCount == 0 ? Constraint.emptyArray
                                           : new Constraint[mainCount];
        mainCount         = 0;
        checkConstraints  = checkCount == 0 ? Constraint.emptyArray
                                            : new Constraint[checkCount];
        checkCount        = 0;
        colRefFK          = new boolean[columnCount];
        colMainFK         = new boolean[columnCount];

        for (int i = 0; i < constraintList.length; i++) {
            switch (constraintList[i].getConstraintType()) {

                case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                    fkConstraints[fkCount] = constraintList[i];

                    ArrayUtil.intIndexesToBooleanArray(
                        constraintList[i].getRefColumns(), colRefFK);

                    fkCount++;
                    break;

                case SchemaObject.ConstraintTypes.MAIN :
                    fkMainConstraints[mainCount] = constraintList[i];

                    ArrayUtil.intIndexesToBooleanArray(
                        constraintList[i].getMainColumns(), colMainFK);

                    if (constraintList[i].hasCoreTriggeredAction()) {
                        referentialActions++;

                        if (constraintList[i].getDeleteAction()
                                == SchemaObject.ReferentialAction.CASCADE) {
                            cascadingDeletes++;
                        }
                    }

                    mainCount++;
                    break;

                case SchemaObject.ConstraintTypes.CHECK :
                    if (constraintList[i].isNotNull()) {
                        break;
                    }

                    checkConstraints[checkCount] = constraintList[i];

                    checkCount++;
                    break;
            }
        }
    }

    void verifyConstraintsIntegrity() {

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == SchemaObject.ConstraintTypes
                    .FOREIGN_KEY || c.getConstraintType() == SchemaObject
                    .ConstraintTypes.MAIN) {
                if (c.getMain()
                        != database.schemaManager.findUserTable(
                            c.getMain().getName().name,
                            c.getMain().getName().schema.name)) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "FK mismatch : "
                                             + c.getName().name);
                }

                if (c.getRef()
                        != database.schemaManager.findUserTable(
                            c.getRef().getName().name,
                            c.getRef().getName().schema.name)) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "FK mismatch : "
                                             + c.getName().name);
                }
            }
        }
    }

    /**
     *  Returns the list of constraints.
     */
    public Constraint[] getConstraints() {
        return constraintList;
    }

    /**
     *  Returns the list of FK constraints.
     */
    public Constraint[] getFKConstraints() {
        return fkConstraints;
    }

    /**
     *  Returns the primary constraint.
     */
    public Constraint getPrimaryConstraint() {
        return hasPrimaryKey() ? constraintList[0]
                               : null;
    }

    /** columnMap is null for deletes */
    void collectFKReadLocks(int[] columnMap, OrderedHashSet set) {

        for (int i = 0; i < fkMainConstraints.length; i++) {
            Constraint constraint  = fkMainConstraints[i];
            Table      ref         = constraint.getRef();
            int[]      mainColumns = constraint.getMainColumns();

            if (ref == this) {
                continue;
            }

            if (columnMap == null) {
                if (constraint.core.hasDeleteAction) {
                    int[] cols =
                        constraint.getDeleteAction()
                        == SchemaObject.ReferentialAction.CASCADE ? null
                                                                  : constraint
                                                                      .getRefColumns();

                    if (set.add(ref.getName())) {
                        ref.collectFKReadLocks(cols, set);
                    }
                }
            } else if (ArrayUtil.haveCommonElement(columnMap, mainColumns)) {
                if (set.add(ref.getName())) {
                    ref.collectFKReadLocks(constraint.getRefColumns(), set);
                }
            }
        }
    }

    /** columnMap is null for deletes */
    void collectFKWriteLocks(int[] columnMap, OrderedHashSet set) {

        for (int i = 0; i < fkMainConstraints.length; i++) {
            Constraint constraint  = fkMainConstraints[i];
            Table      ref         = constraint.getRef();
            int[]      mainColumns = constraint.getMainColumns();

            if (ref == this) {
                continue;
            }

            if (columnMap == null) {
                if (constraint.core.hasDeleteAction) {
                    int[] cols =
                        constraint.getDeleteAction()
                        == SchemaObject.ReferentialAction.CASCADE ? null
                                                                  : constraint
                                                                      .getRefColumns();

                    if (set.add(ref.getName())) {
                        ref.collectFKWriteLocks(cols, set);
                    }
                }
            } else if (ArrayUtil.haveCommonElement(columnMap, mainColumns)) {
                if (constraint.core.hasUpdateAction) {
                    if (set.add(ref.getName())) {
                        ref.collectFKWriteLocks(constraint.getRefColumns(),
                                                set);
                    }
                }
            }
        }
    }

    Constraint getNotNullConstraintForColumn(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isNotNull() && c.notNullColumnIndex == colIndex) {
                return c;
            }
        }

        return null;
    }

    /**
     * Returns the UNIQUE or PK constraint with the given column signature.
     */
    Constraint getUniqueConstraintForColumns(int[] cols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isUniqueWithColumns(cols)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns any foreign key constraint equivalent to the column sets
     */
    Constraint getFKConstraintForColumns(Table tableMain, int[] mainCols,
                                         int[] refCols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isEquivalent(tableMain, mainCols, this, refCols)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns any unique Constraint using this index
     */
    public Constraint getUniqueOrPKConstraintForIndex(Index index) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getMainIndex() == index && (c
                    .getConstraintType() == SchemaObject.ConstraintTypes
                    .UNIQUE || c.getConstraintType() == SchemaObject
                    .ConstraintTypes.PRIMARY_KEY)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns the next constraint of a given type
     */
    int getNextConstraintIndex(int from, int type) {

        for (int i = from, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == type) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  Performs the table level checks and adds a column to the table at the
     *  DDL level. Only used at table creation, not at alter column.
     */
    public void addColumn(ColumnSchema column) {

        String name = column.getName().name;

        if (findColumn(name) >= 0) {
            throw Error.error(ErrorCode.X_42504, name);
        }

        if (column.isIdentity()) {
            if (identityColumn != -1) {
                throw Error.error(ErrorCode.X_42525, name);
            }

            identityColumn   = columnCount;
            identitySequence = column.getIdentitySequence();
        }

        addColumnNoCheck(column);
    }

    public void addColumnNoCheck(ColumnSchema column) {

        columnList.add(column.getName().name, column);

        columnCount++;
    }

    public boolean hasGeneratedColumn() {
        return hasGeneratedValues;
    }

    public boolean hasUpdatedColumn() {
        return hasUpdatedValues;
    }

    public boolean hasLobColumn() {
        return hasLobColumn;
    }

    public boolean hasIdentityColumn() {
        return identityColumn != -1;
    }

    public PeriodDefinition getSystemPeriod() {
        return systemPeriod;
    }

    public int getSystemPeriodStartIndex() {
        return systemPeriodStartColumn;
    }

    public int getSystemPeriodEndIndex() {
        return systemPeriodEndColumn;
    }

    public PeriodDefinition getApplicationPeriod() {
        return applicationPeriod;
    }

    /**
     * Match two valid, equal length, columns arrays for type of columns for
     * referential constraints
     *
     * @param col column array from this Table
     * @param other the other Table object
     * @param othercol column array from the other Table
     */
    void checkReferentialColumnsMatch(int[] col, Table other, int[] othercol) {

        for (int i = 0; i < col.length; i++) {
            Type type      = colTypes[col[i]];
            Type otherType = other.colTypes[othercol[i]];

            if (!type.canCompareDirect(otherType)) {
                throw Error.error(ErrorCode.X_42562);
            }
        }
    }

    /**
     * For removal or addition of columns, constraints and indexes
     *
     * HsqlName objects are used from the old tables but no object is reused.
     *
     * Does not work in this form for FK's as Constraint.ConstraintCore
     * is not transferred to a referencing or referenced table
     */
    Table moveDefinition(Session session, int newType, ColumnSchema[] columns,
                         Constraint constraint, Index index, int[] colIndex,
                         int adjust, OrderedHashSet dropConstraints,
                         OrderedHashSet dropIndexes) {

        boolean newPK = false;

        if (constraint != null
                && constraint.getConstraintType()
                   == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
            newPK = true;
        }

        Table tn;

        if (isText) {
            tn = new TextTable(database, tableName, newType);
            ((TextTable) tn).dataSource  = ((TextTable) this).dataSource;
            ((TextTable) tn).isReversed  = ((TextTable) this).isReversed;
            ((TextTable) tn).isConnected = ((TextTable) this).isConnected;
        } else {
            tn = new Table(database, tableName, newType);
        }

        tn.systemPeriod      = systemPeriod;
        tn.applicationPeriod = applicationPeriod;
        tn.isSystemVersioned = isSystemVersioned;

        if (isTemp) {
            tn.persistenceScope = persistenceScope;
        }

        tn.tableSpace = tableSpace;

        for (int i = 0; i < columnCount; i++) {
            int pos = ArrayUtil.find(colIndex, i);

            if (pos >= 0) {
                if (adjust < 0) {
                    continue;
                } else if (adjust == 0) {
                    if (columns.length != 0) {
                        tn.addColumn(columns[pos]);

                        continue;
                    }
                } else {
                    if (columns.length != 0) {
                        tn.addColumn(columns[pos]);
                    }
                }
            }

            ColumnSchema col = (ColumnSchema) columnList.get(i);

            col = col.duplicate();

            col.setPrimaryKey(false);
            tn.addColumn(col);
        }

        // single column added anywhere or two or more columns addded at the end
        int count = ArrayUtil.countSmallerElements(colIndex, columnCount);

        for (int i = count; i < colIndex.length; i++) {
            tn.addColumn(columns[i]);
        }

        int[]    pkCols    = null;
        HsqlName indexName = getIndex(0).getName();

        if (hasPrimaryKey()
                && !dropConstraints.contains(
                    getPrimaryConstraint().getName())) {
            pkCols = getPrimaryKey();
            pkCols = ArrayUtil.toAdjustedColumnArray(pkCols, colIndex, adjust);
        } else if (newPK) {
            pkCols = constraint.getMainColumns();
            indexName =
                session.database.nameManager.newConstraintIndexName(tableName,
                    constraint.getName(), session.database.sqlSysIndexNames);
        }

        tn.createPrimaryKey(indexName, pkCols, false);

        for (int i = 1; i < indexList.length; i++) {
            Index idx = indexList[i];

            if (dropIndexes.contains(idx.getName())) {
                continue;
            }

            int[] colarr = ArrayUtil.toAdjustedColumnArray(idx.getColumns(),
                colIndex, adjust);
            Index newIdx = tn.createIndexStructure(idx.getName(), colarr,
                                                   idx.getColumnDesc(), null,
                                                   false, idx.isUnique(),
                                                   idx.isConstraint(),
                                                   idx.isForward());

            newIdx.setClustered(idx.isClustered());
            tn.addIndexStructure(newIdx);
        }

        if (index != null) {
            index.setTable(tn);
            tn.addIndexStructure(index);
        }

        HsqlArrayList newList = new HsqlArrayList();

        if (newPK) {
            constraint.core.mainIndex     = tn.indexList[0];
            constraint.core.mainTable     = tn;
            constraint.core.mainTableName = tn.tableName;

            newList.add(constraint);
        }

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (dropConstraints.contains(c.getName())) {
                continue;
            }

            c = c.duplicate();

            c.updateTable(session, this, tn, colIndex, adjust);
            newList.add(c);
        }

        if (!newPK && constraint != null) {
            constraint.updateTable(session, this, tn, new int[]{}, 0);
            newList.add(constraint);
        }

        tn.constraintList = new Constraint[newList.size()];

        newList.toArray(tn.constraintList);
        tn.updateConstraintLists();
        tn.setBestRowIdentifiers();

        tn.triggerList  = triggerList;
        tn.triggerLists = triggerLists;

        for (int i = 0; i < tn.constraintList.length; i++) {
            tn.constraintList[i].compile(session, tn);
        }

        for (int i = 0; i < tn.columnCount; i++) {
            tn.getColumn(i).compile(session, tn);
        }

        return tn;
    }

    /**
     * Used for drop / retype column.
     */
    void checkColumnInCheckConstraint(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == SchemaObject.ConstraintTypes.CHECK
                    && !c.isNotNull() && c.hasColumn(colIndex)) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42502,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     * Used for retype column. Checks whether column is in an FK or is
     * referenced by a FK
     * @param colIndex index
     */
    void checkColumnInFKConstraint(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumn(colIndex) && (c.getConstraintType() == SchemaObject
                    .ConstraintTypes.MAIN || c
                    .getConstraintType() == SchemaObject.ConstraintTypes
                    .FOREIGN_KEY)) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42533,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     * Returns list of constraints dependent only on one column
     */
    OrderedHashSet getDependentConstraints(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumnOnly(colIndex)) {
                set.add(c);
            }
        }

        return set;
    }

    /**
     * Returns list of constraints dependent on more than one column
     */
    OrderedHashSet getContainingConstraints(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumnPlus(colIndex)) {
                set.add(c);
            }
        }

        return set;
    }

    OrderedHashSet getContainingIndexNames(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = indexList.length; i < size; i++) {
            Index index = indexList[i];

            if (ArrayUtil.find(index.getColumns(), colIndex) != -1) {
                set.add(index.getName());
            }
        }

        return set;
    }

    /**
     * Returns list of MAIN constraints dependent on this PK or UNIQUE constraint
     */
    OrderedHashSet getDependentConstraints(Constraint constraint) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = fkMainConstraints.length; i < size; i++) {
            Constraint c = fkMainConstraints[i];

            if (c.core.uniqueName == constraint.getName()) {
                set.add(c);
            }
        }

        return set;
    }

    public OrderedHashSet getDependentExternalConstraints() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == SchemaObject.ConstraintTypes.MAIN
                    || c.getConstraintType()
                       == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                if (c.core.mainTable != c.core.refTable) {
                    set.add(c);
                }
            }
        }

        return set;
    }

    public OrderedHashSet getUniquePKConstraintNames() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == SchemaObject.ConstraintTypes.UNIQUE
                    || c.getConstraintType()
                       == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                set.add(c.getName());
            }
        }

        return set;
    }

    /**
     * Used for column defaults and nullability. Checks whether column is in an
     * FK with a given referential action type.
     *
     * @param colIndex index of column
     * @param actionType referential action of the FK
     */
    void checkColumnInFKConstraint(int colIndex, int actionType) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == SchemaObject.ConstraintTypes
                    .FOREIGN_KEY && c
                    .hasColumn(colIndex) && (actionType == c
                        .getUpdateAction() || actionType == c
                        .getDeleteAction())) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42533,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     *  Returns the identity column index.
     */
    public int getIdentityColumnIndex() {
        return identityColumn;
    }

    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getColumnIndex(String name) {

        int i = findColumn(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }

    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findColumn(String name) {

        int index = columnList.getIndex(name);

        return index;
    }

    /**
     * recreates various flags based on column properties
     */
    void resetDefaultFlags() {

        hasDefaultValues   = false;
        hasGeneratedValues = false;
        hasUpdatedValues   = false;
        hasNotNullColumns  = false;
        hasDomainColumns   = false;
        hasLobColumn       = false;

        for (int i = 0; i < columnCount; i++) {
            ColumnSchema column = getColumn(i);

            colNotNull[i]      = column.isPrimaryKey() || !column.isNullable();
            colDefaults[i]     = column.getDefaultExpression();
            colGenerated[i]    = column.isGenerated();
            colUpdated[i]      = column.isAutoUpdate();
            hasDefaultValues   |= colDefaults[i] != null;
            hasGeneratedValues |= colGenerated[i];
            hasUpdatedValues   |= colUpdated[i];
            hasNotNullColumns  |= colNotNull[i];

            if (colTypes[i].isDomainType()) {
                hasDomainColumns = true;
            }

            if (colTypes[i].isLobType()) {
                hasLobColumn = true;
            }
        }
    }

    public int[] getBestRowIdentifiers() {
        return bestRowIdentifierCols;
    }

    public boolean isBestRowIdentifiersStrict() {
        return bestRowIdentifierStrict;
    }

    public Index getClusteredIndex() {

        for (int i = 0; i < indexList.length; i++) {
            if (indexList[i].isClustered()) {
                return indexList[i];
            }
        }

        return null;
    }

    /**
     *  Finds an existing index for a column
     */
    synchronized Index getIndexForColumn(Session session, int col) {

        int i = bestIndexForColumn[col];

        if (i > -1) {
            return indexList[i];
        }

        switch (tableType) {

            case TableBase.TEMP_TABLE :
            case TableBase.INFO_SCHEMA_TABLE :
            case TableBase.MODULE_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE : {
                Index index = createIndexForColumns(session, new int[]{ col });

                return index;
            }
        }

        return null;
    }

    boolean isIndexed(int colIndex) {
        return bestIndexForColumn[colIndex] != -1;
    }

    int[] getUniqueNotNullColumnGroup(boolean[] usedColumns) {

        for (int i = 0, count = constraintList.length; i < count; i++) {
            Constraint constraint = constraintList[i];

            if (constraint.getConstraintType()
                    == SchemaObject.ConstraintTypes.UNIQUE) {
                int[] indexCols = constraint.getMainColumns();

                if (ArrayUtil.areAllIntIndexesInBooleanArray(
                        indexCols, colNotNull) && ArrayUtil
                            .areAllIntIndexesInBooleanArray(
                                indexCols, usedColumns)) {
                    return indexCols;
                }
            } else if (constraint.getConstraintType()
                       == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                int[] indexCols = constraint.getMainColumns();

                if (ArrayUtil.areAllIntIndexesInBooleanArray(indexCols,
                        usedColumns)) {
                    return indexCols;
                }
            }
        }

        return null;
    }

    boolean areColumnsNotNull(int[] indexes) {
        return ArrayUtil.areAllIntIndexesInBooleanArray(indexes, colNotNull);
    }

    /**
     *  Shortcut for creating default PK's.
     */
    public void createPrimaryKey() {
        createPrimaryKey(null, ValuePool.emptyIntArray, false);
    }

    /**
     *  Creates a single or multi-column primary key and index. sets the
     *  colTypes array. Finalises the creation of the table. (fredt@users)
     */
    public void createPrimaryKey(HsqlName indexName, int[] columns,
                                 boolean columnsNotNull) {

        if (columns == null) {
            columns = ValuePool.emptyIntArray;
        }

        for (int i = 0; i < columns.length; i++) {
            getColumn(columns[i]).setPrimaryKey(true);
        }

        setColumnStructures();

        Type[] primaryKeyTypes = new Type[columns.length];

        ArrayUtil.projectRow(colTypes, columns, primaryKeyTypes);

        HsqlName name = indexName;

        if (name == null) {
            name = database.nameManager.newAutoName("IDX", getSchemaName(),
                    getName(), SchemaObject.INDEX);
        }

        createPrimaryIndex(columns, primaryKeyTypes, name);
        setBestRowIdentifiers();
    }

    public void createPrimaryKeyConstraint(HsqlName indexName, int[] columns,
                                           boolean columnsNotNull) {

        createPrimaryKey(indexName, columns, columnsNotNull);

        Constraint c =
            new Constraint(indexName, this, getPrimaryIndex(),
                           SchemaObject.ConstraintTypes.PRIMARY_KEY);

        addConstraint(c);
    }

    void setColumnStructures() {

        if (colTypes == null) {
            colTypes = new Type[columnCount];
        }

        colDefaults             = new Expression[columnCount];
        colNotNull              = new boolean[columnCount];
        emptyColumnCheckList    = new boolean[columnCount];
        colGenerated            = new boolean[columnCount];
        colUpdated              = new boolean[columnCount];
        defaultColumnMap        = new int[columnCount];
        systemPeriodStartColumn = 0;
        systemPeriodEndColumn   = 0;

        for (int i = 0; i < columnCount; i++) {
            setSingleColumnTypeVars(i);
        }

        resetDefaultFlags();
    }

    void setColumnTypeVars(int i) {
        setSingleColumnTypeVars(i);
        resetDefaultFlags();
    }

    private void setSingleColumnTypeVars(int i) {

        ColumnSchema column   = getColumn(i);
        Type         dataType = column.getDataType();

        colTypes[i]         = dataType;
        colNotNull[i]       = column.isPrimaryKey() || !column.isNullable();
        defaultColumnMap[i] = i;

        if (column.isIdentity()) {
            identitySequence = column.getIdentitySequence();
            identityColumn   = i;
        } else if (identityColumn == i) {
            identitySequence = null;
            identityColumn   = -1;
        }

        colDefaults[i]  = column.getDefaultExpression();
        colGenerated[i] = column.isGenerated();
        colUpdated[i]   = column.isAutoUpdate();

        if (column.isSystemPeriod()) {
            int type = column.getSystemPeriodType();

            if (type == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START) {
                systemPeriodStartColumn = i;
            } else if (type
                       == SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END) {
                systemPeriodEndColumn = i;
            }
        }
    }

    /**
     * Returns direct mapping array.
     */
    public int[] getColumnMap() {
        return defaultColumnMap;
    }

    /**
     * Returns empty mapping array.
     */
    int[] getNewColumnMap() {
        return new int[columnCount];
    }

    boolean[] getColumnCheckList(int[] columnIndexes) {

        boolean[] columnCheckList = new boolean[columnCount];

        for (int i = 0; i < columnIndexes.length; i++) {
            int index = columnIndexes[i];

            if (index > -1) {
                columnCheckList[index] = true;
            }
        }

        return columnCheckList;
    }

    int[] findColumnIndexes(String[] list) {

        int[] cols = new int[list.length];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = findColumn(list[i]);
        }

        return cols;
    }

    int[] getColumnIndexes(OrderedHashSet set) {

        int[] cols = new int[set.size()];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = getColumnIndex((String) set.get(i));

            if (cols[i] == -1) {
                throw Error.error(ErrorCode.X_42501, (String) set.get(i));
            }
        }

        return cols;
    }

    int[] getColumnIndexes(OrderedHashMap list) {

        int[] cols = new int[list.size()];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = ((Integer) list.get(i)).intValue();
        }

        return cols;
    }

    /**
     *  Returns the Column object at the given index
     */
    public ColumnSchema getColumn(int i) {
        return (ColumnSchema) columnList.get(i);
    }

    public OrderedHashSet getColumnNameSet(int[] columnIndexes) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnIndexes.length; i++) {
            set.add(((ColumnSchema) columnList.get(i)).getName());
        }

        return set;
    }

    public OrderedHashSet getColumnNameSet(boolean[] columnCheckList) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                set.add(columnList.get(i));
            }
        }

        return set;
    }

    public void getColumnNames(boolean[] columnCheckList, Set set) {

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                set.add(((ColumnSchema) columnList.get(i)).getName());
            }
        }
    }

    public OrderedHashSet getColumnNameSet() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnCount; i++) {
            set.add(((ColumnSchema) columnList.get(i)).getName());
        }

        return set;
    }

    public String[] getColumnLabels() {

        String[] labels = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            labels[i] = ((ColumnSchema) columnList.get(i)).getName().name;
        }

        return labels;
    }

    /**
     * Returns array for a new row with SQL DEFAULT value for each column n
     * where exists[n] is false. This provides default values only where
     * required and avoids evaluating these values where they will be
     * overwritten.
     */
    public Object[] getNewRowData(Session session) {

        Object[] data = new Object[columnCount];
        int      i;

        if (hasDefaultValues) {
            for (i = 0; i < columnCount; i++) {
                Expression def = colDefaults[i];

                if (def != null) {
                    data[i] = def.getValue(session, colTypes[i]);
                }
            }
        }

        return data;
    }

    boolean hasTrigger(int trigVecIndex) {
        return triggerLists[trigVecIndex].length != 0;
    }

    /**
     * Adds a trigger.
     */
    void addTrigger(TriggerDef td, HsqlName otherName) {

        int index = triggerList.length;

        if (otherName != null) {
            int pos = getTriggerIndex(otherName.name);

            if (pos != -1) {
                index = pos + 1;
            }
        }

        triggerList = (TriggerDef[]) ArrayUtil.toAdjustedArray(triggerList,
                td, index, 1);

        TriggerDef[] list = triggerLists[td.triggerType];

        index = list.length;

        if (otherName != null) {
            for (int i = 0; i < list.length; i++) {
                TriggerDef trigger = list[i];

                if (trigger.getName().name.equals(otherName.name)) {
                    index = i + 1;

                    break;
                }
            }
        }

        list = (TriggerDef[]) ArrayUtil.toAdjustedArray(list, td, index, 1);
        triggerLists[td.triggerType] = list;
    }

    /**
     * Returns a trigger.
     */
    TriggerDef getTrigger(String name) {

        for (int i = triggerList.length - 1; i >= 0; i--) {
            if (triggerList[i].getName().name.equals(name)) {
                return triggerList[i];
            }
        }

        return null;
    }

    public int getTriggerIndex(String name) {

        for (int i = 0; i < triggerList.length; i++) {
            if (triggerList[i].getName().name.equals(name)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Drops a trigger.
     */
    void removeTrigger(TriggerDef trigger) {

        TriggerDef td = null;

        for (int i = 0; i < triggerList.length; i++) {
            td = triggerList[i];

            if (td.getName().name.equals(trigger.getName().name)) {
                td.terminate();

                triggerList =
                    (TriggerDef[]) ArrayUtil.toAdjustedArray(triggerList,
                        null, i, -1);

                break;
            }
        }

        if (td == null) {
            return;
        }

        int index = td.triggerType;

        // look in each trigger in list
        for (int j = 0; j < triggerLists[index].length; j++) {
            td = triggerLists[index][j];

            if (td.getName().name.equals(trigger.getName().name)) {
                triggerLists[index] = (TriggerDef[]) ArrayUtil.toAdjustedArray(
                    triggerLists[index], null, j, -1);

                break;
            }
        }
    }

    /**
     * Used when dropping all triggers.
     */
    void releaseTriggers() {

        // look in each trigger list of each type of trigger
        for (int i = 0; i < TriggerDef.NUM_TRIGS; i++) {
            for (int j = 0; j < triggerLists[i].length; j++) {
                triggerLists[i][j].terminate();
            }

            triggerLists[i] = TriggerDef.emptyArray;
        }

        triggerList = TriggerDef.emptyArray;
    }

    void terminateTriggers() {

        // look in each trigger list of each type of trigger
        for (int i = 0; i < TriggerDef.NUM_TRIGS; i++) {
            for (int j = 0; j < triggerLists[i].length; j++) {
                triggerLists[i][j].terminate();
            }
        }
    }

    /**
     * Returns the index of the Index object of the given name or -1 if not found.
     */
    int getIndexIndex(String indexName) {

        Index[] indexes = indexList;

        for (int i = 0; i < indexes.length; i++) {
            if (indexName.equals(indexes[i].getName().name)) {
                return i;
            }
        }

        // no such index
        return -1;
    }

    /**
     * Returns the Index object of the given name or null if not found.
     */
    Index getIndex(String indexName) {

        Index[] indexes = indexList;
        int     i       = getIndexIndex(indexName);

        return i == -1 ? null
                       : indexes[i];
    }

    /**
     * Returns the Index object of the given name or null if not found.
     */
    Index getUserIndex(String indexName) {

        Index[] indexes = indexList;

        for (int i = 0; i < indexes.length; i++) {
            if (indexName.equals(indexes[i].getName().name)) {
                if (!indexes[i].isConstraint()) {
                    return indexes[i];
                }
            }
        }

        return null;
    }

    /**
     * Returns the Index object of the given name or null if not found.
     */
    Index getSystemIndex(String indexName) {

        Index[] indexes = indexList;

        for (int i = 0; i < indexes.length; i++) {
            if (indexName.equals(indexes[i].getName().name)) {
                if (indexes[i].isConstraint()) {
                    return indexes[i];
                }
            }
        }

        return null;
    }

    /**
     *  Return the position of the constraint within the list
     */
    int getConstraintIndex(String constraintName) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            if (constraintList[i].getName().name.equals(constraintName)) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  return the named constraint
     */
    public Constraint getConstraint(String constraintName) {

        int i = getConstraintIndex(constraintName);

        return (i < 0) ? null
                       : constraintList[i];
    }

    /**
     *  Returns any unique Constraint using this index
     */
    public Constraint getUniqueConstraintForIndex(Index index) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getMainIndex() == index) {
                if (c.getConstraintType() == SchemaObject.ConstraintTypes
                        .PRIMARY_KEY || c.getConstraintType() == SchemaObject
                        .ConstraintTypes.UNIQUE) {
                    return c;
                }
            }
        }

        return null;
    }

    /**
     * remove a named constraint
     */
    void removeConstraint(String name) {

        int index = getConstraintIndex(name);

        if (index != -1) {
            removeConstraint(index);
        }
    }

    void removeConstraint(int index) {

        constraintList =
            (Constraint[]) ArrayUtil.toAdjustedArray(constraintList, null,
                index, -1);

        updateConstraintLists();
    }

    void renameColumn(ColumnSchema column, String newName, boolean isquoted) {

        String oldname = column.getName().name;
        int    i       = getColumnIndex(oldname);

        columnList.setKeyAt(i, newName);
        column.getName().rename(newName, isquoted);
    }

    void renameColumn(ColumnSchema column, HsqlName newName) {

        String oldname = column.getName().name;
        int    i       = getColumnIndex(oldname);

        if (findColumn(newName.name) != -1) {
            throw Error.error(ErrorCode.X_42504);
        }

        columnList.setKeyAt(i, newName.name);
        column.getName().rename(newName);
    }

    public TriggerDef[] getTriggers() {
        return triggerList;
    }

    public boolean isWritable() {
        return !isReadOnly && !database.databaseReadOnly
               && !(database.isFilesReadOnly() && (isCached || isText));
    }

    public boolean isInsertable() {
        return isWritable();
    }

    public boolean isUpdatable() {
        return isWritable();
    }

    public boolean isTriggerInsertable() {
        return false;
    }

    public boolean isTriggerUpdatable() {
        return false;
    }

    public boolean isTriggerDeletable() {
        return false;
    }

    public int[] getUpdatableColumns() {
        return defaultColumnMap;
    }

    public Table getBaseTable() {
        return this;
    }

    public int[] getBaseTableColumnMap() {
        return defaultColumnMap;
    }

    /**
     * Used to create an index automatically for system and temp tables.
     * Used for internal operation tables with null Session param.
     */
    Index createIndexForColumns(Session session, int[] columns) {

        Index index = null;
        HsqlName indexName = database.nameManager.newAutoName("IDX_T",
            getSchemaName(), getName(), SchemaObject.INDEX);

        try {
            index = createAndAddIndexStructure(session, indexName, columns,
                                               null, null, false, false,
                                               false);
        } catch (Throwable t) {
            return null;
        }

        return index;
    }

    void fireTriggers(Session session, int trigVecIndex,
                      RowSetNavigatorDataChange rowSet) {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        TriggerDef[] trigVec = triggerLists[trigVecIndex];

        for (int i = 0, size = trigVec.length; i < size; i++) {
            TriggerDef td         = trigVec[i];
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (td.hasOldTable()) {

                //
            }

            td.pushPair(session, null, null);
        }
    }

    void fireTriggers(Session session, int trigVecIndex,
                      RowSetNavigator rowSet) {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        TriggerDef[] trigVec = triggerLists[trigVecIndex];

        for (int i = 0, size = trigVec.length; i < size; i++) {
            TriggerDef td         = trigVec[i];
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (td.hasOldTable()) {

                //
            }

            td.pushPair(session, null, null);
        }
    }

    /**
     *  Fires all row-level triggers of the given set (trigger type)
     *
     */
    void fireTriggers(Session session, int trigVecIndex, Object[] oldData,
                      Object[] newData, int[] cols) {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        TriggerDef[] trigVec = triggerLists[trigVecIndex];

        for (int i = 0, size = trigVec.length; i < size; i++) {
            TriggerDef td         = trigVec[i];
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (cols != null && td.getUpdateColumnIndexes() != null
                    && !ArrayUtil.haveCommonElement(
                        td.getUpdateColumnIndexes(), cols)) {
                continue;
            }

            if (td.isForEachRow()) {
                switch (td.triggerType) {

                    case Trigger.INSERT_BEFORE_ROW :
                        break;

                    case Trigger.INSERT_AFTER_ROW :
                        if (!sqlTrigger) {
                            newData =
                                (Object[]) ArrayUtil.duplicateArray(newData);
                        }
                        break;

                    case Trigger.UPDATE_AFTER_ROW :
                        if (!sqlTrigger) {
                            oldData =
                                (Object[]) ArrayUtil.duplicateArray(oldData);
                            newData =
                                (Object[]) ArrayUtil.duplicateArray(newData);
                        }
                        break;

                    case Trigger.UPDATE_BEFORE_ROW :
                    case Trigger.DELETE_BEFORE_ROW :
                    case Trigger.DELETE_AFTER_ROW :
                        if (!sqlTrigger) {
                            oldData =
                                (Object[]) ArrayUtil.duplicateArray(oldData);
                        }
                        break;
                }

                td.pushPair(session, oldData, newData);
            } else {
                td.pushPair(session, null, null);
            }
        }
    }

    /**
     *  Enforce max field sizes according to SQL column definition.
     *  SQL92 13.8
     */
    public void enforceRowConstraints(Session session, Object[] data) {

        for (int i = 0; i < columnCount; i++) {
            Type         type = colTypes[i];
            ColumnSchema column;

            if (hasDomainColumns && type.isDomainType()) {
                Constraint[] constraints =
                    type.userTypeModifier.getConstraints();

                column = getColumn(i);

                for (int j = 0; j < constraints.length; j++) {
                    constraints[j].checkCheckConstraint(session, this, column,
                                                        data[i]);
                }
            }

            if (colNotNull[i] && data[i] == null) {
                String     constraintName;
                Constraint c = getNotNullConstraintForColumn(i);

                if (c == null) {
                    if (ArrayUtil.find(getPrimaryKey(), i) > -1) {
                        c = getPrimaryConstraint();
                    }
                }

                constraintName = c == null ? ""
                                           : c.getName().name;
                column         = getColumn(i);

                String[] info = new String[] {
                    constraintName, tableName.statementName,
                    column.getName().statementName
                };

                throw Error.error(null, ErrorCode.X_23502,
                                  ErrorCode.COLUMN_CONSTRAINT, info);
            }
        }
    }

    public void enforceTypeLimits(Session session, Object[] data) {

        int i = 0;

        try {
            for (; i < columnCount; i++) {
                data[i] = colTypes[i].convertToTypeLimits(session, data[i]);
            }
        } catch (HsqlException e) {
            int code = e.getErrorCode();

            if (code == -ErrorCode.X_22001 || code == -ErrorCode.X_22003
                    || code == -ErrorCode.X_22008) {
                ColumnSchema column = getColumn(i);
                String[]     info   = new String[] {
                    "", tableName.statementName, column.getName().statementName
                };

                throw Error.error(e, code, ErrorCode.COLUMN_CONSTRAINT, info);
            }

            throw e;
        }
    }

    int indexTypeForColumn(Session session, int col) {

        int i = bestIndexForColumn[col];

        if (i > -1) {
            return indexList[i].isUnique()
                   && indexList[i].getColumnCount() == 1 ? Index.INDEX_UNIQUE
                                                         : Index
                                                         .INDEX_NON_UNIQUE;
        }

        switch (tableType) {

            case TableBase.TEMP_TABLE :
            case TableBase.INFO_SCHEMA_TABLE :
            case TableBase.MODULE_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE : {
                return Index.INDEX_NON_UNIQUE;
            }
        }

        return Index.INDEX_NONE;
    }

    /**
     *  Finds an existing index for a column group - for persistent tables
     */
    synchronized Index getIndexForColumns(int[] cols) {

        int i = bestIndexForColumn[cols[0]];

        if (i > -1) {
            return indexList[i];
        }

        return null;
    }

    /**
     *  Finds an existing index for an ordered full column group
     */
    Index getFullIndexForColumns(int[] cols) {

        for (int i = 0; i < indexList.length; i++) {
            if (ArrayUtil.haveEqualArrays(indexList[i].getColumns(), cols,
                                          cols.length)) {
                return indexList[i];
            }
        }

        return null;
    }

    /**
     *  Finds an existing index for an unordered full column group
     */
    Index getIndexForAllColumns(int[] cols) {

        for (int i = 0; i < indexList.length; i++) {
            if (ArrayUtil.haveEqualSets(indexList[i].getColumns(), cols,
                                        cols.length)) {
                return indexList[i];
            }
        }

        return null;
    }

    /**
     * Finds an existing index for a column set or create one for temporary
     * tables.
     *
     * synchronized required for shared INFORMATION_SCHEMA etc. tables
     */
    synchronized IndexUse[] getIndexForColumns(Session session,
            OrderedIntHashSet set, int opType, boolean ordered) {

        if (set.isEmpty()) {
            return Index.emptyUseArray;
        }

        IndexUse[] indexUse = findIndexForColumns(session, set, opType,
            ordered);

        if (indexUse.length == 0) {
            Index selected = null;

            // index is not full;
            switch (tableType) {

                case TableBase.TEMP_TABLE :
                case TableBase.INFO_SCHEMA_TABLE :
                case TableBase.MODULE_TABLE :
                case TableBase.FUNCTION_TABLE :
                case TableBase.SYSTEM_SUBQUERY :
                case TableBase.VIEW_TABLE : {
                    selected = createIndexForColumns(session, set.toArray());

                    break;
                }
            }

            if (selected != null) {
                indexUse = selected.asArray();
            }
        }

        return indexUse;
    }

    IndexUse[] findIndexForColumns(Session session, OrderedIntHashSet set,
                                   int opType, boolean ordered) {

        IndexUse[] indexUse = Index.emptyUseArray;

        if (set.isEmpty()) {
            return Index.emptyUseArray;
        }

        for (int i = 0, count = indexList.length; i < count; i++) {
            Index currentIndex = getIndex(i);
            int[] indexcols    = currentIndex.getColumns();
            int matchCount = ordered ? set.getOrderedStartMatchCount(indexcols)
                                     : set.getStartMatchCount(indexcols);

            if (matchCount == 0) {
                continue;
            }

            if (matchCount == set.size()) {
                return currentIndex.asArray();
            }

            if (matchCount == currentIndex.getColumnCount()) {
                if (currentIndex.isUnique()) {
                    return currentIndex.asArray();
                }
            }

            if (indexUse.length == 0
                    && matchCount == currentIndex.getColumnCount()) {
                indexUse = currentIndex.asArray();
            } else {
                IndexUse[] newList =
                    (IndexUse[]) ArrayUtil.resizeArray(indexUse,
                                                       indexUse.length + 1);

                newList[newList.length - 1] = new IndexUse(currentIndex,
                        matchCount);
                indexUse = newList;
            }
        }

        return indexUse;
    }

    /**
     * Returns an index on all the columns
     */
    public Index getFullIndex(Session session) {

        if (fullIndex == null) {
            fullIndex = getFullIndexForColumns(defaultColumnMap);

            if (fullIndex == null) {
                fullIndex = createIndexForColumns(session, defaultColumnMap);
            }
        }

        return fullIndex;
    }

    /**
     *  Return the list of file pointers to root nodes for this table's
     *  indexes.
     */
    public final long[] getIndexRootsArray() {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        long[] roots = new long[indexList.length];

        for (int index = 0; index < indexList.length; index++) {
            CachedObject accessor = store.getAccessor(indexList[index]);

            roots[index] = accessor == null ? -1
                                            : accessor.getPos();
        }

        return roots;
    }

    public void setIndexRoots(long[] roots) {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        long cardinality = store.elementCount();

        store.setAccessors(0, roots, cardinality);
    }

    /**
     *  Sets the index roots.
     */
    void setIndexRoots(Session session, String s) {

        if (!isCached) {
            throw Error.error(ErrorCode.X_42501, tableName.name);
        }

        int       indexCount  = getIndexCount();
        ParserDQL p = new ParserDQL(session, new Scanner(session, s), null);
        long[]    roots       = new long[indexCount];
        long[]    uniqueSize  = new long[indexCount];
        long      cardinality = -1;

        p.read();

        for (int index = 0; index < indexCount; index++) {
            long v = p.readBigint();

            roots[index] = v;
        }

        for (int index = 0; index < indexCount; index++) {
            long v = p.readBigint();

            uniqueSize[index] = v;
        }

        cardinality = p.readBigint();

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        store.setAccessors(0, roots, cardinality);
    }

    public void generateAndCheckData(Session session, Object[] data) {

        if (hasGeneratedValues || systemPeriod != null) {
            setGeneratedColumns(session, data);
        }

        enforceTypeLimits(session, data);

        if (hasDomainColumns || hasNotNullColumns) {
            enforceRowConstraints(session, data);
        }
    }

    /**
     * for default values (not expressions)
     */
    public void generateDefaultForNull(Object[] data) {

        if (hasNotNullColumns) {
            for (int i = 0; i < colDefaults.length; i++) {
                if (data[i] == null) {
                    if (colNotNull[i] && colDefaults[i] != null
                            && colDefaults[i].getType() == OpTypes.VALUE) {
                        data[i] = colDefaults[i].getValue(null);
                    }
                }
            }
        }
    }

    /**
     *  Mid level method for inserting single rows. Performs constraint checks and
     *  fires row level triggers.
     */
    Row insertSingleRow(Session session, PersistentStore store, Object[] data,
                        int[] changedCols) {

        generateAndCheckData(session, data);

        if (isView) {

            // may have domain column
            return null;
        }

        Row row = (Row) store.getNewCachedObject(session, data, true);

        session.addInsertAction(this, store, row, changedCols);

        return row;
    }

    /**
     * Method for inserting system version history row
     */
    Row insertSystemVersionHistoryRow(Session session, PersistentStore store,
                                      Object[] data) {

        TimestampData txTimestamp = session.getTransactionUTC();

        if (txTimestamp.equals(data[systemPeriodStartColumn])) {
            return null;
        }

        Object[] newData = (Object[]) ArrayUtil.duplicateArray(data);

        newData[systemPeriodEndColumn] = txTimestamp;

        Row row = (Row) store.getNewCachedObject(session, newData, true);

        session.database.txManager.addInsertAction(session, this, store, row,
                null);

        return row;
    }

    /**
     * Multi-row insert method. Used for CREATE TABLE AS ... queries.
     */
    void insertIntoTable(Session session, Result result) {

        PersistentStore store = getRowStore(session);
        RowSetNavigator nav   = result.initialiseNavigator();

        while (nav.next()) {
            Object[] data = nav.getCurrent();
            Object[] newData =
                (Object[]) ArrayUtil.resizeArrayIfDifferent(data, columnCount);

            for (int i = 0; i < newData.length; i++) {
                newData[i] = colTypes[i].convertToTypeLimits(session,
                        newData[i]);
            }

            insertData(session, store, newData, true);
        }
    }

    /**
     *
     */
    public void insertNoCheckFromLog(Session session, Object[] data) {

        systemUpdateIdentityValue(data);

        PersistentStore store         = getRowStore(session);
        Row row = (Row) store.getNewCachedObject(session, data, true);
        boolean         enforceUnique = true;

        if (isSystemVersioned) {
            enforceUnique = data[systemPeriodEndColumn].equals(
                DateTimeType.epochLimitTimestamp);
        }

        database.txManager.addInsertAction(session, this, store, row, null);
    }

    /**
     * Used for system table inserts. No checks. No identity
     * columns.
     */
    public int insertSys(Session session, PersistentStore store, Result ins) {

        RowSetNavigator nav   = ins.getNavigator();
        int             count = 0;

        while (nav.next()) {
            insertSys(session, store, nav.getCurrent());

            count++;
        }

        return count;
    }

    /**
     * Used for subquery inserts. No checks. No identity
     * columns.
     */
    void insertResult(Session session, PersistentStore store, Result ins) {

        RowSetNavigator nav = ins.initialiseNavigator();

        while (nav.next()) {
            Object[] data = nav.getCurrent();
            Object[] newData =
                (Object[]) ArrayUtil.resizeArrayIfDifferent(data, columnCount);

            insertData(session, store, newData, true);
        }
    }

    /**
     * Not for general use.
     * Used by ScriptReader to unconditionally insert a row into
     * the table when the .script file is read.
     */
    public void insertFromScript(Session session, PersistentStore store,
                                 Object[] data) {

        boolean enforceUnique = true;

        systemUpdateIdentityValue(data);

        if (isSystemVersioned) {
            enforceUnique = data[systemPeriodEndColumn].equals(
                DateTimeType.epochLimitTimestamp);
        }

        insertData(session, store, data, enforceUnique);
    }

    /**
     * For system operations outside transaction control
     */
    public void insertData(Session session, PersistentStore store,
                           Object[] data, boolean enforceUnique) {

        Row row = (Row) store.getNewCachedObject(session, data, false);

        store.indexRow(session, row);
    }

    /**
     * Used by the system tables only
     */
    public void insertSys(Session session, PersistentStore store,
                          Object[] data) {

        Row row = (Row) store.getNewCachedObject(session, data, false);

        store.indexRow(session, row);
    }

    /**
     * If there is an identity column in the table, sets
     * the value and/or adjusts the identity value for the table.
     */
    protected void setIdentityColumn(Session session, Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (identitySequence.getName() == null) {
                if (id == null) {
                    id = (Number) identitySequence.getValueObject();
                    data[identityColumn] = id;
                } else {
                    identitySequence.userUpdate(id.longValue());
                }
            } else {
                if (id == null) {
                    id = (Number) session.sessionData.getSequenceValue(
                        identitySequence);
                    data[identityColumn] = id;
                }
            }

            if (session != null) {
                session.setLastIdentity(id);
            }
        }
    }

    public Object getColumnDefaultOrGeneratedValue(Session session,
            ColumnSchema column, Object[] data) {

        Object value = null;

        if (column.isGenerated()) {
            Expression e = column.getGeneratingExpression();
            RangeIterator range =
                session.sessionContext.getCheckIterator(getDefaultRanges()[0]);

            range.setCurrent(data);

            value = e.getValue(session, column.getDataType());
        } else if (column.isSystemPeriod()) {
            switch (column.getSystemPeriodType()) {

                case SchemaObject.PeriodSystemColumnType.PERIOD_ROW_START :
                    value = session.getTransactionUTC();
                    break;

                case SchemaObject.PeriodSystemColumnType.PERIOD_ROW_END :
                    value = DateTimeType.epochLimitTimestamp;
                    break;
            }
        } else {
            value = column.getDefaultValue(session);
        }

        return value;
    }

    public void setGeneratedColumns(Session session, Object[] data) {

        if (hasGeneratedValues) {
            for (int i = 0; i < colGenerated.length; i++) {
                if (colGenerated[i]) {
                    ColumnSchema column = getColumn(i);
                    Expression   e      = column.getGeneratingExpression();
                    RangeIterator range =
                        session.sessionContext.getCheckIterator(
                            getDefaultRanges()[0]);

                    range.setCurrent(data);

                    data[i] = e.getValue(session, colTypes[i]);
                }
            }
        }

        if (systemPeriod != null) {
            data[systemPeriodStartColumn] = session.getTransactionUTC();
            data[systemPeriodEndColumn]   = DateTimeType.epochLimitTimestamp;
        }
    }

    public void setUpdatedColumns(Session session, Object[] data,
                                  int[] colMap) {

        if (hasUpdatedValues) {
            for (int i = 0; i < colUpdated.length; i++) {
                if (colUpdated[i]) {
                    if (ArrayUtil.find(colMap, i) < 0) {
                        Expression e = getColumn(i).getUpdateExpression();

                        data[i] = e.getValue(session, colTypes[i]);
                    }
                }
            }
        }
    }

    public void systemSetIdentityColumn(Session session, Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id == null) {
                id = (Number) identitySequence.getValueObject();
                data[identityColumn] = id;
            } else {
                if (identitySequence.getName() == null) {
                    identitySequence.userUpdate(id.longValue());
                }
            }
        }
    }

    /**
     * If there is an identity column in the table, sets
     * the max identity value.
     */
    public void systemUpdateIdentityValue(Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id != null) {
                if (identitySequence.getName() == null) {
                    identitySequence.systemUpdate(id.longValue());
                }
            }
        }
    }

    /**
     * For log statements. Find a single row to delete.
     */
    public Row getDeleteRowFromLog(Session session, Object[] data) {

        Row             row   = null;
        PersistentStore store = getRowStore(session);

        if (hasPrimaryKey()) {
            Index index        = getPrimaryIndex();
            int[] colsSequence = index.getDefaultColumnMap();
            RowIterator it = index.findFirstRow(session, store, data,
                                                colsSequence);

            it.next();

            row = it.getCurrentRow();

            it.release();
        } else if (bestIndex == null) {
            RowIterator it = rowIterator(session);

            while (it.next()) {
                row = it.getCurrentRow();

                if (Table.compareRows(
                        session, row.getData(), data, defaultColumnMap,
                        colTypes) == 0) {
                    break;
                }
            }

            it.release();
        } else {
            RowIterator it = bestIndex.findFirstRow(session, store, data);

            while (it.next()) {
                row = it.getCurrentRow();

                Object[] rowdata = row.getData();

                // reached end of range
                if (bestIndex.compareRowNonUnique(
                        session, rowdata, data, bestIndex.getColumns()) != 0) {
                    row = null;

                    break;
                }

                if (Table.compareRows(
                        session, rowdata, data, defaultColumnMap,
                        colTypes) == 0) {
                    break;
                }
            }

            it.release();
        }

        return row;
    }

    public RowIterator rowIteratorClustered(Session session) {

        PersistentStore store = getRowStore(session);
        Index           index = getClusteredIndex();

        if (index == null) {
            index = getPrimaryIndex();
        }

        return index.firstRow(session, store, null, 0, null);
    }

    public RowIterator rowIteratorForScript(PersistentStore store) {

        Index index = getClusteredIndex();

        if (index == null) {
            index = getPrimaryIndex();
        }

        return index.firstRow(store);
    }

    /**
     * Path used for all stores
     */
    public PersistentStore getRowStore(Session session) {

        if (store != null) {
            return store;
        }

        if (isSessionBased) {
            return session.sessionData.persistentStoreCollection.getStore(
                this);
        }

        return database.persistentStoreCollection.getStore(this);
    }

    public QueryExpression getQueryExpression() {
        return null;
    }

    public Expression getDataExpression() {
        return null;
    }

    public void prepareTable(Session session) {}

    public void materialise(Session session) {}

    public void materialiseCorrelated(Session session) {}
}
