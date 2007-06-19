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

import java.io.IOException;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.index.Index;
import org.hsqldb.index.Node;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.DataFileCache;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.types.DomainType;
import org.hsqldb.types.Type;

// fredt@users 20020130 - patch 491987 by jimbag@users - made optional
// fredt@users 20020405 - patch 1.7.0 by fredt - quoted identifiers
// for sql standard quoted identifiers for column and table names and aliases
// applied to different places
// fredt@users 20020225 - patch 1.7.0 - restructuring
// some methods moved from Database.java, some rewritten
// changes to several methods
// fredt@users 20020225 - patch 1.7.0 - ON DELETE CASCADE
// fredt@users 20020225 - patch 1.7.0 - named constraints
// boucherb@users 20020225 - patch 1.7.0 - multi-column primary keys
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// tony_lai@users 20020820 - patch 595099 - user defined PK name
// tony_lai@users 20020820 - patch 595172 - drop constraint fix
// kloska@users 20021030 - patch 1.7.2 - ON UPDATE CASCADE | SET NULL | SET DEFAULT
// kloska@users 20021112 - patch 1.7.2 - ON DELETE SET NULL | SET DEFAULT
// fredt@users 20021210 - patch 1.7.2 - better ADD / DROP INDEX for non-CACHED tables
// fredt@users 20030901 - patch 1.7.2 - allow multiple nulls for UNIQUE columns
// fredt@users 20030901 - patch 1.7.2 - reworked IDENTITY support
// achnettest@users 20040130 - patch 878288 - bug fix for new indexes in memory tables by Arne Christensen
// boucherb@users 20040327 - doc 1.7.2 - javadoc updates
// boucherb@users 200404xx - patch 1.7.2 - proper uri for getCatalogName
// fredt@users 20050000 - 1.8.0 updates in several areas
// fredt@users 20050220 - patch 1.8.0 enforcement of DECIMAL precision/scale
// fredt@users 1.9.0 referential constraint enforcement moved to CompiledStatementExecutor

/**
 *  Holds the data structures and methods for creation of a database table.
 *
 *
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class Table extends BaseTable implements SchemaObject {

    // types of table
    public static final int SYSTEM_TABLE    = 0;
    public static final int SYSTEM_SUBQUERY = 1;
    public static final int TEMP_TABLE      = 2;
    public static final int MEMORY_TABLE    = 3;
    public static final int CACHED_TABLE    = 4;
    public static final int TEMP_TEXT_TABLE = 5;
    public static final int TEXT_TABLE      = 6;
    public static final int VIEW            = 7;
    public static final int RESULT          = 8;
    public static final int CACHED_RESULT   = 9;

// boucherb@users - for future implementation of SQL standard INFORMATION_SCHEMA
    static final int SYSTEM_VIEW = 10;

    // main properties
// boucherb@users - access changed in support of metadata 1.7.2
    public HashMappedList columnList;                 // columns in table
    private int[]         primaryKeyCols;             // column numbers for primary key
    private Type[]        primaryKeyTypes;            // types for primary key
    private int[]         primaryKeyColsSequence;     // {0,1,2,...}
    int[]                 bestRowIdentifierCols;      // column set for best index
    boolean               bestRowIdentifierStrict;    // true if it has no nullable column
    int[]                 bestIndexForColumn;         // index of the 'best' index for each column
    Index                 bestIndex;                  // the best index overall - null if there is no user-defined index
    int            identityColumn;                    // -1 means no such column
    NumberSequence identitySequence;                  // next value of identity column
    NumberSequence rowIdSequence;                     // next value of optional rowid

// -----------------------------------------------------------------------
    Constraint[]      constraintList;                 // constrainst for the table
    HsqlArrayList[]   triggerLists;                   // array of trigger lists
    Type[]            colTypes;                       // fredt - types of columns
    private boolean[] colNotNull;                     // fredt - modified copy of isNullable() values
    Expression[]    colDefaults;                      // fredt - expressions of DEFAULT values
    private int[]   defaultColumnMap;                 // fred - holding 0,1,2,3,...
    private boolean hasDefaultValues;                 //fredt - shortcut for above

    // properties for subclasses
    protected int        columnCount;                 // inclusive the hidden primary key
    public Database      database;
    public DataFileCache cache;
    protected HsqlName   tableName;                   // SQL name
    private int          tableType;
    protected boolean    isReadOnly;
    protected boolean    isTemp;
    protected boolean    isCached;
    protected boolean    isText;
    private boolean      isView;
    protected boolean    isLogged;
    protected int        indexType;                   // fredt - type of index used
    protected boolean    onCommitPreserve;            // for temp tables

    //
    PersistentStore rowStore;
    Index[]         indexList;                        // vIndex(0) is the primary key index

    /**
     *  Constructor
     *
     * @param  db
     * @param  name
     * @param  type
     * @exception  HsqlException
     */
    public Table(Database db, HsqlName name, int type) throws HsqlException {

        database      = db;
        rowIdSequence = new NumberSequence(null, 0, 1, Type.SQL_BIGINT);

        switch (type) {

            case RESULT :
                break;

            case CACHED_RESULT :
                isCached  = true;
                indexType = Index.DISK_INDEX;
                break;

            case SYSTEM_SUBQUERY :
                isTemp = true;
                break;

            case SYSTEM_TABLE :
                break;

            case CACHED_TABLE :
                if (DatabaseURL.isFileBasedDatabaseType(db.getType())) {
                    cache     = db.logger.getCache();
                    isCached  = true;
                    isLogged  = !database.isFilesReadOnly();
                    indexType = Index.DISK_INDEX;
                    rowStore  = new RowStore();

                    break;
                }

                type = MEMORY_TABLE;
            case MEMORY_TABLE :
                isLogged = !database.isFilesReadOnly();
                break;

            case TEMP_TABLE :
                isTemp = true;
                break;

            case TEMP_TEXT_TABLE :
                if (!DatabaseURL.isFileBasedDatabaseType(db.getType())) {
                    throw Trace.error(Trace.DATABASE_IS_MEMORY_ONLY);
                }

                isTemp     = true;
                isText     = true;
                isReadOnly = true;
                indexType  = Index.POINTER_INDEX;
                rowStore   = new RowStore();
                break;

            case TEXT_TABLE :
                if (!DatabaseURL.isFileBasedDatabaseType(db.getType())) {
                    throw Trace.error(Trace.DATABASE_IS_MEMORY_ONLY);
                }

                isText    = true;
                indexType = Index.POINTER_INDEX;
                rowStore  = new RowStore();
                break;

            case VIEW :
            case SYSTEM_VIEW :
                isView = true;
                break;
        }

        // type may have changed above for CACHED tables
        tableType       = type;
        tableName       = name;
        primaryKeyCols  = null;
        primaryKeyTypes = null;
        identityColumn  = -1;
        columnList      = new HashMappedList();
        indexList       = new Index[0];
        constraintList  = new Constraint[0];
        triggerLists    = new HsqlArrayList[TriggerDef.NUM_TRIGS];

        if (db.isFilesReadOnly() && isFileBased()) {
            this.isReadOnly = true;
        }
    }

    public Table(Session session, Database db, HsqlName name,
                 int type) throws HsqlException {

        this(db, name, type);

        if (type == CACHED_RESULT) {
            cache    = session.sessionData.getResultCache();
            rowStore = new RowStore();
        }
    }

    public Table() {}

    /**
     * returns a basic duplicate of the table without the data structures.
     */
    protected Table duplicate() throws HsqlException {

        Table t = new Table(database, tableName, tableType);

        t.onCommitPreserve = onCommitPreserve;

        return t;
    }

    Table newTransitionTable(HsqlName name) {

        Table transition = new Table();

        name.schema               = SchemaManager.SYSTEM_SCHEMA_HSQLNAME;
        transition.tableName      = name;
        transition.database       = database;
        transition.tableType      = Table.RESULT;
        transition.columnList     = columnList;
        transition.columnCount    = columnCount;
        transition.indexType      = indexType;
        transition.indexList      = new Index[0];
        transition.constraintList = new Constraint[0];

        transition.createPrimaryKey();

        return transition;
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

    public boolean isCached() {
        return isCached;
    }

    public final int getIndexType() {
        return indexType;
    }

    public final int getTableType() {
        return tableType;
    }

    public boolean isDataReadOnly() {
        return isReadOnly;
    }

    public final boolean onCommitPreserve() {
        return onCommitPreserve;
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations
     */
    void checkDataReadOnly() throws HsqlException {

        if (isReadOnly) {
            throw Trace.error(Trace.DATA_IS_READONLY);
        }
    }

// ----------------------------------------------------------------------------
// akede@users - 1.7.2 patch Files readonly
    public void setDataReadOnly(boolean value) throws HsqlException {

        // Changing the Read-Only mode for the table is only allowed if the
        // the database can realize it.
        if (!value && database.isFilesReadOnly() && isFileBased()) {
            throw Trace.error(Trace.DATA_IS_READONLY);
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
     * For text tables
     */
    protected void setDataSource(Session s, String source, boolean isDesc,
                                 boolean newFile) throws HsqlException {
        throw (Trace.error(Trace.TABLE_NOT_FOUND));
    }

    /**
     * For text tables
     */
    public String getDataSource() {
        return null;
    }

    /**
     * For text tables.
     */
    public boolean isDescDataSource() {
        return false;
    }

    /**
     * For text tables.
     */
    public void setHeader(String header) throws HsqlException {
        throw Trace.error(Trace.TEXT_TABLE_HEADER);
    }

    /**
     * For text tables.
     */
    public String getHeader() {
        return null;
    }

    /**
     * determines whether the table is actually connected to the underlying data source.
     *
     *  <p>This method is available for text tables only.</p>
     *
     *  @see setDataSource
     *  @see disconnect
     *  @see isConnected
     */
    public boolean isConnected() {
        return true;
    }

    /**
     * connects the table to the underlying data source.
     *
     *  <p>This method is available for text tables only.</p>
     *
     *  @param session
     *      denotes the current session. Might be <code>null</code>.
     *
     *  @see setDataSource
     *  @see disconnect
     *  @see isConnected
     */
    public void connect(Session session) throws HsqlException {
        throw Trace.error(Trace.CANNOT_CONNECT_TABLE);
    }

    /**
     * disconnects the table from the underlying data source.
     *
     *  <p>This method is available for text tables only.</p>
     *
     *  @param session
     *      denotes the current session. Might be <code>null</code>.
     *
     *  @see setDataSource
     *  @see connect
     *  @see isConnected
     */
    public void disconnect(Session session) throws HsqlException {
        throw Trace.error(Trace.CANNOT_CONNECT_TABLE);
    }

    /**
     *  Adds a constraint.
     */
    public void addConstraint(Constraint c) {

        int i = c.getType() == Constraint.PRIMARY_KEY ? 0
                                                      : constraintList.length;

        constraintList =
            (Constraint[]) ArrayUtil.toAdjustedArray(constraintList, c, i, 1);
    }

    /**
     *  Returns the list of constraints.
     */
    public Constraint[] getConstraints() {
        return constraintList;
    }

    /**
     *  Returns the primary constraint.
     */
    public Constraint getPrimaryConstraint() {
        return primaryKeyCols.length == 0 ? null
                                          : constraintList[0];
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
     * Returns the UNIQUE or PK constraint with the given column signature.
     * Modifies the composition of refTableCols if necessary.
     */
    Constraint getUniqueConstraintForColumns(int[] mainTableCols,
            int[] refTableCols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c    = constraintList[i];
            int        type = c.getType();

            if (type != Constraint.UNIQUE && type != Constraint.PRIMARY_KEY) {
                continue;
            }

            int[] constraintCols = c.getMainColumns();

            if (constraintCols.length != mainTableCols.length) {
                continue;
            }

            if (ArrayUtil.areEqual(constraintCols, mainTableCols,
                                   mainTableCols.length, true)) {
                return c;
            }

            if (ArrayUtil.areEqualSets(constraintCols, mainTableCols)) {
                int[] newRefTableCols = new int[mainTableCols.length];

                for (int j = 0; j < mainTableCols.length; j++) {
                    int pos = ArrayUtil.find(constraintCols, mainTableCols[j]);

                    newRefTableCols[pos] = refTableCols[j];
                }

                for (int j = 0; j < mainTableCols.length; j++) {
                    refTableCols[j] = newRefTableCols[j];
                }

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
     *
     * @param  index
     * @return
     */
    public Constraint getUniqueOrPKConstraintForIndex(Index index) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getMainIndex() == index
                    && (c.getType() == Constraint.UNIQUE
                        || c.getType() == Constraint.PRIMARY_KEY)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns the next constraint of a given type
     *
     * @param  from
     * @param  type
     * @return
     */
    int getNextConstraintIndex(int from, int type) {

        for (int i = from, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getType() == type) {
                return i;
            }
        }

        return -1;
    }

// fredt@users 20020220 - patch 475199 - duplicate column

    /**
     *  Performs the table level checks and adds a column to the table at the
     *  DDL level. Only used at table creation, not at alter column.
     */
    public void addColumn(Column column) throws HsqlException {

        if (findColumn(column.columnName.name) >= 0) {
            throw Trace.error(Trace.COLUMN_ALREADY_EXISTS);
        }

        if (column.isIdentity()) {
            if (identityColumn != -1) {
                throw Trace.error(Trace.SQL_SECOND_IDENTITY_COLUMN,
                                  column.columnName.name);
            }

            identityColumn   = getColumnCount();
            identitySequence = column.getIdentitySequence();
        }

        if (primaryKeyCols != null) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Table");
        }

        columnList.add(column.columnName.name, column);

        columnCount++;
    }

    /**
     *  Returns the HsqlName object fo the table
     */
    public HsqlName getName() {
        return tableName;
    }

    /**
     * Returns the catalog name or null, depending on a database property.
     */
    String getCatalogName() {

        // PRE: database is never null
        return database.getCatalog();
    }

    /**
     * Returns the schema name.
     */
    public HsqlName getSchemaName() {
        return tableName.schema;
    }

    public Grantee getOwner() {
        return tableName.schema.owner;
    }

    public OrderedHashSet getReferences() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < colTypes.length; i++) {
            if (colTypes[i].isDomainType() || colTypes[i].isDistinctType()) {
                HsqlName name = ((SchemaObject) colTypes[i]).getName();

                set.add(name);
            }
        }

        return set;
    }

    public void compile(Session session) throws HsqlException {}

    public int getId() {
        return tableName.hashCode();
    }

    public boolean hasIdentityColumn() {
        return identityColumn != -1;
    }

    public long getNextIdentity() {
        return identitySequence.peek();
    }

    /**
     * Match two columns arrays for length and type of columns
     *
     * @param col column array from this Table
     * @param other the other Table object
     * @param othercol column array from the other Table
     * @throws HsqlException if there is a mismatch
     */
    void checkColumnsMatch(int[] col, Table other,
                           int[] othercol) throws HsqlException {

        if (col.length != othercol.length) {
            throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
        }

        for (int i = 0; i < col.length; i++) {

            // integrity check - should not throw in normal operation
            if (col[i] >= getColumnCount()
                    || othercol[i] >= other.getColumnCount()) {
                throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
            }

            Type type      = colTypes[col[i]];
            Type otherType = other.colTypes[othercol[i]];

            try {
                type.getCombinedType(otherType, Expression.EQUAL);
            } catch (HsqlException e) {
                throw Trace.error(Trace.COLUMN_TYPE_MISMATCH);
            }
        }
    }

    void checkColumnsMatch(Column column, int colIndex) throws HsqlException {

        Type type = colTypes[colIndex];

        try {
            type.getCombinedType(column.getType(), Expression.EQUAL);
        } catch (HsqlException e) {
            throw Trace.error(Trace.COLUMN_TYPE_MISMATCH);
        }
    }

    void setAsType(int newType, DataFileCache newCache) {

        tableType = newType;

        if (newType == Table.MEMORY_TABLE) {
            indexType = Index.MEMORY_INDEX;
            cache     = null;
            isCached  = false;
        }

        if (newType == Table.CACHED_TABLE || newType == Table.CACHED_RESULT) {
            indexType = Index.DISK_INDEX;
            cache     = newCache;
            isCached  = true;
            rowStore  = new RowStore();
        }
    }

    /**
     * For removal or addition of columns, constraints and indexes
     *
     * Does not work in this form for FK's as Constraint.ConstraintCore
     * is not transfered to a referencing or referenced table
     */
    Table moveDefinition(Session session, Column column,
                         Constraint constraint, Index index, int colIndex,
                         int adjust, OrderedHashSet dropConstraints,
                         OrderedHashSet dropIndexes,
                         OrderedHashSet dropTriggers) throws HsqlException {

        boolean newPK = false;

        if (constraint != null
                && constraint.constType == Constraint.PRIMARY_KEY) {
            newPK = true;
        }

        Table tn = duplicate();

        for (int i = 0; i < getColumnCount(); i++) {
            Column col = (Column) columnList.get(i);

            if (i == colIndex) {
                if (column != null) {
                    tn.addColumn(column);
                }

                if (adjust <= 0) {
                    continue;
                }
            }

            tn.addColumn(col);
        }

        if (getColumnCount() == colIndex) {
            tn.addColumn(column);
        }

        int[] pkCols = null;

        if (hasPrimaryKey()
                && !dropConstraints.contains(
                    getPrimaryConstraint().getName())) {
            pkCols = primaryKeyCols;
            pkCols = ArrayUtil.toAdjustedColumnArray(pkCols, colIndex, adjust);
        } else if (newPK) {
            pkCols = constraint.getMainColumns();
        }

        tn.createPrimaryKey(getIndex(0).getName(), pkCols, false);

        for (int i = 1; i < indexList.length; i++) {
            Index idx = indexList[i];

            if (dropIndexes.contains(idx.getName())) {
                continue;
            }

            int[] colarr = ArrayUtil.toAdjustedColumnArray(idx.getColumns(),
                colIndex, adjust);

            idx = tn.createIndexStructure(colarr, idx.getColumnDesc(),
                                          idx.getName(), idx.isUnique(),
                                          idx.isConstraint(), idx.isForward);

            tn.addIndex(idx);
        }

        if (index != null) {
            tn.addIndex(index);
        }

        HsqlArrayList newList = new HsqlArrayList();

        if (newPK) {
            constraint.core.mainIndex = tn.indexList[0];

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
            constraint.updateTable(session, this, tn, -1, 0);
            newList.add(constraint);
        }

        tn.constraintList = new Constraint[newList.size()];

        newList.toArray(tn.constraintList);
        tn.setBestRowIdentifiers();

        if (dropTriggers == null) {
            tn.triggerLists = triggerLists;
        } else {
            tn.triggerLists = new HsqlArrayList[triggerLists.length];

            for (int i = 0; i < triggerLists.length; i++) {
                HsqlArrayList v = triggerLists[i];

                if (v == null) {
                    continue;
                }

                v                  = new HsqlArrayList(v.toArray(), v.size());
                tn.triggerLists[i] = v;

                for (int j = v.size() - 1; j >= 0; j--) {
                    TriggerDef td = (TriggerDef) v.get(j);

                    if (dropTriggers.contains(td.name)) {
                        v.remove(j);
                        td.terminate();
                    }
                }

                if (v.isEmpty()) {
                    triggerLists[i] = null;
                }
            }
        }

        return tn;
    }

    /**
     * Used for drop / retype column.
     */
    void checkColumnInCheckConstraint(int colIndex) throws HsqlException {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.constType == Constraint.CHECK && !c.isNotNull()
                    && c.hasColumn(colIndex)) {
                HsqlName name = c.getName();

                throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                                  name.schema.name + '.' + name.name);
            }
        }
    }

    /**
     * Used for retype column. Checks whether column is in an FK or is
     * referenced by a FK
     * @param colIndex index
     */
    void checkColumnInFKConstraint(int colIndex) throws HsqlException {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumn(colIndex)
                    && (c.getType() == Constraint.MAIN
                        || c.getType() == Constraint.FOREIGN_KEY)) {
                HsqlName name = c.getName();

                throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                                  name.schema.name + '.' + name.name);
            }
        }
    }

    /**
     * Returns list of constraints dependent only on one column
     */
    OrderedHashSet getDependentConstraints(int colIndex) throws HsqlException {

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
    OrderedHashSet getContainingConstraints(int colIndex)
    throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumnPlus(colIndex)) {
                set.add(c);
            }
        }

        return set;
    }

    OrderedHashSet getContainingIndexNames(int colIndex) throws HsqlException {

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
    OrderedHashSet getDependentConstraints(Constraint constraint)
    throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getType() == Constraint.MAIN) {
                if (c.core.uniqueName == c.getName()) {
                    set.add(c);
                }
            }
        }

        return set;
    }

    public OrderedHashSet getDependentExternalConstraints() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getType() == Constraint.MAIN
                    || c.getType() == Constraint.FOREIGN_KEY) {
                if (c.core.mainTable != c.core.refTable) {
                    set.add(c);
                }
            }
        }

        return set;
    }

    /**
     * Used for column defaults and nullability. Checks whether column is in an FK.
     * @param colIndex index of column
     * @param refOnly only check FK columns, not referenced columns
     */
    void checkColumnInFKConstraint(int colIndex,
                                   int actionType) throws HsqlException {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getType() == Constraint.FOREIGN_KEY && c.hasColumn(colIndex)
                    && (actionType == c.getUpdateAction()
                        || actionType == c.getDeleteAction())) {
                HsqlName name = c.getName();

                throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                                  name.schema.name + '.' + name.name);
            }
        }
    }

    /**
     *  Returns the count of user defined columns.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     *  Returns the count of indexes on this table.
     */
    public int getIndexCount() {
        return indexList.length;
    }

    /**
     *  Returns the identity column or null.
     */
    int getIdentityColumn() {
        return identityColumn;
    }

    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getColumnIndex(String c) throws HsqlException {

        int i = findColumn(c);

        if (i == -1) {
            throw Trace.error(Trace.COLUMN_NOT_FOUND, c);
        }

        return i;
    }

    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findColumn(String c) {

        int index = columnList.getIndex(c);

        return index;
    }

    /**
     *  Returns the primary index (user defined or system defined)
     */
    public Index getPrimaryIndex() {
        return getIndex(0);
    }

    /**
     *  Return the user defined primary key column indexes, or empty array for system PK's.
     */
    public int[] getPrimaryKey() {
        return primaryKeyCols;
    }

    public Type[] getPrimaryKeyTypes() {
        return primaryKeyTypes;
    }

    public boolean hasPrimaryKey() {
        return !(primaryKeyCols.length == 0);
    }

    public int[] getBestRowIdentifiers() {
        return bestRowIdentifierCols;
    }

    public boolean isBestRowIdentifiersStrict() {
        return bestRowIdentifierStrict;
    }

    /**
     * This method is called whenever there is a change to table structure and
     * serves two porposes: (a) to reset the best set of columns that identify
     * the rows of the table (b) to reset the best index that can be used
     * to find rows of the table given a column value.
     *
     * (a) gives most weight to a primary key index, followed by a unique
     * address with the lowest count of nullable columns. Otherwise there is
     * no best row identifier.
     *
     * (b) finds for each column an index with a corresponding first column.
     * It uses any type of visible index and accepts the one with the largest
     * column count.
     *
     * bestIndex is the user defined, primary key, the first unique index, or
     * the first non-unique index. NULL if there is no user-defined index.
     *
     */
    public void setBestRowIdentifiers() {

        int[]   briCols      = null;
        int     briColsCount = 0;
        boolean isStrict     = false;
        int     nNullCount   = 0;

        // ignore if called prior to completion of primary key construction
        if (colNotNull == null) {
            return;
        }

        bestIndex          = null;
        bestIndexForColumn = new int[columnList.size()];

        ArrayUtil.fillArray(bestIndexForColumn, -1);

        for (int i = 0; i < indexList.length; i++) {
            Index index     = indexList[i];
            int[] cols      = index.getColumns();
            int   colsCount = index.getVisibleColumns();

            if (i == 0) {

                // ignore system primary keys
                if (hasPrimaryKey()) {
                    isStrict = true;
                } else {
                    continue;
                }
            }

            if (bestIndexForColumn[cols[0]] == -1) {
                bestIndexForColumn[cols[0]] = i;
            } else {
                Index existing = indexList[bestIndexForColumn[cols[0]]];

                if (colsCount > existing.getColumns().length) {
                    bestIndexForColumn[cols[0]] = i;
                }
            }

            if (!index.isUnique()) {
                if (bestIndex == null) {
                    bestIndex = index;
                }

                continue;
            }

            int nnullc = 0;

            for (int j = 0; j < colsCount; j++) {
                if (colNotNull[cols[j]]) {
                    nnullc++;
                }
            }

            if (bestIndex != null) {
                bestIndex = index;
            }

            if (nnullc == colsCount) {
                if (briCols == null || briColsCount != nNullCount
                        || colsCount < briColsCount) {

                    //  nothing found before ||
                    //  found but has null columns ||
                    //  found but has more columns than this index
                    briCols      = cols;
                    briColsCount = colsCount;
                    nNullCount   = colsCount;
                    isStrict     = true;
                }

                continue;
            } else if (isStrict) {
                continue;
            } else if (briCols == null || colsCount < briColsCount
                       || nnullc > nNullCount) {

                //  nothing found before ||
                //  found but has more columns than this index||
                //  found but has fewer not null columns than this index
                briCols      = cols;
                briColsCount = colsCount;
                nNullCount   = nnullc;
            }
        }

        // remove rowID column from bestRowIdentiferCols
        bestRowIdentifierCols = briCols == null
                                || briColsCount == briCols.length ? briCols
                                                                  : ArrayUtil
                                                                  .arraySlice(briCols,
                                                                      0, briColsCount);
        bestRowIdentifierStrict = isStrict;

        if (hasPrimaryKey()) {
            bestIndex = getPrimaryIndex();
        }
    }

    /**
     * Sets the SQL default value for a columm.
     */
    void setDefaultExpression(int columnIndex, Expression def) {

        Column column = getColumn(columnIndex);

        column.setDefaultExpression(def);
        setColumnTypeVars(columnIndex);
    }

    /**
     * sets the flag for the presence of any default expression
     */
    void resetDefaultsFlag() {

        hasDefaultValues = false;

        for (int i = 0; i < colDefaults.length; i++) {
            hasDefaultValues = hasDefaultValues || colDefaults[i] != null;
        }
    }

    public DataFileCache getCache() {
        return cache;
    }

    /**
     *  Used to create an index automatically for system tables or subqueries.
     */
    Index createIndexForColumns(int[] columns) {

        try {
            HsqlName indexName = database.nameManager.newAutoName("IDX_T",
                getSchemaName(), getName(), SchemaObject.INDEX);

            return createIndex(columns, null, indexName, false, false, false);
        } catch (Exception e) {}

        return null;
    }

    /**
     *  Finds an existing index for a column group
     */
    Index getIndexForColumn(int col) {

        int i = bestIndexForColumn[col];

        if (i == 1 && (tableType == Table.SYSTEM_SUBQUERY
                       || tableType == Table.SYSTEM_TABLE)) {
            return createIndexForColumns(new int[]{ col });
        }

        return i == -1 ? null
                       : this.indexList[i];
    }

    /**
     *  Finds an existing index for a column group
     */
    Index getIndexForColumns(int[] cols) {

        int i = bestIndexForColumn[cols[0]];

        if (i == 1 && (tableType == Table.SYSTEM_SUBQUERY
                       || tableType == Table.SYSTEM_TABLE)) {
            return createIndexForColumns(cols);
        }

        return i == -1 ? null
                       : this.indexList[i];
    }

    boolean isIndexed(int colIndex) {
        return bestIndexForColumn[colIndex] != -1;
    }

    /**
     * Finds an existing index for a column set or create one for temporary
     * tables
     */
    Index getIndexForColumns(OrderedIntHashSet set) {

        int   maxMatchCount = 0;
        Index selected      = null;

        if (set.isEmpty()) {
            return null;
        }

        for (int i = 0, count = indexList.length; i < count; i++) {
            Index currentindex = getIndex(i);
            int[] indexcols    = currentindex.getColumns();
            int   matchCount   = set.getOrderedMatchCount(indexcols);

            if (matchCount == 0) {
                continue;
            }

            if (matchCount == indexcols.length) {
                return currentindex;
            }

            if (matchCount > maxMatchCount) {
                maxMatchCount = matchCount;
                selected      = currentindex;
            }
        }

        if (selected == null
                && (tableType == Table.SYSTEM_SUBQUERY
                    || tableType == Table.SYSTEM_TABLE)) {
            return createIndexForColumns(set.toArray());
        }

        return selected;
    }

    boolean hasUniqueNotNullIndexForColumns(boolean[] usedColumns) {

        for (int i = 0, count = indexList.length; i < count; i++) {
            Index currentindex = getIndex(i);
            int[] indexCols    = currentindex.getColumns();

            if (ArrayUtil.areIntIndexesInBooleanArray(indexCols, colNotNull)
                    && ArrayUtil.areIntIndexesInBooleanArray(indexCols,
                        usedColumns)) {
                return true;
            }
        }

        return false;
    }

    boolean areColumnsNotNull(int[] indexes) {
        return ArrayUtil.areIntIndexesInBooleanArray(indexes, colNotNull);
    }

    /**
     *  Return the list of file pointers to root nodes for this table's
     *  indexes.
     */
    public int[] getIndexRootsArray() {

        int[] roots = new int[getIndexCount()];

        for (int i = 0; i < getIndexCount(); i++) {
            roots[i] = indexList[i].getRoot();
        }

        return roots;
    }

    /**
     * Returns the string consisting of file pointers to roots of indexes
     * plus the next identity value (hidden or user defined). This is used
     * with CACHED tables.
     */
    String getIndexRoots() {

        String       roots = StringUtil.getList(getIndexRootsArray(), " ", "");
        StringBuffer s     = new StringBuffer(roots);

/*
        s.append(' ');
        s.append(identitySequence.peek());
*/
        return s.toString();
    }

    /**
     *  Sets the index roots of a cached/text table to specified file
     *  pointers. If a
     *  file pointer is -1 then the particular index root is null. A null index
     *  root signifies an empty table. Accordingly, all index roots should be
     *  null or all should be a valid file pointer/reference.
     */
    public void setIndexRoots(int[] roots) throws HsqlException {

        Trace.check(isCached, Trace.TABLE_NOT_FOUND);

        for (int i = 0; i < getIndexCount(); i++) {
            int p = roots[i];
            Row r = null;

            if (p != -1) {
                r = (CachedRow) rowStore.get(p);
            }

            Node f = null;

            if (r != null) {
                f = r.getNode(i);
            }

            indexList[i].setRoot(null, f);
        }
    }

    /**
     *  Sets the index roots and next identity.
     */
    void setIndexRoots(String s) throws HsqlException {

        Trace.check(isCached, Trace.TABLE_NOT_FOUND);

        Tokenizer t     = new Tokenizer(s);
        int[]     roots = new int[getIndexCount()];

        for (int i = 0; i < getIndexCount(); i++) {
            int v = t.getInt();

            roots[i] = v;
        }

        setIndexRoots(roots);
    }

    /**
     *  Shortcut for creating system table PK's.
     */
    void createPrimaryKey(int[] cols) {
        createPrimaryKey(null, cols, false);
    }

    /**
     *  Shortcut for creating default PK's.
     */
    public void createPrimaryKey() {
        createPrimaryKey(null, null, false);
    }

    /**
     *  Creates a single or multi-column primary key and index. sets the
     *  colTypes array. Finalises the creation of the table. (fredt@users)
     */
    public void createPrimaryKey(HsqlName indexName, int[] columns,
                                 boolean columnsNotNull) {

        if (primaryKeyCols != null) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Table");
        }

        if (columns == null) {
            columns = new int[0];
        } else {
            for (int i = 0; i < columns.length; i++) {
                getColumn(columns[i]).setPrimaryKey(true);
            }
        }

        primaryKeyCols = columns;

        setColumnStructures();
        setPrimaryKeyStructures();

        HsqlName name = indexName;

        if (name == null) {
            name = database.nameManager.newAutoName("IDX", getSchemaName(),
                    getName(), SchemaObject.INDEX);
        }

        createPrimaryIndex(columns, name);
        setBestRowIdentifiers();
    }

    void setPrimaryKeyStructures() {

        primaryKeyTypes = new Type[primaryKeyCols.length];

        ArrayUtil.copyColumnValues(colTypes, primaryKeyCols, primaryKeyTypes);

        primaryKeyColsSequence = new int[primaryKeyCols.length];

        ArrayUtil.fillSequence(primaryKeyColsSequence);
    }

    void setColumnStructures() {

        int columnCount = getColumnCount();

        colTypes         = new Type[columnCount];
        colDefaults      = new Expression[columnCount];
        colNotNull       = new boolean[columnCount];
        defaultColumnMap = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            setColumnTypeVars(i);
        }

        resetDefaultsFlag();
    }

    void setColumnTypeVars(int i) {

        Column column = getColumn(i);

        colTypes[i]         = column.getType();
        colNotNull[i]       = column.isPrimaryKey() || !column.isNullable();
        defaultColumnMap[i] = i;

        if (column.isIdentity()) {
            identitySequence = column.getIdentitySequence();
            identityColumn   = i;
        } else if (identityColumn == i) {
            identityColumn = -1;
        }

        colDefaults[i] = column.getDefaultExpression();

        resetDefaultsFlag();
    }

    void createPrimaryIndex(int[] pkcols, HsqlName name) {

        Index newindex = new Index(database, name, this, pkcols, null,
                                   primaryKeyTypes, true, true, false);

        addIndex(newindex);
    }

    /**
     *  Create new memory-resident index. For MEMORY and TEXT tables.
     */
    public Index createIndex(int[] columns, boolean[] descending,
                             HsqlName name, boolean unique,
                             boolean constraint,
                             boolean forward) throws HsqlException {

        int newindexNo = createIndexStructureGetNo(columns, descending, name,
            unique, constraint, forward);
        Index newIndex = indexList[newindexNo];

        insertIndexNodes(newIndex, newindexNo);

        return newIndex;
    }

    private void insertIndexNodes(Index newIndex,
                                  int newindexNo) throws HsqlException {

        Session       session      = null;
        Index         primaryindex = getPrimaryIndex();
        RowIterator   it           = primaryindex.firstRow(session);
        int           rowCount     = 0;
        HsqlException error        = null;

        try {
            while (it.hasNext()) {
                Row  row      = it.getNext();
                Node backnode = row.getNode(newindexNo - 1);
                Node newnode  = Node.newNode(row, newindexNo, this);

                newnode.nNext  = backnode.nNext;
                backnode.nNext = newnode;

                // count before inserting
                rowCount++;

                newIndex.insert(session, row, newindexNo);
            }

            return;
        } catch (java.lang.OutOfMemoryError e) {
            error = Trace.error(Trace.OUT_OF_MEMORY);
        } catch (HsqlException e) {
            error = e;
        }

        // backtrack on error
        // rowCount rows have been modified
        it = primaryindex.firstRow(session);

        for (int i = 0; i < rowCount; i++) {
            Row  row      = it.getNext();
            Node backnode = row.getNode(0);
            int  j        = newindexNo;

            while (--j > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, null,
                newindexNo, -1);

        setBestRowIdentifiers();

        throw error;
    }

    /**
     * Creates the internal structures for an index.
     */
    Index createAndAddIndexStructure(int[] columns, HsqlName name,
                                     boolean unique, boolean constraint,
                                     boolean forward) {

        int i = createIndexStructureGetNo(columns, null, name, unique,
                                          constraint, forward);

        return indexList[i];
    }

    int createIndexStructureGetNo(int[] columns, boolean[] descending,
                                  HsqlName name, boolean unique,
                                  boolean constraint, boolean forward) {

        Index newindex = createIndexStructure(columns, descending, name,
                                              unique, constraint, forward);
        int indexNo = addIndex(newindex);

        setBestRowIdentifiers();

        return indexNo;
    }

    Index createIndexStructure(int[] columns, boolean[] descending,
                               HsqlName name, boolean unique,
                               boolean constraint, boolean forward) {

        if (primaryKeyCols == null) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "createIndex");
        }

        int    s     = columns.length;
        int[]  cols  = new int[s];
        Type[] types = new Type[s];

        for (int j = 0; j < s; j++) {
            cols[j]  = columns[j];
            types[j] = colTypes[cols[j]];
        }

        Index newIndex = new Index(database, name, this, cols, descending,
                                   types, unique, constraint, forward);

        return newIndex;
    }

    int addIndex(Index index) {

        index.setTable(this);

        int i = 0;

        for (; i < indexList.length; i++) {
            Index current = indexList[i];
            int order = index.getIndexOrderValue()
                        - current.getIndexOrderValue();

            if (order < 0) {
                break;
            }
        }

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, index, i,
                1);

        return i;
    }

    public void setIndexes(Index[] indexes) {
        this.indexList = indexes;
    }

    /**
     * returns false if the table has to be recreated in order to add / drop
     * indexes. Only CACHED tables return false.
     */
    boolean isIndexingMutable() {
        return !isIndexCached();
    }

    /**
     *  Checks for use of a named index in table constraints,
     *  while ignorring a given set of constraints.
     * @throws  HsqlException if index is used in a constraint
     */
    void checkDropIndex(String indexname) throws HsqlException {

        Index index = this.getIndex(indexname);

        if (index == null) {
            throw Trace.error(Trace.INDEX_NOT_FOUND, indexname);
        }

        if (index.isConstraint()) {
            throw Trace.error(Trace.DROP_PRIMARY_KEY, indexname);
        }

        return;
    }

    /**
     *  Returns true if the table has any rows at all.
     */
    public boolean isEmpty(Session session) {

        if (getIndexCount() == 0) {
            return true;
        }

        return getIndex(0).isEmpty(session);
    }

    /**
     * Returns direct mapping array.
     */
    int[] getColumnMap() {
        return defaultColumnMap;
    }

    /**
     * Returns empty mapping array.
     */
    int[] getNewColumnMap() {
        return new int[getColumnCount()];
    }

    /**
     * Returns empty boolean array.
     */
    public boolean[] getNewColumnCheckList() {
        return new boolean[getColumnCount()];
    }

    boolean[] getColumnCheckList(int[] columnIndexes) {

        boolean[] columnCheckList = new boolean[getColumnCount()];

        for (int i = 0; i < columnIndexes.length; i++) {
            int index = columnIndexes[i];

            columnCheckList[index] = true;
        }

        return columnCheckList;
    }

    int[] getColumnIndexes(OrderedHashSet set) throws HsqlException {

        int[] cols = new int[set.size()];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = getColumnIndex((String) set.get(i));
        }

        return cols;
    }

    /**
     *  Returns the Column object at the given index
     */
    public Column getColumn(int i) {
        return (Column) columnList.get(i);
    }

    public OrderedHashSet getColumnSet(int[] columnIndexes) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnIndexes.length; i++) {
            set.add(columnList.get(i));
        }

        return set;
    }

    public OrderedHashSet getColumnSet(boolean[] columnCheckList) {

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
                set.add(((Column) columnList.get(i)).getName());
            }
        }
    }

    public OrderedHashSet getColumnSet() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < getColumnCount(); i++) {
            set.add(columnList.get(i));
        }

        return set;
    }

    public OrderedHashSet getUniqueColumnNameSet() throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnList.size(); i++) {
            set.add(getColumn(i).getName().name);
        }

        if (set.size() != columnList.size()) {
            throw Trace.error(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE);
        }

        return set;
    }

    public void getUniqueColumnNamesInSet(Set columns,
                                          Set result) throws HsqlException {

        for (int i = 0; i < columnList.size(); i++) {
            String columnName = getColumn(i).getName().name;

            if (columns.contains(columnName)) {
                if (result.contains(columnName)) {
                    throw Trace.error(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE);
                }
            }
        }
    }

    /**
     * Returns empty Object array for a new row.
     */
    public Object[] getEmptyRowData() {
        return new Object[getColumnCount()];
    }

    /**
     * Returns array for a new row with SQL DEFAULT value for each column n
     * where exists[n] is false. This provides default values only where
     * required and avoids evaluating these values where they will be
     * overwritten.
     */
    Object[] getNewRowData(Session session,
                           boolean[] exists) throws HsqlException {

        Object[] data = new Object[getColumnCount()];
        int      i;

        if (exists != null && hasDefaultValues) {
            for (i = 0; i < getColumnCount(); i++) {
                Expression def = colDefaults[i];

                if (exists[i] == false && def != null) {
                    data[i] = def.getValue(session, colTypes[i]);
                }
            }
        }

        return data;
    }

    /**
     *  Performs Table structure modification and changes to the index nodes
     *  to remove a given index from a MEMORY or TEXT table. Not for PK index.
     *
     */
    public void dropIndex(Session session,
                          String indexname) throws HsqlException {

        // find the array index for indexname and remove
        int todrop = getIndexIndex(indexname);

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, null,
                todrop, -1);

        setBestRowIdentifiers();
        dropIndexFromRows(session, todrop);
    }

    void dropIndexFromRows(Session session, int index) throws HsqlException {

        RowIterator it = getPrimaryIndex().firstRow(session);

        while (it.hasNext()) {
            Row  row      = it.getNext();
            int  i        = index - 1;
            Node backnode = row.getNode(0);

            while (i-- > 0) {
                backnode = backnode.nNext;
            }

            backnode.nNext = backnode.nNext.nNext;
        }
    }

    /**
     * Moves the data from table to table.
     * The colindex argument is the index of the column that was
     * added or removed. The adjust argument is {-1 | 0 | +1}
     */
    void moveData(Session session, Table from, int colindex,
                  int adjust) throws HsqlException {

        Object colvalue = null;
        Column column   = null;

        if (adjust >= 0 && colindex != -1) {
            column   = getColumn(colindex);
            colvalue = column.getDefaultValue(session);
        }

        RowIterator it = from.getPrimaryIndex().firstRow(session);

        while (it.hasNext()) {
            Row      row  = it.getNext();
            Object[] o    = row.getData();
            Object[] data = getEmptyRowData();

            if (adjust == 0 && colindex != -1) {
                colvalue = column.getType().convertToType(session,
                        o[colindex], from.getColumn(colindex).getType());
            }

            ArrayUtil.copyAdjustArray(o, data, colvalue, colindex, adjust);
            systemSetIdentityColumn(session, data);
            enforceRowConstraints(session, data);

            Row newrow = newRow(data);

            indexRow(session, newrow);
        }
    }

    /**
     *  Mid level method for inserting rows. Performs constraint checks and
     *  fires row level triggers.
     */
    void insertRow(Session session, Object[] data) throws HsqlException {

        setIdentityColumn(session, data);

        if (triggerLists[Trigger.INSERT_BEFORE] != null) {
            fireBeforeTriggers(session, Trigger.INSERT_BEFORE, null, data,
                               null);
        }

        checkRowDataInsert(session, data);
        insertNoCheck(session, data);
    }

    /**
     * Multi-row insert method. Used for SELECT ... INTO tablename queries.
     * These tables are new, empty tables, with no constraints, triggers
     * column default values, column size enforcement whatsoever.
     *
     * Not used for INSERT INTO .... SELECT ... FROM queries
     */
    void insertIntoTable(Session session, Result result) throws HsqlException {

        insertResult(session, result);

        if (!isLogged) {
            return;
        }

        RowSetNavigator nav = result.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data = (Object[]) nav.getNext();

            database.logger.writeInsertStatement(session, this, data);
        }
    }

    /**
     *  Low level method for row insert.
     *  UNIQUE or PRIMARY constraints are enforced by attempting to
     *  add the row to the indexes.
     */
    private void insertNoCheck(Session session,
                               Object[] data) throws HsqlException {

        Row row = newRow(data);

        // this handles the UNIQUE constraints
        indexRow(session, row);

        if (session != null) {
            session.addInsertAction(this, row);
        }

        if (isLogged) {
            database.logger.writeInsertStatement(session, this, data);
        }
    }

    /**
     *
     */
    public void insertNoCheckFromLog(Session session,
                                     Object[] data) throws HsqlException {

        Row r = newRow(data);

        systemUpdateIdentityValue(data);
        indexRow(session, r);

        if (session != null) {
            session.addInsertAction(this, r);
        }
    }

    /**
     *  Low level method for restoring deleted rows
     */
    void insertNoCheckRollback(Session session, Row row,
                               boolean log) throws HsqlException {

        Row newrow = restoreRow(row);

        // instead of new row, use new routine so that the row does not use
        // rowstore.add(), which will allocate new space and different pos
        indexRow(session, newrow);

        if (log && isLogged) {
            database.logger.writeInsertStatement(session, this, row.getData());
        }
    }

    /**
     * Used for system table inserts. No checks. No identity
     * columns.
     */
    public int insertSys(Result ins) throws HsqlException {

        RowSetNavigator nav   = ins.getNavigator();
        int             count = 0;

        while (nav.hasNext()) {
            insertData(null, (Object[]) nav.getNext());

            count++;
        }

        return count;
    }

    /**
     * Used for subquery inserts. No checks. No identity
     * columns.
     */
    int insertResult(Session session, Result ins) throws HsqlException {

        int             count = 0;
        RowSetNavigator nav   = ins.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data = (Object[]) nav.getNext();
            Object[] newData =
                (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                    getColumnCount());

            insertData(session, newData);

            count++;
        }

        return count;
    }

    /**
     * Not for general use.
     * Used by ScriptReader to unconditionally insert a row into
     * the table when the .script file is read.
     */
    public void insertFromScript(Object[] data) throws HsqlException {
        systemUpdateIdentityValue(data);
        insertData(null, data);
    }

    /**
     * Used by the methods above.
     */
    public void insertData(Session session,
                           Object[] data) throws HsqlException {

        Row row = newRow(data);

        indexRow(session, row);
        commitRowToStore(row);
    }

    /**
     * Used by the system tables
     */
    public void insertSys(Object[] data) throws HsqlException {

        Row row = newRow(data);

        indexRow(null, row);
    }

    /**
     * Used by TextCache to insert a row into the indexes when the source
     * file is first read.
     */
    protected void insertFromTextSource(Session session,
                                        CachedRow row) throws HsqlException {

        Object[] data = row.getData();

        systemUpdateIdentityValue(data);
        enforceRowConstraints(session, data);

        int i = 0;

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(null, row, i);
            }
        } catch (HsqlException e) {
            Index   index        = indexList[i];
            boolean isconstraint = index.isConstraint();

            if (isconstraint) {
                throw Trace.error(Trace.VIOLATION_OF_UNIQUE_CONSTRAINT,
                                  index.getName().name);
            }

            throw e;
        }
    }

    /**
     * If there is an identity column in the table, sets
     * the value and/or adjusts the identiy value for the table.
     */
    protected void setIdentityColumn(Session session,
                                     Object[] data) throws HsqlException {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id == null) {
                id = (Number) identitySequence.getValueObject();
                data[identityColumn] = id;
            } else {
                identitySequence.userUpdate(id.longValue());
            }

            if (session != null) {
                session.setLastIdentity(id);
            }
        }
    }

    protected void systemSetIdentityColumn(Session session,
                                           Object[] data)
                                           throws HsqlException {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id == null) {
                id = (Number) identitySequence.getValueObject();
                data[identityColumn] = id;
            } else {
                identitySequence.userUpdate(id.longValue());
            }
        }
    }

    /**
     * If there is an identity column in the table, sets
     * the max identity value.
     */
    protected void systemUpdateIdentityValue(Object[] data)
    throws HsqlException {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id != null) {
                identitySequence.systemUpdate(id.longValue());
            }
        }
    }

    /**
     *  Enforce max field sizes according to SQL column definition.
     *  SQL92 13.8
     */
    void enforceRowConstraints(Session session,
                               Object[] data) throws HsqlException {

        for (int i = 0; i < defaultColumnMap.length; i++) {
            Type type = colTypes[i];

            if (database.sqlEnforceStrictSize) {
                data[i] = type.convertToTypeLimits(data[i]);
            }

            if (type.isDomainType()) {
                Constraint constraints[] =
                    ((DomainType) type).getConstraints();

                for (int j = 0; j < constraints.length; j++) {
                    constraints[i].checkCheckConstraint(session, this,
                                                        (Object) data[i]);
                }
            }

            if (data[i] == null) {
                if (colNotNull[i]) {
                    Trace.throwerror(Trace.TRY_TO_INSERT_NULL,
                                     "column: " + getColumn(i).columnName.name
                                     + " table: " + tableName.name);
                }
            }
        }
    }

    boolean hasTrigger(int trigVecIndex) {
        return triggerLists[trigVecIndex] != null
               && !triggerLists[trigVecIndex].isEmpty();
    }

    void fireAfterTriggers(Session session, int trigVecIndex,
                           HashMappedList rowSet,
                           int[] cols) throws HsqlException {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        HsqlArrayList trigVec = triggerLists[trigVecIndex];

        if (trigVec == null) {
            return;
        }

        for (int i = 0, size = trigVec.size(); i < size; i++) {
            TriggerDef td         = (TriggerDef) trigVec.get(i);
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (cols != null && td.getUpdateColumns() != null
                    && !ArrayUtil.haveCommonElement(td.getUpdateColumns(),
                        cols, cols.length)) {
                continue;
            }

            if (td.isForEachRow()) {
                for (int j = 0; j < rowSet.size(); j++) {
                    Object[] oldData = ((Row) rowSet.getKey(j)).getData();
                    Object[] newData = (Object[]) rowSet.get(j);

                    if (sqlTrigger) {
                        oldData = (Object[]) ArrayUtil.duplicateArray(oldData);
                        newData = (Object[]) ArrayUtil.duplicateArray(newData);
                    }

                    td.pushPair(session, oldData, newData);
                }
            } else {
                td.pushPair(session, null, null);
            }
        }
    }

    void fireAfterTriggers(Session session, int trigVecIndex,
                           RowSetNavigator rowSet) throws HsqlException {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        HsqlArrayList trigVec = triggerLists[trigVecIndex];

        if (trigVec == null) {
            return;
        }

        for (int i = 0, size = trigVec.size(); i < size; i++) {
            TriggerDef td         = (TriggerDef) trigVec.get(i);
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (td.hasOldTable()) {

                //
            }

            if (td.isForEachRow()) {
                while (rowSet.hasNext()) {
                    Object[] oldData = null;
                    Object[] newData = null;

                    switch (td.triggerType) {

                        case Trigger.DELETE_AFTER_ROW :
                            oldData = (Object[]) rowSet.getNext();

                            if (!sqlTrigger) {
                                oldData = (Object[]) ArrayUtil.duplicateArray(
                                    oldData);
                            }
                            break;

                        case Trigger.INSERT_AFTER_ROW :
                            newData = (Object[]) rowSet.getNext();

                            if (!sqlTrigger) {
                                newData = (Object[]) ArrayUtil.duplicateArray(
                                    newData);
                            }
                            break;
                    }

                    td.pushPair(session, oldData, newData);
                }

                rowSet.beforeFirst();
            } else {
                td.pushPair(session, null, null);
            }
        }
    }

    /**
     *  Fires all row-level triggers of the given set (trigger type)
     *
     */
    void fireBeforeTriggers(Session session, int trigVecIndex,
                            Object[] oldData, Object[] newData,
                            int[] cols) throws HsqlException {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        HsqlArrayList trigVec = triggerLists[trigVecIndex];

        if (trigVec == null) {
            return;
        }

        for (int i = 0, size = trigVec.size(); i < size; i++) {
            TriggerDef td         = (TriggerDef) trigVec.get(i);
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (cols != null && td.getUpdateColumns() != null
                    && !ArrayUtil.haveCommonElement(td.getUpdateColumns(),
                        cols, cols.length)) {
                continue;
            }

            if (td.isForEachRow()) {
                switch (td.triggerType) {

                    case Trigger.UPDATE_BEFORE_ROW :
                    case Trigger.DELETE_BEFORE_ROW :
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
     * Adds a trigger.
     */
    void addTrigger(TriggerDef trigDef) {

        if (triggerLists[trigDef.vectorIndex] == null) {
            triggerLists[trigDef.vectorIndex] = new HsqlArrayList();
        }

        triggerLists[trigDef.vectorIndex].add(trigDef);
    }

    /**
     * Drops a trigger.
     */
    TriggerDef getTrigger(String name) {

        // look in each trigger list of each type of trigger
        int numTrigs = TriggerDef.NUM_TRIGS;

        for (int tv = 0; tv < numTrigs; tv++) {
            HsqlArrayList v = triggerLists[tv];

            if (v == null) {
                continue;
            }

            for (int tr = v.size() - 1; tr >= 0; tr--) {
                TriggerDef td = (TriggerDef) v.get(tr);

                if (td.name.name.equals(name)) {
                    return td;
                }
            }
        }

        return null;
    }

    /**
     * Drops a trigger.
     */
    void removeTrigger(String name) {

        // look in each trigger list of each type of trigger
        int numTrigs = TriggerDef.NUM_TRIGS;

        for (int tv = 0; tv < numTrigs; tv++) {
            HsqlArrayList v = triggerLists[tv];

            if (v == null) {
                continue;
            }

            for (int tr = v.size() - 1; tr >= 0; tr--) {
                TriggerDef td = (TriggerDef) v.get(tr);

                if (td.name.name.equals(name)) {
                    v.remove(tr);
                    td.terminate();
                }
            }

            if (v.isEmpty()) {
                triggerLists[tv] = null;
            }
        }
    }

    /**
     * Drops all triggers.
     */
    void dropTriggers() {

        // look in each trigger list of each type of trigger
        int numTrigs = TriggerDef.NUM_TRIGS;

        for (int tv = 0; tv < numTrigs; tv++) {
            HsqlArrayList v = triggerLists[tv];

            if (v == null) {
                continue;
            }

            for (int tr = v.size() - 1; tr >= 0; tr--) {
                TriggerDef td = (TriggerDef) v.get(tr);

                td.terminate();
            }

            triggerLists[tv] = null;
        }
    }

    /**
     *  Delete method for referential  triggered actions.
     */
    void deleteRowAsTriggeredAction(Session session,
                                    Row row) throws HsqlException {

        Object[] data = row.getData();

        deleteNoCheck(session, row, true);
    }

    /**
     *  Mid level row delete method. Fires triggers but no integrity
     *  constraint checks.
     */
    void deleteNoRefCheck(Session session, Row row) throws HsqlException {

        Object[] data = row.getData();

        fireBeforeTriggers(session, Trigger.DELETE_BEFORE, data, null, null);
        deleteNoCheck(session, row, true);
    }

    /**
     * Low level row delete method. Removes the row from the indexes and
     * from the Cache.
     */
    private void deleteNoCheck(Session session, Row row,
                               boolean log) throws HsqlException {

        if (row.isCascadeDeleted()) {
            return;
        }

        Object[] data = row.getData();

        row = row.getUpdatedRow();

        for (int i = indexList.length - 1; i >= 0; i--) {
            Node node = row.getNode(i);

            indexList[i].delete(session, node);
        }

        row.delete();

        if (session != null) {
            session.addDeleteAction(this, row);
        }

        if (log && isLogged) {
            database.logger.writeDeleteStatement(session, this, data);
        }
    }

    /**
     * Basic delete with no logging or referential checks.
     */
    public void delete(Session session, Row row) throws HsqlException {

        for (int i = indexList.length - 1; i >= 0; i--) {
            Node node = row.getNode(i);

            indexList[i].delete(session, node);
        }

        row.delete();
    }

    /**
     * For log statements.
     */
    public void deleteNoCheckFromLog(Session session,
                                     Object[] data) throws HsqlException {

        Row row = null;

        if (hasPrimaryKey()) {
            RowIterator it = getPrimaryIndex().findFirstRow(session, data,
                primaryKeyColsSequence);

            row = it.getNext();
        } else if (bestIndex == null) {
            RowIterator it = getPrimaryIndex().firstRow(session);

            while (true) {
                row = it.getNext();

                if (row == null) {
                    break;
                }

                if (Index.compareRows(
                        session, row.getData(), data, defaultColumnMap,
                        colTypes) == 0) {
                    break;
                }
            }
        } else {
            RowIterator it = bestIndex.findFirstRow(session, data);

            while (true) {
                row = it.getNext();

                if (row == null) {
                    break;
                }

                Object[] rowdata = row.getData();

                // reached end of range
                if (bestIndex.compareRowNonUnique(
                        session, data, bestIndex.getColumns(), rowdata) != 0) {
                    row = null;

                    break;
                }

                if (Index.compareRows(
                        session, rowdata, data, defaultColumnMap,
                        colTypes) == 0) {
                    break;
                }
            }
        }

        if (row == null) {
            return;
        }

        // not necessary for log deletes
        database.txManager.checkDelete(session, row);

        for (int i = indexList.length - 1; i >= 0; i--) {
            Node node = row.getNode(i);

            indexList[i].delete(session, node);
        }

        row.delete();

        if (session != null) {
            session.addDeleteAction(this, row);
        }
    }

    /**
     * Low level row delete method. Removes the row from the indexes and
     * from the Cache. Used by rollback.
     */
    void deleteNoCheckRollback(Session session, Row row,
                               boolean log) throws HsqlException {

        row = indexList[0].findRow(session, row);

        for (int i = indexList.length - 1; i >= 0; i--) {
            Node node = row.getNode(i);

            indexList[i].delete(session, node);
        }

        row.delete();
        removeRowFromStore(row);

        if (log && isLogged) {
            database.logger.writeDeleteStatement(session, this, row.getData());
        }
    }

    void updateRowSet(Session session, HashMappedList rowSet, int[] cols,
                      boolean isTriggeredSet) throws HsqlException {

        for (int i = 0; i < rowSet.size(); i++) {
            Row row = (Row) rowSet.getKey(i);

            if (row.isCascadeDeleted()) {
                if (isTriggeredSet) {
                    rowSet.remove(i);

                    i--;

                    continue;
                } else {
                    throw Trace.error(Trace.TRIGGERED_DATA_CHANGE);
                }
            }
        }

        for (int i = 0; i < rowSet.size(); i++) {
            Row      row  = (Row) rowSet.getKey(i);
            Object[] data = (Object[]) rowSet.get(i);

            checkRowDataUpdate(session, data, cols);
            deleteNoCheck(session, row, true);
        }

        for (int i = 0; i < rowSet.size(); i++) {
            Object[] data = (Object[]) rowSet.get(i);

            insertNoCheck(session, data);
        }
    }

    void checkRowDataInsert(Session session,
                            Object[] data) throws HsqlException {

        enforceRowConstraints(session, data);

        if (database.isReferentialIntegrity()) {
            for (int i = 0, size = constraintList.length; i < size; i++) {
                constraintList[i].checkInsert(session, this, data);
            }
        }
    }

    void checkRowDataUpdate(Session session, Object[] data,
                            int[] cols) throws HsqlException {

        enforceRowConstraints(session, data);

        for (int j = 0; j < constraintList.length; j++) {
            Constraint c = constraintList[j];

            if (c.getType() == Constraint.CHECK && !c.isNotNull()) {
                c.checkCheckConstraint(session, this, data);
            }
        }
    }

    /**
     *  Returns true if table is CACHED
     */
    boolean isIndexCached() {
        return isCached;
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
     *  return the named constriant
     */
    Constraint getConstraint(String constraintName) {

        int i = getConstraintIndex(constraintName);

        return (i < 0) ? null
                       : (Constraint) constraintList[i];
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
    }

    void renameColumn(Column column, String newName,
                      boolean isquoted) throws HsqlException {

        String oldname = column.columnName.name;
        int    i       = getColumnIndex(oldname);

        columnList.setKey(i, newName);
        column.columnName.rename(newName, isquoted);
    }

    void removeDomainOrType(HsqlName name) {

        for (int i = 0; i < colTypes.length; i++) {
            if (colTypes[i].isDomainType() || colTypes[i].isDistinctType()) {
                if (name == ((SchemaObject) colTypes[i]).getName()) {
                    Type baseType = colTypes[i].getParentType();

                    getColumn(i).setType(baseType);
                    setColumnTypeVars(i);
                }
            }
        }
    }

    /**
     *  Returns an array of int valuse indicating the SQL type of the columns
     */
    public Type[] getColumnTypes() {
        return colTypes;
    }

    /**
     *  Returns the Index object at the given index
     */
    public Index getIndex(int i) {
        return indexList[i];
    }

    public Index[] getIndexes() {
        return indexList;
    }

    public HsqlArrayList[] getTriggers() {
        return triggerLists;
    }

    /**
     *  Used by CACHED tables to fetch a Row from the Cache, resulting in the
     *  Row being read from disk if it is not in the Cache.
     *
     *  TEXT tables pass the memory resident Node parameter so that the Row
     *  and its index Nodes can be relinked.
     */
    public CachedRow getRow(int pos, Node primarynode) {

        if (isText) {
            CachedDataRow row = (CachedDataRow) rowStore.get(pos);

            row.nPrimaryNode = primarynode;

            return row;
        } else if (isCached) {
            return (CachedRow) rowStore.get(pos);
        }

        return null;
    }

    /**
     * As above, only for CACHED tables
     */
    public CachedRow getRow(int pos) {
        return (CachedRow) rowStore.get(pos);
    }

    /**
     * As above, only for CACHED tables
     */
    public CachedRow getRow(long id) {
        return (CachedRow) rowStore.get((int) id);
    }

    void registerRow(CachedRow row) {}

    /**
     * called in autocommit mode or by transaction manager when a a delete is committed
     */
    void removeRowFromPersistence(Row row) {

        if (isText && cache != null) {
            rowStore.removePersistence(row.getPos());
        }
    }

    /**
     * called in autocommit mode or by transaction manager when a a delete is committed
     */
    void removeRowFromStore(Row row) throws HsqlException {

        if (isCached || isText && cache != null) {
            rowStore.remove(row.getPos());
        }
    }

    void releaseRowFromStore(Row row) throws HsqlException {

        if (isCached || isText && cache != null) {
            rowStore.release(row.getPos());
        }
    }

    void commitRowToStore(Row row) {

        if (isText && cache != null) {
            rowStore.commit(row);
        }
    }

    public void indexRow(Session session, Row row) throws HsqlException {

        int i = 0;

        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, row, i);
            }
        } catch (HsqlException e) {
            Index   index        = indexList[i];
            boolean isconstraint = index.isConstraint();

            // unique index violation - rollback insert
            for (--i; i >= 0; i--) {
                Node n = row.getNode(i);

                indexList[i].delete(session, n);
            }

            row.delete();
            removeRowFromStore(row);

            throw e;
        }
    }

    /**
     * Inserts into the index at given offset only
     */
    public void indexRow(Session session, Row row,
                         int offset) throws HsqlException {
        indexList[offset].insert(session, row, offset);
    }

    void reindex(Session session, Index index) throws HsqlException {

        int offset = -1;

        for (int i = 0; i < indexList.length; i++) {
            if (indexList[i] == index) {
                offset = i;

                break;
            }
        }

        indexList[offset].clearAll(session);

        RowIterator it = this.getPrimaryIndex().firstRow(session);

        while (it.hasNext()) {
            Row row = it.getNext();

            indexRow(session, row, offset);
        }
    }

    /**
     *
     */
    public void clearAllData(Session session) {

        for (int i = 0; i < indexList.length; i++) {
            indexList[i].clearAll(session);
        }

        rowIdSequence.reset();

        if (identitySequence != null) {
            identitySequence.reset();
        }
    }

    /** @todo -- release the rows from cache */
    void drop() throws HsqlException {}

    public boolean isWritable() {
        return !isReadOnly && !database.databaseReadOnly
               && !(database.isFilesReadOnly() && (isCached || isText));
    }

    public int getRowCount(Session session) throws HsqlException {
        return getPrimaryIndex().size(session);
    }

    /**
     * Necessary when over Integer.MAX_VALUE Row objects have been generated
     * for a memory table.
     */
    public void resetRowId(Session session) throws HsqlException {

        if (isCached) {
            return;
        }

        rowIdSequence = new NumberSequence(null, 0, 1, Type.SQL_BIGINT);

        RowIterator it = getPrimaryIndex().firstRow(session);;

        while (it.hasNext()) {
            Row row = it.getNext();
            int pos = (int) rowIdSequence.getValue();

            row.setPos(pos);
        }
    }

    /**
     *  Factory method instantiates a Row based on table type.
     */
    public Row newRow(Object[] o) throws HsqlException {

        Row row;

        try {
            if (isText) {
                row = new CachedDataRow(this, o);

                rowStore.add(row);
            } else if (isCached) {
                row = new CachedRow(this, o);

                rowStore.add(row);
            } else {
                row = new Row(this, o);

                int pos = (int) rowIdSequence.getValue();

                row.setPos(pos);
            }
        } catch (IOException e) {
            throw new HsqlException(
                e, Trace.getMessage(Trace.GENERAL_IO_ERROR),
                Trace.GENERAL_IO_ERROR);
        }

        return row;
    }

    Row restoreRow(Row oldrow) throws HsqlException {

        Row row;

        try {
            if (isText) {
                row = new CachedDataRow(this, oldrow.oData);

                row.setStorageSize(oldrow.getStorageSize());
                row.setPos(oldrow.getPos());
                rowStore.restore(row);
            } else if (isCached) {
                row = new CachedRow(this, oldrow.oData);

                row.setStorageSize(oldrow.getStorageSize());
                row.setPos(oldrow.getPos());
                rowStore.restore(row);
            } else {
                row = new Row(this, oldrow.oData);

                row.setPos(oldrow.getPos());
            }
        } catch (IOException e) {
            throw new HsqlException(
                e, Trace.getMessage(Trace.GENERAL_IO_ERROR),
                Trace.GENERAL_IO_ERROR);
        }

        return row;
    }

    public class RowStore implements PersistentStore {

        public CachedObject get(int i) {

            try {
                return cache.get(i, this, false);
            } catch (HsqlException e) {
                return null;
            }
        }

        public CachedObject getKeep(int i) {

            try {
                return cache.get(i, this, true);
            } catch (HsqlException e) {
                return null;
            }
        }

        public int getStorageSize(int i) {

            try {
                return cache.get(i, this, false).getStorageSize();
            } catch (HsqlException e) {
                return 0;
            }
        }

        public void add(CachedObject row) throws IOException {
            cache.add(row);
        }

        public void restore(CachedObject row) throws IOException {
            cache.restore(row);
        }

        public CachedObject get(RowInputInterface in) {

            try {
                if (Table.this.isText) {
                    return new CachedDataRow(Table.this, in);
                }

                CachedObject row = new CachedRow(Table.this, in);

                return row;
            } catch (HsqlException e) {
                return null;
            } catch (IOException e1) {
                return null;
            }
        }

        public CachedObject getNewInstance(int size) {
            return null;
        }

        public void remove(int i) {

            try {
                cache.remove(i, this);
            } catch (IOException e) {}
        }

        public void removePersistence(int i) {

            try {
                cache.removePersistence(i, this);
            } catch (IOException e) {

                //
            }
        }

        public void release(int i) {
            cache.release(i);
        }

        public void commit(CachedObject row) {

            try {
                if (Table.this.isText) {
                    cache.saveRow(row);
                }
            } catch (IOException e1) {

                //
            }
        }
    }
}
