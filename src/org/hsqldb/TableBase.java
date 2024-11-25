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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.DataSpaceManager;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;

/**
 * The  base of all HSQLDB table implementations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.7.2
 */
public class TableBase implements Cloneable {

    // types of table
    public static final int INFO_SCHEMA_TABLE = 1;
    public static final int SYSTEM_SUBQUERY   = 2;
    public static final int TEMP_TABLE        = 3;
    public static final int MEMORY_TABLE      = 4;
    public static final int CACHED_TABLE      = 5;
    public static final int TEMP_TEXT_TABLE   = 6;
    public static final int TEXT_TABLE        = 7;
    public static final int VIEW_TABLE        = 8;
    public static final int RESULT_TABLE      = 9;
    public static final int TRANSITION_TABLE  = 10;
    public static final int FUNCTION_TABLE    = 11;
    public static final int SYSTEM_TABLE      = 12;
    public static final int CHANGE_SET_TABLE  = 13;
    public static final int MODULE_TABLE      = 14;

    //
    public static final int SCOPE_ROUTINE     = 20;
    public static final int SCOPE_STATEMENT   = 21;
    public static final int SCOPE_TRANSACTION = 22;
    public static final int SCOPE_SESSION     = 23;
    public static final int SCOPE_FULL        = 24;

    //
    public PersistentStore store;
    public int             persistenceScope;
    public long            persistenceId;
    int                    tableSpace = DataSpaceManager.tableIdDefault;

    //
    Index[]         indexList;                  // first index is the primary key index
    public Database database;
    int[]           bestRowIdentifierCols;      // column set for best index
    boolean         bestRowIdentifierStrict;    // true if it has no nullable column
    int[]           bestIndexForColumn;         // 'best' index for each column
    Index           bestIndex;                  // the best index overall
    Index           fullIndex;                  // index on all columns
    boolean[]       colNotNull;                 // nullability
    Type[]          colTypes;                   // types of columns
    protected int   columnCount;
    boolean[]       emptyColumnCheckList;

    //
    int               tableType;
    protected boolean isReadOnly;
    protected boolean isTemp;
    protected boolean isCached;
    protected boolean isText;
    boolean           isView;
    protected boolean isWithDataSource;
    public boolean    isSessionBased;
    protected boolean isSchemaBased;
    protected boolean isLogged;
    public boolean    isSystemVersioned;
    boolean           hasLobColumn;

    //
    TableBase() {}

    public TableBase duplicate() {

        TableBase copy;

        try {
            copy = (TableBase) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }

        copy.persistenceId = database.persistentStoreCollection.getNextId();

        return copy;
    }

    public final int getTableType() {
        return tableType;
    }

    /**
     * limited type change to info_schema_table
     */
    public final void setTableType(int type) {

        if (tableType == MEMORY_TABLE && type == INFO_SCHEMA_TABLE) {
            tableType        = type;
            persistenceScope = SCOPE_TRANSACTION;
            isSessionBased   = true;
            tableType        = type;
        }
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    public int getSpaceID() {
        return tableSpace;
    }

    public void setSpaceID(int id) {
        tableSpace = id;
    }

    long getId() {
        return 0;
    }

    public final boolean onCommitPreserve() {
        return persistenceScope == TableBase.SCOPE_SESSION;
    }

    public final RowIterator rowIterator(Session session) {
        PersistentStore store = getRowStore(session);

        return getDefaultIndex().firstRow(session, store, null, 0, null);
    }

    public final RowIterator rowIterator(PersistentStore store) {
        return getDefaultIndex().firstRow(store);
    }

    public final int getIndexCount() {
        return indexList.length;
    }

    public final Index getPrimaryIndex() {
        return indexList.length > 0
               ? indexList[0]
               : null;
    }

    public Index getDefaultIndex() {
        return indexList[0];
    }

    public final Type[] getPrimaryKeyTypes() {
        return indexList[0].getColumnTypes();
    }

    public final boolean hasPrimaryKey() {
        return indexList[0].getColumnCount() > 0;
    }

    public final int[] getPrimaryKey() {
        return indexList[0].getColumns();
    }

    /**
     *  Returns an array of Type indicating the SQL type of the columns
     */
    public final Type[] getColumnTypes() {
        return colTypes;
    }

    /**
     *  Returns the Index object at the given index
     */
    public final Index getIndex(int i) {
        return indexList[i];
    }

    /**
     *  Returns the indexes
     */
    public final Index[] getIndexList() {
        return indexList;
    }

    /**
     * Returns empty boolean array.
     */
    public final boolean[] getNewColumnCheckList() {
        return new boolean[getColumnCount()];
    }

    public final boolean[] getEmptyColumnCheckList() {
        return emptyColumnCheckList;
    }

    /**
     *  Returns the count of all visible columns.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     *  Returns the count of all columns.
     */
    public final int getDataColumnCount() {
        return colTypes.length;
    }

    public boolean isSystemVersioned() {
        return isSystemVersioned;
    }

    /**
     * This method is called whenever there is a change to table structure and
     * serves two purposes: (a) to reset the best set of columns that identify
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
    public final void setBestRowIdentifiers() {

        int[]   briCols      = null;
        int     briColsCount = 0;
        boolean isStrict     = false;
        int     nNullCount   = 0;

        // ignore if called prior to completion of primary key construction
        if (colNotNull == null) {
            return;
        }

        bestIndex          = null;
        bestIndexForColumn = new int[colTypes.length];

        ArrayUtil.fillArray(bestIndexForColumn, -1);

        for (int i = 0; i < indexList.length; i++) {
            Index index     = indexList[i];
            int[] cols      = index.getColumns();
            int   colsCount = index.getColumnCount();

            if (colsCount == 0) {
                continue;
            }

            if (i == 0) {
                isStrict = true;
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
                if (briCols == null
                        || briColsCount != nNullCount
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
            } else if (briCols == null
                       || colsCount < briColsCount
                       || nnullc > nNullCount) {

                //  nothing found before ||
                //  found but has more columns than this index||
                //  found but has fewer not null columns than this index
                briCols      = cols;
                briColsCount = colsCount;
                nNullCount   = nnullc;
            }
        }

        if (briCols == null || briColsCount == briCols.length) {
            bestRowIdentifierCols = briCols;
        } else {
            bestRowIdentifierCols = ArrayUtil.arraySlice(
                briCols,
                0,
                briColsCount);
        }

        bestRowIdentifierStrict = isStrict;

        if (indexList[0].getColumnCount() > 0) {
            bestIndex = indexList[0];
        }
    }

    public boolean[] getColumnNotNull() {
        return this.colNotNull;
    }

    public final void createPrimaryIndex(
            int[] pkcols,
            Type[] pktypes,
            HsqlName name) {
        Index newIndex = getNewPrimaryIndex(pkcols, pktypes, name);

        addIndexStructure(newIndex);
    }

    Index getNewPrimaryIndex(int[] pkcols, Type[] pktypes, HsqlName name) {

        long id = database.persistentStoreCollection.getNextId();

        return database.logger.newIndex(
            name,
            id,
            this,
            pkcols,
            null,
            null,
            pktypes,
            true,
            pkcols.length > 0,
            pkcols.length > 0,
            false);
    }

    public final Index createAndAddIndexStructure(
            Session session,
            HsqlName name,
            int[] columns,
            boolean[] descending,
            boolean[] nullsLast,
            boolean unique,
            boolean constraint,
            boolean forward) {

        Index newindex = createIndexStructure(
            name,
            columns,
            descending,
            nullsLast,
            false,
            unique,
            constraint,
            forward);

        addIndex(session, newindex);

        return newindex;
    }

    public final Index createIndexStructure(
            HsqlName name,
            int[] columns,
            boolean[] descending,
            boolean[] nullsLast,
            boolean primaryKey,
            boolean unique,
            boolean constraint,
            boolean forward) {

        int    s     = columns.length;
        int[]  cols  = new int[s];
        Type[] types = new Type[s];

        for (int j = 0; j < s; j++) {
            cols[j]  = columns[j];
            types[j] = colTypes[cols[j]];
        }

        long id = database.persistentStoreCollection.getNextId();
        Index newIndex = database.logger.newIndex(
            name,
            id,
            this,
            cols,
            descending,
            nullsLast,
            types,
            primaryKey,
            unique,
            constraint,
            forward);

        return newIndex;
    }

    /**
     *  Performs Table structure modification and changes to the index nodes
     *  to remove a given index from a MEMORY or TEXT table. Not for PK index.
     *
     */
    public void dropIndex(Session session, int todrop) {

        Index[] list = (Index[]) ArrayUtil.toAdjustedArray(
            indexList,
            null,
            todrop,
            -1);

        for (int i = 0; i < list.length; i++) {
            list[i].setPosition(i);
        }

        resetAccessorKeys(session, list);

        indexList = list;

        setBestRowIdentifiers();
    }

    final void addIndexStructure(Index index) {
        indexList = getNewIndexArray(index, indexList);

        setBestRowIdentifiers();
    }

    static Index[] getNewIndexArray(Index index, Index[] list) {

        int i = 0;

        for (; i < list.length; i++) {
            Index current = list[i];
            int order = index.getIndexOrderValue()
                        - current.getIndexOrderValue();

            if (order < 0) {
                break;
            }
        }

        boolean replacePK = index.isPrimaryKey()
                            && list.length > 0
                            && list[0].isPrimaryKey();

        if (replacePK) {
            list    = (Index[]) ArrayUtil.duplicateArray(list);
            list[0] = index;
        } else {
            list = (Index[]) ArrayUtil.toAdjustedArray(list, index, i, 1);
        }

        for (i = 0; i < list.length; i++) {
            list[i].setPosition(i);
        }

        return list;
    }

    final void addIndex(Session session, Index index) {

        Index[] list = getNewIndexArray(index, indexList);

        try {
            resetAccessorKeys(session, list);
        } catch (HsqlException e) {
            for (int i = 0; i < indexList.length; i++) {
                indexList[i].setPosition(i);
            }

            throw e;
        }

        indexList = list;

        setBestRowIdentifiers();
    }

    private void resetAccessorKeys(Session session, Index[] indexes) {
        if (store != null) {
            store.resetAccessorKeys(session, indexes);
        }
    }

    public final void setIndexes(Index[] indexes) {
        this.indexList = indexes;
    }

    public final Object[] getEmptyRowData() {
        return new Object[getDataColumnCount()];
    }

    /**
     *  Create new memory-resident index. For MEMORY and TEXT tables.
     */
    public final Index createIndex(
            Session session,
            HsqlName name,
            int[] columns,
            boolean[] descending,
            boolean[] nullsLast,
            boolean unique,
            boolean constraint,
            boolean forward) {

        Index newIndex = createAndAddIndexStructure(
            session,
            name,
            columns,
            descending,
            nullsLast,
            unique,
            constraint,
            forward);

        return newIndex;
    }

    /*
     * @todo - this is not for general use, as it returns true when table has no
     * rows, but not where it has rows that are not visible by session.
     * current usage is fine.
     */

    /**
     *  Returns true if the table has any rows at all.
     */
    public final boolean isEmpty(Session session) {

        if (getIndexCount() == 0) {
            return true;
        }

        PersistentStore store = getRowStore(session);

        return getIndex(0).isEmpty(store);
    }

    public PersistentStore getRowStore(Session session) {
        return store == null
               ? session.sessionData.persistentStoreCollection.getStore(this)
               : store;
    }
}
