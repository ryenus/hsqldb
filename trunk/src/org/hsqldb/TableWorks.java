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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.DataFileCache;

/**
 * The methods in this class perform alterations to the structure of an
 * existing table which may result in a new Table object
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.7.0
 */
public class TableWorks {

    OrderedHashSet   emptySet = new OrderedHashSet();
    private Database database;
    private Table    table;
    private Session  session;

    public TableWorks(Session session, Table table) {

        this.database = table.database;
        this.table    = table;
        this.session  = session;
    }

    public Table getTable() {
        return table;
    }

    void checkCreateForeignKey(Constraint c) throws HsqlException {

        // check column overlap
        if (c.core.mainTable == table) {
            if (ArrayUtil.haveCommonElement(c.core.refCols, c.core.mainCols,
                                            c.core.refCols.length)) {
                throw Trace.error(Trace.COLUMN_IS_IN_CONSTRAINT);
            }
        }

        // column defaults
        boolean check = c.core.updateAction == Constraint.SET_DEFAULT
                        || c.core.deleteAction == Constraint.SET_DEFAULT;

        if (check) {
            for (int i = 0; i < c.core.refCols.length; i++) {
                Column     col     = table.getColumn(c.core.refCols[i]);
                Expression defExpr = col.getDefaultExpression();

                if (defExpr == null) {
                    String columnName = col.columnName.name;

                    throw Trace.error(Trace.NO_DEFAULT_VALUE_FOR_COLUMN,
                                      new Object[]{ columnName });
                }
            }
        }

        database.schemaManager.checkConstraintExists(c.getName().name,
                table.getSchemaName().name, false);

        // duplicate name check for a new table
        if (table.getConstraint(c.getName().name) != null) {
            throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
        }

        // existing FK check
        if (table.getFKConstraintForColumns(
                c.core.mainTable, c.core.mainCols, c.core.refCols) != null) {
            throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
        }

        if (c.core.mainTable.isTemp() != table.isTemp()) {
            throw Trace.error(Trace.FOREIGN_KEY_NOT_ALLOWED);
        }

        if (c.core.mainTable.getUniqueConstraintForColumns(
                c.core.mainCols, c.core.refCols) == null) {
            throw Trace.error(Trace.SQL_CONSTRAINT_REQUIRED,
                              c.core.mainTable.getName().statementName);
        }

        // check after UNIQUE check
        c.core.mainTable.checkColumnsMatch(c.core.mainCols, table,
                                           c.core.refCols);
    }

    /**
     * Creates a foreign key on an existing table. Foreign keys are enforced
     * by indexes on both the referencing (child) and referenced (main) tables.
     *
     * <p> Since version 1.7.2, a unique constraint on the referenced columns
     * must exist. The non-unique index on the referencing table is now always
     * created whether or not a PK or unique constraint index on the columns
     * exist. Foriegn keys on temp tables can reference
     * other temp tables with the same rules above. Foreign keys on permanent
     * tables cannot reference temp tables. Duplicate foreign keys are now
     * disallowed.
     *
     * @throws HsqlException
     */
    void addForeignKey(Constraint c) throws HsqlException {

        checkCreateForeignKey(c);

        Constraint uniqueConstraint =
            c.core.mainTable.getUniqueConstraintForColumns(c.core.mainCols,
                c.core.refCols);
        Index mainIndex = uniqueConstraint.getMainIndex();

        Constraint.checkReferencedRows(session, table, c.core.refCols,
                                       mainIndex);

        int offset = database.schemaManager.getTableIndex(table);
        boolean isForward =
            offset != -1
            && offset < database.schemaManager.getTableIndex(c.core.mainTable);
        HsqlName indexName = database.nameManager.newAutoName("IDX",
            table.getSchemaName(), table.getName(), SchemaObject.INDEX);
        Index refIndex = table.createIndexStructure(c.core.refCols, null,
            indexName, false, true, isForward);
        HsqlName mainName = database.nameManager.newAutoName("REF",
            c.getName().name, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);

        c.core.uniqueName = uniqueConstraint.getName();
        c.core.mainName   = mainName;
        c.core.mainIndex  = mainIndex;
        c.core.refTable   = table;
        c.core.refName    = c.getName();
        c.core.refIndex   = refIndex;

        Table tn = table.moveDefinition(session, null, c, refIndex, -1, 0,
                                        emptySet, emptySet, null);

        tn.moveData(session, table, -1, 0);
        c.core.mainTable.addConstraint(new Constraint(mainName, c));
        database.schemaManager.addDatabaseObject(c);

        table = tn;

        setNewTableInSchema(table);
        updateConstraints(table, emptySet);
    }

    /**
     * Checks if the attributes of the Column argument, c, are compatible with
     * the operation of adding such a Column to the Table argument, table.
     *
     * @param col the Column to add to the Table, t
     * @param constraints column constraints
     * @param existingPK
     * @throws HsqlException if the operation of adding the Column, c, to
     *      the table t is not valid
     */
    void checkAddColumn(Column col) throws HsqlException {

        boolean existingPK = table.hasPrimaryKey();

        if (table.isText() && !table.isEmpty(session)) {
            throw Trace.error(Trace.OPERATION_NOT_SUPPORTED);
        }

        if (table.findColumn(col.columnName.name) != -1) {
            throw Trace.error(Trace.COLUMN_ALREADY_EXISTS);
        }

        if (col.isPrimaryKey() && existingPK) {
            throw Trace.error(Trace.SECOND_PRIMARY_KEY);
        }

        if (!table.isEmpty(session) && col.getDefaultExpression() == null
                && (!col.isNullable() || col.isPrimaryKey())
                && !col.isIdentity()) {
            throw Trace.error(Trace.BAD_ADD_COLUMN_DEFINITION);
        }
    }

    void addColumn(Column column, int colIndex,
                   HsqlArrayList constraints) throws HsqlException {

        Index      index          = null;
        Table      originalTable  = table;
        Constraint mainConstraint = null;
        boolean    addFK          = false;
        boolean    addUnique      = false;

        checkAddColumn(column);

        Constraint c = (Constraint) constraints.get(0);

        if (c.getType() == Constraint.PRIMARY_KEY) {
            c.core.mainCols = new int[]{ colIndex };

            database.schemaManager.checkConstraintExists(c.getName().name,
                    table.getSchemaName().name, false);

            if (table.hasPrimaryKey()) {
                throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
            }

            addUnique = true;
        } else {
            c = null;
        }

        table = table.moveDefinition(session, column, c, null, colIndex, 1,
                                     emptySet, emptySet, null);

        for (int i = 1; i < constraints.size(); i++) {
            c = (Constraint) constraints.get(i);

            switch (c.constType) {

                case Constraint.UNIQUE : {
                    if (addUnique) {
                        throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
                    }

                    addUnique       = true;
                    c.core.mainCols = new int[]{ colIndex };

                    database.schemaManager.checkConstraintExists(
                        c.getName().name, table.getSchemaName().name, false);

                    HsqlName indexName =
                        database.nameManager.newAutoName("IDX",
                                                         c.getName().name,
                                                         table.getSchemaName(),
                                                         table.getName(),
                                                         SchemaObject.INDEX);

                    // create an autonamed index
                    index =
                        table.createAndAddIndexStructure(c.getMainColumns(),
                                                         indexName, true,
                                                         true, false);
                    c.core.mainTable = table;
                    c.core.mainIndex = index;

                    table.addConstraint(c);

                    break;
                }
                case Constraint.FOREIGN_KEY : {
                    if (addFK) {
                        throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
                    }

                    addFK          = true;
                    c.core.refCols = new int[]{ colIndex };

                    boolean isSelf = originalTable == c.core.mainTable;

                    if (isSelf) {
                        c.core.mainTable = table;
                    }

                    c.setColumnsIndexes(table);
                    checkCreateForeignKey(c);;

                    Constraint uniqueConstraint =
                        c.core.mainTable.getUniqueConstraintForColumns(
                            c.core.mainCols, c.core.refCols);
                    int offset =
                        database.schemaManager.getTableIndex(originalTable);
                    boolean isforward =
                        !isSelf
                        && offset
                           < database.schemaManager.getTableIndex(
                               c.core.mainTable);
                    HsqlName indexName =
                        database.nameManager.newAutoName("IDX",
                                                         c.getName().name,
                                                         table.getSchemaName(),
                                                         table.getName(),
                                                         SchemaObject.INDEX);

                    index = table.createAndAddIndexStructure(c.getRefColumns(),
                            indexName, false, true, isforward);
                    c.core.uniqueName = uniqueConstraint.getName();
                    c.core.mainName = database.nameManager.newAutoName("REF",
                            c.core.refName.name, table.getSchemaName(),
                            table.getName(), SchemaObject.INDEX);
                    c.core.mainIndex = uniqueConstraint.getMainIndex();
                    c.core.refIndex  = index;

                    table.addConstraint(c);

                    mainConstraint = new Constraint(c.core.mainName, c);

                    break;
                }
                case Constraint.CHECK :
                    c.prepareCheckConstraint(session, table);
                    table.addConstraint(c);

                    if (c.isNotNull) {
                        column.setNullable(false);
                        table.setColumnTypeVars(colIndex);
                    }
                    break;
            }
        }

        table.moveData(session, originalTable, colIndex, 1);

        if (mainConstraint != null) {
            mainConstraint.getMain().addConstraint(mainConstraint);
        }

        registerConstraintNames(constraints);
        setNewTableInSchema(table);
        updateConstraints(table, emptySet);
    }

    void updateConstraints(OrderedHashSet tableSet,
                           OrderedHashSet dropConstraints)
                           throws HsqlException {

        for (int i = 0; i < tableSet.size(); i++) {
            Table t = (Table) tableSet.get(i);

            updateConstraints(t, dropConstraints);
        }
    }

    void removeConstraints(OrderedHashSet tableSet,
                           OrderedHashSet dropConstraints) {

        for (int i = 0; i < tableSet.size(); i++) {
            Table t = (Table) tableSet.get(i);

            for (int j = 0; j < dropConstraints.size(); j++) {
                HsqlName name  = (HsqlName) dropConstraints.get(j);
                int      index = t.getConstraintIndex(name.name);

                if (index != -1) {
                    t.removeConstraint(name.name);
                }
            }
        }
    }

    void updateConstraints(Table t,
                           OrderedHashSet dropConstraints)
                           throws HsqlException {

        for (int i = t.constraintList.length - 1; i >= 0; i--) {
            Constraint c = t.constraintList[i];

            if (dropConstraints.contains(c.getName())) {
                t.constraintList =
                    (Constraint[]) ArrayUtil.toAdjustedArray(t.constraintList,
                        null, i, -1);

                continue;
            }

            if (c.getType() == Constraint.FOREIGN_KEY) {
                Table mainT = database.schemaManager.getUserTable(session,
                    c.core.mainTable.getName());
                Constraint mainC = mainT.getConstraint(c.getMainName().name);

                mainC.core = c.core;
            } else if (c.getType() == Constraint.MAIN) {
                Table refT = database.schemaManager.getUserTable(session,
                    c.core.refTable.getName());
                Constraint refC = refT.getConstraint(c.getRefName().name);

                refC.core = c.core;
            }
        }
    }

    void makeNewTables(OrderedHashSet tableSet,
                       OrderedHashSet dropConstraintSet,
                       OrderedHashSet dropIndexSet) throws HsqlException {

        for (int i = 0; i < tableSet.size(); i++) {
            Table      t  = (Table) tableSet.get(i);
            TableWorks tw = new TableWorks(session, t);

            tw.makeNewTable(dropConstraintSet, dropIndexSet);
            tableSet.set(i, tw.getTable());
        }
    }

    /**
     * Drops constriants and their indexes in table. Uses set of names.
     */
    void makeNewTable(OrderedHashSet dropConstraintSet,
                      OrderedHashSet dropIndexSet) throws HsqlException {

        Table tn = table.moveDefinition(session, null, null, null, -1, 0,
                                        dropConstraintSet, dropIndexSet, null);

        if (tn.indexList.length == table.indexList.length) {
            return;
        }

        tn.moveData(session, table, -1, 0);

        table = tn;
    }

    void setNewTablesInSchema(OrderedHashSet tableSet) throws HsqlException {

        for (int i = 0; i < tableSet.size(); i++) {
            Table t = (Table) tableSet.get(i);

            setNewTableInSchema(t);
        }
    }

    /**
     * Because of the way indexes and column data are held in memory and on
     * disk, it is necessary to recreate the table when an index is added to a
     * non-empty table cached table.
     *
     * <p> With empty tables, Index objects are simply added
     *
     * <p> With MEOMRY and TEXT tables, a new index is built up and nodes for
     * earch row are interlinked (fredt@users)
     *
     * @param col int[]
     * @param name HsqlName
     * @param unique boolean
     * @return new index
     * @throws HsqlException normally for lack of resources
     */
    Index addIndex(int[] col, HsqlName name,
                   boolean unique) throws HsqlException {

        Index newindex;

        if (table.isEmpty(session) || table.isIndexingMutable()) {
            newindex = table.createIndex(session, col, null, name, unique,
                                         false, false);

            database.schemaManager.clearTempTables(session, table);
        } else {
            newindex = table.createIndexStructure(col, null, name, unique,
                                                  false, false);

            Table tn = table.moveDefinition(session, null, null, newindex, -1,
                                            0, emptySet, emptySet, null);

            tn.moveData(session, table, -1, 0);

            table = tn;

            setNewTableInSchema(table);
            updateConstraints(table, emptySet);
        }

        database.schemaManager.clearTempTables(session, table);
        database.schemaManager.addDatabaseObject(newindex);
        database.schemaManager.recompileDependentObjects(table);

        return newindex;
    }

    void addPrimaryKey(Constraint constraint,
                       HsqlName name) throws HsqlException {

        if (table.hasPrimaryKey()) {
            throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
        }

        database.schemaManager.checkConstraintExists(name.name,
                table.getSchemaName().name, false);

        Table tn = table.moveDefinition(session, null, constraint, null, -1,
                                        0, emptySet, emptySet, null);

        tn.moveData(session, table, -1, 0);

        table = tn;

        database.schemaManager.addDatabaseObject(constraint);
        setNewTableInSchema(table);
        updateConstraints(table, emptySet);
    }

    /**
     * A unique constraint relies on a unique indexe on the table. It can cover
     * a single column or multiple columns.
     *
     * <p> All unique constraint names are generated by Database.java as unique
     * within the database. Duplicate constraints (more than one unique
     * constriant on the same set of columns) are not allowed. (fredt@users)
     *
     * @param cols int[]
     * @param name HsqlName
     * @throws HsqlException
     */
    void addUniqueConstraint(int[] cols, HsqlName name) throws HsqlException {

        database.schemaManager.checkConstraintExists(name.name,
                table.getSchemaName().name, false);

        if (table.getUniqueConstraintForColumns(cols) != null) {
            throw Trace.error(Trace.CONSTRAINT_ALREADY_EXISTS);
        }

        // create an autonamed index
        HsqlName indexname = database.nameManager.newAutoName("IDX",
            name.name, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);
        Index index = table.createIndexStructure(cols, null, indexname, true,
            true, false);
        Constraint constraint = new Constraint(name, table, index,
                                               Constraint.UNIQUE);
        Table tn = table.moveDefinition(session, null, constraint, index, -1,
                                        0, emptySet, emptySet, null);

        tn.moveData(session, table, -1, 0);

        table = tn;

        database.schemaManager.addDatabaseObject(constraint);
        setNewTableInSchema(table);
        updateConstraints(table, emptySet);
    }

    void addCheckConstraint(Constraint c) throws HsqlException {

        database.schemaManager.checkConstraintExists(c.getName().name,
                table.getSchemaName().name, false);
        c.prepareCheckConstraint(session, table);
        table.addConstraint(c);

        if (c.isNotNull) {
            Column column = table.getColumn(c.notNullColumnIndex);

            column.setNullable(false);
            table.setColumnTypeVars(c.notNullColumnIndex);
        }

        database.schemaManager.addDatabaseObject(c);
    }

    /**
     * Because of the way indexes and column data are held in memory and on
     * disk, it is necessary to recreate the table when an index is added to or
     * removed from a non-empty table.
     *
     * <p> Originally, this method would break existing foreign keys as the
     * table order in the DB was changed. The new table is now linked in place
     * of the old table (fredt@users)
     *
     * @param indexName String
     * @throws HsqlException
     */
    void dropIndex(String indexName) throws HsqlException {

        Index index;

        index = table.getIndex(indexName);

        if (table.isIndexingMutable()) {
            table.dropIndex(session, indexName);
        } else {
            OrderedHashSet indexSet = new OrderedHashSet();

            indexSet.add(table.getIndex(indexName).getName());

            Table tn = table.moveDefinition(session, null, null, null, -1, 0,
                                            emptySet, indexSet, null);

            tn.moveData(session, table, -1, 0);
            updateConstraints(tn, emptySet);
            setNewTableInSchema(tn);

            table = tn;
        }

        if (!index.isConstraint()) {
            database.schemaManager.removeDatabaseObject(index.getName());
        }

        database.schemaManager.recompileDependentObjects(table);
    }

    /**
     *
     * @param colIndex int
     * @throws HsqlException
     */
    void dropColumn(int colIndex, boolean cascade) throws HsqlException {

        OrderedHashSet constraintNameSet = new OrderedHashSet();
        OrderedHashSet dependentConstraints =
            table.getDependentConstraints(colIndex);
        OrderedHashSet cascadingConstraints =
            table.getContainingConstraints(colIndex);
        OrderedHashSet indexNameSet = table.getContainingIndexNames(colIndex);
        HsqlName       columnName   = table.getColumn(colIndex).getName();
        OrderedHashSet referencingObjects =
            database.schemaManager.getReferencingObjects(table.getName(),
                columnName);

        if (table.isText() && !table.isEmpty(session)) {
            throw Trace.error(Trace.OPERATION_NOT_SUPPORTED);
        }

        if (!cascade) {
            if (!cascadingConstraints.isEmpty()) {
                Constraint c    = (Constraint) cascadingConstraints.get(0);
                HsqlName   name = c.getName();

                throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                                  name.schema.name + '.' + name.name);
            }

            if (!referencingObjects.isEmpty()) {
                for (int i = 0; i < referencingObjects.size(); i++) {
                    HsqlName name = (HsqlName) referencingObjects.get(i);

                    throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                                      name.schema.name + '.' + name.name);
                }
            }
        }

        dependentConstraints.addAll(cascadingConstraints);
        cascadingConstraints.clear();

        OrderedHashSet tableSet = new OrderedHashSet();

        for (int i = 0; i < dependentConstraints.size(); i++) {
            Constraint c = (Constraint) dependentConstraints.get(i);

            if (c.constType == Constraint.FOREIGN_KEY) {
                tableSet.add(c.getMain());
                constraintNameSet.add(c.getMainName());
                constraintNameSet.add(c.getRefName());
                indexNameSet.add(c.getRefIndex().getName());
            }

            if (c.constType == Constraint.MAIN) {
                tableSet.add(c.getRef());
                constraintNameSet.add(c.getMainName());
                constraintNameSet.add(c.getRefName());
                indexNameSet.add(c.getRefIndex().getName());
            }

            constraintNameSet.add(c.getName());
        }

        makeNewTables(tableSet, constraintNameSet, indexNameSet);

        Table tn = table.moveDefinition(session, null, null, null, colIndex,
                                        -1, constraintNameSet, indexNameSet,
                                        null);

        tn.moveData(session, table, colIndex, -1);
        setNewTableInSchema(tn);
        setNewTablesInSchema(tableSet);
        updateConstraints(tn, emptySet);
        updateConstraints(tableSet, constraintNameSet);

        table = tn;

        database.schemaManager.removeDatabaseObjects(referencingObjects);
        database.schemaManager.recompileDependentObjects(table);
        deRegisterConstraintNames(constraintNameSet);
    }

    void registerConstraintNames(HsqlArrayList constraints)
    throws HsqlException {

        for (int i = 0; i < constraints.size(); i++) {
            Constraint c = (Constraint) constraints.get(i);

            switch (c.constType) {

                case Constraint.PRIMARY_KEY :
                case Constraint.UNIQUE :
                case Constraint.CHECK :
                    database.schemaManager.addDatabaseObject(c);
            }
        }
    }

    void deRegisterConstraintNames(OrderedHashSet nameSet)
    throws HsqlException {

        for (int i = 0; i < nameSet.size(); i++) {
            HsqlName name = (HsqlName) nameSet.get(i);

            database.schemaManager.removeReferencedObject(name);
        }
    }

    /**
     * Drop a named constraint
     *
     * @param name String
     * @throws HsqlException
     */
    void dropConstraint(String name, boolean cascade) throws HsqlException {

        Constraint constraint = table.getConstraint(name);

        if (constraint == null) {
            throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                              Trace.TableWorks_dropConstraint, new Object[] {
                name, table.getName().name
            });
        }

        switch (constraint.getType()) {

            case Constraint.MAIN :
                throw Trace.error(Trace.DROP_SYSTEM_CONSTRAINT);
            case Constraint.PRIMARY_KEY :
            case Constraint.UNIQUE : {
                OrderedHashSet dependentConstraints =
                    table.getDependentConstraints(constraint);

                // throw if unique constraint is referenced by foreign key
                if (!cascade && !dependentConstraints.isEmpty()) {
                    Constraint c = (Constraint) dependentConstraints.get(0);

                    throw Trace.error(Trace.DROP_FK_INDEX, c.getName().name);
                }

                OrderedHashSet tableSet          = new OrderedHashSet();
                OrderedHashSet constraintNameSet = new OrderedHashSet();
                OrderedHashSet indexNameSet      = new OrderedHashSet();

                for (int i = 0; i < dependentConstraints.size(); i++) {
                    Constraint c = (Constraint) dependentConstraints.get(i);
                    Table      t = c.getMain();

                    if (t != table) {
                        tableSet.add(t);
                    }

                    t = c.getRef();

                    if (t != table) {
                        tableSet.add(t);
                    }

                    constraintNameSet.add(c.getMainName());
                    constraintNameSet.add(c.getRefName());
                    indexNameSet.add(c.getRefIndex().getName());
                }

                constraintNameSet.add(constraint.getName());

                if (constraint.getType() == Constraint.UNIQUE) {
                    indexNameSet.add(constraint.getMainIndex().getName());
                }

                Table tn = table.moveDefinition(session, null, null, null, -1,
                                                0, constraintNameSet,
                                                indexNameSet, null);

                tn.moveData(session, table, -1, 0);
                makeNewTables(tableSet, constraintNameSet, indexNameSet);

                if (constraint.getType() == Constraint.PRIMARY_KEY) {
                    int[] cols = constraint.getMainColumns();

                    for (int i = 0; i < cols.length; i++) {
                        tn.getColumn(cols[i]).setPrimaryKey(false);
                        tn.setColumnTypeVars(cols[i]);
                    }
                }

                setNewTableInSchema(tn);
                setNewTablesInSchema(tableSet);
                updateConstraints(tn, emptySet);
                updateConstraints(tableSet, constraintNameSet);

                table = tn;

                this.deRegisterConstraintNames(constraintNameSet);

                // handle cascadingConstraints and cascadingTables
                break;
            }
            case Constraint.FOREIGN_KEY : {
                OrderedHashSet constraints = new OrderedHashSet();
                Table          mainTable   = constraint.getMain();
                HsqlName       mainName    = constraint.getMainName();
                boolean        isSelf      = mainTable == table;

                constraints.add(mainName);
                constraints.add(constraint.getRefName());

                OrderedHashSet indexes = new OrderedHashSet();

                indexes.add(constraint.getRefIndex().getName());

                Table tn = table.moveDefinition(session, null, null, null, -1,
                                                0, constraints, indexes, null);

                tn.moveData(session, table, -1, 0);
                setNewTableInSchema(tn);

                if (!isSelf) {
                    mainTable.removeConstraint(mainName.name);
                }

                table = tn;

                table.database.schemaManager.removeConstraintName(
                    constraint.getName());

                break;
            }
            case Constraint.CHECK :
                table.removeConstraint(name);

                if (constraint.isNotNull) {
                    Column column =
                        table.getColumn(constraint.notNullColumnIndex);

                    column.setNullable(false);
                    table.setColumnTypeVars(constraint.notNullColumnIndex);
                }

                table.database.schemaManager.removeConstraintName(
                    constraint.getName());
                break;
        }
    }

    /**
     * Allows changing the type or addition of an IDENTITY sequence.
     *
     * @param oldCol Column
     * @param newCol Column
     * @throws HsqlException
     */
    void retypeColumn(Column oldCol, Column newCol) throws HsqlException {

        boolean notAllowed = false;
        int     oldType    = oldCol.getType().type;
        int     newType    = newCol.getType().type;

        switch (newType) {

            case Types.SQL_BLOB :
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                notAllowed = !(oldType == Types.SQL_BINARY
                               || oldType == Types.SQL_VARBINARY);
                break;

            default :
        }

        switch (oldType) {

            case Types.SQL_BLOB :
            case Types.SQL_CLOB :
                notAllowed = true;
                break;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                notAllowed = !(newType == Types.SQL_BINARY
                               || newType == Types.SQL_VARBINARY);
                break;

            case Types.OTHER :
            case Types.JAVA_OBJECT :
                notAllowed = true;
                break;

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                switch (newType) {

                    case Types.SQL_DATE :
                    case Types.SQL_TIME :
                    case Types.SQL_TIMESTAMP :
                        notAllowed = true;
                    default :
                }
                break;

            case Types.SQL_DATE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP :
                switch (newType) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                    case Types.SQL_BIGINT :
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        notAllowed = true;
                    default :
                }
                break;
        }

        if (notAllowed && !(newType == oldType || table.isEmpty(session))) {
            throw Trace.error(Trace.INVALID_CONVERSION);
        }

        int colIndex = table.getColumnIndex(oldCol.columnName.name);

        // if there is a multi-column PK, do not change the PK attributes
        if (newCol.isIdentity() && table.hasIdentityColumn()
                && table.identityColumn != colIndex) {
            throw Trace.error(Trace.SQL_SECOND_IDENTITY_COLUMN);
        }

        if (table.getPrimaryKey().length > 1) {
            newCol.setPrimaryKey(oldCol.isPrimaryKey());

            if (ArrayUtil.find(table.getPrimaryKey(), colIndex) != -1) {}
        } else if (table.hasPrimaryKey()) {
            if (oldCol.isPrimaryKey()) {
                newCol.setPrimaryKey(true);
            } else if (newCol.isPrimaryKey()) {
                throw Trace.error(Trace.SECOND_PRIMARY_KEY);
            }
        } else if (newCol.isPrimaryKey()) {
            throw Trace.error(Trace.PRIMARY_KEY_NOT_ALLOWED);
        }

        // apply and return if only metadata change is required
        boolean meta = newType == oldType;

        meta &= oldCol.isNullable() == newCol.isNullable();
        meta &= oldCol.getType().scale() == newCol.getType().scale();
        meta &= (oldCol.getType().size() == newCol.getType().size()
                 || (oldCol.getType().size() < newCol.getType().size()
                     && (oldType == Types.SQL_VARCHAR
                         || oldType == Types.SQL_VARBINARY
                         || oldType == Types.SQL_DECIMAL
                         || oldType == Types.SQL_NUMERIC)));
        meta &= (oldCol.isIdentity() == newCol.isIdentity());

        if (meta) {

            // size of some types may be increased with this command
            // default expressions can change
            oldCol.setType(newCol);
            oldCol.setDefaultExpression(newCol.getDefaultExpression());

            if (newCol.isIdentity()) {
                oldCol.setIdentity(newCol.getIdentitySequence());
            }

            table.setColumnTypeVars(colIndex);
            table.resetDefaultsFlag();

            return;
        }

        database.schemaManager.checkColumnIsReferenced(
            table.getColumn(colIndex).getName());
        table.checkColumnInCheckConstraint(colIndex);
        table.checkColumnInFKConstraint(colIndex);
        checkConvertColDataType(oldCol, newCol);
        retypeColumn(newCol, colIndex);
    }

    /**
     *
     * @param oldCol Column
     * @param newCol Column
     * @throws HsqlException
     */
    void checkConvertColDataType(Column oldCol,
                                 Column newCol) throws HsqlException {

        int         colIndex = table.getColumnIndex(oldCol.columnName.name);
        RowIterator it       = table.getPrimaryIndex().firstRow(session);

        while (it.hasNext()) {
            Row    row = it.getNext();
            Object o   = row.getData()[colIndex];

            newCol.getType().convertToType(session, o, oldCol.getType());
        }
    }

    /**
     *
     * @param column Column
     * @param colIndex int
     * @throws HsqlException
     */
    void retypeColumn(Column column, int colIndex) throws HsqlException {

        if (table.isText() && !table.isEmpty(session)) {
            throw Trace.error(Trace.OPERATION_NOT_SUPPORTED);
        }

        Table tn = table.moveDefinition(session, column, null, null, colIndex,
                                        0, emptySet, emptySet, null);

        tn.moveData(session, table, colIndex, 0);
        updateConstraints(tn, emptySet);
        setNewTableInSchema(tn);

        table = tn;
    }

    /**
     * performs the work for changing the nullability of a column
     *
     * @param column Column
     * @param nullable boolean
     * @throws HsqlException
     */
    void setColNullability(Column column,
                           boolean nullable) throws HsqlException {

        Constraint c        = null;
        int        colIndex = table.getColumnIndex(column.columnName.name);

        if (column.isNullable() == nullable) {
            return;
        }

        if (nullable) {
            if (column.isPrimaryKey()) {
                throw Trace.error(Trace.TRY_TO_INSERT_NULL);
            }

            table.checkColumnInFKConstraint(colIndex, Constraint.SET_NULL);
            removeColumnNotNullConstraints(colIndex);
        } else {
            RowIterator it = table.getPrimaryIndex().firstRow(session);

            while (it.hasNext()) {
                Row    row = it.getNext();
                Object o   = row.getData()[colIndex];

                if (o == null) {
                    throw Trace.error(Trace.TRY_TO_INSERT_NULL);
                }
            }

            HsqlName constName = database.nameManager.newAutoName("CT",
                table.getSchemaName(), table.getName(),
                SchemaObject.CONSTRAINT);

            c       = new Constraint(constName, null, Constraint.CHECK);
            c.check = new Expression(column);

            c.prepareCheckConstraint(session, table);
            column.setNullable(false);
            table.addConstraint(c);
            table.setColumnTypeVars(colIndex);
            database.schemaManager.addDatabaseObject(c);
        }
    }

    /**
     * performs the work for changing the default value of a column
     *
     * @param colIndex int
     * @param def Expression
     * @throws HsqlException
     */
    void setColDefaultExpression(int colIndex,
                                 Expression def) throws HsqlException {

        if (def == null) {
            table.checkColumnInFKConstraint(colIndex, Constraint.SET_DEFAULT);
        }

        table.setDefaultExpression(colIndex, def);
    }

    /**
     * Changes the type of a table
     *
     * @param session Session
     * @param newType int
     * @throws HsqlException
     * @return boolean
     */
    public boolean setTableType(Session session,
                                int newType) throws HsqlException {

        int           currentType = table.getTableType();
        DataFileCache newCache    = null;

        if (currentType == newType) {
            return false;
        }

        switch (newType) {

            case Table.CACHED_RESULT :
                if (currentType == Table.RESULT) {
                    newCache = session.sessionData.getResultCache();

                    break;
                }

                return false;

            case Table.CACHED_TABLE :
                if (currentType == Table.MEMORY_TABLE) {
                    newCache = database.logger.getCache();

                    break;
                }

                return false;

            case Table.MEMORY_TABLE :
                if (currentType == Table.CACHED_TABLE) {
                    break;
                }

                return false;

            default :
            case Table.TEMP_TABLE :
            case Table.SYSTEM_SUBQUERY :
            case Table.RESULT :
                return false;
        }

        Table tn;

        try {
            tn = table.moveDefinition(session, null, null, null, -1, 0,
                                      emptySet, emptySet, null);

            tn.setAsType(newType, newCache);
            tn.moveData(session, table, -1, 0);
            updateConstraints(tn, emptySet);
        } catch (HsqlException e) {
            return false;
        }

        switch (newType) {

            case Table.CACHED_TABLE :
            case Table.MEMORY_TABLE :
                setNewTableInSchema(tn);
            default :
        }

        table = tn;

        return true;
    }

    /**
     *
     * @param newTable Table
     */
    void setNewTableInSchema(Table newTable) {

        int i = database.schemaManager.getTableIndex(newTable);

        if (i != -1) {
            database.schemaManager.setTable(i, newTable);
        }
    }

    void removeColumnNotNullConstraints(int colIndex) throws HsqlException {

        for (int i = table.constraintList.length - 1; i >= 0; i--) {
            Constraint c = table.constraintList[i];

            if (c.isNotNull) {
                if (c.notNullColumnIndex == colIndex) {
                    database.schemaManager.removeConstraintName(c.getName());
                }

                table.removeConstraint(i);
            }
        }

        Column column = table.getColumn(colIndex);

        column.setNullable(true);
        table.setColumnTypeVars(colIndex);
    }
}
