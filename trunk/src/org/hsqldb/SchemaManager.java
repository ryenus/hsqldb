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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FilteredIterator;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.List;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;

/**
 * Manages all SCHEMA related database objects
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.8.0
 */
public class SchemaManager {

    Database   database;
    HsqlName   defaultSchemaHsqlName;
    int        defaultTableType = TableBase.MEMORY_TABLE;
    HsqlName[] catalogNameArray;
    long       schemaChangeTimestamp;
    Table      dualTable;

    //
    OrderedHashMap<String, Schema> schemaMap        = new OrderedHashMap<>();
    MultiValueHashMap<HsqlName, HsqlName> referenceMap =
        new MultiValueHashMap<>();
    UserSchemaFilter               userSchemaFilter = new UserSchemaFilter();

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    //
    public SchemaManager(Database database) {

        this.database         = database;
        defaultSchemaHsqlName = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        catalogNameArray      = new HsqlName[]{ database.getCatalogName() };

        Schema schema = new Schema(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME,
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner);

        schemaMap.put(schema.getName().name, schema);

        try {
            schema.charsetLookup.add(Charset.SQL_TEXT, false);
            schema.charsetLookup.add(Charset.SQL_IDENTIFIER_CHARSET, false);
            schema.charsetLookup.add(Charset.SQL_CHARACTER, false);
            schema.collationLookup.add(Collation.getDefaultInstance(), false);
            schema.collationLookup.add(
                Collation.getDefaultIgnoreCaseInstance(),
                false);
            schema.typeLookup.add(TypeInvariants.CARDINAL_NUMBER, false);
            schema.typeLookup.add(TypeInvariants.YES_OR_NO, false);
            schema.typeLookup.add(TypeInvariants.CHARACTER_DATA, false);
            schema.typeLookup.add(TypeInvariants.SQL_IDENTIFIER, false);
            schema.typeLookup.add(TypeInvariants.TIME_STAMP, false);
            schema.typeLookup.add(TypeInvariants.NCNAME, false);
            schema.typeLookup.add(TypeInvariants.URI, false);
        } catch (HsqlException e) {}
    }

    public void setSchemaChangeTimestamp() {
        schemaChangeTimestamp = database.txManager.getSystemChangeNumber();
    }

    public long getSchemaChangeTimestamp() {
        return schemaChangeTimestamp;
    }

    // pre-defined
    public HsqlName getSQLJSchemaHsqlName() {
        return SqlInvariants.SQLJ_SCHEMA_HSQLNAME;
    }

    // SCHEMA management
    public void createPublicSchema() {

        writeLock.lock();

        try {
            HsqlName name = database.nameManager.newHsqlName(
                null,
                SqlInvariants.PUBLIC_SCHEMA,
                SchemaObject.SCHEMA);
            Schema schema = new Schema(
                name,
                database.getGranteeManager().getDBARole());

            defaultSchemaHsqlName = schema.getName();

            schemaMap.put(schema.getName().name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Creates a schema belonging to the given grantee.
     */
    public void createSchema(HsqlName name, Grantee owner) {

        writeLock.lock();

        try {
            SqlInvariants.checkSchemaNameNotSystem(name.name);

            Schema schema = new Schema(name, owner);

            schemaMap.add(name.name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    public void dropSchema(Session session, String name, boolean cascade) {

        writeLock.lock();

        try {
            Schema schema = schemaMap.get(name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            if (SqlInvariants.isLobsSchemaName(name)) {
                throw Error.error(ErrorCode.X_42503, name);
            }

            if (!cascade && !schema.isEmpty()) {
                throw Error.error(ErrorCode.X_2B000);
            }

            OrderedHashSet<HsqlName> externalReferences =
                new OrderedHashSet<>();

            getCascadingReferencesToSchema(
                schema.getName(),
                externalReferences);
            removeSchemaObjects(externalReferences);

            Iterator<SchemaObject> tableIterator = schema.schemaObjectIterator(
                SchemaObject.TABLE);

            while (tableIterator.hasNext()) {
                Table        table = ((Table) tableIterator.next());
                Constraint[] list  = table.getFKConstraints();

                for (int i = 0; i < list.length; i++) {
                    Constraint constraint = list[i];

                    if (constraint.getMain().getSchemaName()
                            != schema.getName()) {
                        constraint.getMain()
                                  .removeConstraint(
                                      constraint.getMainName().name);
                        removeReferencesFrom(constraint);
                    }
                }

                removeTable(session, table);
            }

            Iterator<SchemaObject> sequenceIterator =
                schema.schemaObjectIterator(
                    SchemaObject.SEQUENCE);

            while (sequenceIterator.hasNext()) {
                NumberSequence sequence =
                    ((NumberSequence) sequenceIterator.next());

                database.getGranteeManager().removeDbObject(sequence.getName());
            }

            schema.release();
            schemaMap.remove(name);

            if (defaultSchemaHsqlName.name.equals(name)) {
                schema = new Schema(
                    defaultSchemaHsqlName,
                    database.getGranteeManager().getDBARole());
                defaultSchemaHsqlName = schema.getName();

                schemaMap.put(schema.getName().name, schema);
            } else {
                HsqlName schemaName = schema.getName();

                // these are called last and in this particular order
                database.getUserManager().removeSchemaReference(schemaName);
                database.getSessionManager().removeSchemaReference(schemaName);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void renameSchema(HsqlName name, HsqlName newName) {

        writeLock.lock();

        try {
            Schema schema = schemaMap.get(name.name);
            Schema exists = schemaMap.get(newName.name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_42501, name.name);
            }

            if (exists != null) {
                throw Error.error(ErrorCode.X_42504, newName.name);
            }

            SqlInvariants.checkSchemaNameNotSystem(name.name);
            SqlInvariants.checkSchemaNameNotSystem(newName.name);

            int index = schemaMap.getIndex(name.name);

            schema.getName().rename(newName);
            schemaMap.set(index, newName.name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    public void release() {

        writeLock.lock();

        try {
            Iterator<Schema> it = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = it.next();

                schema.release();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public String[] getSchemaNamesArray() {

        readLock.lock();

        try {
            String[] array = new String[schemaMap.size()];

            schemaMap.keysToArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public Schema[] getAllSchemas() {

        readLock.lock();

        try {
            Schema[] objects = new Schema[schemaMap.size()];

            schemaMap.valuesToArray(objects);

            return objects;
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Schema> getUserSchemaIterator() {
        return new FilteredIterator<>(
            schemaMap.values().iterator(),
            userSchemaFilter);
    }

    public HsqlName getUserSchemaHsqlName(String name) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_3F000, name);
            }

            if (schema.getName() == SqlInvariants.INFORMATION_SCHEMA_HSQLNAME) {
                throw Error.error(ErrorCode.X_3F000, name);
            }

            return schema.getName();
        } finally {
            readLock.unlock();
        }
    }

    public Grantee toSchemaOwner(String name) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(name);

            return schema == null
                   ? null
                   : schema.getOwner();
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName getDefaultSchemaHsqlName() {
        return defaultSchemaHsqlName;
    }

    public void setDefaultSchemaHsqlName(HsqlName name) {
        defaultSchemaHsqlName = name;
    }

    public boolean schemaExists(String name) {

        readLock.lock();

        try {
            return schemaMap.containsKey(name);
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName findSchemaHsqlName(String name) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(name);

            if (schema == null) {
                return null;
            }

            return schema.getName();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * If schemaName is null, return the default schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * throw.
     */
    public HsqlName getSchemaHsqlName(String name) {

        if (name == null) {
            return defaultSchemaHsqlName;
        }

        HsqlName schemaName = findSchemaHsqlName(name);

        if (schemaName == null) {
            throw Error.error(ErrorCode.X_3F000, name);
        }

        return schemaName;
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) {
        return getSchemaHsqlName(name).name;
    }

    public Schema findSchema(String name) {

        readLock.lock();

        try {
            return schemaMap.get(name);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * drop all schemas with the given authorisation
     */
    public void dropSchemas(Session session, Grantee grantee, boolean cascade) {

        writeLock.lock();

        try {
            HsqlArrayList<Schema> list = getSchemas(grantee);
            Iterator<Schema>      it   = list.iterator();

            while (it.hasNext()) {
                Schema schema = it.next();

                dropSchema(session, schema.getName().name, cascade);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public HsqlArrayList<Schema> getSchemas(Grantee grantee) {

        readLock.lock();

        try {
            HsqlArrayList<Schema> list = new HsqlArrayList<>();
            Iterator<Schema>      it   = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = it.next();

                if (grantee.equals(schema.getOwner())) {
                    list.add(schema);
                }
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    public boolean hasSchemas(Grantee grantee) {

        readLock.lock();

        try {
            Iterator<Schema> it = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = it.next();

                if (grantee.equals(schema.getOwner())) {
                    return true;
                }
            }

            return false;
        } finally {
            readLock.unlock();
        }
    }

    /**
     *  Returns an HsqlArrayList containing references to all non-system
     *  tables and views of the given type.
     */
    public HsqlArrayList<Table> getAllTables(boolean withLobTables) {

        readLock.lock();

        try {
            HsqlArrayList<Table> allTables = new HsqlArrayList<>();
            String[]             schemas   = getSchemaNamesArray();

            for (int i = 0; i < schemas.length; i++) {
                String name = schemas[i];

                if (!withLobTables && SqlInvariants.isLobsSchemaName(name)) {
                    continue;
                }

                if (SqlInvariants.isSystemSchemaName(name)) {
                    continue;
                }

                OrderedHashMap<String, Table> current = getTables(name);

                allTables.addAll(current.values());
            }

            return allTables;
        } finally {
            readLock.unlock();
        }
    }

    public OrderedHashMap<String, Table> getTables(String schema) {

        readLock.lock();

        try {
            Schema temp = schemaMap.get(schema);

            return temp.tableList;
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName[] getCatalogNameArray() {
        return catalogNameArray;
    }

    public HsqlName[] getCatalogAndBaseTableNames() {

        readLock.lock();

        try {
            OrderedHashSet<HsqlName> names  = new OrderedHashSet<>();
            HsqlArrayList<Table>     tables = getAllTables(false);

            for (int i = 0; i < tables.size(); i++) {
                Table table = tables.get(i);

                if (!table.isTemp()) {
                    names.add(table.getName());
                }
            }

            names.add(database.getCatalogName());

            HsqlName[] array = new HsqlName[names.size()];

            names.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName[] getCatalogAndBaseTableNames(HsqlName name) {

        if (name == null) {
            return catalogNameArray;
        }

        readLock.lock();

        try {
            switch (name.type) {

                case SchemaObject.SCHEMA : {
                    if (findSchemaHsqlName(name.name) == null) {
                        return catalogNameArray;
                    }

                    OrderedHashSet<HsqlName> names = new OrderedHashSet<>();

                    names.add(database.getCatalogName());

                    OrderedHashMap<String, Table> list = getTables(name.name);

                    for (int i = 0; i < list.size(); i++) {
                        names.add(list.get(i).getName());
                    }

                    HsqlName[] array = new HsqlName[names.size()];

                    names.toArray(array);

                    return array;
                }

                case SchemaObject.GRANTEE : {
                    return catalogNameArray;
                }

                case SchemaObject.INDEX :
                case SchemaObject.CONSTRAINT :
                default :
            }

            SchemaObject object = findSchemaObject(
                name.name,
                name.schema.name,
                name.type);

            if (object == null) {
                return catalogNameArray;
            }

            HsqlName                 parent = object.getName().parent;
            OrderedHashSet<HsqlName> references = getReferencesTo(
                object.getName());
            OrderedHashSet<HsqlName> names  = new OrderedHashSet<>();

            names.add(database.getCatalogName());

            if (parent != null) {
                SchemaObject parentObject = findSchemaObject(
                    parent.name,
                    parent.schema.name,
                    parent.type);

                if (parentObject != null
                        && parentObject.getName().type == SchemaObject.TABLE) {
                    names.add(parentObject.getName());
                }
            }

            if (object.getName().type == SchemaObject.TABLE) {
                names.add(object.getName());
            }

            for (int i = 0; i < references.size(); i++) {
                HsqlName reference = references.get(i);

                if (reference.type == SchemaObject.TABLE) {
                    Table table = findUserTable(
                        reference.name,
                        reference.schema.name);

                    if (table != null && !table.isTemp()) {
                        names.add(reference);
                    }
                }
            }

            HsqlName[] array = new HsqlName[names.size()];

            names.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public void checkSchemaObjectNotExists(HsqlName name) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(name.schema.name);

            schema.checkObjectNotExists(name);
        } finally {
            readLock.unlock();
        }
    }

    public Table getUserTable(HsqlName name) {
        return getUserTable(name.name, name.schema.name);
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session.
     *  Throws if the table does not exist in the context.
     */
    public Table getUserTable(String name, String schema) {

        Table t = findUserTable(name, schema);

        if (t == null) {
            String longName = schema == null
                              ? name
                              : schema + '.' + name;

            throw Error.error(ErrorCode.X_42501, longName);
        }

        return t;
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified schema. It excludes system tables.
     *  Returns null if the table does not exist in the context.
     */
    public Table findUserTable(String name, String schemaName) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema == null) {
                return null;
            }

            int i = schema.tableList.getIndex(name);

            if (i == -1) {
                return null;
            }

            return schema.tableList.get(i);
        } finally {
            readLock.unlock();
        }
    }

    /**
     *  Returns the specified session context table.
     *  Returns null if the table does not exist in the context.
     */
    public Table findSessionTable(Session session, String name) {
        return session.sessionContext.findSessionTable(name);
    }

    /**
     * Drops the specified user-defined view or table from this Database object.
     *
     * <p> The process of dropping a table or view includes:
     * <OL>
     * <LI> checking that the specified Session's currently connected User has
     * the right to perform this operation and refusing to proceed if not by
     * throwing.
     * <LI> checking for referential constraints that conflict with this
     * operation and refusing to proceed if they exist by throwing.</LI>
     * <LI> removing the specified Table from this Database object.
     * <LI> removing any exported foreign keys Constraint objects held by any
     * tables referenced by the table to be dropped. This is especially
     * important so that the dropped Table ceases to be referenced, eventually
     * allowing its full garbage collection.
     * <LI>
     * </OL>
     *
     * <p>
     *
     * @param session the connected context in which to perform this operation
     * @param table if true and if the Table to drop does not exist, fail
     *   silently, else throw
     * @param cascade true if the name argument refers to a View
     */
    public void dropTableOrView(Session session, Table table, boolean cascade) {

        writeLock.lock();

        try {
            if (table.isView()) {
                dropView(table, cascade);
            } else {
                dropTable(session, table, cascade);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void dropView(Table table, boolean cascade) {

        Schema schema = schemaMap.get(table.getSchemaName().name);

        removeSchemaObject(table.getName(), cascade);
        removeTableDependentReferences(table);
        schema.triggerLookup.removeParent(table.getName());
    }

    private void dropTable(Session session, Table table, boolean cascade) {

        Schema schema = schemaMap.get(table.getSchemaName().name);
        OrderedHashSet<Constraint> externalConstraints =
            table.getDependentExternalConstraints();
        OrderedHashSet<HsqlName> externalReferences = new OrderedHashSet<>();

        getCascadingReferencesTo(table.getName(), externalReferences);

        if (!cascade) {
            for (int i = 0; i < externalConstraints.size(); i++) {
                Constraint c       = externalConstraints.get(i);
                HsqlName   refName = c.getRefName();

                if (c.getConstraintType()
                        == SchemaObject.ConstraintTypes.MAIN) {
                    throw Error.error(
                        ErrorCode.X_42533,
                        refName.getSchemaQualifiedStatementName());
                }
            }

            if (!externalReferences.isEmpty()) {
                int i = 0;

                for (; i < externalReferences.size(); i++) {
                    HsqlName name = externalReferences.get(i);

                    if (name.parent == table.getName()) {
                        continue;
                    }

                    throw Error.error(
                        ErrorCode.X_42502,
                        name.getSchemaQualifiedStatementName());
                }
            }
        }

        OrderedHashSet<Table>    tableSet          = new OrderedHashSet<>();
        OrderedHashSet<HsqlName> constraintNameSet = new OrderedHashSet<>();
        OrderedHashSet<HsqlName> indexNameSet      = new OrderedHashSet<>();

        for (int i = 0; i < externalConstraints.size(); i++) {
            Constraint c = externalConstraints.get(i);
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

        OrderedHashSet<HsqlName> uniqueConstraintNames =
            table.getUniquePKConstraintNames();
        TableWorks tw = new TableWorks(session, table);

        tableSet = tw.dropConstraintsAndIndexes(
            tableSet,
            constraintNameSet,
            indexNameSet);

        tw.setNewTablesInSchema(tableSet);
        tw.updateConstraints(tableSet, constraintNameSet);
        removeSchemaObjects(externalReferences);
        removeTableDependentReferences(table);
        removeReferencesTo(uniqueConstraintNames);
        removeReferencesTo(table.getName());
        removeReferencesFrom(table);
        schema.tableList.remove(table.getName().name);
        schema.indexLookup.removeParent(table.getName());
        schema.constraintLookup.removeParent(table.getName());
        schema.triggerLookup.removeParent(table.getName());
        removeTable(session, table);
        recompileDependentObjects(tableSet);
    }

    private void removeTable(Session session, Table table) {

        database.getGranteeManager().removeDbObject(table.getName());
        table.releaseTriggers();

        if (!table.isView() && table.hasLobColumn()) {
            RowIterator it = table.rowIterator(session);

            while (it.next()) {
                Object[] data = it.getCurrent();

                session.sessionData.adjustLobUsageCount(table, data, -1);
            }
        }

        if (table.isTemp) {
            Session[] sessions = database.sessionManager.getAllSessions();

            for (int i = 0; i < sessions.length; i++) {
                sessions[i].sessionData.persistentStoreCollection.removeStore(
                    table);
            }
        } else {
            database.persistentStoreCollection.removeStore(table);
        }
    }

    public void setTable(int index, Table table) {

        writeLock.lock();

        try {
            Schema schema = schemaMap.get(table.getSchemaName().name);

            schema.tableList.set(index, table.getName().name, table);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Returns index of a table or view in the HashMappedList that
     *  contains the table objects for this Database.
     *
     * @param  table the Table object
     * @return  the index of the specified table or view, or -1 if not found
     */
    public int getTableIndex(Table table) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(table.getSchemaName().name);

            if (schema == null) {
                return -1;
            }

            HsqlName name = table.getName();

            return schema.tableList.getIndex(name.name);
        } finally {
            readLock.unlock();
        }
    }

    public void recompileDependentObjects(OrderedHashSet<Table> tableSet) {

        writeLock.lock();

        try {
            OrderedHashSet<HsqlName> set = new OrderedHashSet<>();

            for (int i = 0; i < tableSet.size(); i++) {
                Table table = tableSet.get(i);

                set.addAll(getReferencesTo(table.getName()));
            }

            Session session = database.sessionManager.getSysSession();

            for (int i = 0; i < set.size(); i++) {
                HsqlName name = set.get(i);

                switch (name.type) {

                    case SchemaObject.ASSERTION :
                    case SchemaObject.CONSTRAINT :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.SPECIFIC_ROUTINE :
                    case SchemaObject.TRIGGER :
                    case SchemaObject.VIEW :
                        SchemaObject object = findSchemaObject(name);

                        object.compile(session, null);
                        break;

                    default :
                }
            }

            if (Error.TRACE) {
                HsqlArrayList<Table> list = getAllTables(false);

                for (int i = 0; i < list.size(); i++) {
                    Table t = list.get(i);

                    t.verifyConstraintsIntegrity();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * After addition or removal of columns and indexes all views that
     * reference the table should be recompiled.
     */
    public void recompileDependentObjects(Table table) {

        writeLock.lock();

        try {
            OrderedHashSet<HsqlName> set = new OrderedHashSet<>();

            getCascadingReferencesTo(table.getName(), set);

            Session session = database.sessionManager.getSysSession();

            for (int i = 0; i < set.size(); i++) {
                HsqlName name = set.get(i);

                switch (name.type) {

                    case SchemaObject.ASSERTION :
                    case SchemaObject.CONSTRAINT :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.SPECIFIC_ROUTINE :
                    case SchemaObject.TRIGGER :
                    case SchemaObject.VIEW : {
                        SchemaObject object = findSchemaObject(name);

                        object.compile(session, null);
                        break;
                    }

                    default :
                }
            }

            if (Error.TRACE) {
                HsqlArrayList<Table> list = getAllTables(false);

                for (int i = 0; i < list.size(); i++) {
                    Table t = list.get(i);

                    t.verifyConstraintsIntegrity();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Collation getCollation(
            Session session,
            String name,
            String schemaName) {

        Collation collation = null;

        if (schemaName == null
                || SqlInvariants.INFORMATION_SCHEMA.equals(schemaName)) {
            try {
                collation = Collation.getCollation(name);
            } catch (HsqlException e) {}
        }

        if (collation == null) {
            schemaName = session.getSchemaName(schemaName);
            collation = (Collation) getSchemaObject(
                name,
                schemaName,
                SchemaObject.COLLATION);
        }

        return collation;
    }

    public NumberSequence findSequence(
            Session session,
            String name,
            String schemaName) {

        NumberSequence seq = getSequence(
            name,
            session.getSchemaName(schemaName),
            false);

        if (seq == null && schemaName == null) {
            schemaName = session.getSchemaName(null);

            ReferenceObject ref = findSynonym(
                name,
                schemaName,
                SchemaObject.SEQUENCE);

            if (ref != null) {
                seq = getSequence(
                    ref.target.name,
                    ref.target.schema.name,
                    false);
            }
        }

        return seq;
    }

    public NumberSequence getSequence(
            String name,
            String schemaName,
            boolean raise) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema != null) {
                NumberSequence object = schema.sequenceList.get(name);

                if (object != null) {
                    return object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getUserDefinedType(
            String name,
            String schemaName,
            boolean raise) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type findDomainOrUDT(
            Session session,
            String name,
            String prefix,
            String prePrefix,
            String prePrePrefix) {

        readLock.lock();

        try {
            Type type = (Type) findSchemaObject(
                session,
                name,
                prefix,
                prePrefix,
                SchemaObject.TYPE);

            return type;
        } finally {
            readLock.unlock();
        }
    }

    public Type getDomain(String name, String schemaName, boolean raise) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null && ((Type) object).isDomainType()) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getDistinctType(String name, String schemaName, boolean raise) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null && ((Type) object).isDistinctType()) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject getSchemaObject(
            String name,
            String schemaName,
            int type) {

        readLock.lock();

        try {
            SchemaObject object = findSchemaObject(name, schemaName, type);

            if (object == null) {
                throw Error.error(SchemaObjectSet.getErrorCode, name);
            }

            return object;
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject getCharacterSet(
            Session session,
            String name,
            String schemaName) {

        if (schemaName == null
                || SqlInvariants.INFORMATION_SCHEMA.equals(schemaName)) {
            if (name.equals("SQL_IDENTIFIER")) {
                return Charset.SQL_IDENTIFIER_CHARSET;
            }

            if (name.equals("SQL_TEXT")) {
                return Charset.SQL_TEXT;
            }

            if (name.equals("LATIN1")) {
                return Charset.LATIN1;
            }

            if (name.equals("ASCII_GRAPHIC")) {
                return Charset.ASCII_GRAPHIC;
            }
        }

        if (schemaName == null) {
            schemaName = session.getSchemaName(null);
        }

        return getSchemaObject(name, schemaName, SchemaObject.CHARSET);
    }

    public Table findTable(
            Session session,
            String name,
            String prefix,
            String prePrefix) {

        return (Table) findSchemaObject(
            session,
            name,
            prefix,
            prePrefix,
            SchemaObject.TABLE);
    }

    public SchemaObject findSchemaObject(
            Session session,
            String name,
            String prefix,
            String prePrefix,
            int type) {

        // catalog resolution here
        if (prePrefix != null
                && !prePrefix.equals(database.getCatalogName().name)) {
            return null;
        }

        if (type == SchemaObject.TABLE) {
            if (prefix == null) {
                if (session.database.sqlSyntaxOra
                        || session.database.sqlSyntaxDb2
                        || session.isProcessingScript()) {
                    if (Tokens.T_DUAL.equals(name)) {
                        return dualTable;
                    }
                }

                // in future there will be a default module for
                // session tables and variables and anonymous
                // procedural sql blocks, which can eliminate this code
                Table t = findSessionTable(session, name);

                if (t != null) {
                    return t;
                }
            } else if (prePrefix == null) {

                // allow parsing in-routine table names in older .script files
                if (Tokens.T_SESSION.equals(prefix)
                        || Tokens.T_MODULE.equals(prefix)) {
                    Table t = findSessionTable(session, name);

                    if (t != null) {
                        return t;
                    }
                }
            }
        }

        if (prefix == null) {
            prefix = session.getSchemaName(null);
        }

        if (type == SchemaObject.TABLE
                && SqlInvariants.INFORMATION_SCHEMA.equals(prefix)
                && database.dbInfo != null) {
            Table t = database.dbInfo.getSystemTable(session, name);

            if (t != null) {
                return t;
            }
        }

        return findSchemaObject(name, prefix, type);
    }

    public ReferenceObject findSynonym(
            String name,
            String schemaName,
            int type) {

        Schema schema = schemaMap.get(schemaName);

        if (schema == null) {
            return null;
        }

        ReferenceObject reference = schema.findReference(name, type);

        return reference;
    }

    public SchemaObject findAnySchemaObjectForSynonym(
            String name,
            String schemaName) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema == null) {
                return null;
            }

            return schema.findAnySchemaObjectForSynonym(name);
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject findSchemaObject(
            String name,
            String schemaName,
            int type) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            if (schema == null) {
                return null;
            }

            if (type == SchemaObject.SCHEMA) {
                return schema;
            }

            return schema.findSchemaObject(name, type);
        } finally {
            readLock.unlock();
        }
    }

    // INDEX management

    /**
     * Returns the table that has an index with the given name and schema.
     */
    Table findUserTableForIndex(
            Session session,
            String name,
            String schemaName) {

        readLock.lock();

        try {
            Schema   schema    = schemaMap.get(schemaName);
            HsqlName indexName = schema.indexLookup.getName(name);

            if (indexName == null) {
                return null;
            }

            return findUserTable(indexName.parent.name, schemaName);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Drops the index with the specified name.
     */
    void dropIndex(Session session, HsqlName name) {

        writeLock.lock();

        try {
            Table      t  = getUserTable(name.parent);
            TableWorks tw = new TableWorks(session, t);

            tw.dropIndex(name.name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Drops the constraint with the specified name.
     */
    void dropConstraint(Session session, HsqlName name, boolean cascade) {

        writeLock.lock();

        try {
            Table      t  = getUserTable(name.parent);
            TableWorks tw = new TableWorks(session, t);

            tw.dropConstraint(name.name, cascade);
        } finally {
            writeLock.unlock();
        }
    }

    void removeDependentObjects(HsqlName name) {

        writeLock.lock();

        try {
            Schema schema = schemaMap.get(name.schema.name);

            schema.indexLookup.removeParent(name);
            schema.constraintLookup.removeParent(name);
            schema.triggerLookup.removeParent(name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Removes any foreign key Constraint objects (exported keys) held by any
     *  tables referenced by the specified table. <p>
     *
     *  This method is called as the last step of a successful call to
     *  dropTable() in order to ensure that the dropped Table ceases to be
     *  referenced when enforcing referential integrity.
     *
     * @param  toDrop The table to which other tables may be holding keys.
     *      This is a table that is in the process of being dropped.
     */
    void removeExportedKeys(Table toDrop) {

        writeLock.lock();

        try {

            // toDrop.schema may be null because it is not registered
            Schema schema = schemaMap.get(toDrop.getSchemaName().name);

            for (int i = 0; i < schema.tableList.size(); i++) {
                Table        table       = schema.tableList.get(i);
                Constraint[] constraints = table.getConstraints();

                for (int j = constraints.length - 1; j >= 0; j--) {
                    Table refTable = constraints[j].getRef();

                    if (toDrop == refTable) {
                        table.removeConstraint(j);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Iterator<SchemaObject> databaseObjectIterator(
            String schemaName,
            int type) {

        readLock.lock();

        try {
            Schema schema = schemaMap.get(schemaName);

            return schema.schemaObjectIterator(type);
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<SchemaObject> databaseObjectIterator(int type) {

        readLock.lock();

        try {
            Iterator<Schema>       schemas   = schemaMap.values().iterator();
            Iterator<SchemaObject> dbObjects = new WrapperIterator<>();

            while (schemas.hasNext()) {
                Schema schema = schemas.next();
                Iterator<SchemaObject> iterator = schema.schemaObjectIterator(
                    type);

                if (iterator.hasNext()) {
                    dbObjects = new WrapperIterator<>(dbObjects, iterator);
                }
            }

            return dbObjects;
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Table> databaseTableIterator() {

        readLock.lock();

        try {
            Iterator<Schema> schemas = schemaMap.values().iterator();
            Iterator<Table>  tables  = new WrapperIterator<>();

            while (schemas.hasNext()) {
                Schema          schema   = schemas.next();
                Iterator<Table> iterator = schema.tableList.values().iterator();

                if (iterator.hasNext()) {
                    tables = new WrapperIterator<>(tables, iterator);
                }
            }

            return tables;
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Constraint> databaseCheckConstraintIterator() {

        return new Iterator<Constraint>() {

            Iterator<SchemaObject> constraints = databaseObjectIterator(
                SchemaObject.CONSTRAINT);
            Constraint current;
            boolean    b = filterToNext();
            public boolean hasNext() {
                return current != null;
            }
            public Constraint next() {

                Constraint value = current;

                filterToNext();

                return value;
            }
            private boolean filterToNext() {

                while (constraints.hasNext()) {
                    current = (Constraint) constraints.next();

                    if (current.constType
                            == SchemaObject.ConstraintTypes.CHECK) {
                        return true;
                    }
                }

                current = null;

                return false;
            }
        };
    }

    // references
    private void addReferencesFrom(SchemaObject object) {

        OrderedHashSet<HsqlName> set  = object.getReferences();
        HsqlName                 name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = set.get(i);

            referenceMap.put(referenced, name);
        }
    }

    private void removeReferencesTo(OrderedHashSet<HsqlName> set) {

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = set.get(i);

            referenceMap.remove(referenced);
        }
    }

    private void removeReferencesTo(HsqlName referenced) {
        referenceMap.remove(referenced);
    }

    private void removeReferencesFrom(SchemaObject object) {

        HsqlName                 name = object.getName();
        OrderedHashSet<HsqlName> set  = object.getReferences();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = set.get(i);

            referenceMap.remove(referenced, name);
        }
    }

    private void removeTableDependentReferences(Table table) {

        OrderedHashSet<HsqlName> mainSet = table.getReferencesForDependents();

        for (int i = 0; i < mainSet.size(); i++) {
            HsqlName     name = mainSet.get(i);
            SchemaObject object;

            switch (name.type) {

                case SchemaObject.CONSTRAINT :
                    object = table.getConstraint(name.name);
                    break;

                case SchemaObject.TRIGGER :
                    object = table.getTrigger(name.name);
                    break;

                case SchemaObject.COLUMN :
                    object = table.getColumn(table.getColumnIndex(name.name));
                    break;

                default :
                    continue;
            }

            removeReferencesFrom(object);
        }
    }

    public OrderedHashSet<HsqlName> getReferencesTo(HsqlName object) {

        readLock.lock();

        try {
            OrderedHashSet<HsqlName> set = new OrderedHashSet<>();
            Iterator<HsqlName>       it = referenceMap.getValuesIterator(
                object);

            while (it.hasNext()) {
                HsqlName name = it.next();

                set.add(name);
            }

            return set;
        } finally {
            readLock.unlock();
        }
    }

    public OrderedHashSet<HsqlName> getReferencesTo(
            HsqlName table,
            HsqlName column) {

        readLock.lock();

        try {
            OrderedHashSet<HsqlName> set = new OrderedHashSet<>();
            Iterator<HsqlName>       it  = referenceMap.getValuesIterator(
                table);

            while (it.hasNext()) {
                HsqlName                 name       = it.next();
                SchemaObject             object     = findSchemaObject(name);
                OrderedHashSet<HsqlName> references = object.getReferences();

                if (references.contains(column)) {
                    set.add(name);
                }
            }

            it = referenceMap.getValuesIterator(column);

            while (it.hasNext()) {
                HsqlName name = it.next();

                set.add(name);
            }

            return set;
        } finally {
            readLock.unlock();
        }
    }

    private boolean isReferenced(HsqlName object) {

        writeLock.lock();

        try {
            return referenceMap.containsKey(object);
        } finally {
            writeLock.unlock();
        }
    }

    //
    public void getCascadingReferencesTo(
            HsqlName object,
            OrderedHashSet<HsqlName> set) {

        readLock.lock();

        try {
            OrderedHashSet<HsqlName> newSet = new OrderedHashSet<>();
            Iterator<HsqlName>       it = referenceMap.getValuesIterator(
                object);

            while (it.hasNext()) {
                HsqlName name  = it.next();
                boolean  added = set.add(name);

                if (added) {
                    newSet.add(name);
                }
            }

            for (int i = 0; i < newSet.size(); i++) {
                HsqlName name = newSet.get(i);

                getCascadingReferencesTo(name, set);
            }
        } finally {
            readLock.unlock();
        }
    }

    public void getCascadingReferencesToSchema(
            HsqlName schema,
            OrderedHashSet<HsqlName> set) {

        Iterator<HsqlName> mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = mainIterator.next();

            if (name.schema != schema) {
                continue;
            }

            getCascadingReferencesTo(name, set);
        }

        for (int i = set.size() - 1; i >= 0; i--) {
            HsqlName name = set.get(i);

            if (name.schema == schema) {
                set.remove(i);
            }
        }
    }

    public MultiValueHashMap<HsqlName, HsqlName> getReferencesToSchema(
            String schemaName) {

        MultiValueHashMap<HsqlName, HsqlName> map = new MultiValueHashMap<>();
        Iterator<HsqlName> mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = mainIterator.next();

            if (!name.schema.name.equals(schemaName)) {
                continue;
            }

            Iterator<HsqlName> it = referenceMap.getValuesIterator(name);

            while (it.hasNext()) {
                map.put(name, it.next());
            }
        }

        return map;
    }

    //
    public HsqlName getSchemaObjectName(
            HsqlName schemaName,
            String name,
            int type,
            boolean raise) {

        readLock.lock();

        try {
            SchemaObject object;

            if (type == SchemaObject.SCHEMA) {
                object = schemaMap.get(name);
            } else {
                object = findSchemaObject(name, schemaName.name, type);
            }

            if (object == null) {
                if (raise) {
                    throw Error.error(SchemaObjectSet.getErrorCode);
                } else {
                    return null;
                }
            }

            return object.getName();
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject findSchemaObject(HsqlName name) {

        readLock.lock();

        try {
            String nameString;
            String schemaString;

            if (name.type == SchemaObject.SCHEMA) {
                nameString   = name.schema.name;
                schemaString = null;
            } else {
                nameString   = name.name;
                schemaString = name.schema.name;
            }

            return findSchemaObject(nameString, schemaString, name.type);
        } finally {
            readLock.unlock();
        }
    }

    public void checkColumnIsReferenced(HsqlName tableName, HsqlName name) {

        OrderedHashSet<HsqlName> set = getReferencesTo(tableName, name);

        if (!set.isEmpty()) {
            HsqlName objectName = set.get(0);

            throw Error.error(
                ErrorCode.X_42502,
                objectName.getSchemaQualifiedStatementName());
        }
    }

    public void checkObjectIsReferenced(HsqlName name) {

        OrderedHashSet<HsqlName> set     = getReferencesTo(name);
        HsqlName                 refName = null;

        for (int i = 0; i < set.size(); i++) {
            refName = set.get(i);

            // except columns of same table
            if (refName.parent != name) {
                break;
            }

            refName = null;
        }

        if (refName == null) {
            return;
        }

        if (name.type == SchemaObject.CONSTRAINT) {
            return;
        }

        int errorCode = ErrorCode.X_42502;

        if (refName.type == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            errorCode = ErrorCode.X_42533;
        }

        throw Error.error(errorCode, refName.getSchemaQualifiedStatementName());
    }

    public void checkSchemaNameCanChange(HsqlName name) {

        readLock.lock();

        try {
            Iterator<HsqlName> it       = referenceMap.values().iterator();
            HsqlName           refError = null;

            mainLoop:
            while (it.hasNext()) {
                HsqlName refName = it.next();

                switch (refName.type) {

                    case SchemaObject.VIEW :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.TRIGGER :
                    case SchemaObject.SPECIFIC_ROUTINE :
                        if (refName.schema == name) {
                            refError = refName;
                            break mainLoop;
                        }

                        break;

                    default :
                }
            }

            if (refError == null) {
                return;
            }

            throw Error.error(
                ErrorCode.X_42502,
                refError.getSchemaQualifiedStatementName());
        } finally {
            readLock.unlock();
        }
    }

    public void addSchemaObject(SchemaObject object) {

        writeLock.lock();

        try {
            HsqlName name   = object.getName();
            Schema   schema = schemaMap.get(name.schema.name);

            switch (name.type) {

                case SchemaObject.TABLE : {
                    OrderedHashSet<HsqlName> refs =
                        ((Table) object).getReferencesForDependents();

                    for (int i = 0; i < refs.size(); i++) {
                        HsqlName ref = refs.get(i);

                        switch (ref.type) {

                            case SchemaObject.COLUMN : {
                                int index = ((Table) object).findColumn(
                                    ref.name);
                                ColumnSchema column =
                                    ((Table) object).getColumn(
                                        index);

                                addSchemaObject(column);
                                break;
                            }
                        }
                    }

                    break;
                }

                case SchemaObject.COLUMN : {
                    OrderedHashSet<HsqlName> refs = object.getReferences();

                    if (refs == null || refs.isEmpty()) {
                        return;
                    }

                    addReferencesFrom(object);

                    return;
                }
            }

            schema.addSchemaObject(database.nameManager, object, false);
            addReferencesFrom(object);
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObject(HsqlName name, boolean cascade) {

        writeLock.lock();

        try {
            OrderedHashSet<HsqlName> objectSet = new OrderedHashSet<>();

            switch (name.type) {

                case SchemaObject.ROUTINE :
                case SchemaObject.PROCEDURE :
                case SchemaObject.FUNCTION : {
                    RoutineSchema routine = (RoutineSchema) findSchemaObject(
                        name);

                    if (routine != null) {
                        Routine[] specifics = routine.getSpecificRoutines();

                        for (int i = 0; i < specifics.length; i++) {
                            getCascadingReferencesTo(
                                specifics[i].getSpecificName(),
                                objectSet);
                        }
                    }
                }

                break;

                case SchemaObject.SEQUENCE :
                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                case SchemaObject.TYPE :
                case SchemaObject.CHARSET :
                case SchemaObject.COLLATION :
                case SchemaObject.SPECIFIC_ROUTINE :
                    getCascadingReferencesTo(name, objectSet);
                    break;

                case SchemaObject.DOMAIN :
                    OrderedHashSet<HsqlName> set = getReferencesTo(name);
                    Iterator<HsqlName>       it  = set.iterator();

                    while (it.hasNext()) {
                        HsqlName ref = it.next();

                        if (ref.type != SchemaObject.COLUMN) {
                            throw Error.error(
                                ErrorCode.X_42502,
                                ref.getSchemaQualifiedStatementName());
                        }
                    }

                    break;
            }

            if (objectSet.isEmpty()) {
                removeSchemaObject(name);

                return;
            }

            if (!cascade) {
                HsqlName objectName = objectSet.get(0);

                throw Error.error(
                    ErrorCode.X_42502,
                    objectName.getSchemaQualifiedStatementName());
            }

            objectSet.add(name);
            removeSchemaObjects(objectSet);
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObjects(OrderedHashSet<HsqlName> set) {

        writeLock.lock();

        try {
            for (int i = 0; i < set.size(); i++) {
                HsqlName name = set.get(i);

                if (name.parent != null) {
                    removeSchemaObject(name);
                }
            }

            for (int i = 0; i < set.size(); i++) {
                HsqlName name = set.get(i);

                if (name.parent == null) {
                    removeSchemaObject(name);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObject(HsqlName name) {

        writeLock.lock();

        try {
            Schema          schema = schemaMap.get(name.schema.name);
            SchemaObject    object = null;
            SchemaObjectSet set    = null;

            switch (name.type) {

                case SchemaObject.SEQUENCE :
                    set    = schema.sequenceLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.TABLE :
                case SchemaObject.VIEW : {
                    set    = schema.tableLookup;
                    object = set.getObject(name.name);
                    break;
                }

                case SchemaObject.COLUMN : {
                    Table table = (Table) findSchemaObject(name.parent);

                    if (table != null) {
                        object = table.getColumn(
                            table.getColumnIndex(name.name));
                    }

                    break;
                }

                case SchemaObject.CHARSET :
                    set    = schema.charsetLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.COLLATION :
                    set    = schema.collationLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.PROCEDURE : {
                    set = schema.procedureLookup;

                    RoutineSchema routine = (RoutineSchema) set.getObject(
                        name.name);

                    object = routine;

                    Routine[] specifics = routine.getSpecificRoutines();

                    for (int i = 0; i < specifics.length; i++) {
                        removeSchemaObject(specifics[i].getSpecificName());
                    }

                    break;
                }

                case SchemaObject.FUNCTION : {
                    set = schema.functionLookup;

                    RoutineSchema routine = (RoutineSchema) set.getObject(
                        name.name);

                    object = routine;

                    Routine[] specifics = routine.getSpecificRoutines();

                    for (int i = 0; i < specifics.length; i++) {
                        removeSchemaObject(specifics[i].getSpecificName());
                    }

                    break;
                }

                case SchemaObject.SPECIFIC_ROUTINE : {
                    set = schema.specificRLookup;

                    Routine routine = (Routine) set.getObject(name.name);

                    object = routine;

                    routine.routineSchema.removeSpecificRoutine(routine);

                    if (routine.routineSchema.getSpecificRoutines().length
                            == 0) {
                        removeSchemaObject(routine.getName());
                    }

                    break;
                }

                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    set    = schema.typeLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.INDEX :
                    set = schema.indexLookup;
                    break;

                case SchemaObject.CONSTRAINT : {
                    set = schema.constraintLookup;

                    if (name.parent.type == SchemaObject.TABLE) {
                        Table table = schema.tableList.get(name.parent.name);

                        object = table.getConstraint(name.name);

                        table.removeConstraint(name.name);
                    } else if (name.parent.type == SchemaObject.DOMAIN) {
                        Type type = (Type) schema.typeLookup.getObject(
                            name.parent.name);

                        object = type.userTypeModifier.getConstraint(name.name);

                        type.userTypeModifier.removeConstraint(name.name);
                    }

                    break;
                }

                case SchemaObject.TRIGGER : {
                    set = schema.triggerLookup;

                    Table table = schema.tableList.get(name.parent.name);

                    object = table.getTrigger(name.name);

                    if (object != null) {
                        table.removeTrigger((TriggerDef) object);
                    }

                    break;
                }

                case SchemaObject.REFERENCE : {
                    set    = schema.referenceLookup;
                    object = set.getObject(name.name);
                    break;
                }

                default :
                    throw Error.runtimeError(
                        ErrorCode.U_S0500,
                        "SchemaManager");
            }

            if (object != null) {
                database.getGranteeManager().removeDbObject(name);
                removeReferencesFrom(object);
            }

            if (set != null) {
                set.remove(name.name);
            }

            removeReferencesTo(name);
        } finally {
            writeLock.unlock();
        }
    }

    public void renameSchemaObject(HsqlName name, HsqlName newName) {

        writeLock.lock();

        try {
            if (name.schema != newName.schema) {
                throw Error.error(ErrorCode.X_42505, newName.schema.name);
            }

            checkObjectIsReferenced(name);

            Schema schema = schemaMap.get(name.schema.name);

            schema.renameObject(name, newName);
        } finally {
            writeLock.unlock();
        }
    }

    public void replaceReferences(
            SchemaObject oldObject,
            SchemaObject newObject) {

        writeLock.lock();

        try {
            removeReferencesFrom(oldObject);
            addReferencesFrom(newObject);
        } finally {
            writeLock.unlock();
        }
    }

    public List<String> getSQLArray() {

        readLock.lock();

        try {
            OrderedHashSet<HsqlName>     resolved   = new OrderedHashSet<>();
            OrderedHashSet<SchemaObject> unresolved = new OrderedHashSet<>();
            HsqlArrayList<String>        list       = new HsqlArrayList<>();
            Iterator<Schema>             schemas    = getUserSchemaIterator();

            while (schemas.hasNext()) {
                Schema schema = schemas.next();

                list.add(schema.getSQL());

                for (int round = 0; round < Schema.scriptSequenceOne.length;
                        round++) {
                    int objectType = Schema.scriptSequenceOne[round];

                    list.addAll(
                        schema.getSQLArray(objectType, resolved, unresolved));
                }

                while (true) {
                    Iterator<SchemaObject> it = unresolved.iterator();

                    if (!it.hasNext()) {
                        break;
                    }

                    OrderedHashSet<SchemaObject> newResolved =
                        new OrderedHashSet<>();

                    SchemaObjectSet.addAllSQL(
                        resolved,
                        unresolved,
                        list,
                        it,
                        newResolved);
                    unresolved.removeAll(newResolved);

                    if (newResolved.isEmpty()) {
                        break;
                    }
                }
            }

            // all NO SQL functions have been scripted, others are scripted at the end
            for (int round = 0; round < Schema.scriptSequenceTwo.length;
                    round++) {
                schemas = getUserSchemaIterator();

                while (schemas.hasNext()) {
                    Schema schema     = schemas.next();
                    int    objectType = Schema.scriptSequenceTwo[round];

                    list.addAll(
                        schema.getSQLArray(objectType, resolved, unresolved));
                }
            }

            while (true) {
                Iterator<SchemaObject> it = unresolved.iterator();

                if (!it.hasNext()) {
                    break;
                }

                OrderedHashSet<SchemaObject> newResolved =
                    new OrderedHashSet<>();

                SchemaObjectSet.addAllSQL(
                    resolved,
                    unresolved,
                    list,
                    it,
                    newResolved);
                unresolved.removeAll(newResolved);

                if (newResolved.isEmpty()) {
                    break;
                }
            }

            // forward reference routines
            Iterator<SchemaObject> it = unresolved.iterator();

            while (it.hasNext()) {
                SchemaObject object = it.next();

                if (object instanceof Routine) {
                    list.add(((Routine) object).getSQLDeclaration());
                }
            }

            it = unresolved.iterator();

            while (it.hasNext()) {
                SchemaObject object = it.next();

                if (object instanceof Routine) {
                    list.add(((Routine) object).getSQLAlter());
                } else {
                    list.add(object.getSQL());
                }
            }

            it = unresolved.iterator();

            while (it.hasNext()) {
                SchemaObject object = it.next();

                if (object instanceof ReferenceObject) {
                    list.add(object.getSQL());
                }
            }

            schemas = getUserSchemaIterator();

            while (schemas.hasNext()) {
                Schema                schema = schemas.next();
                HsqlArrayList<String> t      = schema.getTriggerSQLArray();

                if (t.size() > 0) {
                    list.add(schema.getSetSchemaSQL());
                    list.addAll(t);
                }
            }

            schemas = schemaMap.values().iterator();

            while (schemas.hasNext()) {
                Schema schema = schemas.next();

                list.addAll(schema.getSequenceRestartSQLArray());
            }

            if (defaultSchemaHsqlName != null) {
                StringBuilder sb = new StringBuilder(64);

                sb.append(Tokens.T_SET)
                  .append(' ')
                  .append(Tokens.T_DATABASE)
                  .append(' ')
                  .append(Tokens.T_DEFAULT)
                  .append(' ')
                  .append(Tokens.T_INITIAL)
                  .append(' ')
                  .append(Tokens.T_SCHEMA)
                  .append(' ')
                  .append(defaultSchemaHsqlName.statementName);
                list.add(sb.toString());
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    public List<String> getTablePropsSQLArray(boolean withHeader) {

        readLock.lock();

        try {
            HsqlArrayList<Table>  tableList = getAllTables(false);
            HsqlArrayList<String> list      = new HsqlArrayList<>();

            for (int i = 0; i < tableList.size(); i++) {
                Table t = tableList.get(i);

                if (t.isText()) {
                    String[] ddl = t.getSQLForTextSource(withHeader);

                    list.addAll(ddl);
                }

                String ddl = t.getSQLForReadOnly();

                if (ddl != null) {
                    list.add(ddl);
                }

                if (t.isCached()) {
                    ddl = t.getSQLForClustered();

                    if (ddl != null) {
                        list.add(ddl);
                    }
                }
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    public List<String> getTableSpaceSQLArray() {

        readLock.lock();

        try {
            HsqlArrayList<Table>  tableList = getAllTables(true);
            HsqlArrayList<String> list      = new HsqlArrayList<>();

            for (int i = 0; i < tableList.size(); i++) {
                Table t = tableList.get(i);

                if (t.isCached()) {
                    String ddl = t.getSQLForTableSpace();

                    if (ddl != null) {
                        list.add(ddl);
                    }
                }
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    public List<String> getIndexRootsSQLArray() {

        readLock.lock();

        try {
            long[][]              rootsArray = getIndexRoots();
            HsqlArrayList<Table>  tableList  = getAllTables(true);
            HsqlArrayList<String> list       = new HsqlArrayList<>();

            for (int i = 0; i < rootsArray.length; i++) {
                Table table = tableList.get(i);

                if (rootsArray[i] != null
                        && rootsArray[i].length > 0
                        && rootsArray[i][0] != -1) {
                    String ddl = table.getIndexRootsSQL(rootsArray[i]);

                    list.add(ddl);
                }
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    long[][] tempIndexRoots;

    public void setTempIndexRoots(long[][] roots) {
        tempIndexRoots = roots;
    }

    public long[][] getIndexRoots() {

        readLock.lock();

        try {
            if (tempIndexRoots != null) {
                long[][] roots = tempIndexRoots;

                tempIndexRoots = null;

                return roots;
            }

            HsqlArrayList<Table>  allTables = getAllTables(true);
            HsqlArrayList<long[]> list      = new HsqlArrayList<>();

            for (int i = 0, size = allTables.size(); i < size; i++) {
                Table t = allTables.get(i);

                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    long[] roots = t.getIndexRootsArray();

                    list.add(roots);
                } else {
                    list.add(null);
                }
            }

            long[][] array = new long[list.size()][];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * called after the completion of defrag
     */
    public void setIndexRoots(long[][] roots) {

        readLock.lock();

        try {
            HsqlArrayList<Table> allTables =
                database.schemaManager.getAllTables(
                    true);

            for (int i = 0, size = allTables.size(); i < size; i++) {
                Table t = allTables.get(i);

                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    long[] rootsArray = roots[i];

                    if (rootsArray != null) {
                        t.setIndexRoots(rootsArray);
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public void setDefaultTableType(int type) {
        defaultTableType = type;
    }

    public int getDefaultTableType() {
        return defaultTableType;
    }

    public void createSystemTables() {

        dualTable = TableUtil.newSingleColumnTable(
            database,
            SqlInvariants.DUAL_TABLE_HSQLNAME,
            TableBase.SYSTEM_TABLE,
            SqlInvariants.DUAL_COLUMN_HSQLNAME,
            Type.SQL_VARCHAR);

        dualTable.insertSys(
            database.sessionManager.getSysSession(),
            dualTable.getRowStore(null),
            new Object[]{ "X" });
        dualTable.setDataReadOnly(true);
    }

    static class UserSchemaFilter implements FilteredIterator.Filter<Schema> {

        public boolean test(Schema schema) {

            String name = schema.getName().name;

            return !SqlInvariants.isLobsSchemaName(name)
                   && !SqlInvariants.isSystemSchemaName(name);
        }
    }
}
