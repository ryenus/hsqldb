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
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.persist.Logger;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.types.DomainType;
import org.hsqldb.types.DistinctType;

/**
 * Manages all SCHEMA related database objects
 *
 * @author fredt@users
 * @version  1.9.0
 * @since 1.8.0
 */
public class SchemaManager {

    public static final String   SYSTEM_SCHEMA      = "SYSTEM_SCHEMA";
    public static final String   DEFINITION_SCHEMA  = "DEFINITION_SCHEMA";
    public static final String   INFORMATION_SCHEMA = "INFORMATION_SCHEMA";
    public static final String   PUBLIC_SCHEMA      = "PUBLIC";
    public static final HsqlName INFORMATION_SCHEMA_HSQLNAME;
    public static final HsqlName DEFINITION_SCHEMA_HSQLNAME;
    public static final HsqlName SYSTEM_SCHEMA_HSQLNAME;
    Database                     database;
    HsqlName                     defaultSchemaHsqlName;
    HashMappedList               schemaMap    = new HashMappedList();
    MultiValueHashMap            referenceMap = new MultiValueHashMap();

    static {
        INFORMATION_SCHEMA_HSQLNAME =
            HsqlNameManager.newHsqlSystemObjectName(INFORMATION_SCHEMA);
        DEFINITION_SCHEMA_HSQLNAME =
            HsqlNameManager.newHsqlSystemObjectName(DEFINITION_SCHEMA);
        SYSTEM_SCHEMA_HSQLNAME =
            HsqlNameManager.newHsqlSystemObjectName(SYSTEM_SCHEMA);
        INFORMATION_SCHEMA_HSQLNAME.owner = GranteeManager.getSystemRole();
        DEFINITION_SCHEMA_HSQLNAME.owner  = GranteeManager.getSystemRole();
        SYSTEM_SCHEMA_HSQLNAME.owner      = GranteeManager.getSystemRole();
    }

    SchemaManager(Database database) {

        this.database = database;

        Schema schema = new Schema();

        defaultSchemaHsqlName = schema.name;

        schemaMap.put(schema.name.name, schema);
    }

    // SCHEMA management

    /**
     * Creates a schema belonging to the given grantee.
     */
    void createSchema(HsqlName name, Grantee owner) throws HsqlException {

        if (DEFINITION_SCHEMA.equals(name.name)
                || INFORMATION_SCHEMA.equals(name.name)
                || SYSTEM_SCHEMA.equals(name.name)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        Schema schema = new Schema(name, owner);

        schemaMap.add(name.name, schema);
    }

    void dropSchema(String name, boolean cascade) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(name);

        if (schema == null) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        if (cascade) {
            OrderedHashSet externalReferences = new OrderedHashSet();

            getCascadingSchemaReferences(schema.getName(), externalReferences);
            removeSchemaObjects(externalReferences);
        } else {
            if (!schema.isEmpty()) {
                throw Trace.error(Trace.DEPENDENT_DATABASE_OBJECT_EXISTS);
            }
        }

        Iterator tableIterator =
            schema.schemaObjectIterator(SchemaObject.TABLE);

        while (tableIterator.hasNext()) {
            Table table = ((Table) tableIterator.next());

            database.getGranteeManager().removeDbObject(table.getName());
            table.drop();
        }

        Iterator sequenceIterator =
            schema.schemaObjectIterator(SchemaObject.SEQUENCE);

        while (tableIterator.hasNext()) {
            NumberSequence sequence =
                ((NumberSequence) sequenceIterator.next());

            database.getGranteeManager().removeDbObject(sequence.getName());
        }

        schema.clearStructures();
        schemaMap.remove(name);

        if (defaultSchemaHsqlName.name.equals(name)) {
            schema                = new Schema();
            defaultSchemaHsqlName = schema.name;

            schemaMap.put(schema.name.name, schema);
        }

        // these are called last and in this particular order
        database.getUserManager().removeSchemaReference(name);
        database.getSessionManager().removeSchemaReference(schema);
    }

    void renameSchema(HsqlName name, HsqlName newName) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(name.name);
        Schema exists = (Schema) schemaMap.get(newName.name);

        if (schema == null || exists != null
                || INFORMATION_SCHEMA.equals(newName)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              schema == null ? name.name
                                             : newName.name);
        }

        schema.name.rename(newName);

        int index = schemaMap.getIndex(name);

        schemaMap.set(index, newName.name, schema);
    }

    void clearStructures() {

        Iterator it = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            schema.clearStructures();
        }
    }

    public Iterator userSchemaNameIterator() {
        return schemaMap.keySet().iterator();
    }

    HsqlName toSchemaHsqlName(String name) {

        Schema schema = (Schema) schemaMap.get(name);

        return schema == null ? null
                              : schema.name;
    }

    public Grantee toSchemaOwner(String name) {

        // Note that INFORMATION_SCHEMA and DEFINITION_SCHEMA aren't in the
        // backing map.
        // This may not be the most elegant solution, but it is the safest
        // (without doing a code review for implications of adding
        // them to the map).
        if (INFORMATION_SCHEMA_HSQLNAME.name.equals(name)) {
            return INFORMATION_SCHEMA_HSQLNAME.owner;
        } else if (DEFINITION_SCHEMA_HSQLNAME.name.equals(name)) {
            return DEFINITION_SCHEMA_HSQLNAME.owner;
        }

        Schema schema = (Schema) schemaMap.get(name);

        return schema == null ? null
                              : schema.owner;
    }

    public HsqlName getDefaultSchemaHsqlName() {
        return defaultSchemaHsqlName;
    }

    boolean schemaExists(String name) {

        return INFORMATION_SCHEMA.equals(name)
               || DEFINITION_SCHEMA.equals(name)
               || schemaMap.containsKey(name);
    }

    /**
     * If schemaName is null, return the current schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * throw.
     */
    public HsqlName getSchemaHsqlName(String name) throws HsqlException {

        if (name == null) {
            return defaultSchemaHsqlName;
        }

        if (INFORMATION_SCHEMA.equals(name)) {
            return INFORMATION_SCHEMA_HSQLNAME;
        } else if (DEFINITION_SCHEMA.equals(name)) {
            return DEFINITION_SCHEMA_HSQLNAME;
        }

        Schema schema = ((Schema) schemaMap.get(name));

        if (schema == null) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS, name);
        }

        return schema.name;
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) throws HsqlException {
        return getSchemaHsqlName(name).name;
    }

    /**
     * Iterator includes INFORMATION_SCHEMA and DEFINITION_SCHEMA
     */
    public Iterator fullSchemaNamesIterator() {

        Object[] sysSchem = new Object[] {
            INFORMATION_SCHEMA, DEFINITION_SCHEMA
        };

        return new WrapperIterator(new WrapperIterator(sysSchem),
                                   schemaMap.keySet().iterator());
    }

    /**
     * is a schema read-only
     */
    public boolean isSystemSchema(HsqlName schema) {

        return INFORMATION_SCHEMA_HSQLNAME == schema
               || DEFINITION_SCHEMA_HSQLNAME == schema
               || SYSTEM_SCHEMA_HSQLNAME == schema;
    }

    public boolean isSystemSchema(String schema) {

        return INFORMATION_SCHEMA.equals(schema)
               || DEFINITION_SCHEMA.equals(schema)
               || SYSTEM_SCHEMA.equals(schema);
    }

    /**
     * is a grantee the authorization of any schema
     */
    boolean isAuthorisation(Grantee grantee) {

        Iterator schemas = userSchemaNameIterator();

        while (schemas.hasNext()) {
            String schemaName = (String) schemas.next();

            if (grantee.equals(toSchemaOwner(schemaName))) {
                return true;
            }
        }

        return false;
    }

    /**
     * drop all schemas with the given authorisation
     */
    void dropSchemas(Grantee grantee, boolean cascade) throws HsqlException {

        HsqlArrayList list = getSchemas(grantee);
        Iterator      it   = list.iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            dropSchema(schema.name.name, cascade);
        }
    }

    HsqlArrayList getSchemas(Grantee grantee) {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            if (grantee.equals(schema.owner)) {
                list.add(schema);
            }
        }

        return list;
    }

    boolean hasSchemas(Grantee grantee) {

        Iterator it = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            if (grantee.equals(schema.owner)) {
                return true;
            }
        }

        return false;
    }

    /**
     *  Returns an HsqlArrayList containing references to all non-system
     *  tables and views. This includes all tables and views registered with
     *  this Database.
     */
    public HsqlArrayList getAllTables() {

        Iterator      schemas   = userSchemaNameIterator();
        HsqlArrayList alltables = new HsqlArrayList();

        while (schemas.hasNext()) {
            String         name    = (String) schemas.next();
            HashMappedList current = getTables(name);

            alltables.addAll(current.values());
        }

        return alltables;
    }

    public HashMappedList getTables(String schema) {

        Schema temp = (Schema) schemaMap.get(schema);

        return temp.tableList;
    }

    void checkSchemaObjectNotExists(HsqlName name) throws HsqlException {

        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = null;

        switch (name.type) {

            case SchemaObject.SEQUENCE :
                set = schema.sequenceLookup;
                break;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                set = schema.tableLookup;
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                set = schema.typeLookup;
                break;

            case SchemaObject.INDEX :
                set = schema.indexLookup;
                break;

            case SchemaObject.CONSTRAINT :
                set = schema.constraintLookup;
                break;

            case SchemaObject.TRIGGER :
                set = schema.triggerLookup;
                break;
        }

        set.checkAdd(name);
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session, or any system table of the given
     *  name. It excludes any temp tables created in other Sessions.
     *  Throws if the table does not exist in the context.
     */
    public Table getTable(Session session, String name,
                          String schema) throws HsqlException {

        Table t = null;

        if (schema == null) {
            t = findSessionTable(session, name, schema);
        }

        if (t == null) {
            schema = session.getSchemaName(schema);
            t      = findUserTable(session, name, schema);
        }

        if (t == null) {
            if (INFORMATION_SCHEMA.equals(schema) && database.dbInfo != null) {
                t = database.dbInfo.getSystemTable(session, name);
            }
        }

        if (t == null) {
            throw Trace.error(Trace.TABLE_NOT_FOUND, name);
        }

        return t;
    }

    public Table getUserTable(Session session,
                              HsqlName name) throws HsqlException {
        return getUserTable(session, name.name, name.schema.name);
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session. It excludes system tables and
     *  any temp tables created in different Sessions.
     *  Throws if the table does not exist in the context.
     */
    public Table getUserTable(Session session, String name,
                              String schema) throws HsqlException {

        Table t = findUserTable(session, name, schema);

        if (t == null) {
            throw Trace.error(Trace.TABLE_NOT_FOUND, name);
        }

        return t;
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified schema. It excludes system tables.
     *  Returns null if the table does not exist in the context.
     */
    public Table findUserTable(Session session, String name,
                               String schemaName) {

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema == null) {
            return null;
        }

        int i = schema.tableList.getIndex(name);

        if (i == -1) {
            return null;
        }

        return (Table) schema.tableList.get(i);
    }

    /**
     *  Returns the specified session context table.
     *  Returns null if the table does not exist in the context.
     */
    public Table findSessionTable(Session session, String name,
                                  String schemaName) {
        return session.findSessionTable(name);
    }

    /**
     * Clear copies of a temporary table from all sessions apart from one.
     */
    void clearTempTables(Session exclude, Table table) {

        Session[] sessions = database.sessionManager.getAllSessions();
        Index[]   indexes  = table.getIndexes();

        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] != exclude) {
                for (int j = 0; j < indexes.length; j++) {
                    sessions[i].sessionData.dropIndex(indexes[j].getName(),
                                                      false);
                }
            }
        }
    }

    /**
     *  Drops the specified user-defined view or table from this Database
     *  object. <p>
     *
     *  The process of dropping a table or view includes:
     *  <OL>
     *    <LI> checking that the specified Session's currently connected User
     *    has the right to perform this operation and refusing to proceed if
     *    not by throwing.
     *    <LI> checking for referential constraints that conflict with this
     *    operation and refusing to proceed if they exist by throwing.</LI>
     *
     *    <LI> removing the specified Table from this Database object.
     *    <LI> removing any exported foreign keys Constraint objects held by
     *    any tables referenced by the table to be dropped. This is especially
     *    important so that the dropped Table ceases to be referenced,
     *    eventually allowing its full garbage collection.
     *    <LI>
     *  </OL>
     *  <p>
     *
     * @param  name of the table or view to drop
     * @param  ifExists if true and if the Table to drop does not exist, fail
     *      silently, else throw
     * @param  isView true if the name argument refers to a View
     * @param  session the connected context in which to perform this
     *      operation
     * @throws  HsqlException if any of the checks listed above fail
     */
    void dropTableOrView(Session session, Table table,
                         boolean cascade) throws HsqlException {

// ft - concurrent
        session.commit();

        if (table.isView()) {
            removeSchemaObject(table.getName(), cascade);
        } else {
            dropTable(session, table, cascade);
        }
    }

    void dropTable(Session session, Table table,
                   boolean cascade) throws HsqlException {

        Schema schema    = (Schema) schemaMap.get(table.getSchemaName().name);
        int    dropIndex = schema.tableList.getIndex(table.getName().name);
        OrderedHashSet externalConstraints =
            table.getDependentExternalConstraints();
        OrderedHashSet externalReferences =
            getReferencingObjects(table.getName());

        if (!cascade) {
            for (int i = 0; i < externalConstraints.size(); i++) {
                Constraint c       = (Constraint) externalConstraints.get(i);
                HsqlName   name    = c.getName();
                HsqlName   refname = c.getRef().getName();

                if (c.getType() == Constraint.MAIN) {
                    throw Trace.error(Trace.TABLE_REFERENCED_CONSTRAINT,
                                      Trace.Database_dropTable, new Object[] {
                        name.schema.name + '.' + name.name,
                        refname.schema.name + '.' + refname.name
                    });
                }
            }

            if (!externalReferences.isEmpty()) {
                HsqlName name = (HsqlName) externalReferences.get(0);

                throw Trace.error(Trace.TABLE_REFERENCED_VIEW,
                                  name.schema.name + '.' + name.name);
            }
        }

        OrderedHashSet tableSet          = new OrderedHashSet();
        OrderedHashSet constraintNameSet = new OrderedHashSet();
        OrderedHashSet indexNameSet      = new OrderedHashSet();

        for (int i = 0; i < externalConstraints.size(); i++) {
            Constraint c = (Constraint) externalConstraints.get(i);
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

        TableWorks tw = new TableWorks(session, table);

        tw.makeNewTables(tableSet, constraintNameSet, indexNameSet);
        tw.setNewTablesInSchema(tableSet);
        tw.updateConstraints(tableSet, constraintNameSet);
        removeSchemaObjects(externalReferences);
        removeReferencedObject(table.getName());
        schema.tableList.remove(dropIndex);
        database.getGranteeManager().removeDbObject(table.getName());
        schema.triggerLookup.removeParent(table.tableName);
        schema.indexLookup.removeParent(table.tableName);
        schema.constraintLookup.removeParent(table.tableName);
        table.dropTriggers();
        table.drop();
    }

    void setTable(int index, Table table) {

        Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

        schema.tableList.set(index, table.getName().name, table);
    }

    /**
     *  Returns index of a table or view in the HashMappedList that
     *  contains the table objects for this Database.
     *
     * @param  table the Table object
     * @return  the index of the specified table or view, or -1 if not found
     */
    int getTableIndex(Table table) {

        Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

        if (schema == null) {
            return -1;
        }

        HsqlName name = table.getName();

        return schema.tableList.getIndex(name.name);
    }

    /**
     * After addition or removal of columns and indexes all views that
     * reference the table should be recompiled.
     */
    void recompileDependentObjects(Table table) throws HsqlException {

        OrderedHashSet set     = getReferencingObjects(table.getName());
        Session        session = database.sessionManager.getSysSession();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            switch (name.type) {

                case SchemaObject.VIEW :
                case SchemaObject.CONSTRAINT :
                case SchemaObject.ASSERTION :
                    SchemaObject object = getSchemaObject(name);

                    object.compile(session);
                    break;
            }
        }
    }

    // SEQUENCE management
    void logSequences(Session session, Logger logger) throws HsqlException {

        for (int i = 0, size = schemaMap.size(); i < size; i++) {
            Schema         schema       = (Schema) schemaMap.get(i);
            HashMappedList sequenceList = schema.sequenceList;

            for (int j = 0; j < sequenceList.size(); j++) {
                NumberSequence seq = (NumberSequence) sequenceList.get(j);

                if (seq.resetWasUsed()) {
                    logger.writeSequenceStatement(session, seq);
                }
            }
        }
    }

    NumberSequence getSequence(String name, String schemaName,
                               boolean raise) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema != null) {
            NumberSequence object =
                (NumberSequence) schema.sequenceList.get(name);

            if (object != null) {
                return object;
            }
        }

        if (raise) {
            throw Trace.error(Trace.SEQUENCE_NOT_FOUND, name);
        }

        return null;
    }

    public DomainType getDomain(String name, String schemaName,
                                boolean raise) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema != null) {
            SchemaObject object = schema.typeLookup.getObject(name);

            if (object instanceof DomainType) {
                return (DomainType) object;
            }
        }

        if (raise) {
            throw Trace.error(Trace.SEQUENCE_NOT_FOUND, name);
        }

        return null;
    }

    public DistinctType getDistinctType(String name, String schemaName,
                                        boolean raise) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema != null) {
            SchemaObject object = schema.typeLookup.getObject(name);

            if (object instanceof DistinctType) {
                return (DistinctType) object;
            }
        }

        if (raise) {
            throw Trace.error(Trace.SEQUENCE_NOT_FOUND, name);
        }

        return null;
    }

    public SchemaObject findSchemaObject(String name, String schemaName,
                                         int type) {

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema == null) {
            return null;
        }

        SchemaObjectSet set = null;
        HsqlName        tableName;
        Table           table;

        switch (type) {

            case SchemaObject.SEQUENCE :
                return schema.sequenceLookup.getObject(name);

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return schema.sequenceLookup.getObject(name);

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return schema.sequenceLookup.getObject(name);

            case SchemaObject.INDEX :
                set       = schema.indexLookup;
                tableName = set.getName(name);

                if (tableName == null) {
                    return null;
                }

                table = (Table) schema.tableList.get(name);

                return table.getIndex(name);

            case SchemaObject.CONSTRAINT :
                set       = schema.indexLookup;
                tableName = set.getName(name);

                if (tableName == null) {
                    return null;
                }

                table = (Table) schema.tableList.get(name);

                return table.getConstraint(name);

            case SchemaObject.TRIGGER :
                set       = schema.indexLookup;
                tableName = set.getName(name);

                if (tableName == null) {
                    return null;
                }

                table = (Table) schema.tableList.get(name);

                return table.getTrigger(name);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SchemaManager");
        }
    }

    // INDEX management

    /**
     * Returns the table that has an index with the given name and schema.
     */
    Table findUserTableForIndex(Session session, String name,
                                String schemaName) {

        Schema   schema    = (Schema) schemaMap.get(schemaName);
        HsqlName indexName = schema.indexLookup.getName(name);

        if (indexName == null) {
            return null;
        }

        return findUserTable(session, indexName.parent.name, schemaName);
    }

    /**
     * Drops the index with the specified name.
     */
    void dropIndex(Session session, String indexname, String schema,
                   boolean ifExists) throws HsqlException {

        Table t = findUserTableForIndex(session, indexname, schema);

        if (t == null) {
            if (ifExists) {
                return;
            } else {
                throw Trace.error(Trace.INDEX_NOT_FOUND, indexname);
            }
        }

        Index index = t.getIndex(indexname);

        if (index.isConstraint()) {
            throw Trace.error(Trace.DROP_PRIMARY_KEY, indexname);
        }

        session.commit();

        TableWorks tw = new TableWorks(session, t);

        tw.dropIndex(indexname);
    }

    void removeDependentObjects(HsqlName name) {

        Schema schema = (Schema) schemaMap.get(name.schema.name);

        schema.indexLookup.removeParent(name);
        schema.constraintLookup.removeParent(name);
        schema.triggerLookup.removeParent(name);
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

        // toDrop.schema may be null because it is not registerd
        Schema schema = (Schema) schemaMap.get(toDrop.getSchemaName().name);

        for (int i = 0; i < schema.tableList.size(); i++) {
            Table table = (Table) schema.tableList.get(i);

            for (int j = table.constraintList.length - 1; j >= 0; j--) {
                Table refTable = table.constraintList[j].getRef();

                if (toDrop == refTable) {
                    table.constraintList =
                        (Constraint[]) ArrayUtil.toAdjustedArray(
                            table.constraintList, null, j, -1);
                }
            }
        }
    }

    public Iterator databaseObjectIterator(String schemaName, int type) {

        Schema schema = (Schema) schemaMap.get(schemaName);

        return schema.schemaObjectIterator(type);
    }

    public Iterator databaseObjectIterator(int type) {

        Iterator it      = schemaMap.values().iterator();
        Iterator objects = new WrapperIterator();

        while (it.hasNext()) {
            Schema temp = (Schema) it.next();

            objects = new WrapperIterator(objects,
                                          temp.schemaObjectIterator(type));
        }

        return objects;
    }

    // references
    private void addReferences(SchemaObject object) {

        OrderedHashSet set = object.getReferences();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            if (referenced.type == SchemaObject.COLUMN) {
                referenceMap.put(referenced.parent, object.getName());
            } else {
                referenceMap.put(referenced, object.getName());
            }
        }
    }

    private void removeReferencedObject(HsqlName referenced) {
        referenceMap.remove(referenced);
    }

    private void removeReferencingObject(SchemaObject object) {

        OrderedHashSet set = object.getReferences();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            referenceMap.remove(referenced, object.getName());
        }
    }

    OrderedHashSet getReferencingObjects(HsqlName object) {

        OrderedHashSet set = new OrderedHashSet();
        Iterator       it  = referenceMap.get(object);

        while (it.hasNext()) {
            HsqlName name = (HsqlName) it.next();

            set.add(name);
        }

        return set;
    }

    OrderedHashSet getReferencingObjects(HsqlName table,
                                         HsqlName column)
                                         throws HsqlException {

        OrderedHashSet set = new OrderedHashSet();
        Iterator       it  = referenceMap.get(table);

        while (it.hasNext()) {
            HsqlName       name       = (HsqlName) it.next();
            SchemaObject   object     = getSchemaObject(name);
            OrderedHashSet references = object.getReferences();

            if (references.contains(column)) {
                set.add(name);
            }
        }

        return set;
    }

    private boolean isReferenced(HsqlName object) {
        return referenceMap.containsKey(object);
    }

    //
    private void getCascadingReferences(HsqlName object, OrderedHashSet set) {

        OrderedHashSet newSet = new OrderedHashSet();
        Iterator       it     = referenceMap.get(object);

        while (it.hasNext()) {
            HsqlName name  = (HsqlName) it.next();
            boolean  added = set.add(name);

            if (added) {
                newSet.add(name);
            }
        }

        for (int i = 0; i < newSet.size(); i++) {
            HsqlName name = (HsqlName) newSet.get(i);

            getCascadingReferences(name, set);
        }
    }

    //
    private void getCascadingSchemaReferences(HsqlName schema,
            OrderedHashSet set) {

        Iterator mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = (HsqlName) mainIterator.next();

            if (name.schema != schema) {
                continue;
            }

            getCascadingReferences(name, set);
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            if (name.schema == schema) {
                set.remove(i);

                i--;
            }
        }
    }

    //
    HsqlName getSchemaObjectName(HsqlName schemaName, String name,
                                 int type) throws HsqlException {

        Schema          schema = (Schema) schemaMap.get(schemaName.name);
        SchemaObjectSet set    = null;

        switch (type) {

            case SchemaObject.SEQUENCE :
                set = schema.sequenceLookup;
                break;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                set = schema.tableLookup;
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                set = schema.typeLookup;
                break;

            case SchemaObject.INDEX :
                set = schema.indexLookup;
                break;

            case SchemaObject.CONSTRAINT :
                set = schema.constraintLookup;
                break;

            case SchemaObject.TRIGGER :
                set = schema.triggerLookup;
                break;
        }

        set.checkExists(name);

        return set.getName(name);
    }

    SchemaObject getSchemaObject(HsqlName name) throws HsqlException {

        Schema schema = (Schema) schemaMap.get(name.schema.name);

        switch (name.type) {

            case SchemaObject.SEQUENCE :
                return (SchemaObject) schema.sequenceList.get(name.name);

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return (SchemaObject) schema.tableList.get(name.name);

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return schema.typeLookup.getObject(name.name);

            case SchemaObject.TRIGGER : {
                HsqlName tableName = name.parent;
                Table    table = (Table) schema.tableList.get(tableName.name);

                return table.getTrigger(name.name);
            }
            case SchemaObject.CONSTRAINT : {
                HsqlName tableName = name.parent;
                Table    table = (Table) schema.tableList.get(tableName.name);

                return table.getConstraint(name.name);
            }
            case SchemaObject.ASSERTION :
                return null;

            case SchemaObject.INDEX :
                HsqlName tableName = name.parent;
                Table    table = (Table) schema.tableList.get(tableName.name);

                return table.getIndex(name.name);
        }

        return null;
    }

    void checkColumnIsReferenced(HsqlName name) throws HsqlException {

        OrderedHashSet set = getReferencingObjects(name.parent, name);

        if (!set.isEmpty()) {
            name = (HsqlName) set.get(0);

            throw Trace.error(Trace.COLUMN_IS_REFERENCED,
                              name.schema.name + '.' + name.name);
        }
    }

    void checkObjectIsReferenced(HsqlName name) throws HsqlException {

        OrderedHashSet set = getReferencingObjects(name);

        if (set.isEmpty()) {
            return;
        }

        int      error   = Trace.TABLE_REFERENCED_VIEW;
        HsqlName refName = (HsqlName) set.get(0);

        switch (name.type) {

            case SchemaObject.VIEW :
            case SchemaObject.TABLE :
                if (refName.type == SchemaObject.VIEW) {
                    error = Trace.TABLE_REFERENCED_VIEW;
                } else {
                    error = Trace.TABLE_REFERENCED_CONSTRAINT;
                }
                break;

            case SchemaObject.SEQUENCE :
                error = Trace.SEQUENCE_REFERENCED_BY_VIEW;
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                error = Trace.SQL_OBJECT_IS_REFERENCED;
                break;
        }

        throw Trace.error(error, name.schema.name + '.' + refName.name);
    }

    void addSchemaObject(SchemaObject object) throws HsqlException {

        HsqlName        name   = object.getName();
        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = null;

        switch (name.type) {

            case SchemaObject.SEQUENCE :
                set = schema.sequenceLookup;
                break;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                set = schema.tableLookup;
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                set = schema.typeLookup;
                break;

            case SchemaObject.INDEX :
                set = schema.indexLookup;
                break;

            case SchemaObject.CONSTRAINT :
                set = schema.constraintLookup;
                break;

            case SchemaObject.TRIGGER :
                set = schema.triggerLookup;
                break;
        }

        set.add(object);
        addReferences(object);
    }

    void removeSchemaObject(HsqlName name,
                            boolean cascade) throws HsqlException {

        OrderedHashSet objectSet = new OrderedHashSet();

        switch (name.type) {

            case SchemaObject.SEQUENCE :
            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                getCascadingReferences(name, objectSet);
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                break;
        }

        if (objectSet.isEmpty()) {
            removeSchemaObject(name);

            return;
        }

        if (!cascade) {
            throw Trace.error(Trace.SQL_OBJECT_IS_REFERENCED);
        }

        objectSet.add(name);
        removeSchemaObjects(objectSet);
    }

    void removeSchemaObjects(OrderedHashSet set) throws HsqlException {

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            removeSchemaObject(name);
        }
    }

    void removeSchemaObject(HsqlName name) throws HsqlException {

        Schema          schema = (Schema) schemaMap.get(name.schema.name);
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

                set.remove(name.name);

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
                    Table table =
                        (Table) schema.tableList.get(name.parent.name);

                    object = table.getConstraint(name.name);

                    table.removeConstraint(name.name);
                } else if (name.parent.type == SchemaObject.DOMAIN) {
                    DomainType domain =
                        (DomainType) schema.tableList.get(name.parent.name);

                    object = domain.getConstraint(name.name);

                    domain.removeConstraint(name.name);
                }

                break;
            }
            case SchemaObject.TRIGGER : {
                set = schema.triggerLookup;

                Table table = (Table) schema.tableList.get(name.parent.name);

                object = table.getTrigger(name.name);

                table.removeTrigger(name.name);

                break;
            }
        }

        if (object != null) {
            database.getGranteeManager().removeDbObject(object.getName());
            removeReferencingObject(object);
        }

        set.remove(name.name);
        removeReferencedObject(name);
    }

    void renameSchemaObject(HsqlName name,
                            HsqlName newName) throws HsqlException {

        if (name.schema != newName.schema) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        checkObjectIsReferenced(name);

        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = null;

        switch (name.type) {

            case SchemaObject.SEQUENCE :
                set = schema.sequenceLookup;
                break;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                set = schema.tableLookup;
                break;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                set = schema.typeLookup;
                break;

            case SchemaObject.INDEX :
                set = schema.indexLookup;
                break;

            case SchemaObject.CONSTRAINT :
                set = schema.constraintLookup;
                break;

            case SchemaObject.TRIGGER :
                set = schema.triggerLookup;
                break;
        }

        set.rename(name, newName);
    }

    public class Schema {

        HsqlName        name;
        SchemaObjectSet triggerLookup;
        SchemaObjectSet constraintLookup;
        SchemaObjectSet indexLookup;
        SchemaObjectSet tableLookup;
        SchemaObjectSet sequenceLookup;
        SchemaObjectSet typeLookup;
        HashMappedList  tableList;
        HashMappedList  sequenceList;
        Grantee         owner;

        Schema() {

            this(database.nameManager
                .newHsqlName(PUBLIC_SCHEMA, false, SchemaObject
                    .SCHEMA), database.getGranteeManager().getDBARole());
        }

        Schema(String name, boolean isquoted, Grantee owner) {
            this(database.nameManager.newHsqlName(
                name, isquoted, SchemaObject.SCHEMA), owner);
        }

        Schema(HsqlName name, Grantee owner) {

            this.name        = name;
            triggerLookup    = new SchemaObjectSet(SchemaObject.TRIGGER);
            indexLookup      = new SchemaObjectSet(SchemaObject.INDEX);
            constraintLookup = new SchemaObjectSet(SchemaObject.CONSTRAINT);
            tableLookup      = new SchemaObjectSet(SchemaObject.TABLE);
            sequenceLookup   = new SchemaObjectSet(SchemaObject.SEQUENCE);
            typeLookup       = new SchemaObjectSet(SchemaObject.TYPE);
            tableList        = (HashMappedList) tableLookup.map;
            sequenceList     = (HashMappedList) sequenceLookup.map;
            this.owner       = owner;
            name.owner       = owner;
        }

        boolean isEmpty() {
            return sequenceList.isEmpty() && tableList.isEmpty();
        }

        Iterator schemaObjectIterator(int type) {

            switch (type) {

                case SchemaObject.SEQUENCE :
                    return sequenceLookup.map.values().iterator();

                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                    return tableLookup.map.values().iterator();

                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    return typeLookup.map.values().iterator();

                case SchemaObject.INDEX :
                case SchemaObject.CONSTRAINT :
                case SchemaObject.TRIGGER :
                default :
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION,
                        "SchemaObjectSet");
            }
        }

        void clearStructures() {

            for (int i = 0; i < tableList.size(); i++) {
                Table table = (Table) tableList.get(i);

                table.dropTriggers();
            }

            tableList.clear();
            sequenceList.clear();

            triggerLookup    = null;
            indexLookup      = null;
            constraintLookup = null;
            sequenceLookup   = null;
            tableLookup      = null;
            typeLookup       = null;
        }

        public HsqlName getName() {
            return name;
        }

        public Grantee getOwner() {
            return owner;
        }
    }
}
