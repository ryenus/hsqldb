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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Type;

/**
 * Representation of a Schema.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.7.3
 * @since 1.9.0
*/
public final class Schema implements SchemaObject {
    //J-

    static int[] scriptSequenceOne = new int[] {
        CHARSET,
        COLLATION,
        TYPE,
        SEQUENCE,
        FUNCTION,
    };

    static int[] scriptSequenceTwo = new int[] {
        TABLE,
        PROCEDURE,
        TRIGGER,
        REFERENCE,
        MODULE
    };
    //J+
    private final HsqlName name;
    SchemaObjectSet  assertionLookup;
    SchemaObjectSet  charsetLookup;
    SchemaObjectSet  collationLookup;
    SchemaObjectSet  conditionLookup;
    SchemaObjectSet  constraintLookup;
    SchemaObjectSet  functionLookup;
    SchemaObjectSet  indexLookup;
    SchemaObjectSet  moduleLookup;
    SchemaObjectSet  procedureLookup;
    SchemaObjectSet  referenceLookup;
    SchemaObjectSet  sequenceLookup;
    SchemaObjectSet  specificRoutineLookup;
    SchemaObjectSet  tableLookup;
    SchemaObjectSet  triggerLookup;
    SchemaObjectSet  typeLookup;
    OrderedHashMap<String, Table> tableList;
    OrderedHashMap<String, NumberSequence> sequenceList;
    OrderedHashMap<String, ReferenceObject> referenceList;
    long             changeTimestamp;

    public Schema(HsqlName name, Grantee owner) {

        this.name        = name;
        assertionLookup  = new SchemaObjectSet(SchemaObject.ASSERTION);
        charsetLookup    = new SchemaObjectSet(SchemaObject.CHARSET);
        collationLookup  = new SchemaObjectSet(SchemaObject.COLLATION);
        constraintLookup = new SchemaObjectSet(SchemaObject.CONSTRAINT);
        conditionLookup  = new SchemaObjectSet(SchemaObject.EXCEPTION);
        functionLookup   = new SchemaObjectSet(SchemaObject.FUNCTION);
        indexLookup      = new SchemaObjectSet(SchemaObject.INDEX);
        moduleLookup     = new SchemaObjectSet(SchemaObject.MODULE);
        procedureLookup  = new SchemaObjectSet(SchemaObject.PROCEDURE);
        referenceLookup  = new SchemaObjectSet(SchemaObject.REFERENCE);
        sequenceLookup   = new SchemaObjectSet(SchemaObject.SEQUENCE);
        specificRoutineLookup = new SchemaObjectSet(SchemaObject.SPECIFIC_ROUTINE);
        tableLookup      = new SchemaObjectSet(SchemaObject.TABLE);
        triggerLookup    = new SchemaObjectSet(SchemaObject.TRIGGER);
        typeLookup       = new SchemaObjectSet(SchemaObject.TYPE);
        sequenceList     = (OrderedHashMap<String, NumberSequence>) sequenceLookup.getMap();
        referenceList    = (OrderedHashMap<String, ReferenceObject>) referenceLookup.getMap();
        tableList        = (OrderedHashMap<String, Table>) tableLookup.getMap();
        name.owner       = owner;
    }

    public int getType() {
        return SchemaObject.SCHEMA;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return null;
    }

    public HsqlName getCatalogName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.owner;
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public String getSQL() {

        StringBuilder sb = new StringBuilder(128);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(getName().statementName).append(' ');
        sb.append(Tokens.T_AUTHORIZATION).append(' ');
        sb.append(getOwner().getName().getStatementName());

        return sb.toString();
    }

    String getSetSchemaSQL() {

        StringBuilder sb = new StringBuilder();

        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(name.statementName);

        return sb.toString();
    }

    public HsqlArrayList<String> getSQLArray(int objectType, OrderedHashSet<HsqlName> resolved,
                                     OrderedHashSet<SchemaObject> unresolved) {

        HsqlArrayList<String> list = new HsqlArrayList<>();

        switch (objectType) {

            case CHARSET :
                charsetLookup.getSQL(list, resolved, unresolved);
                break;

            case COLLATION :
                collationLookup.getSQL(list, resolved, unresolved);
                break;

            case FUNCTION :
                functionLookup.getSQL(list, resolved, unresolved);
                break;

            case PROCEDURE :
                procedureLookup.getSQL(list, resolved, unresolved);
                break;

            case REFERENCE :
                referenceLookup.getSQL(list, resolved, unresolved);
                break;

            case SEQUENCE :
                sequenceLookup.getSQL(list, resolved, unresolved);
                break;

            case TABLE :
                tableLookup.getSQL(list, resolved, unresolved);
                break;

            case TYPE :
                typeLookup.getSQL(list, resolved, unresolved);
                break;
        }

        return list;
    }

    public HsqlArrayList<String> getSequenceRestartSQLArray() {

        HsqlArrayList<String>    list = new HsqlArrayList<>();
        Iterator<NumberSequence> it   = sequenceList.values().iterator();

        while (it.hasNext()) {
            NumberSequence sequence = it.next();
            String         ddl      = sequence.getRestartSQL();

            list.add(ddl);
        }

        return list;
    }

    public HsqlArrayList<String> getTriggerSQLArray() {

        HsqlArrayList<String> list = new HsqlArrayList<>();
        Iterator<Table> it = tableList.values().iterator();

        while (it.hasNext()) {
            Table    table = it.next();
            HsqlArrayList<String> ddl   = table.getTriggerSQLArray();

            list.addAll(ddl);
        }

        return list;
    }

    boolean isEmpty() {

        return sequenceLookup.isEmpty() && tableLookup.isEmpty()
               && typeLookup.isEmpty() && charsetLookup.isEmpty()
               && collationLookup.isEmpty() && specificRoutineLookup.isEmpty();
    }

    private SchemaObjectSet getObjectSet(int type) {

        switch (type) {

            case SchemaObject.ASSERTION :
                return assertionLookup;

            case SchemaObject.CHARSET :
                return charsetLookup;

            case SchemaObject.COLLATION :
                return collationLookup;

            case SchemaObject.CONSTRAINT :
                return constraintLookup;

            case SchemaObject.EXCEPTION :
                return conditionLookup;

            case SchemaObject.FUNCTION :
                return functionLookup;

            case SchemaObject.INDEX :
                return indexLookup;

            case SchemaObject.MODULE :
                return moduleLookup;

            case SchemaObject.PROCEDURE :
                return procedureLookup;

            case SchemaObject.REFERENCE :
                return referenceLookup;

            case SchemaObject.SEQUENCE :
                return sequenceLookup;

            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup;

            case SchemaObject.TRIGGER :
                return triggerLookup;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Schema");
        }
    }

    public Iterator<SchemaObject> schemaObjectIterator(int type) {

        switch (type) {

            case SchemaObject.ASSERTION :
                return assertionLookup.getIterator();

            case SchemaObject.CHARSET :
                return charsetLookup.getIterator();

            case SchemaObject.COLLATION :
                return collationLookup.getIterator();

            case SchemaObject.CONSTRAINT :
                return constraintsIterator();

            case SchemaObject.EXCEPTION :
                return conditionLookup.getIterator();

            case SchemaObject.FUNCTION :
                return functionLookup.getIterator();

            case SchemaObject.INDEX :
                return indexLookup.getIterator();

            case SchemaObject.MODULE :
                return moduleLookup.getIterator();

            case SchemaObject.PROCEDURE :
                return procedureLookup.getIterator();

            case SchemaObject.REFERENCE :
                return referenceLookup.getIterator();

            case SchemaObject.ROUTINE :
                Iterator<SchemaObject> functions = functionLookup.getIterator();

                return new WrapperIterator<>(
                        functions, procedureLookup.getIterator());

            case SchemaObject.SEQUENCE :
                return sequenceLookup.getIterator();

            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup.getIterator();

            case SchemaObject.TRIGGER :
                return triggerLookup.getIterator();

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup.getIterator();

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup.getIterator();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Schema");
        }
    }

    public Iterator<SchemaObject> constraintsIterator() {

        return new Iterator<SchemaObject>() {

            Iterator<HsqlName> names = constraintLookup.getNameIterator();
            Constraint current;
            boolean b = filterToNext();

            public boolean hasNext() {
                return current != null;
            }

            public Constraint next() {

                Constraint value = current;

                filterToNext();

                return value;
            }

            private boolean filterToNext() {

                current = null;

                while (names.hasNext()) {
                    HsqlName name = names.next();

                    if (name.parent == null) {
                        continue;
                    }

                    switch (name.parent.type) {

                        case SchemaObject.TABLE: {
                            Table table =
                                    (Table) findSchemaObject(
                                            name.parent.name,
                                            SchemaObject.TABLE);

                            if (table == null) {
                                continue;
                            }

                            current = table.getConstraint(name.name);

                            break;
                        }
                        case SchemaObject.DOMAIN: {
                            Type domain =
                                    (Type) findSchemaObject(
                                            name.parent.name,
                                            SchemaObject.DOMAIN);

                            if (domain == null) {
                                continue;
                            }

                            current = domain.userTypeModifier.getConstraint(
                                    name.name);

                            break;
                        }
                    }
                    return true;
                }

                return false;
            }
        };
    }

    SchemaObject findAnySchemaObjectForSynonym(String name) {

        int[] types = {
            SchemaObject.SEQUENCE, SchemaObject.TABLE, SchemaObject.ROUTINE
        };

        for (int i = 0; i < types.length; i++) {
            SchemaObject object = findSchemaObject(name, types[i]);

            if (object != null) {
                return object;
            }
        }

        return null;
    }

    /**
     * synonyms are allowed for a table, view, sequence, procedure,
     * function, package, materialized view, user-defined type.
     */
    ReferenceObject findReference(String name, int type) {

        ReferenceObject ref = referenceList.get(name);
        int             targetType;

        if (ref == null) {
            return null;
        }

        targetType = ref.getTarget().type;

        if (targetType == type) {
            return ref;
        }

        switch (type) {

            case SchemaObject.TABLE :
                if (targetType == SchemaObject.VIEW) {
                    return ref;
                }
                break;

            case SchemaObject.ROUTINE :
                if (targetType == SchemaObject.FUNCTION
                        || targetType == SchemaObject.PROCEDURE) {
                    return ref;
                }
        }

        return null;
    }

    SchemaObject findSchemaObject(String name, int type) {

        SchemaObjectSet set;
        HsqlName        objectName;
        Table           table;

        switch (type) {

            case SchemaObject.CHARSET :
                return charsetLookup.getObject(name);

            case SchemaObject.COLLATION :
                return collationLookup.getObject(name);

            case SchemaObject.CONSTRAINT :
                set        = constraintLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                if (objectName.parent.type == SchemaObject.TABLE) {
                    table = tableList.get(objectName.parent.name);

                    if (table == null) {
                        return null;
                    }

                    return table.getConstraint(name);
                } else if (objectName.parent.type == SchemaObject.DOMAIN) {
                    Type domain = (Type) typeLookup.getObject(objectName.parent.name);
                    
                    return domain.userTypeModifier.getConstraint(objectName.name);
                }

            case SchemaObject.EXCEPTION :
                return conditionLookup.getObject(name);

            case SchemaObject.FUNCTION :
                return functionLookup.getObject(name);

            case SchemaObject.INDEX :
                set        = indexLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = tableList.get(objectName.parent.name);

                return table.getIndex(name);

            case SchemaObject.MODULE :
                return moduleLookup.getObject(name);

            case SchemaObject.PROCEDURE :
                return procedureLookup.getObject(name);

            case SchemaObject.SEQUENCE :
                return sequenceLookup.getObject(name);

            case SchemaObject.REFERENCE :
                return referenceLookup.getObject(name);

            case SchemaObject.ROUTINE : {
                SchemaObject object = procedureLookup.getObject(name);

                if (object == null) {
                    object = functionLookup.getObject(name);
                }

                return object;
            }

            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup.getObject(name);

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup.getObject(name);

            case SchemaObject.TRIGGER :
                set        = triggerLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = tableList.get(objectName.parent.name);

                return table.getTrigger(name);

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup.getObject(name);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaManager");
        }
    }

    public void addSchemaObject(HsqlNameManager nameManager,
                                SchemaObject object, boolean replace) {

        HsqlName        name = object.getName();
        SchemaObjectSet set  = this.getObjectSet(name.type);

        switch (name.type) {

            case SchemaObject.PROCEDURE :
            case SchemaObject.FUNCTION : {
                RoutineSchema routine =
                    (RoutineSchema) set.getObject(name.name);

                if (routine == null) {
                    routine = new RoutineSchema(name.type, name);

                    routine.addSpecificRoutine(nameManager, (Routine) object,
                                               replace);
                    set.checkAdd(name);

                    SchemaObjectSet specificSet =
                        getObjectSet(SchemaObject.SPECIFIC_ROUTINE);

                    specificSet.checkAdd(((Routine) object).getSpecificName());
                    set.add(routine, replace);
                    specificSet.add(object, replace);
                } else {
                    SchemaObjectSet specificSet =
                        getObjectSet(SchemaObject.SPECIFIC_ROUTINE);
                    HsqlName specificName =
                        ((Routine) object).getSpecificName();

                    if (specificName != null) {
                        specificSet.checkAdd(specificName);
                    }

                    routine.addSpecificRoutine(nameManager, (Routine) object,
                                               replace);
                    specificSet.add(object, replace);
                }

                return;
            }
        }

        set.add(object, replace);
    }

    public void checkObjectNotExists(HsqlName name) {

        SchemaObjectSet set = getObjectSet(name.type);

        set.checkAdd(name);
    }


    public void renameObject(HsqlName name, HsqlName newName) {
        SchemaObjectSet set = getObjectSet(name.type);

        set.rename(name, newName);
    }
    void release() {

        for (int i = 0; i < tableList.size(); i++) {
            Table table = tableList.get(i);

            table.terminateTriggers();
        }

        charsetLookup         = null;
        collationLookup       = null;
        conditionLookup       = null;
        constraintLookup      = null;
        functionLookup        = null;
        indexLookup           = null;
        moduleLookup          = null;
        procedureLookup       = null;
        sequenceLookup        = null;
        specificRoutineLookup = null;
        tableLookup           = null;
        triggerLookup         = null;
        typeLookup            = null;

        tableList.clear();
        sequenceList.clear();
        referenceList.clear();
    }
}
