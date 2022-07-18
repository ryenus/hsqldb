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
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.rights.Grantee;

/**
 * Representation of a Schema.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.5.1
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
    private HsqlName name;
    SchemaObjectSet  triggerLookup;
    SchemaObjectSet  constraintLookup;
    SchemaObjectSet  indexLookup;
    SchemaObjectSet  tableLookup;
    SchemaObjectSet  sequenceLookup;
    SchemaObjectSet  typeLookup;
    SchemaObjectSet  charsetLookup;
    SchemaObjectSet  collationLookup;
    SchemaObjectSet  procedureLookup;
    SchemaObjectSet  functionLookup;
    SchemaObjectSet  specificRoutineLookup;
    SchemaObjectSet  assertionLookup;
    SchemaObjectSet  referenceLookup;
    SchemaObjectSet  conditionLookup;
    SchemaObjectSet  moduleLookup;
    OrderedHashMap   tableList;
    OrderedHashMap   sequenceList;
    OrderedHashMap   referenceList;
    long             changeTimestamp;

    public Schema(HsqlName name, Grantee owner) {

        this.name        = name;
        triggerLookup    = new SchemaObjectSet(SchemaObject.TRIGGER);
        indexLookup      = new SchemaObjectSet(SchemaObject.INDEX);
        constraintLookup = new SchemaObjectSet(SchemaObject.CONSTRAINT);
        tableLookup      = new SchemaObjectSet(SchemaObject.TABLE);
        sequenceLookup   = new SchemaObjectSet(SchemaObject.SEQUENCE);
        typeLookup       = new SchemaObjectSet(SchemaObject.TYPE);
        charsetLookup    = new SchemaObjectSet(SchemaObject.CHARSET);
        collationLookup  = new SchemaObjectSet(SchemaObject.COLLATION);
        procedureLookup  = new SchemaObjectSet(SchemaObject.PROCEDURE);
        functionLookup   = new SchemaObjectSet(SchemaObject.FUNCTION);
        specificRoutineLookup =
            new SchemaObjectSet(SchemaObject.SPECIFIC_ROUTINE);
        assertionLookup = new SchemaObjectSet(SchemaObject.ASSERTION);
        referenceLookup = new SchemaObjectSet(SchemaObject.REFERENCE);
        conditionLookup = new SchemaObjectSet(SchemaObject.EXCEPTION);
        moduleLookup    = new SchemaObjectSet(SchemaObject.MODULE);
        tableList       = (OrderedHashMap) tableLookup.map;
        sequenceList    = (OrderedHashMap) sequenceLookup.map;
        referenceList   = (OrderedHashMap) referenceLookup.map;
        name.owner      = owner;
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

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

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

    static String getCommentSQL(HsqlName name, String typeName) {

        if (name.comment == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        sb.append(Tokens.T_COMMENT).append(' ').append(Tokens.T_ON);
        sb.append(' ').append(typeName).append(' ');
        sb.append(name.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_IS).append(' ');
        sb.append(StringConverter.toQuotedString(name.comment, '\'',
                true));

        return sb.toString();
    }

    public HsqlArrayList getSQLArray(int objectType, OrderedHashSet resolved,
                                     OrderedHashSet unresolved) {

        HsqlArrayList list = new HsqlArrayList();

        switch (objectType) {

            case CHARSET :
                charsetLookup.getSQL(list, resolved, unresolved);
                break;

            case COLLATION :
                collationLookup.getSQL(list, resolved, unresolved);
                break;

            case TYPE :
                typeLookup.getSQL(list, resolved, unresolved);
                break;

            case SEQUENCE :
                sequenceLookup.getSQL(list, resolved, unresolved);
                break;

            case FUNCTION :
                functionLookup.getSQL(list, resolved, unresolved);
                break;

            case TABLE :
                tableLookup.getSQL(list, resolved, unresolved);
                break;

            case PROCEDURE :
                procedureLookup.getSQL(list, resolved, unresolved);
                break;

            case REFERENCE :
                referenceLookup.getSQL(list, resolved, unresolved);
                break;
        }

        return list;
    }

    public HsqlArrayList getSequenceRestartSQL() {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = sequenceLookup.map.values().iterator();

        while (it.hasNext()) {
            NumberSequence sequence = (NumberSequence) it.next();
            String         ddl      = sequence.getRestartSQL();

            list.add(ddl);
        }

        return list;
    }

    public HsqlArrayList getTriggerSQL() {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = tableLookup.map.values().iterator();

        while (it.hasNext()) {
            Table    table = (Table) it.next();
            String[] ddl   = table.getTriggerSQL();

            list.addAll(ddl);
        }

        return list;
    }

    boolean isEmpty() {

        return sequenceLookup.isEmpty() && tableLookup.isEmpty()
               && typeLookup.isEmpty() && charsetLookup.isEmpty()
               && collationLookup.isEmpty() && specificRoutineLookup.isEmpty();
    }

    public SchemaObjectSet getObjectSet(int type) {

        switch (type) {

            case SchemaObject.SEQUENCE :
                return sequenceLookup;

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup;

            case SchemaObject.CHARSET :
                return charsetLookup;

            case SchemaObject.COLLATION :
                return collationLookup;

            case SchemaObject.PROCEDURE :
                return procedureLookup;

            case SchemaObject.FUNCTION :
                return functionLookup;

            case SchemaObject.ROUTINE :
                return functionLookup;

            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup;

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup;

            case SchemaObject.ASSERTION :
                return assertionLookup;

            case SchemaObject.TRIGGER :
                return triggerLookup;

            case SchemaObject.EXCEPTION :
                return conditionLookup;

            case SchemaObject.MODULE :
                return moduleLookup;

            case SchemaObject.REFERENCE :
                return referenceLookup;

            case SchemaObject.INDEX :
                return indexLookup;

            case SchemaObject.CONSTRAINT :
                return constraintLookup;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Schema");
        }
    }

    public Iterator schemaObjectIterator(int type) {

        switch (type) {

            case SchemaObject.SEQUENCE :
                return sequenceLookup.map.values().iterator();

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup.map.values().iterator();

            case SchemaObject.CHARSET :
                return charsetLookup.map.values().iterator();

            case SchemaObject.COLLATION :
                return collationLookup.map.values().iterator();

            case SchemaObject.PROCEDURE :
                return procedureLookup.map.values().iterator();

            case SchemaObject.FUNCTION :
                return functionLookup.map.values().iterator();

            case SchemaObject.ROUTINE :
                Iterator functions = functionLookup.map.values().iterator();

                return new WrapperIterator(
                    functions, procedureLookup.map.values().iterator());

            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup.map.values().iterator();

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup.map.values().iterator();

            case SchemaObject.ASSERTION :
                return assertionLookup.map.values().iterator();

            case SchemaObject.EXCEPTION :
                return conditionLookup.map.values().iterator();

            case SchemaObject.MODULE :
                return moduleLookup.map.values().iterator();

            case SchemaObject.TRIGGER :
                return triggerLookup.map.values().iterator();

            case SchemaObject.REFERENCE :
                return referenceLookup.map.values().iterator();

            case SchemaObject.INDEX :
                return indexLookup.map.values().iterator();

            case SchemaObject.CONSTRAINT :
                return constraintLookup.map.values().iterator();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Schema");
        }
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

        ReferenceObject ref = (ReferenceObject) referenceList.get(name);
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

        SchemaObjectSet set = null;
        HsqlName        objectName;
        Table           table;

        switch (type) {

            case SchemaObject.SEQUENCE :
                return sequenceLookup.getObject(name);

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup.getObject(name);

            case SchemaObject.CHARSET :
                return charsetLookup.getObject(name);

            case SchemaObject.COLLATION :
                return collationLookup.getObject(name);

            case SchemaObject.PROCEDURE :
                return procedureLookup.getObject(name);

            case SchemaObject.FUNCTION :
                return functionLookup.getObject(name);

            case SchemaObject.ROUTINE : {
                SchemaObject object = procedureLookup.getObject(name);

                if (object == null) {
                    object = functionLookup.getObject(name);
                }

                return object;
            }
            case SchemaObject.SPECIFIC_ROUTINE :
                return specificRoutineLookup.getObject(name);

            case SchemaObject.EXCEPTION :
                return conditionLookup.getObject(name);

            case SchemaObject.MODULE :
                return moduleLookup.getObject(name);

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup.getObject(name);

            case SchemaObject.INDEX :
                set        = indexLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = (Table) tableList.get(objectName.parent.name);

                return table.getIndex(name);

            case SchemaObject.CONSTRAINT :
                set        = constraintLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = (Table) tableList.get(objectName.parent.name);

                if (table == null) {
                    return null;
                }

                return table.getConstraint(name);

            case SchemaObject.TRIGGER :
                set        = triggerLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = (Table) tableList.get(objectName.parent.name);

                return table.getTrigger(name);

            case SchemaObject.REFERENCE :
                return referenceLookup.getObject(name);

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

    void release() {

        for (int i = 0; i < tableList.size(); i++) {
            Table table = (Table) tableList.get(i);

            table.terminateTriggers();
        }

        triggerLookup         = null;
        indexLookup           = null;
        constraintLookup      = null;
        charsetLookup         = null;
        collationLookup       = null;
        procedureLookup       = null;
        functionLookup        = null;
        specificRoutineLookup = null;
        conditionLookup       = null;
        moduleLookup          = null;
        sequenceLookup        = null;
        tableLookup           = null;
        typeLookup            = null;

        tableList.clear();
        sequenceList.clear();
        referenceList.clear();
    }
}
