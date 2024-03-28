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

/**
 * Collection of SQL schema objects of a specific type in a schema
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class SchemaObjectSet {

    static final int addErrorCode = ErrorCode.X_42504;
    static final int getErrorCode = ErrorCode.X_42501;
    private final OrderedHashMap<String, SchemaObject> map;
    private final OrderedHashMap<String, HsqlName>     nameMap;
    private final int                                  type;

    SchemaObjectSet(int type) {

        this.type = type;

        switch (type) {

            case SchemaObject.ASSERTION :
            case SchemaObject.CHARSET :
            case SchemaObject.COLLATION :
            case SchemaObject.DOMAIN :
            case SchemaObject.EXCEPTION :
            case SchemaObject.FUNCTION :
            case SchemaObject.MODULE :
            case SchemaObject.PROCEDURE :
            case SchemaObject.REFERENCE :
            case SchemaObject.SEQUENCE :
            case SchemaObject.SPECIFIC_ROUTINE :
            case SchemaObject.TABLE :    // includes VIEW
            case SchemaObject.TRIGGER :
            case SchemaObject.TYPE :
                map     = new OrderedHashMap<>();
                nameMap = null;
                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                map     = null;
                nameMap = new OrderedHashMap<>();
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaObjectSet");
        }
    }

    OrderedHashMap<String, ? extends SchemaObject> getMap() {
        return map;
    }

    HsqlName getName(String name) {

        switch (type) {

            default :
                SchemaObject object = map.get(name);

                return object == null
                       ? null
                       : object.getName();

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX : {
                return nameMap.get(name);
            }
        }
    }

    public SchemaObject getObject(String name) {

        switch (type) {

            default :
                return map.get(name);

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaObjectSet");
        }
    }

    public Iterator<SchemaObject> getIterator() {

        switch (type) {

            default :
                return map.values().iterator();

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaObjectSet");
        }
    }

    public Iterator<HsqlName> getNameIterator() {

        switch (type) {

            default :
                return nameIterator(map.values().iterator());

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                return nameMap.values().iterator();
        }
    }

    private Iterator<HsqlName> nameIterator(Iterator<SchemaObject> it) {

        return new Iterator<HsqlName>() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public HsqlName next() {
                return it.next().getName();
            }
        };
    }

    public boolean contains(String name) {
        return map.containsKey(name);
    }

    void checkAdd(HsqlName name) {

        switch (type) {

            default :
                if (map.containsKey(name.name)) {
                    throw Error.error(addErrorCode, name.name);
                }

                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                if (nameMap.containsKey(name.name)) {
                    throw Error.error(addErrorCode, name.name);
                }

                break;
        }
    }

    void checkExists(String name) {

        switch (type) {

            default :
                if (!map.containsKey(name)) {
                    throw Error.error(getErrorCode, name);
                }

                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                if (!nameMap.containsKey(name)) {
                    throw Error.error(getErrorCode, name);
                }

                break;
        }
    }

    boolean isEmpty() {

        switch (type) {

            default :
                return map.isEmpty();

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                return nameMap.isEmpty();
        }
    }

    public void add(SchemaObject object, boolean replace) {

        HsqlName name = object.getName();

        if (type == SchemaObject.SPECIFIC_ROUTINE) {
            name = ((Routine) object).getSpecificName();
        }

        switch (type) {

            default :
                if (!replace && map.containsKey(name.name)) {
                    throw Error.error(addErrorCode, name.name);
                }

                map.put(name.name, object);
                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                if (!replace && nameMap.containsKey(name.name)) {
                    throw Error.error(addErrorCode, name.name);
                }

                nameMap.put(name.name, name);
                break;
        }
    }

    void remove(String name) {

        switch (type) {

            default :
                map.remove(name);
                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
                nameMap.remove(name);
                break;
        }
    }

    void removeParent(HsqlName parent) {

        switch (type) {

            default : {
                Iterator<SchemaObject> it = map.values().iterator();

                while (it.hasNext()) {
                    SchemaObject object = it.next();

                    if (object.getName().parent.equals(parent)) {
                        it.remove();
                    }
                }

                break;
            }

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX : {
                Iterator<HsqlName> it = nameMap.values().iterator();

                while (it.hasNext()) {
                    HsqlName name = it.next();

                    if (name.parent.equals(parent)) {
                        it.remove();
                    }
                }

                break;
            }
        }
    }

    void rename(HsqlName name, HsqlName newName) {

        HsqlName objectName;

        switch (newName.type) {

            default : {
                SchemaObject object = map.get(name.name);

                if (object == null) {
                    throw Error.error(getErrorCode, name.name);
                } else if (type == SchemaObject.SPECIFIC_ROUTINE) {
                    objectName = ((Routine) object).getSpecificName();
                } else {
                    objectName = object.getName();
                }

                if (map.containsKey(newName.name)) {
                    throw Error.error(addErrorCode, newName.name);
                }

                int i = map.getIndex(name.name);

                map.setKeyAt(i, newName.name);
                objectName.rename(newName);
                break;
            }

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX : {
                objectName = nameMap.get(name.name);

                if (objectName == null) {
                    throw Error.error(getErrorCode, name.name);
                }

                if (nameMap.containsKey(newName.name)) {
                    throw Error.error(addErrorCode, newName.name);
                }

                int i = nameMap.getIndex(name.name);

                nameMap.setKeyAt(i, newName.name);
                objectName.rename(newName);
                break;
            }
        }
    }

    public static String getName(int type) {

        switch (type) {

            case SchemaObject.ASSERTION :
                return Tokens.T_ASSERTION;

            case SchemaObject.CHARSET :
                return Tokens.T_CHARACTER + ' ' + Tokens.T_SET;

            case SchemaObject.COLLATION :
                return Tokens.T_COLLATION;

            case SchemaObject.CONSTRAINT :
                return Tokens.T_CONSTRAINT;

            case SchemaObject.DOMAIN :
                return Tokens.T_DOMAIN;

            case SchemaObject.EXCEPTION :
                return Tokens.T_EXCEPTION;

            case SchemaObject.FUNCTION :
                return Tokens.T_FUNCTION;

            case SchemaObject.INDEX :
                return Tokens.T_INDEX;

            case SchemaObject.MODULE :
                return Tokens.T_MODULE;

            case SchemaObject.PARAMETER :
                return Tokens.T_PARAMETER;

            case SchemaObject.PERIOD :
                return Tokens.T_PERIOD;

            case SchemaObject.PROCEDURE :
                return Tokens.T_PROCEDURE;

            case SchemaObject.REFERENCE :
                return Tokens.T_SYNONYM;

            case SchemaObject.SEQUENCE :
                return Tokens.T_SEQUENCE;

            case SchemaObject.SPECIFIC_ROUTINE :
                return Tokens.T_SPECIFIC + ' ' + Tokens.T_ROUTINE;

            case SchemaObject.TABLE :
                return Tokens.T_TABLE;

            case SchemaObject.TRIGGER :
                return Tokens.T_TRIGGER;

            case SchemaObject.TYPE :
                return Tokens.T_TYPE;

            case SchemaObject.VIEW :
                return Tokens.T_VIEW;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaObjectSet");
        }
    }

    void getSQL(
            HsqlArrayList<String> list,
            OrderedHashSet<HsqlName> resolved,
            OrderedHashSet<SchemaObject> unresolved) {

        // HsqlName lists are not persisted with this method
        if (type == SchemaObject.CONSTRAINT || type == SchemaObject.INDEX) {
            return;
        }

        if (map.isEmpty()) {
            return;
        }

        Iterator<SchemaObject> it = map.values().iterator();

        if (type == SchemaObject.FUNCTION || type == SchemaObject.PROCEDURE) {
            OrderedHashSet<SchemaObject> set = new OrderedHashSet<>();

            while (it.hasNext()) {
                RoutineSchema routineSchema = (RoutineSchema) it.next();

                for (int i = 0; i < routineSchema.routines.length; i++) {
                    Routine routine = routineSchema.routines[i];

                    set.add(routine);
                }
            }

            it = set.iterator();
        }

        addAllSQL(resolved, unresolved, list, it, null);
    }

    static void addAllSQL(
            OrderedHashSet<HsqlName> resolved,
            OrderedHashSet<SchemaObject> unresolved,
            HsqlArrayList<String> list,
            Iterator<SchemaObject> it,
            OrderedHashSet<SchemaObject> newResolved) {

        while (it.hasNext()) {
            SchemaObject             object     = it.next();
            boolean                  isResolved = true;
            OrderedHashSet<HsqlName> references;

            if (object.getType() == SchemaObject.TABLE) {
                ((Table) object).setForwardConstraints(resolved);

                references = ((Table) object).getReferencesForScript();
            } else {
                references = object.getReferences();
            }

            for (int j = 0; j < references.size(); j++) {
                HsqlName name = references.get(j);

                if (SqlInvariants.isSchemaNameSystem(name)) {
                    continue;
                }

                switch (name.type) {

                    case SchemaObject.TABLE :
                        if (!resolved.contains(name)) {
                            isResolved = false;
                        }

                        break;

                    case SchemaObject.COLUMN : {
                        if (object.getType() == SchemaObject.TABLE) {
                            int index = ((Table) object).findColumn(name.name);
                            ColumnSchema column = ((Table) object).getColumn(
                                index);

                            if (!isChildObjectResolved(column, resolved)) {
                                isResolved = false;
                            }

                            break;
                        }

                        if (!resolved.contains(name.parent)) {
                            isResolved = false;
                        }

                        break;
                    }

                    case SchemaObject.CONSTRAINT : {
                        if (name.parent == object.getName()) {
                            Constraint constraint =
                                ((Table) object).getConstraint(
                                    name.name);

                            if (constraint.getConstraintType()
                                    == SchemaObject.ConstraintTypes.CHECK) {
                                if (!isChildObjectResolved(constraint,
                                                           resolved)) {
                                    isResolved = false;
                                }
                            }
                        }

                        // only UNIQUE constraint referenced by FK in table
                        break;
                    }

                    case SchemaObject.CHARSET :
                        if (name.schema == null) {
                            continue;
                        }

                    // fall through
                    case SchemaObject.COLLATION :
                    case SchemaObject.DOMAIN :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.SEQUENCE :
                    case SchemaObject.SPECIFIC_ROUTINE :
                    case SchemaObject.TYPE :
                        if (!resolved.contains(name)) {
                            isResolved = false;
                        }

                        break;

                    default :
                }
            }

            if (!isResolved) {
                unresolved.add(object);
                continue;
            }

            HsqlName name;

            if (object.getType() == SchemaObject.FUNCTION
                    || object.getType() == SchemaObject.PROCEDURE) {
                name = ((Routine) object).getSpecificName();
            } else {
                name = object.getName();
            }

            resolved.add(name);

            if (newResolved != null) {
                newResolved.add(object);
            }

            switch (object.getType()) {

                case SchemaObject.TABLE : {
                    list.addAll(((Table) object).getSQL(resolved, unresolved));

                    String comment = object.getName()
                                           .getCommentSQL(Tokens.T_TABLE);

                    if (comment != null) {
                        list.add(comment);
                    }

                    for (int j = 0; j < ((Table) object).getColumnCount();
                            j++) {
                        ColumnSchema column = ((Table) object).getColumn(j);

                        comment = column.getName()
                                        .getCommentSQL(Tokens.T_COLUMN);

                        if (comment != null) {
                            list.add(comment);
                        }
                    }

                    break;
                }

                case SchemaObject.VIEW : {
                    list.add(object.getSQL());

                    String comment = object.getName()
                                           .getCommentSQL(Tokens.T_TABLE);

                    if (comment != null) {
                        list.add(comment);
                    }

                    break;
                }

                case SchemaObject.FUNCTION :
                case SchemaObject.PROCEDURE : {
                    Routine routine = ((Routine) object);

                    if (routine.isRecursive) {
                        list.add(((Routine) object).getSQLDeclaration());
                        list.add(((Routine) object).getSQLAlter());
                    } else {
                        list.add(object.getSQL());
                    }

                    String comment = object.getName()
                                           .getCommentSQL(Tokens.T_ROUTINE);

                    if (comment != null) {
                        list.add(comment);
                    }

                    break;
                }

                case SchemaObject.TRIGGER : {
                    list.add(object.getSQL());

                    String comment = object.getName()
                                           .getCommentSQL(Tokens.T_TRIGGER);

                    if (comment != null) {
                        list.add(comment);
                    }

                    break;
                }

                case SchemaObject.SEQUENCE : {
                    list.add(object.getSQL());

                    String comment = object.getName()
                                           .getCommentSQL(Tokens.T_SEQUENCE);

                    if (comment != null) {
                        list.add(comment);
                    }

                    break;
                }

                case SchemaObject.CONSTRAINT : {
                    list.add(object.getSQL());

                    String added = ((Constraint) object).getAlterSQL();

                    if (!added.isEmpty()) {
                        list.add(added);
                    }

                    break;
                }

                default :
                    list.add(object.getSQL());
            }
        }
    }

    static boolean isChildObjectResolved(
            SchemaObject object,
            OrderedHashSet<HsqlName> resolved) {

        OrderedHashSet<HsqlName> refs = object.getReferences();

        for (int i = 0; i < refs.size(); i++) {
            HsqlName name = refs.get(i);

            if (SqlInvariants.isSchemaNameSystem(name)) {
                continue;
            }

            if (!resolved.contains(name)) {
                return false;
            }
        }

        return true;
    }
}
