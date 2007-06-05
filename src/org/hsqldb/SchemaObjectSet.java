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

import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.HashMap;

public class SchemaObjectSet {

    HashMap map;
    int     type;

    SchemaObjectSet(int type) {

        this.type = type;

        switch (type) {

            case SchemaObject.VIEW :
            case SchemaObject.TABLE :
            case SchemaObject.SEQUENCE :
            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                map = new HashMappedList();
                break;

            case SchemaObject.CONSTRAINT :
            case SchemaObject.INDEX :
            case SchemaObject.TRIGGER :
                map = new HashMap();
                break;
        }
    }

    HsqlName getName(String name) {

        switch (type) {

            case SchemaObject.VIEW :
            case SchemaObject.TABLE :
            case SchemaObject.SEQUENCE :
            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return ((SchemaObject) map.get(name)).getName();

            default :
                return (HsqlName) map.get(name);
        }
    }

    SchemaObject getObject(String name) {

        switch (type) {

            case SchemaObject.VIEW :
            case SchemaObject.TABLE :
            case SchemaObject.SEQUENCE :
            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return (SchemaObject) map.get(name);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SchemaObjectSet");
        }
    }

    boolean contains(String name) {
        return map.containsKey(name);
    }

    void checkAdd(HsqlName name) throws HsqlException {

        if (map.containsKey(name.name)) {
            int code = getAddErrorCode(name.type);

            throw Trace.error(code, name.name);
        }
    }

    void checkExists(String name) throws HsqlException {

        if (!map.containsKey(name)) {
            int code = getGetErrorCode(type);

            throw Trace.error(code, name);
        }
    }

    void add(SchemaObject object) throws HsqlException {

        HsqlName name = object.getName();

        if (map.containsKey(name.name)) {
            int code = getAddErrorCode(name.type);

            throw Trace.error(code, name.name);
        }

        Object value = object;

        switch (name.type) {

            case SchemaObject.CONSTRAINT :
            case SchemaObject.ASSERTION :
            case SchemaObject.INDEX :
            case SchemaObject.TRIGGER :
                value = name;
        }

        map.put(name.name, value);
    }

    void remove(String name) throws HsqlException {
        map.remove(name);
    }

    void removeParent(HsqlName parent) {

        Iterator it = map.values().iterator();

        while (it.hasNext()) {
            HsqlName name = (HsqlName) it.next();

            if (name.parent == parent) {
                it.remove();
            }
        }
    }

    void rename(HsqlName name, HsqlName newName) throws HsqlException {

        if (map.containsKey(newName.name)) {
            int code = getAddErrorCode(name.type);

            throw Trace.error(code, newName.name);
        }

        switch (newName.type) {

            case SchemaObject.VIEW :
            case SchemaObject.TABLE :
            case SchemaObject.SEQUENCE :
            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE : {
                int i = ((HashMappedList) map).getIndex(name.name);

                if (i == -1) {
                    int code = getGetErrorCode(name.type);

                    throw Trace.error(code, name.name);
                }

                SchemaObject object =
                    (SchemaObject) ((HashMappedList) map).get(i);

                object.getName().rename(newName);
                ((HashMappedList) map).setKey(i, name.name);

                break;
            }
            case SchemaObject.CONSTRAINT :
            case SchemaObject.ASSERTION :
            case SchemaObject.INDEX :
            case SchemaObject.TRIGGER : {
                map.remove(name.name);
                name.rename(newName);
                map.put(name.name, name);

                break;
            }
        }
    }

    int getAddErrorCode(int type) {

        int code;

        switch (type) {

            case SchemaObject.VIEW :
                code = Trace.VIEW_ALREADY_EXISTS;
                break;

            case SchemaObject.TABLE :
                code = Trace.TABLE_ALREADY_EXISTS;
                break;

            case SchemaObject.SEQUENCE :
                code = Trace.SEQUENCE_ALREADY_EXISTS;
                break;

            case SchemaObject.DOMAIN :
                code = Trace.SEQUENCE_ALREADY_EXISTS;    // TODO
                break;

            case SchemaObject.TYPE :
                code = Trace.SEQUENCE_ALREADY_EXISTS;    // TODO
                break;

            case SchemaObject.CONSTRAINT :
                code = Trace.CONSTRAINT_ALREADY_EXISTS;
                break;

            case SchemaObject.ASSERTION :
                code = Trace.CONSTRAINT_ALREADY_EXISTS;
                break;

            case SchemaObject.INDEX :
                code = Trace.INDEX_ALREADY_EXISTS;
                break;

            case SchemaObject.TRIGGER :
                code = Trace.TRIGGER_ALREADY_EXISTS;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SchemaObjectSet");
        }

        return code;
    }

    int getGetErrorCode(int type) {

        int code;

        switch (type) {

            case SchemaObject.VIEW :
                code = Trace.VIEW_NOT_FOUND;
                break;

            case SchemaObject.TABLE :
                code = Trace.TABLE_NOT_FOUND;
                break;

            case SchemaObject.SEQUENCE :
                code = Trace.SEQUENCE_NOT_FOUND;
                break;

            case SchemaObject.DOMAIN :
                code = Trace.SEQUENCE_NOT_FOUND;    // TODO
                break;

            case SchemaObject.TYPE :
                code = Trace.SEQUENCE_NOT_FOUND;    // TODO
                break;

            case SchemaObject.CONSTRAINT :
                code = Trace.CONSTRAINT_NOT_FOUND;
                break;

            case SchemaObject.ASSERTION :
                code = Trace.CONSTRAINT_NOT_FOUND;
                break;

            case SchemaObject.INDEX :
                code = Trace.INDEX_NOT_FOUND;
                break;

            case SchemaObject.TRIGGER :
                code = Trace.TRIGGER_NOT_FOUND;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SchemaObjectSet");
        }

        return code;
    }
}
