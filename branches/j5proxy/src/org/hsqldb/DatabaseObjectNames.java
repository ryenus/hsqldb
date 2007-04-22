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
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.Iterator;

/**
 * Transitional container for object names that are unique across the
 * DB instance but are owned by different DB objects. Currently names for
 * Index and Trigger objects.<p>
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
class DatabaseObjectNames {

    HashMap nameList = new HashMap();

    DatabaseObjectNames() {}

    boolean containsName(String name) {
        return nameList.containsKey(name);
    }

    HsqlName getOwner(String name) {
        return (HsqlName) nameList.get(name);
    }

    void addName(String name, HsqlName owner,
                 int errorcode) throws HsqlException {

        // should not contain name
        if (containsName(name)) {
            throw Trace.error(errorcode, name);
        }

        nameList.put(name, owner);
    }

    void rename(String name, String newname,
                int errorcode) throws HsqlException {

        HsqlName owner = (HsqlName) nameList.get(name);

        addName(newname, owner, errorcode);
        nameList.remove(name);
    }

    Object removeName(String name) throws HsqlException {

        Object owner = nameList.remove(name);

        if (owner == null) {

            // should contain name
            throw Trace.runtimeError(Trace.GENERAL_ERROR,
                                     "DatabaseObjectNames");
        }

        return owner;
    }

    void removeOwner(HsqlName owner) {

        Iterator it = nameList.values().iterator();

        while (it.hasNext()) {
            Object currentvalue = it.next();

            if (owner.equals(currentvalue)) {
                it.remove();
            }
        }
    }
}
