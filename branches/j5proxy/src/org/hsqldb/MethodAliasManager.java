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

import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.HsqlArrayList;

public class MethodAliasManager {

    private HashMap aliasMap;

    public MethodAliasManager() {
        aliasMap = Library.getAliasMap();
    }

    public void addAlias(String alias, String methodFQN) {
        aliasMap.put(alias, methodFQN);
    }

    /**
     *  Returns a map from Java method-call name aliases to the
     *  fully-qualified names of the Java methods themsleves.
     */
    public HashMap getAliasMap() {
        return aliasMap;
    }

    /**
     *  Returns the fully qualified name for the Java method corresponding to
     *  the given method alias. If there is no Java method, then returns the
     *  alias itself.
     */
    public String getJavaName(String s) {

        String alias = (String) aliasMap.get(s);

        return (alias == null) ? s
                               : alias;
    }

    /**
     * Retrieves a map from each distinct value of this object's database
     * SQL routine CALL alias map to the list of keys in the input map
     * mapping to that value. <p>
     *
     * @return The requested map
     */
    public HashMap getInverseAliasMap() {

        HashMap       mapIn;
        HashMap       mapOut;
        Iterator      keys;
        Object        key;
        Object        value;
        HsqlArrayList keyList;

        // TODO:
        // update Database to dynamically maintain its own
        // inverse alias map.  This will make things *much*
        // faster for our  purposes here, without appreciably
        // slowing down Database
        mapIn  = aliasMap;
        mapOut = new HashMap();
        keys   = mapIn.keySet().iterator();

        while (keys.hasNext()) {
            key     = keys.next();
            value   = mapIn.get(key);
            keyList = (HsqlArrayList) mapOut.get(value);

            if (keyList == null) {
                keyList = new HsqlArrayList();

                mapOut.put(value, keyList);
            }

            keyList.add(key);
        }

        return mapOut;
    }
}
