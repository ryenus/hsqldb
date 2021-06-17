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


package org.hsqldb.index;

import org.hsqldb.Table;
import org.hsqldb.TypeInvariants;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.types.Type;

/**
 * Holds results of index check
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.1
 * @since 2.5.1
 */
public class IndexStats {

    public static final IndexStats[] emptyArray = new IndexStats[0];

    //
    public static final int sizes            = 1;
    public static final int checkSpaces      = 2;
    public static final int checkRows        = 3;
    public static final int checkIndexSpaces = 4;
    public static final int setVersion       = 5;
    public static final int findRoots        = 6;
    public static final int fixAll           = 8;

    //
    public Index           index;
    public PersistentStore store;
    public boolean         hasErrors;
    public long            errorCount;
    public long            loopCount;
    public long            goodRowCount;
    public boolean         reindexed;    // set after a reindex
    HsqlArrayList          unorderedList = new HsqlArrayList();

    public static Result newEmptyResult() {

        String[] names = new String[] {
            "TABLE_NAME", "INDEX_NAME", "INFO"
        };
        Type[]   types = new Type[] {
            Type.SQL_VARCHAR_DEFAULT, TypeInvariants.SQL_IDENTIFIER,
            Type.SQL_VARCHAR_DEFAULT
        };
        Result result = Result.newMultiColumnResult(names, types);

        return result;
    }

    public void addTableStats(Result result) {

        Object[] data = new Object[] {
            ((Table) index.getTable()).getName()
                .getSchemaQualifiedStatementName(),
            "", "rows " + store.elementCount()
        };

        result.navigator.add(data);
    }

    public void addStats(Result result) {

        {
            Object[] data = new Object[] {
                ((Table) index.getTable()).getName()
                    .getSchemaQualifiedStatementName(),
                index.getName().getStatementName(),
                "readable rows " + goodRowCount
            };

            result.navigator.add(data);
        }

        if (errorCount != 0) {
            Object[] data = new Object[] {
                "", "", "error rows " + errorCount
            };

            result.navigator.add(data);
        }

        if (loopCount != 0) {
            Object[] data = new Object[] {
                "", "", "loop rows " + loopCount
            };

            result.navigator.add(data);
        }

        for (int i = 0; i < unorderedList.size(); i++) {
            Object[] data = new Object[] {
                "", "", (Object) unorderedList.get(i)
            };

            result.navigator.add(data);
        }
    }

    public void addReindexedStats(Result result) {

        {
            Object[] data = new Object[] {
                ((Table) index.getTable()).getName()
                    .getSchemaQualifiedStatementName(),
                index.getName().getStatementName(), "reindexed"
            };

            result.navigator.add(data);
        }
    }
}
