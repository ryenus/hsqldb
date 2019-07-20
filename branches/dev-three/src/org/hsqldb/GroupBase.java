/* Copyright (c) 2001-2019, The HSQL Development Group
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

import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;

public class GroupBase {
    public IntKeyHashMap baseColumns;
    public boolean isBasic = true;

    public GroupBase(Expression[] expressions, Object[] exprColumns, int index) {
        baseColumns = new IntKeyHashMap();
        for (int i = 0; i < expressions.length; i++) {
            addToBaseColumns(expressions[i], exprColumns, index);
        }
    }

    void resolveReferences(RangeGroup range) {

    }

    private void addToBaseColumns(Expression e, Object[] exprColumns, int indexLimitVisible) {
        if (e.groupingType != 0) {
            isBasic = false;
        }
        if (e.opType == OpTypes.NONE) {
            return;
        }
        if (e.nodes.length == 0) {
            boolean added = false;
            for (int i = 0; i < indexLimitVisible; i++) {
                Expression expr = (Expression) exprColumns[i];
                String alias = e.getAlias();
                if (alias.equals(expr.getColumnName()) || alias.equals(expr.getAlias())) {
                    baseColumns.put(expr.getAlias().hashCode(), e);
                    added = true;
                    break;
                }
            }
            if (!added) {
                baseColumns.put(e.getAlias().hashCode(), e);
            }
        } else {
            if (e.opType == OpTypes.ROW || e.opType == OpTypes.VALUELIST) {
                for (int i = 0; i < e.nodes.length; i++) {
                    addToBaseColumns(e.nodes[i], exprColumns, indexLimitVisible);
                }
            } else {
                baseColumns.put(e.getSQL().hashCode(), e);
                return;
            }
        }
    }

    Iterator iterator() {
        return baseColumns.values().iterator();
    }
}
