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

import java.util.Arrays;

import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Transforms a tree of GROUPING SETS variants into normalised form for
 * building the result of GROUP BY queries.
 *
 * Code modifications to ParserDQL, QuerySpecification, RowSetNavigatorData,
 * SetFunctionValueAggregate and other classes by the author form parts of
 * added support for GROUPING SETS, ROLLUP, CUBE and GROUPING functionality.
 *
 * @author Nicholas Quek (kocolipy@users dot sourceforge.net)
 * @version 2.6.0
 * @since 2.5.1
 */
public class GroupSet {

    Expression[]  groupExpressions;
    HsqlArrayList sets     = new HsqlArrayList();
    int           nullSets = 0;
    boolean       isDistinctGroups;

    public GroupSet(Expression[] expressions, boolean isDistinct) {
        groupExpressions = expressions;
        isDistinctGroups = isDistinct;
    }

    public Iterator getIterator() {
        return sets.iterator();
    }

    public int isGrouped(List current, Expression e) {

        int count = 0;

        if (current == null) {
            return (1 << (e.nodes.length)) - 1;    //Total Aggregate case
        }

        for (int i = 0; i < e.nodes.length; i++) {
            count = count << 1;

            int colIndex = e.nodes[i].columnIndex;

            if (!current.contains(colIndex)) {
                count += 1;
            }
        }

        return count;
    }

    public void process() {

        // columns in grouping sets by name
        HsqlArrayList tmp = evaluate(groupExpressions);
        Iterator      it  = tmp.iterator();

        outerloop:
        while (it.hasNext()) {
            List set = (List) it.next();

            if (set.isEmpty()) {
                if (isDistinctGroups) {
                    nullSets = 1;
                } else {
                    nullSets++;
                }
            } else {
                if (isDistinctGroups) {
                    OrderedHashSet newSet = new OrderedHashSet();

                    newSet.addAll(set);

                    for (int i = 0; i < sets.size(); i++) {
                        List current = (List) sets.get(i);

                        if (current.size() == newSet.size()) {
                            if (newSet.containsAll(current)) {
                                continue outerloop;
                            }
                        }
                    }

                    set = newSet;
                }

                sets.add(set);
            }
        }
    }

    private static int getColumnIndex(Expression e) {
        return e.resultTableColumnIndex;
    }

    private static HsqlArrayList evaluate(Expression e) {

        if (e.opType == OpTypes.NONE) {
            HsqlArrayList sets = new HsqlArrayList();

            sets.add(new HsqlArrayList());

            return sets;
        }

        Expression[] exprs = e.nodes;

        if (exprs.length == 0
                || (e.opType != OpTypes.VALUELIST
                    && e.opType != OpTypes.ROW)) {
            exprs    = new Expression[1];
            exprs[0] = e;
        }

        switch (e.groupingType) {

            case Tokens.CUBE :
                return powerSet(exprs);

            case Tokens.ROLLUP :
                return rollUp(exprs);

            case Tokens.SETS :
                return grouping(exprs);

            default :
                if (e.nodes.length == 0) {
                    HsqlArrayList sets  = new HsqlArrayList();
                    HsqlArrayList inner = new HsqlArrayList();

                    inner.add(getColumnIndex(e));
                    sets.add(inner);

                    return sets;
                }

                return evaluate(e.nodes);
        }
    }

    private static HsqlArrayList evaluate(Expression[] e) {

        HsqlArrayList sets = new HsqlArrayList();

        if (e.length == 0) {
            sets.add(new HsqlArrayList());

            return sets;
        }

        if (e[0] == null) {
            return sets;
        }

        HsqlArrayList first   = evaluate(e[0]);
        HsqlArrayList results = evaluate(Arrays.copyOfRange(e, 1, e.length));
        Iterator      it      = results.iterator();

        while (it.hasNext()) {
            HsqlArrayList current = (HsqlArrayList) it.next();
            Iterator      itFirst = first.iterator();

            while (itFirst.hasNext()) {
                HsqlArrayList newSet = new HsqlArrayList();
                HsqlArrayList next   = (HsqlArrayList) itFirst.next();

                newSet.addAll(next);
                newSet.addAll(current);
                sets.add(newSet);
            }
        }

        return sets;
    }

    private static HsqlArrayList powerSet(Expression[] expressions) {

        HsqlArrayList sets = new HsqlArrayList();

        if (expressions.length == 0) {
            sets.add(new HsqlArrayList());

            return sets;
        }

        HsqlArrayList first;

        if (expressions[0].nodes.length != 0
                && (expressions[0].opType == OpTypes.VALUELIST
                    || expressions[0].opType == OpTypes.ROW)) {
            first = evaluate(expressions[0]);
        } else {
            first = new HsqlArrayList();

            HsqlArrayList tmp = new HsqlArrayList();

            tmp.add(getColumnIndex(expressions[0]));
            first.add(tmp);
        }

        HsqlArrayList results = powerSet(Arrays.copyOfRange(expressions, 1,
            expressions.length));
        Iterator itFirst = first.iterator();

        while (itFirst.hasNext()) {
            HsqlArrayList current = (HsqlArrayList) itFirst.next();
            Iterator      it      = results.iterator();

            while (it.hasNext()) {
                HsqlArrayList newSet = new HsqlArrayList();
                HsqlArrayList next   = (HsqlArrayList) it.next();

                newSet.addAll(current);
                newSet.addAll(next);

                if (!newSet.isEmpty()) {
                    sets.add(newSet);
                }
            }
        }

        sets.addAll(results);

        return sets;
    }

    private static HsqlArrayList rollUp(Expression[] expressions) {

        HsqlArrayList sets = new HsqlArrayList();

        if (expressions.length == 0) {
            sets.add(new HsqlArrayList());

            return sets;
        }

        HsqlArrayList first;

        if (expressions[0].nodes.length != 0
                && (expressions[0].opType == OpTypes.VALUELIST
                    || expressions[0].opType == OpTypes.ROW)) {
            first = evaluate(expressions[0]);
        } else {
            first = new HsqlArrayList();

            HsqlArrayList tmp = new HsqlArrayList();

            tmp.add(getColumnIndex(expressions[0]));
            first.add(tmp);
        }

        HsqlArrayList results = rollUp(Arrays.copyOfRange(expressions, 1,
            expressions.length));
        Iterator it = results.iterator();

        while (it.hasNext()) {
            HsqlArrayList current = (HsqlArrayList) it.next();
            Iterator      itFirst = first.iterator();

            while (itFirst.hasNext()) {
                HsqlArrayList next   = (HsqlArrayList) itFirst.next();
                HsqlArrayList newSet = new HsqlArrayList();

                newSet.addAll(next);
                newSet.addAll(current);
                sets.add(newSet);
            }
        }

        sets.add(new HsqlArrayList());

        return sets;
    }

    private static HsqlArrayList grouping(Expression[] expressions) {

        HsqlArrayList sets = new HsqlArrayList();

        if (expressions.length == 0) {
            return sets;
        }

        if (expressions[0].nodes.length != 0
                && (expressions[0].opType == OpTypes.VALUELIST
                    || expressions[0].opType == OpTypes.ROW)) {
            sets = evaluate(expressions[0]);
        } else {
            sets = new HsqlArrayList();

            HsqlArrayList tmp = new HsqlArrayList();

            if (expressions[0].opType != OpTypes.NONE) {
                tmp.add(getColumnIndex(expressions[0]));
            }

            sets.add(tmp);
        }

        HsqlArrayList results = grouping(Arrays.copyOfRange(expressions, 1,
            expressions.length));

        sets.addAll(results);

        return sets;
    }
}
