/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.OpTypes;
import org.hsqldb.Row;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Type;

// fredt@users 20020221 - patch 513005 by sqlbob@users - corrections
// fredt@users 20020225 - patch 1.7.0 - changes to support cascading deletes
// tony_lai@users 20020820 - patch 595052 - better error message
// fredt@users 20021205 - patch 1.7.2 - changes to method signature
// fredt@users - patch 1.8.0 - reworked the interface and comparison methods
// fredt@users - patch 1.8.0 - improved reliability for cached indexes

/**
 * Implementation of an AVL tree with parent pointers in nodes. Subclasses
 * of Node implement the tree node objects for memory or disk storage. An
 * Index has a root Node that is linked with other nodes using Java Object
 * references or file pointers, depending on Node implementation.<p>
 * An Index object also holds information on table columns (in the form of int
 * indexes) that are covered by it.(fredt@users)
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class Index implements SchemaObject {

    // fields
    private final long      persistenceId;
    private final HsqlName  name;
    private final boolean[] colCheck;
    private final int[]     colIndex;
    private final Type[]    colTypes;
    private final boolean[] colDesc;
    private final boolean[] nullsLast;
    private final int[]     pkCols;
    private final Type[]    pkTypes;
    private final boolean   isUnique;    // DDL uniqueness
    private final boolean   useRowId;
    private final boolean   isConstraint;
    public final boolean    isForward;
    private int             depth;
    private static final IndexRowIterator emptyIterator =
        new IndexRowIterator(null, (PersistentStore) null, null, null);
    private final TableBase table;
    private int             position;

    //
    ReentrantReadWriteLock           lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    //
    public static final Index[] emptyIndexArray = new Index[]{};

    /**
     * Constructor declaration
     *
     * @param name HsqlName of the index
     * @param id persistnece id
     * @param table table of the index
     * @param columns array of column indexes
     * @param descending boolean[]
     * @param nullsLast boolean[]
     * @param colTypes array of column types
     * @param unique is this a unique index
     * @param constraint does this index belonging to a constraint
     * @param forward is this an auto-index for an FK that refers to a table
     *   defined after this table
     * @id
     */
    public Index(HsqlName name, long id, TableBase table, int[] columns,
                 boolean[] descending, boolean[] nullsLast, Type[] colTypes,
                 boolean unique, boolean constraint, boolean forward) {

        persistenceId  = id;
        this.name      = name;
        colIndex       = columns;
        this.colTypes  = colTypes;
        colDesc        = descending == null ? new boolean[columns.length]
                                            : descending;
        this.nullsLast = nullsLast == null ? new boolean[columns.length]
                                           : nullsLast;
        isUnique       = unique;
        isConstraint   = constraint;
        isForward      = forward;
        this.table     = table;
        this.pkCols    = table.getPrimaryKey();
        this.pkTypes   = table.getPrimaryKeyTypes();
        useRowId = (!isUnique && pkCols.length == 0) || (colIndex.length == 0);
        colCheck       = table.getNewColumnCheckList();

        ArrayUtil.intIndexesToBooleanArray(colIndex, colCheck);
    }

    public int getType() {
        return SchemaObject.INDEX;
    }

    /**
     * Returns the HsqlName object
     */
    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return null;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getDDL() {

        StringBuffer sb = new StringBuffer();

        sb = new StringBuffer(64);

        sb.append(Tokens.T_CREATE).append(' ');

        if (isUnique()) {
            sb.append(Tokens.T_UNIQUE).append(' ');
        }

        sb.append(Tokens.T_INDEX).append(' ');
        sb.append(getName().statementName);
        sb.append(' ').append(Tokens.T_ON).append(' ');
        sb.append(((Table) table).getName().getSchemaQualifiedStatementName());

        int[] col = getColumns();
        int   len = getVisibleColumns();

        getColumnList(((Table) table), col, len, sb);

        return sb.toString();
    }

    private static void getColumnList(Table t, int[] col, int len,
                                      StringBuffer a) {

        a.append('(');

        for (int i = 0; i < len; i++) {
            a.append(t.getColumn(col[i]).getName().statementName);

            if (i < len - 1) {
                a.append(',');
            }
        }

        a.append(')');
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    /**
     * Returns the count of visible columns used
     */
    public int getVisibleColumns() {
        return colIndex.length;
    }

    /**
     * Returns the count of visible columns used
     */
    public int getColumnCount() {
        return colIndex.length;
    }

    /**
     * Is this a UNIQUE index?
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Does this index belong to a constraint?
     */
    public boolean isConstraint() {
        return isConstraint;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public int[] getColumns() {
        return colIndex;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public Type[] getColumnTypes() {
        return colTypes;
    }

    public boolean[] getColumnDesc() {
        return colDesc;
    }

    /**
     * Returns the node count.
     */
    public int size(PersistentStore store) throws HsqlException {

        int count = 0;

        readLock.lock();

        try {
            RowIterator it = firstRow(null, store);

            while (it.hasNext()) {
                it.getNextRow();

                count++;
            }

            return count;
        } finally {
            readLock.unlock();
        }
    }

    public boolean isEmpty(PersistentStore store) {

        readLock.lock();

        try {
            return store.getAccessor(this) == null;
        } finally {
            readLock.unlock();
        }
    }

    public int sizeEstimate(PersistentStore store) throws HsqlException {

        readLock.lock();

        try {
            firstRow(store);

            return (int) (1L << depth);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Insert a node into the index
     */
    public void insert(Session session, PersistentStore store, Row row,
                       int offset) throws HsqlException {

        Node    n;
        Node    x;
        boolean isleft  = true;
        int     compare = -1;

        writeLock.lock();

        try {
            n = (Node) store.getAccessor(this);
            x = n;

            if (n == null) {
                store.setAccessor(this, row.getNode(offset));

                return;
            }

            while (true) {
                Row currentRow = n.getRow(store);

                compare = compareRowForInsertOrDelete(session, row,
                                                      currentRow);

                if (compare == 0) {
                    throw Error.error(ErrorCode.X_23505);
                }

                isleft = compare < 0;
                x      = n;
                n      = child(store, x, isleft);

                if (n == null) {
                    break;
                }
            }

            x = set(store, x, isleft, row.getNode(offset));

            balance(store, x, isleft);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Balances part of the tree after an alteration to the index.
     */
    private void balance(PersistentStore store, Node x,
                         boolean isleft) throws HsqlException {

        while (true) {
            int sign = isleft ? 1
                              : -1;

            switch (x.getBalance() * sign) {

                case 1 :
                    x = x.setBalance(store, 0);

                    return;

                case 0 :
                    x = x.setBalance(store, -sign);
                    break;

                case -1 :
                    Node l = child(store, x, isleft);

                    if (l.getBalance() == -sign) {
                        replace(store, x, l);

                        x = set(store, x, isleft, child(store, l, !isleft));
                        l = set(store, l, !isleft, x);
                        x = x.setBalance(store, 0);
                        l = l.setBalance(store, 0);
                    } else {
                        Node r = child(store, l, !isleft);

                        replace(store, x, r);

                        l = set(store, l, !isleft, child(store, r, isleft));
                        r = set(store, r, isleft, l);
                        x = set(store, x, isleft, child(store, r, !isleft));
                        r = set(store, r, !isleft, x);

                        int rb = r.getBalance();

                        x = x.setBalance(store, (rb == -sign) ? sign
                                                              : 0);
                        l = l.setBalance(store, (rb == sign) ? -sign
                                                             : 0);
                        r = r.setBalance(store, 0);
                    }

                    return;
            }

            if (x.isRoot()) {
                return;
            }

            isleft = x.isFromLeft(store);
            x      = x.getParent(store);
        }
    }

    public void delete(PersistentStore store, Node x) throws HsqlException {

        if (x == null) {
            return;
        }

        Node n;

        writeLock.lock();

        try {
            if (x.getLeft(store) == null) {
                n = x.getRight(store);
            } else if (x.getRight(store) == null) {
                n = x.getLeft(store);
            } else {
                Node d = x;

                x = x.getLeft(store);

                while (true) {
                    Node temp = x.getRight(store);

                    if (temp == null) {
                        break;
                    }

                    x = temp;
                }

                // x will be replaced with n later
                n = x.getLeft(store);

                // swap d and x
                int b = x.getBalance();

                x = x.setBalance(store, d.getBalance());
                d = d.setBalance(store, b);

                // set x.parent
                Node xp = x.getParent(store);
                Node dp = d.getParent(store);

                if (d.isRoot()) {
                    store.setAccessor(this, x);
                }

                x = x.setParent(store, dp);

                if (dp != null) {
                    if (dp.isRight(d)) {
                        dp = dp.setRight(store, x);
                    } else {
                        dp = dp.setLeft(store, x);
                    }
                }

                // relink d.parent, x.left, x.right
                if (d.equals(xp)) {
                    d = d.setParent(store, x);

                    if (d.isLeft(x)) {
                        x = x.setLeft(store, d);

                        Node dr = d.getRight(store);

                        x = x.setRight(store, dr);
                    } else {
                        x = x.setRight(store, d);

                        Node dl = d.getLeft(store);

                        x = x.setLeft(store, dl);
                    }
                } else {
                    d  = d.setParent(store, xp);
                    xp = xp.setRight(store, d);

                    Node dl = d.getLeft(store);
                    Node dr = d.getRight(store);

                    x = x.setLeft(store, dl);
                    x = x.setRight(store, dr);
                }

                // apprently no-ops
                x.getRight(store).setParent(store, x);
                x.getLeft(store).setParent(store, x);

                // set d.left, d.right
                d = d.setLeft(store, n);

                if (n != null) {
                    n = n.setParent(store, d);
                }

                d = d.setRight(store, null);
                x = d;
            }

            boolean isleft = x.isFromLeft(store);

            replace(store, x, n);

            n = x.getParent(store);

            x.delete();

            while (n != null) {
                x = n;

                int sign = isleft ? 1
                                  : -1;

                switch (x.getBalance() * sign) {

                    case -1 :
                        x = x.setBalance(store, 0);
                        break;

                    case 0 :
                        x = x.setBalance(store, sign);

                        return;

                    case 1 :
                        Node r = child(store, x, !isleft);
                        int  b = r.getBalance();

                        if (b * sign >= 0) {
                            replace(store, x, r);

                            x = set(store, x, !isleft,
                                    child(store, r, isleft));
                            r = set(store, r, isleft, x);

                            if (b == 0) {
                                x = x.setBalance(store, sign);
                                r = r.setBalance(store, -sign);

                                return;
                            }

                            x = x.setBalance(store, 0);
                            r = r.setBalance(store, 0);
                            x = r;
                        } else {
                            Node l = child(store, r, isleft);

                            replace(store, x, l);

                            b = l.getBalance();
                            r = set(store, r, isleft,
                                    child(store, l, !isleft));
                            l = set(store, l, !isleft, r);
                            x = set(store, x, !isleft,
                                    child(store, l, isleft));
                            l = set(store, l, isleft, x);
                            x = x.setBalance(store, (b == sign) ? -sign
                                                                : 0);
                            r = r.setBalance(store, (b == -sign) ? sign
                                                                 : 0);
                            l = l.setBalance(store, 0);
                            x = l;
                        }
                }

                isleft = x.isFromLeft(store);
                n      = x.getParent(store);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public RowIterator findFirstRowIterator(Session session,
            PersistentStore store, Object[] rowdata,
            int[] rowColMap) throws HsqlException {

        Node node = findNotNull(session, store, rowdata, rowColMap, true);

        return getIterator(session, store, node);
    }

    public RowIterator findFirstRowForDelete(Session session,
            PersistentStore store, Object[] rowdata,
            int[] rowColMap) throws HsqlException {

        Node node = findNotNull(session, store, rowdata, rowColMap, true);

        return getIterator(session, store, node);
    }

    public Row findFirstRow(Session session, PersistentStore store,
                            Object[] rowdata,
                            int[] rowColMap) throws HsqlException {

        Node node = findNotNull(session, store, rowdata, rowColMap, true);

        return node == null ? null
                            : node.getRow(store);
    }

    /**
     * Finds an existing row
     */
    public Row findRow(Session session, PersistentStore store,
                       Row row) throws HsqlException {

        Node node = search(session, store, row);

        return node == null ? null
                            : node.getRow(store);
    }

    public boolean exists(Session session, PersistentStore store,
                          Object[] rowdata,
                          int[] rowColMap) throws HsqlException {
        return findNotNull(session, store, rowdata, rowColMap, true) != null;
    }

    public RowIterator emptyIterator() {
        return emptyIterator;
    }

    /**
     * Finds a foreign key referencing rows (in child table)
     *
     * @param rowdata array containing data for the index columns
     * @param rowColMap map of the data to columns
     * @param first true if the first matching node is required, false if any node
     * @return matching node or null
     * @throws HsqlException
     */
    private Node findNotNull(Session session, PersistentStore store,
                             Object[] rowdata, int[] rowColMap,
                             boolean first) throws HsqlException {

        Node x = (Node) store.getAccessor(this);
        Node n;
        Node result = null;

        if (hasNull(rowdata, rowColMap)) {
            return null;
        }

        while (x != null) {
            int i = this.compareRowNonUnique(rowdata, rowColMap,
                                             x.getRow(store).getData());

            if (i == 0) {
                if (!first) {
                    result = x;

                    break;
                } else if (result == x) {
                    break;
                }

                result = x;
                n      = x.getLeft(store);
            } else if (i > 0) {
                n = x.getRight(store);
            } else {
                n = x.getLeft(store);
            }

            if (n == null) {
                break;
            }

            x = n;
        }

// MVCC 190
        if (session == null) {
            return result;
        }

        while (result != null) {
            Row row = result.getRow(store);

            if (compareRowNonUnique(rowdata, rowColMap, row.getData()) != 0) {
                result = null;

                break;
            }

            if (session.database.txManager.canRead(session, row)) {
                break;
            }

            result = next(store, result);
        }

        return result;
    }

    /**
     * Determines if a table row has a null column for any of the columns given
     * in the rowColMap array.
     */
    public static boolean hasNull(Object[] row, int[] rowColMap) {

        int count = rowColMap.length;

        for (int i = 0; i < count; i++) {
            if (row[rowColMap[i]] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determines if a table row has a null column for any of the indexed
     * columns.
     */
    boolean hasNull(Object[] data) {

        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return the first node equal to the indexdata object. Use visible columns
     * only. The coldata has the same column mapping as this index.
     *
     * @session session object
     * @param coldata array containing index column data
     * @match count of columns to match
     * @return iterator
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] coldata,
                                    int match) throws HsqlException {

        readLock.lock();

        try {
            Node x     = (Node) store.getAccessor(this);
            Node found = null;
            boolean unique = isUnique && match == colIndex.length
                             && !hasNull(coldata);

            while (x != null) {
                int c = compareRowNonUnique(coldata,
                                            x.getRow(store).getData(), match);

                if (c == 0) {
                    found = x;

/*
// MVCC
                    if (unique) {
                        break;
                    }
*/

                    x = x.getLeft(store);
                } else if (c < 0) {
                    x = x.getLeft(store);
                } else {
                    x = x.getRight(store);
                }
            }

            return getIterator(session, store, found);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Return the first node equal to the rowdata object. Use visible columns
     * only. The rowdata has the same column mapping as this table.
     *
     * @session session object
     * @param rowdata array containing table row data
     * @return iterator
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata) throws HsqlException {

        readLock.lock();

        try {
            Node    x      = (Node) store.getAccessor(this);
            Node    found  = null;
            boolean unique = isUnique && !hasNull(rowdata, colIndex);

            while (x != null) {
                int c = compareRowNonUnique(rowdata, colIndex,
                                            x.getRow(store).getData());

                if (c == 0) {
                    found = x;
/*
// MVCC
                if (unique) {
                    break;
                }
*/
                    x = x.getLeft(store);
                } else if (c < 0) {
                    x = x.getLeft(store);
                } else {
                    x = x.getRight(store);
                }
            }

// MVCC
            while (found != null) {
                Row row = found.getRow(store);

                if (compareRowNonUnique(rowdata, colIndex, row.getData())
                        != 0) {
                    found = null;

                    break;
                }

                if (session == null
                        || session.database.txManager.canRead(session, row)) {
                    break;
                }

                found = next(store, found);
            }

            return getIterator(session, store, found);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Finds the first node that is larger or equal to the given one based
     * on the first column of the index only.
     *
     * @param value value to match
     * @param compare comparison Expression type
     *
     * @return iterator
     *
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object value,
                                    int compare) throws HsqlException {

        readLock.lock();

        try {
            if (compare == OpTypes.SMALLER
                    || compare == OpTypes.SMALLER_EQUAL) {
                return findFirstRowNotNull(session, store);
            }

            boolean isEqual = compare == OpTypes.EQUAL
                              || compare == OpTypes.IS_NULL;
            Node x     = (Node) store.getAccessor(this);
            int  iTest = 1;

            if (compare == OpTypes.GREATER) {
                iTest = 0;
            }

            if (value == null && !isEqual) {
                return emptyIterator;
            }

/*
        // this method returns the correct node only with the following conditions
        boolean check = compare == OpCodes.BIGGER
                        || compare == OpCodes.EQUAL
                        || compare == OpCodes.BIGGER_EQUAL;

        if (!check) {
            Trace.doAssert(false, "Index.findFirst");
        }
*/
            while (x != null) {
                boolean t = colTypes[0].compare(
                    value, x.getRow(store).getData()[colIndex[0]]) >= iTest;

                if (t) {
                    Node r = x.getRight(store);

                    if (r == null) {
                        break;
                    }

                    x = r;
                } else {
                    Node l = x.getLeft(store);

                    if (l == null) {
                        break;
                    }

                    x = l;
                }
            }

/*
        while (x != null
                && Column.compare(value, x.getData()[colIndex_0], colType_0)
                   >= iTest) {
            x = next(x);
        }
*/
            while (x != null) {
                Object colvalue = x.getRow(store).getData()[colIndex[0]];
                int    result   = colTypes[0].compare(value, colvalue);

                if (result >= iTest) {
                    x = next(store, x);
                } else {
                    if (isEqual) {
                        if (result != 0) {
                            x = null;
                        }
                    } else if (colvalue == null) {
                        x = next(store, x);

                        continue;
                    }

                    break;
                }
            }

// MVCC
            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (compare == OpTypes.EQUAL
                        && colTypes[0].compare(
                            value, row.getData()[colIndex[0]]) != 0) {
                    x = null;

                    break;
                }

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Finds the first node where the data is not null.
     *
     * @return iterator
     *
     * @throws HsqlException
     */
    public RowIterator findFirstRowNotNull(Session session,
                                           PersistentStore store)
                                           throws HsqlException {

        readLock.lock();

        try {
            Node x = (Node) store.getAccessor(this);

            while (x != null) {
                boolean t = colTypes[0].compare(
                    null, x.getRow(store).getData()[colIndex[0]]) >= 0;

                if (t) {
                    Node r = x.getRight(store);

                    if (r == null) {
                        break;
                    }

                    x = r;
                } else {
                    Node l = x.getLeft(store);

                    if (l == null) {
                        break;
                    }

                    x = l;
                }
            }

            while (x != null) {
                Object colvalue = x.getRow(store).getData()[colIndex[0]];

                if (colvalue == null) {
                    x = next(store, x);
                } else {
                    break;
                }
            }

// MVCC
            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            readLock.unlock();
        }
    }

    public void checkIndex(PersistentStore store) {

        readLock.lock();

        try {
            Node x = (Node) store.getAccessor(this);

            checkNodes(x, null);

            Node l = x;
            Node r = null;

            while (l != null) {
                x = l;
                l = x.getLeft(store);
                r = x.getRight(store);

                checkNodes(l, r);
            }

            while (x != null) {
                l = x.getLeft(store);
                r = x.getLeft(store);

                checkNodes(l, r);

                try {
                    x = next(store, x);
                } catch (HsqlException e) {}
            }
        } finally {
            readLock.unlock();
        }
    }

    void checkNodes(Node l, Node r) {

        if (l != null && l.getBalance() == -2) {
            System.out.print("broken");
        }

        if (r != null && r.getBalance() == -2) {
            System.out.print("broken");
        }
    }

    /**
     * Returns the row for the first node of the index
     *
     * @return Iterator for first row
     *
     * @throws HsqlException
     */
    public RowIterator firstRow(Session session,
                                PersistentStore store) throws HsqlException {

        int tempDepth = 0;

        readLock.lock();

        try {
            Node x = (Node) store.getAccessor(this);
            Node l = x;

            while (l != null) {
                x = l;
                l = x.getLeft(store);

                tempDepth++;
            }

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            depth = tempDepth;

            readLock.unlock();
        }
    }

    public RowIterator firstRow(PersistentStore store) {

        int tempDepth = 0;

        readLock.lock();

        try {
            depth = 0;

            Node x = (Node) store.getAccessor(this);
            Node l = x;

            while (l != null) {
                x = l;
                l = x.getLeft(store);

                tempDepth++;
            }

            return getIterator(null, store, x);
        } finally {
            depth = tempDepth;

            readLock.unlock();
        }
    }

    /**
     * Returns the row for the last node of the index
     *
     * @return last row
     *
     * @throws HsqlException
     */
    public Row lastRow(Session session,
                       PersistentStore store) throws HsqlException {

        readLock.lock();

        try {
            Node x = (Node) store.getAccessor(this);
            Node l = x;

            while (l != null) {
                x = l;
                l = x.getRight(store);
            }

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = last(store, x);
            }

            return x == null ? null
                             : x.getRow(store);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the node after the given one
     *
     * @param x node
     *
     * @return next node
     *
     * @throws HsqlException
     */
    private Node next(PersistentStore store, Node x) throws HsqlException {

        if (x == null) {
            return null;
        }

        readLock.lock();

        try {
            Node r = x.getRight(store);

            if (r != null) {
                x = r;

                Node l = x.getLeft(store);

                while (l != null) {
                    x = l;
                    l = x.getLeft(store);
                }

                return x;
            }

            Node ch = x;

            x = x.getParent(store);

            while (x != null && ch.equals(x.getRight(store))) {
                ch = x;
                x  = x.getParent(store);
            }

            return x;
        } finally {
            readLock.unlock();
        }
    }

    private Node last(PersistentStore store, Node x) throws HsqlException {

        if (x == null) {
            return null;
        }

        readLock.lock();

        try {
            Node left = x.getLeft(store);

            if (left != null) {
                x = left;

                Node right = x.getRight(store);

                while (right != null) {
                    x     = right;
                    right = x.getRight(store);
                }

                return x;
            }

            Node ch = x;

            x = x.getParent(store);

            while (x != null && ch.equals(x.getLeft(store))) {
                ch = x;
                x  = x.getParent(store);
            }

            return x;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns either child node
     *
     * @param x node
     * @param isleft boolean
     *
     * @return child node
     *
     * @throws HsqlException
     */
    private static Node child(PersistentStore store, Node x,
                              boolean isleft) throws HsqlException {
        return isleft ? x.getLeft(store)
                      : x.getRight(store);
    }

    /**
     * Replace x with n
     *
     * @param x node
     * @param n node
     *
     * @throws HsqlException
     */
    private void replace(PersistentStore store, Node x,
                         Node n) throws HsqlException {

        if (x.isRoot()) {
            if (n != null) {
                n = n.setParent(store, null);
            }

            store.setAccessor(this, n);
        } else {
            set(store, x.getParent(store), x.isFromLeft(store), n);
        }
    }

    /**
     * Set a node as child of another
     *
     * @param x parent node
     * @param isleft boolean
     * @param n child node
     *
     * @throws HsqlException
     */
    private static Node set(PersistentStore store, Node x, boolean isleft,
                            Node n) throws HsqlException {

        if (isleft) {
            x = x.setLeft(store, n);
        } else {
            x = x.setRight(store, n);
        }

        if (n != null) {
            n.setParent(store, x);
        }

        return x;
    }

    /**
     * Find a node with matching data
     *
     * @param row row data
     *
     * @return matching node
     *
     * @throws HsqlException
     */
    private Node search(Session session, PersistentStore store,
                        Row row) throws HsqlException {

        readLock.lock();

        try {
            Node x = (Node) store.getAccessor(this);

            while (x != null) {
                int c = compareRowForInsertOrDelete(session, row,
                                                    x.getRow(store));

                if (c == 0) {
                    return x;
                } else if (c < 0) {
                    x = x.getLeft(store);
                } else {
                    x = x.getRight(store);
                }
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Compares two table rows based on the columns of this index. The rowColMap
     * parameter specifies which columns of the other table are to be compared
     * with the colIndex columns of this index. The rowColMap can cover all
     * or only some columns of this index.
     *
     * @param a row from another table
     * @param rowColMap column indexes in the other table
     * @param b a full row in this table
     *
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    public int compareRowNonUnique(Object[] a, int[] rowColMap,
                                   Object[] b) throws HsqlException {

        int fieldcount = rowColMap.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = colTypes[j].compare(a[rowColMap[j]], b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * As above but use the index column data
     */
    public int compareRowNonUnique(Object[] a, Object[] b,
                                   int fieldcount) throws HsqlException {

        for (int j = 0; j < fieldcount; j++) {
            int i = colTypes[j].compare(a[j], b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * compares two full table rows based on a set of columns
     *
     * @param a a full row
     * @param b a full row
     * @param cols array of column indexes to compare
     * @param coltypes array of column types for the full row
     *
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    public static int compareRows(Object[] a, Object[] b, int[] cols,
                                  Type[] coltypes) throws HsqlException {

        int fieldcount = cols.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = coltypes[cols[j]].compare(a[cols[j]], b[cols[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Compare two rows of the table for inserting rows into unique indexes
     * Supports descending columns.
     *
     * @param newRow data
     * @param existingRow data
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    private int compareRowForInsertOrDelete(Session session, Row newRow,
            Row existingRow) throws HsqlException {

        Object[] a       = newRow.getData();
        Object[] b       = existingRow.getData();
        int      j       = 0;
        boolean  hasNull = false;

        for (; j < colIndex.length; j++) {
            Object  currentvalue = a[colIndex[j]];
            Object  othervalue   = b[colIndex[j]];
            int     i = colTypes[j].compare(currentvalue, othervalue);
            boolean nulls        = currentvalue == null || othervalue == null;

            if (i != 0) {
                if (colDesc[j] && !nulls) {
                    i = -i;
                }

                if (nullsLast[j] && nulls) {
                    i = -i;
                }

                return i;
            }

            if (currentvalue == null) {
                hasNull = true;
            }
        }

        if (isUnique && !useRowId && !hasNull) {
            if (session == null
                    || session.database.txManager.canRead(session,
                        existingRow)) {

                //* debug 190
//                session.database.txManager.canRead(session, existingRow);
                return 0;
            } else {
                int difference = newRow.getPos() - existingRow.getPos();

                return difference;
            }
        }

        for (j = 0; j < pkCols.length; j++) {
            Object currentvalue = a[pkCols[j]];
            int    i = pkTypes[j].compare(currentvalue, b[pkCols[j]]);

            if (i != 0) {
                return i;
            }
        }

        if (useRowId) {
            int difference = newRow.getPos() - existingRow.getPos();

            if (difference < 0) {
                difference = -1;
            } else if (difference > 0) {
                difference = 1;
            }

            return difference;
        }

        if (session == null
                || session.database.txManager.canRead(session, existingRow)) {
            return 0;
        } else {
            int difference = newRow.getPos() - existingRow.getPos();

            if (difference < 0) {
                difference = -1;
            } else if (difference > 0) {
                difference = 1;
            }

            return difference;
        }
    }

    /**
     * Returns a value indicating the order of different types of index in
     * the list of indexes for a table. The position of the groups of Indexes
     * in the list in ascending order is as follows:
     *
     * primary key index
     * unique constraint indexes
     * autogenerated foreign key indexes for FK's that reference this table or
     *  tables created before this table
     * user created indexes (CREATE INDEX)
     * autogenerated foreign key indexes for FK's that reference tables created
     *  after this table
     *
     * Among a group of indexes, the order is based on the order of creation
     * of the index.
     *
     * @return ordinal value
     */
    public int getIndexOrderValue() {

        if (isConstraint) {
            return isForward ? 4
                             : isUnique ? 0
                                        : 1;
        } else {
            return 2;
        }
    }

    Node getVisibleNode(Session session, PersistentStore store, Node node) {

        readLock.lock();

        try {
            if (session == null) {
                return node;
            }

            while (node != null) {
                try {
                    Row row = node.getRow(store);

                    if (session.database.txManager.canRead(session, row)) {
                        break;
                    }

                    node = Index.this.next(store, node);
                } catch (Exception e) {

                    //* debug 190
//                    e.printStackTrace();
                    throw new NoSuchElementException(e.getMessage());
                }
            }

            return node;
        } finally {
            readLock.unlock();
        }
    }

    public IndexRowIterator getIterator(Session session,
                                        PersistentStore store, Node x) {

        if (x == null) {
            return emptyIterator;
        } else {
            IndexRowIterator it = new IndexRowIterator(session, store, this,
                x);

            return it;
        }
    }

    public static final class IndexRowIterator implements RowIterator {

        final Session         session;
        final PersistentStore store;
        final Index           index;
        Node                  nextnode;
        Row                   lastrow;
        IndexRowIterator      last;
        IndexRowIterator      next;
        IndexRowIterator      lastInSession;
        IndexRowIterator      nextInSession;

        /**
         * When session == null, rows from all sessions are returned
         */
        public IndexRowIterator(Session session, PersistentStore store,
                                Index index, Node node) {

            this.session = session;
            this.store   = store;
            this.index   = index;

            if (index == null) {
                return;
            }

            nextnode = index.getVisibleNode(session, store, node);
        }

        public boolean hasNext() {
            return nextnode != null;
        }

        public Row getNextRow() {

            if (nextnode == null) {
                return null;
            }

            index.readLock.lock();

            try {
                nextnode = index.getVisibleNode(session, store, nextnode);

                if (nextnode == null) {
                    release();

                    return null;
                }

                lastrow  = nextnode.getRow(store);
                nextnode = index.next(store, nextnode);
                nextnode = index.getVisibleNode(session, store, nextnode);

                if (nextnode == null) {
                    release();
                }

                return lastrow;
            } catch (Exception e) {
                throw new NoSuchElementException(e.getMessage());
            } finally {
                index.readLock.unlock();
            }
        }

        public void remove() throws HsqlException {
            index.table.delete(store, lastrow);
        }

        public void release() {}
    }
}
