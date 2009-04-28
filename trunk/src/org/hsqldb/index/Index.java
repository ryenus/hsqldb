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

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;

public interface Index extends SchemaObject {

    Index[] emptyArray = new Index[]{};

    RowIterator emptyIterator();

    public int getPosition();

    public void setPosition(int position);

    public long getPersistenceId();

    /**
     * Returns the count of visible columns used
     */
    public int getVisibleColumns();

    public int getColumnCount();

    /**
     * Is this a UNIQUE index?
     */
    public boolean isUnique();

    /**
     * Does this index belong to a constraint?
     */
    public boolean isConstraint();

    /**
     * Returns the array containing column indexes for index
     */
    public int[] getColumns();

    /**
     * Returns the array containing column indexes for index
     */
    public Type[] getColumnTypes();

    /**
     * Returns the count of visible columns used
     */
    public boolean[] getColumnDesc();

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
    public int getIndexOrderValue();

    public boolean isForward();

    /**
     * Returns the node count.
     */
    public int size(PersistentStore store) throws HsqlException;

    public int sizeEstimate(PersistentStore store) throws HsqlException;

    public boolean isEmpty(PersistentStore store);

    public void checkIndex(PersistentStore store) throws HsqlException;

    /**
     * Insert a node into the index
     */
    public void insert(Session session, PersistentStore store,
                       Row row) throws HsqlException;

    public void delete(PersistentStore store, Row row) throws HsqlException;

    public boolean exists(Session session, PersistentStore store,
                          Object[] rowdata,
                          int[] rowColMap) throws HsqlException;

    /**
     * Return the first node equal to the indexdata object. The rowdata has
     * the same column mapping as this index.
     *
     * @param session session object
     * @param store store object
     * @param coldata array containing index column data
     * @param match count of columns to match
     * @return iterator
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata,
                                    int match) throws HsqlException;

    /**
     * Return the first node equal to the rowdata object.
     * The rowdata has the same column mapping as this table.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @return iterator
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata) throws HsqlException;

    /**
     * Return the first node equal to the rowdata object.
     * The rowdata has the column mapping privided in rowColMap.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @return iterator
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata,
                                    int[] rowColMap) throws HsqlException;

    /**
     * Finds the first node that is larger or equal to the given one based
     * on the first column of the index only.
     *
     * @param session session object
     * @param store store object
     * @param value value to match
     * @param compare comparison Expression type
     *
     * @return iterator
     *
     * @throws HsqlException
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object value,
                                    int compare) throws HsqlException;

    /**
     * Finds the first node where the data is not null.
     *
     * @return iterator
     *
     * @throws HsqlException
     */
    public RowIterator findFirstRowNotNull(Session session,
                                           PersistentStore store)
                                           throws HsqlException;

    public RowIterator firstRow(PersistentStore store);

    /**
     * Returns the row for the first node of the index
     *
     * @return Iterator for first row
     *
     * @throws HsqlException
     */
    public RowIterator firstRow(Session session,
                                PersistentStore store) throws HsqlException;

    /**
     * Returns the row for the last node of the index
     *
     * @return last row
     *
     * @throws HsqlException
     */
    public Row lastRow(Session session,
                       PersistentStore store) throws HsqlException;

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
                                   Object[] b) throws HsqlException;

    public int compareRowNonUnique(Object[] a, int[] rowColMap, Object[] b,
                                   int fieldCount) throws HsqlException;

    /**
     * As above but use the index column data
     */
    public int compareRowNonUnique(Object[] a, Object[] b,
                                   int fieldcount) throws HsqlException;
}
