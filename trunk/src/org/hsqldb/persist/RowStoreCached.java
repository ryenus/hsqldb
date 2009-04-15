/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.persist;

import java.io.IOException;

import org.hsqldb.CachedRow;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.index.DiskNode;
import org.hsqldb.index.Index;
import org.hsqldb.index.Node;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for CACHED tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreCached implements PersistentStore {

    final TableBase                 table;
    final PersistentStoreCollection manager;
    private Index[]                 indexList    = Index.emptyArray;
    CachedObject[]                  accessorList = CachedObject.emptyArray;
    DataFileCache                   cache;

    public RowStoreCached(PersistentStoreCollection manager,
                          DataFileCache cache, TableBase table) {

        this.manager      = manager;
        this.table        = table;
        this.indexList    = table.getIndexList();
        this.accessorList = new CachedObject[indexList.length];
        this.cache        = cache;

        manager.setStore(table, this);
    }

    public CachedObject get(int i) {

        try {
            CachedObject object = cache.get(i, this, false);

            if (object instanceof Row && ((Row) object).rowAction == null) {
                table.database.txManager.setTransactionInfo((Row) object);
            }

            return object;
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject getKeep(int i) {

        try {
            CachedObject object = cache.get(i, this, true);

            if (object instanceof Row && ((Row) object).rowAction == null) {
                table.database.txManager.setTransactionInfo((Row) object);
            }

            return object;
        } catch (HsqlException e) {
            return null;
        }
    }

    public int getStorageSize(int i) {

        try {
            return cache.get(i, this, false).getStorageSize();
        } catch (HsqlException e) {
            return 0;
        }
    }

    public void add(CachedObject object) throws HsqlException {

        int size = cache.rowOut.getSize((CachedRow) object)
                   + table.getIndexCount() * DiskNode.SIZE_IN_BYTE;

        size = ((size + cache.cachedRowPadding - 1) / cache.cachedRowPadding)
               * cache.cachedRowPadding;

        object.setStorageSize(size);
        cache.add(object);
    }

    public void restore(CachedObject row) throws HsqlException {
        row.restore();
        cache.restore(row);
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new CachedRow(table, in);
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }
    }

    public CachedObject getNewCachedObject(Session session,
                                           Object object)
                                           throws HsqlException {

        Row row = new CachedRow(table, (Object[]) object);

        add(row);

        if (session != null) {
            RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);
        }

        return row;
    }

    public void removeAll() {
        ArrayUtil.fillArray(accessorList, null);
    }

    public void remove(int i) {
        cache.remove(i, this);
    }

    public void removePersistence(int i) {}

    public void release(int i) {
        cache.release(i);
    }

    public void commit(CachedObject row) {}

    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        this.cache = cache;
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);

        cache = null;
    }

    public CachedObject getAccessor(Index key) {

        Index index    = (Index) key;
        int   position = index.getPosition();

        if (position >= accessorList.length) {
            return null;
        }

        return accessorList[position];
    }

    public void setAccessor(Index key, CachedObject accessor) {

        Index index = (Index) key;

        accessorList[index.getPosition()] = accessor;
    }

    public void setAccessor(Index key, int accessor) {

        CachedObject object = get(accessor);

        if (object != null) {
            Node node = ((Row) object).getNode(key.getPosition());

            object = node;
        }

        setAccessor(key, object);
    }

    public void resetAccessorKeys(Index[] keys) {

        if (indexList.length == 0 || indexList[0] == null
                || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreCached");
    }

    public CachedObject getNewInstance(int size) {
        return null;
    }
}
