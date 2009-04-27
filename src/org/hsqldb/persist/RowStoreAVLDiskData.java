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

import org.hsqldb.CachedDataRow;
import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.TableBase;

/*
 * Implementation of PersistentStore for TEXT tables.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowStoreText extends RowStoreCached implements PersistentStore {

    public RowStoreText(PersistentStoreCollection manager, TableBase table) {
        super(manager, null, table);
    }

    public boolean isMemory() {
        return false;
    }

    public void add(CachedObject object) throws HsqlException {

        int size = object.getRealSize(cache.rowOut);

        object.setStorageSize(size);
        cache.add(object);
    }

    public CachedObject get(RowInputInterface in) {

        try {
            return new CachedDataRow(table, in);
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }
    }

    public CachedObject getNewCachedObject(Session session,
                                           Object object)
                                           throws HsqlException {

        Row row = new CachedDataRow(table, (Object[]) object);

        add(row);
        RowAction.addAction(session, RowAction.ACTION_INSERT, table, row);

        return row;
    }

    public void removeAll() {

        // does not yet clear the storage
    }

    public void remove(int i) {

        if (cache != null) {
            cache.remove(i, this);
        }
    }

    public void removePersistence(int i) {

        try {
            if (cache != null) {
                cache.removePersistence(i);
            }
        } catch (HsqlException e) {

            //
        }
    }

    public void release(int i) {

        if (cache != null) {
            cache.release(i);
        }
    }

    public void commit(CachedObject row) {

        try {
            if (cache != null) {
                cache.saveRow(row);
            }
        } catch (HsqlException e1) {}
    }

    public void release() {

        ArrayUtil.fillArray(accessorList, null);
        table.database.logger.closeTextCache((Table) table);

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
}
