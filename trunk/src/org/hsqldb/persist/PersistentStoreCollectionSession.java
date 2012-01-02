/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.store.ValuePool;

/**
 * Collection of PersistenceStore itmes currently used by a session.
 * An item is retrieved based on key returned by
 * TableBase.getPersistenceId().
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.9.0
 */
public class PersistentStoreCollectionSession
implements PersistentStoreCollection {

    private final Session        session;
    private final LongKeyHashMap rowStoreMapSession     = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapTransaction = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapStatement   = new LongKeyHashMap();
    private HsqlDeque            rowStoreListStatement;

    public PersistentStoreCollectionSession(Session session) {
        this.session = session;
    }

    public void setStore(Object key, PersistentStore store) {

        TableBase table = (TableBase) key;

        switch (table.persistenceScope) {

            case TableBase.SCOPE_STATEMENT :
                if (store == null) {
                    rowStoreMapStatement.remove(table.getPersistenceId());
                } else {
                    rowStoreMapStatement.put(table.getPersistenceId(), store);
                }
                break;

            // SYSTEM_TABLE + INFO_SCHEMA_TABLE
            case TableBase.SCOPE_FULL :
            case TableBase.SCOPE_TRANSACTION :
                if (store == null) {
                    rowStoreMapTransaction.remove(table.getPersistenceId());
                } else {
                    rowStoreMapTransaction.put(table.getPersistenceId(),
                                               store);
                }
                break;

            case TableBase.SCOPE_SESSION :
                if (store == null) {
                    rowStoreMapSession.remove(table.getPersistenceId());
                } else {
                    rowStoreMapSession.put(table.getPersistenceId(), store);
                }
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "PersistentStoreCollectionSession");
        }
    }

    public PersistentStore getViewStore(long persistenceId) {
        return (PersistentStore) rowStoreMapStatement.get(persistenceId);
    }

    public PersistentStore getStore(Object key) {

        try {
            TableBase       table = (TableBase) key;
            PersistentStore store;

            switch (table.persistenceScope) {

                case TableBase.SCOPE_STATEMENT :
                    store = (PersistentStore) rowStoreMapStatement.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }

                    return store;

                // SYSTEM_TABLE + INFO_SCHEMA_TABLE
                case TableBase.SCOPE_FULL :
                case TableBase.SCOPE_TRANSACTION :
                    store = (PersistentStore) rowStoreMapTransaction.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }

                    if (table.getTableType() == TableBase.INFO_SCHEMA_TABLE) {
                        session.database.dbInfo.setStore(session,
                                                         (Table) table, store);
                    }

                    return store;

                case TableBase.SCOPE_SESSION :
                    store = (PersistentStore) rowStoreMapSession.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }

                    return store;
            }
        } catch (HsqlException e) {}

        throw Error.runtimeError(ErrorCode.U_S0500,
                                 "PersistentStoreCollectionSession");
    }

    public void clearAllTables() {

        clearSessionTables();
        clearTransactionTables();
        clearStatementTables();
        closeResultCache();
    }

    public void clearResultTables(long actionTimestamp) {

        if (rowStoreMapSession.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapSession.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            if (store.getTimestamp() == actionTimestamp) {
                store.release();
                it.remove();
            }
        }
    }

    public void clearSessionTables() {

        if (rowStoreMapSession.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapSession.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }

        rowStoreMapSession.clear();
    }

    public void clearTransactionTables() {

        if (rowStoreMapTransaction.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapTransaction.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }

        rowStoreMapTransaction.clear();
    }

    public void clearStatementTables() {

        if (rowStoreMapStatement.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapStatement.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }

        rowStoreMapStatement.clear();
    }

    public void registerIndex(Table table) {

        PersistentStore store = findStore(table);

        if (store == null) {
            return;
        }

        store.resetAccessorKeys(table.getIndexList());
    }

    public PersistentStore findStore(Table table) {

        PersistentStore store = null;

        switch (table.persistenceScope) {

            case TableBase.SCOPE_STATEMENT :
                store = (PersistentStore) rowStoreMapStatement.get(
                    table.getPersistenceId());
                break;

            // SYSTEM_TABLE + INFO_SCHEMA_TABLE
            case TableBase.SCOPE_FULL :
            case TableBase.SCOPE_TRANSACTION :
                store = (PersistentStore) rowStoreMapTransaction.get(
                    table.getPersistenceId());
                break;

            case TableBase.SCOPE_SESSION :
                store = (PersistentStore) rowStoreMapSession.get(
                    table.getPersistenceId());
                break;
        }

        return store;
    }

    public void moveData(Table oldTable, Table newTable, int colIndex,
                         int adjust) {

        PersistentStore oldStore = findStore(oldTable);

        if (oldStore == null) {
            return;
        }

        PersistentStore newStore = getStore(newTable);

        try {
            newStore.moveData(session, oldStore, colIndex, adjust);
        } catch (HsqlException e) {
            newStore.release();
            setStore(newTable, null);

            throw e;
        }

        setStore(oldTable, null);
    }

    public void push() {

        if (rowStoreListStatement == null) {
            rowStoreListStatement = new HsqlDeque();
        }

        if (rowStoreMapStatement.isEmpty()) {
            rowStoreListStatement.add(ValuePool.emptyObjectArray);

            return;
        }

        Object[] array = rowStoreMapStatement.toArray();

        rowStoreListStatement.add(array);
        rowStoreMapStatement.clear();
    }

    public void pop() {

        Object[] array = (Object[]) rowStoreListStatement.removeLast();

        clearStatementTables();

        for (int i = 0; i < array.length; i++) {
            PersistentStore store = (PersistentStore) array[i];

            rowStoreMapStatement.put(store.getTable().getPersistenceId(),
                                     store);
        }
    }

    DataFileCacheSession resultCache;

    public DataFileCacheSession getResultCache() {

        if (resultCache == null) {
            String path = session.database.logger.getTempDirectoryPath();

            if (path == null) {
                return null;
            }

            try {
                resultCache =
                    new DataFileCacheSession(session.database,
                                             path + "/session_"
                                             + Long.toString(session.getId()));

                resultCache.open(false);
            } catch (Throwable t) {
                return null;
            }
        }

        return resultCache;
    }

    public void closeResultCache() {

        if (resultCache != null) {
            try {
                resultCache.close(false);
                resultCache.deleteFile();
            } catch (HsqlException e) {}

            resultCache = null;
        }
    }
}
