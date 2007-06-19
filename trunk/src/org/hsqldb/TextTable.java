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

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.persist.TextCache;

// tony_lai@users 20020820 - patch 595099 - user define PK name

/**
 * Subclass of Table to handle TEXT data source. <p>
 *
 * Extends Table to provide the notion of an SQL base table object whose
 * data is read from and written to a text format data file.
 *
 * @author sqlbob@users (RMP)
 * @version    1.8.0
 */
class TextTable extends org.hsqldb.Table {

    private String  dataSource  = "";
    private boolean isReversed  = false;
    private boolean isConnected = false;

    /**
     *  Constructs a new TextTable from the given arguments.
     *
     * @param  db the owning database
     * @param  name the table's HsqlName
     * @param  type (normal or temp text table)
     * @param  sessionid the id of the owning session (for temp table)
     * @exception  HsqlException  Description of the Exception
     */
    TextTable(Database db, HsqlNameManager.HsqlName name,
              int type) throws HsqlException {
        super(db, name, type);
    }

    /**
     * common handling for all errors during <code>connect</code>
     */
    private void onConnectError(Session session) {

        if (cache != null) {
            try {
                cache.close(false);
            } catch (HsqlException ex) {}
        }

        cache = null;

        clearAllData(session);
    }

    public boolean isConnected() {
        return isConnected;
    }

    /**
     * connects to the data source
     */
    public void connect(Session session) throws HsqlException {
        connect(session, isReadOnly);
    }

    /**
     * connects to the data source
     */
    private void connect(Session session,
                         boolean withReadOnlyData) throws HsqlException {

        // Open new cache:
        if ((dataSource.length() == 0) || isConnected) {

            // nothing to do
            return;
        }

        try {
            cache = database.logger.openTextCache(this, dataSource,
                                                  withReadOnlyData,
                                                  isReversed);

            // read and insert all the rows from the source file
            CachedRow row     = null;
            int       nextpos = 0;

            if (((TextCache) cache).ignoreFirst) {
                nextpos += ((TextCache) cache).readHeaderLine();
            }

            while (true) {
                row = (CachedRow) rowStore.get(nextpos);

                if (row == null) {
                    break;
                }

                nextpos = row.getPos() + row.getStorageSize();

                row.setNewNodes();
                insertFromTextSource(session, row);
            }
        } catch (HsqlException e) {
            int linenumber = cache == null ? 0
                                           : ((TextCache) cache)
                                               .getLineNumber();

            onConnectError(session);

            // everything is in order here.
            // At this point table should either have a valid (old) data
            // source and cache or have an empty source and null cache.
            throw Trace.error(Trace.TEXT_FILE, new Object[] {
                new Integer(linenumber), e.getMessage()
            });
        } catch (java.lang.RuntimeException t) {
            onConnectError(session);

            throw t;
        }

        isConnected = true;
        isReadOnly  = withReadOnlyData;
    }

    /**
     * disconnects from the data source
     */
    public void disconnect(Session session) throws HsqlException {

        // Close old cache:
        database.logger.closeTextCache(this);

        cache = null;

        clearAllData(session);

        isConnected = false;
    }

    /**
     * This method does some of the work involved with managing the creation
     * and openning of the cache, the rest is done in Log.java and
     * TextCache.java.
     *
     * Better clarification of the role of the methods is needed.
     */
    private void openCache(Session session, String dataSourceNew,
                           boolean isReversedNew,
                           boolean isReadOnlyNew) throws HsqlException {

        if (dataSourceNew == null) {
            dataSourceNew = "";
        }

        disconnect(session);

        dataSource = dataSourceNew;
        isReversed = (isReversedNew && dataSource.length() > 0);

        connect(session, isReadOnlyNew);
    }

    /**
     * High level command to assign a data source to the table definition.
     * Reassigns only if the data source or direction has changed.
     */
    protected void setDataSource(Session session, String dataSourceNew,
                                 boolean isReversedNew,
                                 boolean newFile) throws HsqlException {

        if (getTableType() == Table.TEMP_TEXT_TABLE) {
            ;
        } else {
            session.getUser().checkSchemaUpdateOrGrantRights(
                getSchemaName().name);
        }

        dataSourceNew = dataSourceNew.trim();

        if (newFile && FileUtil.exists(dataSourceNew)) {
            throw Trace.error(Trace.TEXT_SOURCE_EXISTS, dataSourceNew);
        }

        //-- Open if descending, direction changed, file changed, or not connected currently
        if (isReversedNew || (isReversedNew != isReversed)
                || !dataSource.equals(dataSourceNew) || !isConnected) {
            openCache(session, dataSourceNew, isReversedNew, isReadOnly);
        }

        if (isReversed) {
            isReadOnly = true;
        }
    }

    public String getDataSource() {
        return dataSource;
    }

    public boolean isDescDataSource() {
        return isReversed;
    }

    public void setHeader(String header) throws HsqlException {

        if (cache != null && ((TextCache) cache).ignoreFirst) {
            ((TextCache) cache).setHeader(header);

            return;
        }

        throw Trace.error(Trace.TEXT_TABLE_HEADER);
    }

    public String getHeader() {

        String header = cache == null ? null
                                      : ((TextCache) cache).getHeader();

        return header == null ? null
                              : StringConverter.toQuotedString(header, '\"',
                              true);
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations. This class will return
     * a more appropriate message when there is no data source.
     */
    void checkDataReadOnly() throws HsqlException {

        if (dataSource.length() == 0) {
            throw Trace.error(Trace.UNKNOWN_DATA_SOURCE);
        }

        if (isReadOnly) {
            throw Trace.error(Trace.DATA_IS_READONLY);
        }
    }

    public boolean isDataReadOnly() {
        return !isConnected() || super.isDataReadOnly();
    }

    public void setDataReadOnly(boolean value) throws HsqlException {

        if (!value) {
            if (isReversed) {
                throw Trace.error(Trace.DATA_IS_READONLY);
            }

            if (database.isFilesReadOnly()) {
                throw Trace.error(Trace.DATABASE_IS_READONLY);
            }
        }

        openCache(null, dataSource, isReversed, value);

        isReadOnly = value;
    }

    boolean isIndexCached() {
        return false;
    }

    protected Table duplicate() throws HsqlException {
        return new TextTable(database, tableName, getTableType());
    }

    void drop() throws HsqlException {
        openCache(null, "", false, false);
    }

    void setIndexRoots(String s) throws HsqlException {

        // do nothing
    }
}
