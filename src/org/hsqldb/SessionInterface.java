/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.TimeZone;

import org.hsqldb.error.HsqlException;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;

/**
 * Interface to Session and its remote proxy objects. Used by the
 * implementations of JDBC interfaces to communicate with the database at
 * the session level.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.7.2
 */
public interface SessionInterface {

    public interface AttributePos {

        int INFO_ID      = 0;
        int INFO_INTEGER = 1;
        int INFO_BOOLEAN = 2;
        int INFO_VARCHAR = 3;
        int INFO_LIMIT   = 4;
    }

    public interface Attributes {

        int INFO_ISOLATION           = 0;
        int INFO_AUTOCOMMIT          = 1;
        int INFO_CONNECTION_READONLY = 2;
        int INFO_CATALOG             = 3;
        int INFO_TIMEZONE            = 4;
    }

    //
    int TX_READ_UNCOMMITTED = 1;
    int TX_READ_COMMITTED   = 2;
    int TX_REPEATABLE_READ  = 4;
    int TX_SERIALIZABLE     = 8;

    //
    int lobStreamBlockSize = 512 * 1024;

    Result execute(Result r);

    RowSetNavigatorClient getRows(long navigatorId, int offset, int size);

    void closeNavigator(long id);

    void close();

    boolean isClosed();

    boolean isReadOnlyDefault();

    void setReadOnlyDefault(boolean readonly);

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    int getIsolation();

    void setIsolationDefault(int level);

    void startPhasedTransaction();

    void prepareCommit();

    void commit(boolean chain);

    void rollback(boolean chain);

    void rollbackToSavepoint(String name);

    void savepoint(String name);

    void releaseSavepoint(String name);

    void addWarning(HsqlException warning);

    Result cancel(Result r);

    Object getAttribute(int id);

    void setAttribute(int id, Object value);

    void setAttributeFromResult(Result result);

    long getId();

    int getRandomId();

    void resetSession();

    String getInternalConnectionURL();

    BlobDataID createBlob(long length);

    ClobDataID createClob(long length);

    Result allocateResultLob(ResultLob result);

    Scanner getScanner();

    Calendar getCalendar();

    Calendar getCalendarGMT();

    SimpleDateFormat getSimpleDateFormatGMT();

    TimeZone getTimeZone();

    int getZoneSeconds();

    int getStreamBlockSize();

    HsqlProperties getClientProperties();

    JDBCConnection getJDBCConnection();

    void setJDBCConnection(JDBCConnection connection);

    String getDatabaseUniqueName();
}
