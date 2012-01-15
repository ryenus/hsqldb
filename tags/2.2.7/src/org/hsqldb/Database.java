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


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.dbinfo.DatabaseInformation;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LobManager;
import org.hsqldb.persist.Logger;
import org.hsqldb.persist.PersistentStoreCollectionDatabase;
import org.hsqldb.result.Result;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.User;
import org.hsqldb.rights.UserManager;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Collation;

// incorporates following contributions
// boucherb@users - javadoc comments
// Ocke Jansen (oj@openoffice dot org) - file access api

/**
 * Database is the root class for HSQL Database Engine database. <p>
 *
 * It holds the data structures that form an HSQLDB database instance.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.6
 * @since 1.9.0
 */
public class Database {

    int                        databaseID;
    String                     databaseUniqueName;
    String                     databaseType;
    private final String       canonicalPath;
    public HsqlProperties      urlProperties;
    private final String       path;
    public Collation           collation;
    public DatabaseInformation dbInfo;

    /** indicates the state of the database */
    private volatile int dbState;
    public Logger        logger;

    /** true means that all tables are readonly. */
    boolean databaseReadOnly;

    /**
     * true means that all CACHED and TEXT tables are readonly.
     *  MEMORY tables are updatable but updates are not persisted.
     */
    private boolean filesReadOnly;

    /** true means filesReadOnly */
    private boolean filesInJar;

    /**
     * Defaults are used in version upgrades, but overridden by
     *  databaseProperties or URL properties for new databases.
     */
    public boolean                sqlEnforceTypes        = false;
    public boolean                sqlEnforceRefs         = false;
    public boolean                sqlEnforceSize         = true;
    public boolean                sqlEnforceNames        = false;
    public boolean                sqlEnforceTDCD         = true;
    public boolean                sqlEnforceTDCU         = true;
    public boolean                sqlTranslateTTI        = true;
    public boolean                sqlConcatNulls         = true;
    public boolean                sqlUniqueNulls         = true;
    public boolean                sqlNullsFirst          = true;
    public boolean                sqlConvertTruncate     = true;
    public int                    sqlAvgScale            = 0;
    public boolean                sqlDoubleNaN           = true;
    public boolean                sqlLongvarIsLob        = false;
    public boolean                sqlSyntaxDb2           = false;
    public boolean                sqlSyntaxMss           = false;
    public boolean                sqlSyntaxMys           = false;
    public boolean                sqlSyntaxOra           = false;
    public boolean                sqlSyntaxPgs           = false;
    private boolean               isReferentialIntegrity = true;
    public HsqlDatabaseProperties databaseProperties;
    private final boolean         shutdownOnNoConnection;
    int                           resultMaxMemoryRows;

    // schema invarient objects
    public UserManager     userManager;
    public GranteeManager  granteeManager;
    public HsqlNameManager nameManager;

    // session related objects
    public SessionManager     sessionManager;
    public TransactionManager txManager;
    public int defaultIsolationLevel = SessionInterface.TX_READ_COMMITTED;
    public boolean            txConflictRollback = true;

    // schema objects
    public SchemaManager schemaManager;

    //
    public PersistentStoreCollectionDatabase persistentStoreCollection;

    //
    public LobManager lobManager;

    //
    public CheckpointRunner checkpointRunner;

    //
    public static final int DATABASE_ONLINE       = 1;
    public static final int DATABASE_OPENING      = 2;
    public static final int DATABASE_CLOSING      = 3;
    public static final int DATABASE_SHUTDOWN     = 4;
    public static final int CLOSEMODE_IMMEDIATELY = 1;
    public static final int CLOSEMODE_NORMAL      = 2;
    public static final int CLOSEMODE_COMPACT     = 3;
    public static final int CLOSEMODE_SCRIPT      = 4;

    /**
     *  Constructs a new Database object.
     *
     * @param type is the type of the database: "mem:", "file:", "res:"
     * @param path is the given path to the database files
     * @param canonicalPath is the canonical path
     * @param props property overrides placed on the connect URL
     * @exception  HsqlException if the specified name and path
     *      combination is illegal or unavailable, or the database files the
     *      name and path resolves to are in use by another process
     */
    Database(String type, String path, String canonicalPath,
             HsqlProperties props) {

        setState(Database.DATABASE_SHUTDOWN);

        this.databaseType  = type;
        this.path          = path;
        this.canonicalPath = canonicalPath;
        this.urlProperties = props;

        if (databaseType == DatabaseURL.S_RES) {
            filesInJar    = true;
            filesReadOnly = true;
        }

        logger = new Logger(this);
        shutdownOnNoConnection =
            urlProperties.isPropertyTrue(HsqlDatabaseProperties.url_shutdown);
        lobManager = new LobManager(this);
    }

    /**
     * Opens this database.  The database should be opened after construction.
     */
    synchronized void open() {

        if (!isShutdown()) {
            return;
        }

        reopen();
    }

    /**
     * Opens this database.  The database should be opened after construction.
     * or reopened by the close(int closemode) method during a
     * "shutdown compact". Closes the log if there is an error.
     */
    void reopen() {

        boolean isNew = false;

        setState(DATABASE_OPENING);

        try {
            nameManager    = new HsqlNameManager(this);
            granteeManager = new GranteeManager(this);
            userManager    = new UserManager(this);
            schemaManager  = new SchemaManager(this);
            persistentStoreCollection =
                new PersistentStoreCollectionDatabase();
            isReferentialIntegrity = true;
            sessionManager         = new SessionManager(this);
            collation              = collation.getDatabaseInstance();
            dbInfo = DatabaseInformation.newDatabaseInformation(this);
            txManager              = new TransactionManager2PL(this);

            lobManager.createSchema();
            sessionManager.getSysLobSession().setSchema(
                SqlInvariants.LOBS_SCHEMA);
            schemaManager.setSchemaChangeTimestamp();
            schemaManager.createSystemTables();

            // completed metadata
            logger.openPersistence();

            isNew = logger.isNewDatabase;

            if (isNew) {
                String username = urlProperties.getProperty("user", "SA");
                String password = urlProperties.getProperty("password", "");

                userManager.createFirstUser(username, password);
                schemaManager.createPublicSchema();
                lobManager.initialiseLobSpace();
                logger.checkpoint(false);
            }

            lobManager.open();
            dbInfo.setWithContent(true);

            checkpointRunner = new CheckpointRunner();
        } catch (Throwable e) {
            logger.closePersistence(Database.CLOSEMODE_IMMEDIATELY);
            logger.releaseLock();
            setState(DATABASE_SHUTDOWN);
            clearStructures();
            DatabaseManager.removeDatabase(this);

            if (!(e instanceof HsqlException)) {
                e = Error.error(ErrorCode.GENERAL_ERROR, e);
            }

            logger.logSevereEvent("could not reopen database", e);

            throw (HsqlException) e;
        }

        setState(DATABASE_ONLINE);
    }

    /**
     * Clears the data structuress, making them elligible for garbage collection.
     */
    void clearStructures() {

        if (schemaManager != null) {
            schemaManager.clearStructures();
        }

        granteeManager   = null;
        userManager      = null;
        nameManager      = null;
        schemaManager    = null;
        sessionManager   = null;
        dbInfo           = null;
        checkpointRunner = null;
    }

    /**
     *  Returns the database ID.
     */
    public int getDatabaseID() {
        return this.databaseID;
    }

    /**
     * Returns a unique String identifier for the database.
     */
    public String getUniqueName() {
        return databaseUniqueName;
    }

    public void setUniqueName(String name) {
        databaseUniqueName = name;
    }

    /**
     *  Returns the type of the database: "mem", "file", "res"
     */
    public String getType() {
        return databaseType;
    }

    /**
     *  Returns the path of the database
     */
    public String getPath() {
        return path;
    }

    public HsqlName getCatalogName() {
        return nameManager.getCatalogName();
    }

    /**
     *  Returns the database properties.
     */
    public HsqlDatabaseProperties getProperties() {
        return databaseProperties;
    }

    /**
     * Returns the SessionManager for the database.
     */
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public boolean isReadOnly() {
        return databaseReadOnly;
    }

    /**
     *  Returns true if database has been shut down, false otherwise
     */
    boolean isShutdown() {
        return dbState == DATABASE_SHUTDOWN;
    }

    /**
     *  Constructs a new Session that operates within (is connected to) the
     *  context of this Database object. <p>
     *
     *  If successful, the new Session object initially operates on behalf of
     *  the user specified by the supplied user name.
     *
     * Throws if username or password is invalid.
     */
    synchronized Session connect(String username, String password,
                                 String zoneString, int timeZoneSeconds) {

        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
        }

        User user = userManager.getUser(username, password);
        Session session = sessionManager.newSession(this, user,
            databaseReadOnly, true, zoneString, timeZoneSeconds);

        return session;
    }

    /**
     *  Puts this Database object in global read-only mode. After
     *  this call, all existing and future sessions are limited to read-only
     *  transactions. Any following attempts to update the state of the
     *  database will result in throwing an HsqlException.
     */
    public void setReadOnly() {
        databaseReadOnly = true;
        filesReadOnly    = true;
    }

    /**
     * After this call all CACHED and TEXT tables will be set to read-only
     * mode. Changes to MEMORY tables will NOT
     * be stored or updated in the script file. This mode is intended for
     * use with read-only media where data should not be persisted.
     */
    public void setFilesReadOnly() {
        filesReadOnly = true;
    }

    /**
     * Is this in filesReadOnly mode?
     */
    public boolean isFilesReadOnly() {
        return filesReadOnly;
    }

    /**
     * Is this in filesInJar mode?
     */
    public boolean isFilesInJar() {
        return filesInJar;
    }

    /**
     *  Returns the UserManager for this Database.
     */
    public UserManager getUserManager() {
        return userManager;
    }

    /**
     *  Returns the GranteeManager for this Database.
     */
    public GranteeManager getGranteeManager() {
        return granteeManager;
    }

    /**
     *  Sets the isReferentialIntegrity attribute.
     */
    public void setReferentialIntegrity(boolean ref) {
        isReferentialIntegrity = ref;
    }

    /**
     *  Is referential integrity currently enforced?
     */
    public boolean isReferentialIntegrity() {
        return isReferentialIntegrity;
    }

    public int getResultMaxMemoryRows() {
        return resultMaxMemoryRows;
    }

    public void setResultMaxMemoryRows(int size) {
        resultMaxMemoryRows = size;
    }

    public void setStrictNames(boolean mode) {
        sqlEnforceNames = mode;
    }

    public void setStrictColumnSize(boolean mode) {
        sqlEnforceSize = mode;
    }

    public void setStrictReferences(boolean mode) {
        sqlEnforceRefs = mode;
    }

    public void setStrictTypes(boolean mode) {
        sqlEnforceTypes = mode;
    }

    public void setStrictTDCD(boolean mode) {
        sqlEnforceTDCD = mode;
    }

    public void setStrictTDCU(boolean mode) {
        sqlEnforceTDCU = mode;
    }

    public void setTranslateTTI(boolean mode) {
        sqlTranslateTTI = mode;
    }

    public void setNullsFirst(boolean mode) {
        sqlNullsFirst = mode;
    }

    public void setConcatNulls(boolean mode) {
        sqlConcatNulls = mode;
    }

    public void setUniqueNulls(boolean mode) {
        sqlUniqueNulls = mode;
    }

    public void setConvertTrunc(boolean mode) {
        sqlConvertTruncate = mode;
    }

    public void setDoubleNaN(boolean mode) {
        sqlDoubleNaN = mode;
    }

    public void setAvgScale(int scale) {
        sqlAvgScale = scale;
    }

    public void setLongVarIsLob(boolean mode) {
        sqlLongvarIsLob = mode;
    }

    public void setSyntaxDb2(boolean mode) {
        sqlSyntaxDb2 = mode;
    }

    public void setSyntaxMss(boolean mode) {
        sqlSyntaxMss = mode;
    }

    public void setSyntaxMys(boolean mode) {
        sqlSyntaxMys = mode;
    }

    public void setSyntaxOra(boolean mode) {
        sqlSyntaxOra = mode;
    }

    public void setSyntaxPgs(boolean mode) {
        sqlSyntaxPgs = mode;
    }

    /**
     *  Called by the garbage collector on this Databases object when garbage
     *  collection determines that there are no more references to it.
     */
    protected void finalize() {

        if (getState() != DATABASE_ONLINE) {
            return;
        }

        try {
            close(CLOSEMODE_IMMEDIATELY);
        } catch (HsqlException e) {    // it's too late now
        }
    }

    void closeIfLast() {

        if (shutdownOnNoConnection && sessionManager.isEmpty()
                && dbState == this.DATABASE_ONLINE) {
            try {
                close(CLOSEMODE_NORMAL);
            } catch (HsqlException e) {}
        }
    }

    /**
     *  Closes this Database using the specified mode. <p>
     *
     * <ol>
     *  <LI> closemode -1 performs SHUTDOWN IMMEDIATELY, equivalent
     *       to a poweroff or crash.
     *
     *  <LI> closemode 0 performs a normal SHUTDOWN that
     *      checkpoints the database normally.
     *
     *  <LI> closemode 1 performs a shutdown compact that scripts
     *       out the contents of any CACHED tables to the log then
     *       deletes the existing *.data file that contains the data
     *       for all CACHED table before the normal checkpoint process
     *       which in turn creates a new, compact *.data file.
     * </ol>
     */
    public void close(int closemode) {

        HsqlException he = null;

        // multiple simultaneous close
        synchronized (this) {
            if (getState() != DATABASE_ONLINE) {
                return;
            }

            setState(DATABASE_CLOSING);
        }

        sessionManager.closeAllSessions();

        if (filesReadOnly) {
            closemode = CLOSEMODE_IMMEDIATELY;
        }

        /**
         * @todo  fredt - impact of possible error conditions in closing the log
         * should be investigated for the CLOSEMODE_COMPACT mode
         */
        logger.closePersistence(closemode);
        lobManager.close();
        sessionManager.close();

        try {
            if (closemode == CLOSEMODE_COMPACT) {
                clearStructures();
                reopen();
                setState(DATABASE_CLOSING);
                logger.closePersistence(CLOSEMODE_NORMAL);
                lobManager.close();
            }
        } catch (Throwable t) {
            if (t instanceof HsqlException) {
                he = (HsqlException) t;
            } else {
                he = Error.error(ErrorCode.GENERAL_ERROR, t);
            }
        }

        checkpointRunner.stop();
        logger.releaseLock();
        setState(DATABASE_SHUTDOWN);
        clearStructures();

        // fredt - this could change to avoid removing a db from the
        // DatabaseManager repository if there are pending getDatabase()
        // calls
        DatabaseManager.removeDatabase(this);

        // todo - when hsqldb.sql. logging is supported, add another call
        FrameworkLogger.clearLoggers("hsqldb.db." + getUniqueName());

        if (he != null) {
            throw he;
        }
    }

    private void setState(int state) {
        dbState = state;
    }

    int getState() {
        return dbState;
    }

    String getStateString() {

        int state = getState();

        switch (state) {

            case DATABASE_CLOSING :
                return "DATABASE_CLOSING";

            case DATABASE_ONLINE :
                return "DATABASE_ONLINE";

            case DATABASE_OPENING :
                return "DATABASE_OPENING";

            case DATABASE_SHUTDOWN :
                return "DATABASE_SHUTDOWN";

            default :
                return "UNKNOWN";
        }
    }

    public String[] getSettingsSQL() {

        HsqlArrayList list = new HsqlArrayList();

        if (!getCatalogName().name.equals(
                HsqlNameManager.DEFAULT_CATALOG_NAME)) {
            String name = getCatalogName().statementName;

            list.add("ALTER CATALOG PUBLIC RENAME TO " + name);
        }

        if (!collation.isDefaultCollation()) {
            String name = collation.getName().statementName;

            list.add("SET DATABASE COLLATION " + name);
        }

        HashMappedList lobTables =
            schemaManager.getTables(SqlInvariants.LOBS_SCHEMA);

        for (int i = 0; i < lobTables.size(); i++) {
            Table table = (Table) lobTables.get(i);

            if (table.isCached()) {
                StringBuffer sb = new StringBuffer();

                sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE);
                sb.append(' ');
                sb.append(table.getName().getSchemaQualifiedStatementName());
                sb.append(' ').append(Tokens.T_TYPE).append(' ');
                sb.append(Tokens.T_CACHED);
                list.add(sb.toString());
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    /**
     * Returns the schema and authorisation statements for the database.
     */
    public Result getScript(boolean indexRoots) {

        Result r = Result.newSingleColumnResult("COMMAND");

        // properties
        String[] list = logger.getPropertiesSQL();

        addRows(r, list);

        list = getSettingsSQL();

        addRows(r, list);

        list = getGranteeManager().getSQL();

        addRows(r, list);

        // schemas and schema objects such as tables, sequences, etc.
        list = schemaManager.getSQLArray();

        addRows(r, list);

        // optional comments on tables etc.
        list = schemaManager.getCommentsArray();

        addRows(r, list);

        // index roots
        if (indexRoots) {
            list = schemaManager.getIndexRootsSQL();

            addRows(r, list);
        }

        // text headers - readonly - clustered
        list = schemaManager.getTablePropsSQL(!indexRoots);

        addRows(r, list);

        // password complexity
        list = getUserManager().getAuthenticationSQL();

        addRows(r, list);

        // user session start schema names
        list = getUserManager().getInitialSchemaSQL();

        addRows(r, list);

        // grantee rights
        list = getGranteeManager().getRightstSQL();

        addRows(r, list);

        return r;
    }

    private static void addRows(Result r, String[] sql) {

        if (sql == null) {
            return;
        }

        for (int i = 0; i < sql.length; i++) {
            String[] s = new String[1];

            s[0] = sql[i];

            r.initialiseNavigator().add(s);
        }
    }

    public String getURI() {
        return databaseType + canonicalPath;
    }

    public String getCanonicalPath() {
        return canonicalPath;
    }

    public HsqlProperties getURLProperties() {
        return urlProperties;
    }

    class CheckpointRunner implements Runnable {

        private volatile boolean waiting;
        private Object           timerTask;

        public void run() {

            try {
                Session sysSession = sessionManager.newSysSession();
                Statement checkpoint =
                    ParserCommand.getAutoCheckpointStatement(Database.this);

                sysSession.executeCompiledStatement(
                    checkpoint, ValuePool.emptyObjectArray);
                sysSession.close();

                waiting = false;
            } catch (Exception e) {

                // ignore exceptions
                // may be InterruptedException or IOException
            }
        }

        public void start() {

            if (!logger.isLogged()) {
                return;
            }

            synchronized (this) {
                if (waiting) {
                    return;
                }

                waiting = true;
            }

            timerTask = DatabaseManager.getTimer().scheduleAfter(0, this);
        }

        public void stop() {

            HsqlTimer.cancel(timerTask);

            timerTask = null;
            waiting   = false;
        }
    }
}
