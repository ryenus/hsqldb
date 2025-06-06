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

import java.util.TimeZone;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Session.TimeoutManager;
import org.hsqldb.dbinfo.DatabaseInformation;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.map.ValuePool;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LobManager;
import org.hsqldb.persist.Logger;
import org.hsqldb.persist.PersistentStoreCollectionDatabase;
import org.hsqldb.result.Result;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.User;
import org.hsqldb.rights.UserManager;
import org.hsqldb.types.Collation;

// incorporates following contributions
// campbell-burnet@users - javadoc comments

/**
 * Database is the root class for HSQL Database Engine database. <p>
 *
 * It holds the data structures that form an HSQLDB database instance.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class Database {

    int                        databaseID;
    HsqlName                   databaseUniqueName;
    DatabaseType               databaseType;
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
    public int                    sqlAvgScale            = 0;
    public boolean                sqlRestrictExec        = false;
    public boolean                sqlCharLiteral         = true;
    public boolean                sqlConcatNulls         = true;
    public boolean                sqlConvertTruncate     = true;
    public boolean                sqlDoubleNaN           = true;
    public boolean                sqlEnforceTypes        = false;
    public boolean                sqlEnforceRefs         = false;
    public boolean                sqlEnforceSize         = true;
    public boolean                sqlEnforceNames        = false;
    public boolean                sqlEnforceTDCD         = true;
    public boolean                sqlEnforceTDCU         = true;
    public boolean                sqlIgnoreCase          = false;
    public boolean                sqlLiveObject          = false;
    public boolean                sqlLongvarIsLob        = false;
    public boolean                sqlLowerCaseIdentifier = false;
    public boolean                sqlNullsFirst          = true;
    public boolean                sqlNullsOrder          = true;
    public int                    sqlMaxRecursive        = 256;
    public boolean                sqlRegularNames        = true;
    public boolean                sqlSyntaxDb2           = false;
    public boolean                sqlSyntaxMss           = false;
    public boolean                sqlSyntaxMys           = false;
    public boolean                sqlSyntaxOra           = false;
    public boolean                sqlSyntaxPgs           = false;
    public boolean                sqlSysIndexNames       = false;
    public boolean                sqlTranslateTTI        = true;
    public boolean                sqlTruncateTrailing    = true;
    public boolean                sqlUniqueNulls         = true;
    public int                    recoveryMode           = 0;
    private boolean               isReferentialIntegrity = true;
    public HsqlDatabaseProperties databaseProperties;
    private final boolean         shutdownOnNoConnection;
    int                           resultMaxMemoryRows;

    // schema invariant objects
    public UserManager     userManager;
    public GranteeManager  granteeManager;
    public HsqlNameManager nameManager;

    // session related objects
    public SessionManager     sessionManager;
    public TransactionManager txManager;
    public int defaultIsolationLevel = SessionInterface.TX_READ_COMMITTED;
    public boolean            txConflictRollback  = true;
    public boolean            txInterruptRollback = false;

    // schema objects
    public SchemaManager schemaManager;

    //
    public PersistentStoreCollectionDatabase persistentStoreCollection;

    //
    public LobManager lobManager;

    //
    public CheckpointRunner checkpointRunner;
    public TimeoutRunner    timeoutRunner;

    //
    Result updateZeroResult = Result.updateZeroResult;

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
     * @exception org.hsqldb.error.HsqlException if the specified name and path
     *      combination is illegal or unavailable, or the database files the
     *      name and path resolves to are in use by another process
     */
    Database(
            DatabaseType type,
            String path,
            String canonicalPath,
            HsqlProperties props) {

        setState(Database.DATABASE_SHUTDOWN);

        this.databaseType  = type;
        this.path          = path;
        this.canonicalPath = canonicalPath;
        this.urlProperties = props;

        if (databaseType == DatabaseType.DB_RES) {
            filesInJar    = true;
            filesReadOnly = true;
        }

        logger = new Logger(this);
        shutdownOnNoConnection = urlProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_shutdown);
        recoveryMode = urlProperties.getIntegerProperty(
            HsqlDatabaseProperties.url_recover,
            0);
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

        boolean isNew;

        setState(DATABASE_OPENING);

        try {
            createObjectStructures();

            // completed metadata
            logger.open();

            isNew = logger.isNewDatabase;

            if (isNew) {
                String username = urlProperties.getProperty("user", "SA");
                String password = urlProperties.getProperty("password", "");

                userManager.createFirstUser(username, password);
                schemaManager.createPublicSchema();
                logger.checkpoint(null, false, false);
            }

            lobManager.open();
            dbInfo.setWithContent(true);

            checkpointRunner = new CheckpointRunner();
            timeoutRunner    = new TimeoutRunner();
        } catch (Throwable e) {
            logger.logSevereEvent("could not reopen database", e);
            logger.close(Database.CLOSEMODE_IMMEDIATELY);
            logger.releaseLock();
            setState(DATABASE_SHUTDOWN);
            clearStructures();
            DatabaseManager.removeDatabase(this);

            if (e instanceof HsqlException) {
                throw(e);
            } else {
                throw(Error.error(ErrorCode.GENERAL_ERROR, e));
            }
        }

        setState(DATABASE_ONLINE);
    }

    /**
     * Clears the data structures, making them elligible for garbage collection.
     */
    void clearStructures() {

        if (schemaManager != null) {
            schemaManager.release();
        }

        if (checkpointRunner != null) {
            checkpointRunner.stop();
        }

        if (timeoutRunner != null) {
            timeoutRunner.stop();
        }

        lobManager       = null;
        granteeManager   = null;
        userManager      = null;
        nameManager      = null;
        schemaManager    = null;
        sessionManager   = null;
        dbInfo           = null;
        checkpointRunner = null;
        timeoutRunner    = null;
    }

    public void createObjectStructures() {

        nameManager               = new HsqlNameManager(this);
        databaseUniqueName = nameManager.newHsqlName(
            "",
            false,
            SchemaObject.DATABASE);
        lobManager                = new LobManager(this);
        granteeManager            = new GranteeManager(this);
        userManager               = new UserManager(this);
        schemaManager             = new SchemaManager(this);
        persistentStoreCollection = new PersistentStoreCollectionDatabase(this);
        isReferentialIntegrity    = true;
        sessionManager            = new SessionManager(this);
        collation                 = Collation.newDatabaseInstance();
        dbInfo = DatabaseInformation.newDatabaseInformation(this);
        txManager                 = new TransactionManager2PL(this);

        lobManager.createSchema();
        sessionManager.getSysLobSession().setSchema(SqlInvariants.LOBS_SCHEMA);
        schemaManager.setSchemaChangeTimestamp();
        schemaManager.createSystemTables();
    }

    /**
     *  Returns the database ID.
     */
    public int getDatabaseID() {
        return this.databaseID;
    }

    public HsqlName getName() {
        return databaseUniqueName;
    }

    /**
     * Returns a unique String identifier for the database.
     */
    public String getNameString() {
        return databaseUniqueName.name;
    }

    public void setDatabaseName(String name) {
        databaseUniqueName.rename(name, false);
    }

    /**
     *  Returns the type of the database: "mem", "file", "res"
     */
    public DatabaseType getType() {
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
    synchronized Session connect(
            String username,
            String password,
            TimeZone zone) {

        if (getState() != DATABASE_ONLINE) {
            throw Error.error(ErrorCode.X_08001);
        }

        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
        }

        User user = userManager.getUser(username, password);
        Session session = sessionManager.newSession(
            this,
            user,
            databaseReadOnly,
            true,
            zone);

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

    public void setLiveObject(boolean mode) {
        sqlLiveObject = mode;
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

    public void setRestrictExec(boolean mode) {
        sqlRestrictExec = mode;
    }

    public void setStrictNames(boolean mode) {
        sqlEnforceNames = mode;
    }

    public void setRegularNames(boolean mode) {
        sqlRegularNames = mode;

        nameManager.setSqlRegularNames(mode);
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

    public void setNullsOrder(boolean mode) {
        sqlNullsOrder = mode;
    }

    public void setCharacterLiteral(boolean mode) {
        sqlCharLiteral = mode;
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

    public void setTruncateTrailing(boolean mode) {
        sqlTruncateTrailing = mode;
    }

    public void setDoubleNaN(boolean mode) {
        sqlDoubleNaN = mode;
    }

    public void setAvgScale(int scale) {
        sqlAvgScale = scale;
    }

    public void setMaxRecursive(int value) {
        sqlMaxRecursive = value;
    }

    public void setLongVarIsLob(boolean mode) {
        sqlLongvarIsLob = mode;
    }

    public void setIgnoreCase(boolean mode) {
        sqlIgnoreCase = mode;
    }

    public void setSysIndexNames(boolean mode) {
        sqlSysIndexNames = mode;
    }

    public void setLowerCaseIndentifer(boolean mode) {
        this.sqlLowerCaseIdentifier = mode;
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

    void closeIfLast() {

        if (sessionManager.isEmpty() && dbState == DATABASE_ONLINE) {
            if (shutdownOnNoConnection) {
                try {
                    close(CLOSEMODE_NORMAL);
                } catch (HsqlException e) {}
            } else {
                logger.synchLog();
            }
        }
    }

    /**
     *  Closes this Database using the specified mode.
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

        /*
         * impact of possible error conditions in closing the log
         * for the CLOSEMODE_COMPACT mode
         */
        boolean result = logger.close(closemode);

        lobManager.close();
        sessionManager.close();

        try {
            if (result && closemode == CLOSEMODE_COMPACT) {
                clearStructures();
                reopen();
                txManager.setSystemChangeNumber(
                    txManager.getSystemChangeNumber() + 1);
                setState(DATABASE_CLOSING);
                sessionManager.closeAllSessions();
                logger.close(CLOSEMODE_NORMAL);
                lobManager.close();
                sessionManager.close();
            }
        } catch (Throwable t) {
            if (t instanceof HsqlException) {
                he = (HsqlException) t;
            } else {
                he = Error.error(ErrorCode.GENERAL_ERROR, t);
            }
        }

        logger.releaseLock();
        setState(DATABASE_SHUTDOWN);
        clearStructures();

        // fredt - this could change to avoid removing a db from the
        // DatabaseManager repository if there are pending getDatabase()
        // calls
        DatabaseManager.removeDatabase(this);

        // todo - when hsqldb.sql. framework logging is supported, add another call
        FrameworkLogger.clearLoggers("hsqldb.db." + getNameString());

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

    public List<String> getSettingsSQLArray() {

        HsqlArrayList<String> list = new HsqlArrayList<>();
        StringBuilder         sb   = new StringBuilder();

        if (!getCatalogName().name.equals(SqlInvariants.DEFAULT_CATALOG_NAME)) {
            String name = getCatalogName().statementName;

            sb.append("ALTER CATALOG PUBLIC RENAME TO ").append(name);
            list.add(sb.toString());
            sb.setLength(0);
        }

        list.add(collation.getDatabaseCollationSQL());

        OrderedHashMap<String, Table> lobTables = schemaManager.getTables(
            SqlInvariants.LOBS_SCHEMA);

        for (int i = 0; i < lobTables.size(); i++) {
            Table table = lobTables.get(i);

            if (table.isCached()) {
                sb.append(Tokens.T_SET)
                  .append(' ')
                  .append(Tokens.T_TABLE)
                  .append(' ')
                  .append(table.getName().getSchemaQualifiedStatementName())
                  .append(' ')
                  .append(Tokens.T_TYPE)
                  .append(' ')
                  .append(Tokens.T_CACHED);
                list.add(sb.toString());
                sb.setLength(0);
            }
        }

        return list;
    }

    /**
     * Returns the schema and authorisation statements for the database.
     */
    public Result getScript(boolean indexRoots) {

        Result r = Result.newSingleColumnResult("COMMAND");

        // properties
        List<String> list = logger.getPropertiesSQLArray(indexRoots);

        r.addRows(list);

        list = getSettingsSQLArray();

        r.addRows(list);

        list = granteeManager.getSQLArray();

        r.addRows(list);

        // schemas and schema objects such as tables, sequences, etc.
        list = schemaManager.getSQLArray();

        r.addRows(list);

        // table spaces
        list = schemaManager.getTableSpaceSQLArray();

        r.addRows(list);

        // index roots
        if (indexRoots) {
            list = schemaManager.getIndexRootsSQLArray();

            r.addRows(list);
        }

        // text headers - readonly - clustered
        list = schemaManager.getTablePropsSQLArray(!indexRoots);

        r.addRows(list);

        // password complexity
        list = userManager.getAuthenticationSQLArray();

        r.addRows(list);

        // user session start schema names
        list = userManager.getInitialSchemaSQLArray();

        r.addRows(list);

        // grantee rights
        list = granteeManager.getRightsSQLArray();

        r.addRows(list);

        return r;
    }

    public String getURI() {
        return databaseType.value() + canonicalPath;
    }

    public String getCanonicalPath() {
        return canonicalPath;
    }

    public HsqlProperties getURLProperties() {
        return urlProperties;
    }

    public TimeoutRunner getTimeoutRunner() {
        return timeoutRunner;
    }

    class CheckpointRunner implements Runnable {

        private volatile boolean waiting;
        private Object           timerTask;

        public void run() {

            Statement checkpoint = ParserCommand.getAutoCheckpointStatement(
                Database.this);
            Session sysSession = sessionManager.newSysSession();

            try {
                sysSession.executeCompiledStatement(
                    checkpoint,
                    ValuePool.emptyObjectArray,
                    0);
            } catch (Throwable e) {

                // ignore exceptions
                // may be InterruptedException or IOException
            } finally {
                try {
                    sysSession.commit(false);
                    sysSession.close();
                } catch (Throwable t) {}

                waiting = false;
            }
        }

        public void start() {

            // started only when maxLogSize is reached in file: or when deleted lobs exceed maxLogSize in mem:
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

    /**
     * Sessions running a statement with timeout are added to the list. The first
     * session starts the runner. Sessions are not removed until closed.<p>
     *
     * The runner is called at second intervals. It handles the countdown for
     * each session currently running a statement with timeout. If timeout
     * is reached, the runner aborts the statement.
     */
    static class TimeoutRunner implements Runnable {

        private Object                        timerTask;
        private HsqlArrayList<TimeoutManager> timeoutList;
        int                                   abortCount;

        public void run() {

            try {
                synchronized (this) {
                    long systemMillis = System.currentTimeMillis();

                    for (int i = 0; i < timeoutList.size(); i++) {
                        TimeoutManager timeOut = timeoutList.get(i);

                        if (timeOut.isClosed()) {
                            timeoutList.remove(i);

                            i--;
                            continue;
                        }

                        boolean result = timeOut.checkTimeout(systemMillis);

                        if (result) {
                            abortCount++;
                        }
                    }
                }
            } catch (Throwable e) {

                // ignore exceptions
                // may be InterruptedException or IOException
            }
        }

        public void stop() {

            synchronized (this) {
                if (timerTask == null) {
                    return;
                }

                HsqlTimer.cancel(timerTask);

                timerTask   = null;
                timeoutList = null;
            }
        }

        public void addSession(TimeoutManager timeout) {

            synchronized (this) {
                if (timerTask == null) {
                    start();
                }

                timeoutList.add(timeout);
            }
        }

        private void start() {

            timeoutList = new HsqlArrayList<>();
            timerTask = DatabaseManager.getTimer()
                                       .schedulePeriodicallyAfter(
                                           1000,
                                           1000,
                                           this,
                                           true);
        }
    }
}
