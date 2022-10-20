/* Copyright (c) 2001-2022, The HSQL Development Group
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
package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseType;
import org.hsqldb.DatabaseURL;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LockFile;
import org.hsqldb.persist.Logger;

/**
 * Provides support for deleting by database URL the files that compose an
 * HSQLDB database.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7
 * @since 2.0.1
 */
@SuppressWarnings("FinalClass")
public final class HsqldbEmbeddedDatabaseDeleter {

    private static final java.util.logging.Logger LOG
            = java.util.logging.Logger.getLogger(
                    HsqldbEmbeddedDatabaseDeleter.class.getName());
    private static final String[] CORE_FILE_NAME_EXENTION = new String[]{
        Logger.backupFileExtension,
        Logger.dataFileExtension,
        Logger.logFileExtension,
        Logger.lobsFileExtension,
        Logger.scriptFileExtension,
        Logger.propertiesFileExtension};
    private static final String[] INFO_LOG_FILE_NAME_EXTENSION = new String[]{
        Logger.appLogFileExtension,
        Logger.sqlLogFileExtension
    };

    private static final String FILE_SCHEME = DatabaseURL.S_FILE;
    private static final String JDBC_URL_PREFIX = DatabaseURL.S_URL_PREFIX;
    private static final String CONNECTION_TYPE_KEY = DatabaseURL.url_connection_type;
    private static final String DATABASE_PATH_KEY = DatabaseURL.url_database;
    private static final String NEW_FILE_NAME_EXT = Logger.newFileExtension;
    private static final String OLD_FILE_NAME_EXT = Logger.oldFileExtension;

    /**
     * Attempts to delete all all existing database files and directories
     * denoted by the given {@code databaseUrl}.
     *
     * @param databaseUrl should be valid as per
     *                    {@link DatabaseURL#parseURL(String, boolean, boolean)}
     * @return {@code true} if the {@code databaseUrl} is valid, the connection
     *         scheme is for {@link DatabaseType#DB_FILE} and existing database
     *         files and directories denoted by the {@code databaseUrl} are
     *         successfully deleted; else {@code false}.
     */
    @SuppressWarnings({"UseSpecificCatch", "BroadCatchBlock", "TooBroadCatch"})
    public static boolean deleteDatabase(final String databaseUrl) {
        if (databaseUrl == null) {
            LOG.log(Level.SEVERE, "null database url provided.");
            return false;
        } else if (databaseUrl.isEmpty()) {
            LOG.log(Level.SEVERE, "empty database url provided.");
            return false;
        } else if (!databaseUrl.equals(databaseUrl.trim())) {
            LOG.log(Level.SEVERE, "leading or trailing whitespace database url"
                    + " provided.");
            return false;
        }
        final boolean hasPrefix = databaseUrl.startsWith(JDBC_URL_PREFIX);
        final boolean noPath = false;
        final HsqlProperties info = DatabaseURL.parseURL(databaseUrl, hasPrefix,
                noPath);
        final String connectionType = (info == null)
                ? null
                : info.getProperty(CONNECTION_TYPE_KEY);
        if (!FILE_SCHEME.equals(connectionType)) {
            LOG.log(Level.SEVERE,
                    "Deletion not supported for database type: {0}",
                    connectionType);
            return false;
        }
        final String databasePath = info == null
                ? null
                : info.getProperty(DATABASE_PATH_KEY);
        if (databasePath == null) {
            LOG.log(Level.SEVERE,
                    "Malformed database url provided: {0}",
                    databaseUrl);
            return false;
        }
        final File databaseFile = toCanonicalOrAbsoluteFile(databasePath);
        final Database database = DatabaseManager.lookupDatabaseObject(DatabaseType.DB_FILE, databasePath);
        if (database != null) {
            LOG.log(Level.SEVERE, "Database is still registered: file:{0}", databaseFile);
            return false;
        }
        boolean success = true;
        LockFile lockFile = null;
        try {
            lockFile = LockFile.newLockFileLock(databasePath);
            for (int i = 0; i < CORE_FILE_NAME_EXENTION.length; i++) {
                success &= deleteFile(databasePath
                        + CORE_FILE_NAME_EXENTION[i] + NEW_FILE_NAME_EXT);
                success &= deleteFile(databasePath
                        + CORE_FILE_NAME_EXENTION[i] + OLD_FILE_NAME_EXT);
                success &= deleteFile(databasePath
                        + CORE_FILE_NAME_EXENTION[i]);
            }
            for (int i = 0; i < INFO_LOG_FILE_NAME_EXTENSION.length; i++) {
                success &= deleteFile(databasePath + INFO_LOG_FILE_NAME_EXTENSION[i]);
            }
            final File dbTempFile = toCanonicalOrAbsoluteFile(databasePath + ".tmp");
            success &= deleteTree(dbTempFile);
        } catch (Throwable ex) {
            success = false;
            String message = String.format("LockFile.newLockFileLock(%s)",
                    databaseFile);
            LOG.log(Level.SEVERE, message, ex);
        } finally {
            if (lockFile != null) {
                try {
                    lockFile.tryRelease();
                    success &= deleteFile(databasePath + ".lck");
                } catch (Throwable ex) {
                    success = false;
                    LOG.log(Level.SEVERE, "lockFile.tryRelease()", ex);
                }
            }
        }

        if (databasePath.endsWith(File.separator)) {
            success &= deleteTree(new File(databasePath));
        }
        return success;
    }

    private static boolean deleteFile(final String filePath) {
        assert filePath != null : "String paramter filePath must not be null.";
        return deleteFile(toCanonicalOrAbsoluteFile(filePath));
    }

    private static boolean deleteFile(final File f) {
        assert f != null : "file paramter f must not be null.";
        final File file = toCanonicalOrAbsoluteFile(f);
        final Path path = Paths.get(file.getPath());
        boolean exists = false;
        boolean isFile = false;
        boolean isDirectory = false;
        boolean deleted = false;
        boolean error = true;
        try {
            exists = Files.exists(path);
            isFile = Files.isRegularFile(path);
            if (isFile) {
                deleted = Files.deleteIfExists(path);
            } else {
                isDirectory = Files.isDirectory(path);
            }
            error = false;
            if (exists) {
                if (deleted) {
                    LOG.log(Level.INFO, "Deleted: {0}", file);
                } else if (isFile) {
                    LOG.log(Level.WARNING, "Not deleted: {0}", file);
                } else if (isDirectory) {
                    LOG.log(Level.WARNING, "Is directory: {0}", file);
                } else {
                    LOG.log(Level.WARNING,
                            "Not a regular file or directory: {0} ", file);
                }
            }
        } catch (NullPointerException npe) {
            LOG.log(Level.SEVERE, "null file parameter", npe);
        } catch (SecurityException se) {
            LOG.log(Level.SEVERE, "Security restriction", se);
        } catch (DirectoryNotEmptyException dne) {
            LOG.log(Level.SEVERE, file + " delete aborted.", dne);
        } catch (IOException ioe) {
            LOG.log(Level.SEVERE, file + " delete aborted.", ioe);
        }

        return !error & (deleted || !exists || !isFile);
    }

    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    private static boolean deleteTree(final File file) {
        assert file != null : "File paramter must not be null.";
        final File root = toCanonicalOrAbsoluteFile(file);
        final boolean exists = root.exists();
        if (!exists) {
            LOG.log(Level.INFO, "Does not exist: {0}", root);
            return true;
        }
        boolean isDirectory = root.isDirectory();
        if (!isDirectory) {
            LOG.log(Level.SEVERE, "Not a directory: {0}", root);
            return false;
        }
        boolean success = true;
        try {
            final File[] entries = root.listFiles();
            int count = entries == null ? -1 : entries.length;
            for (int i = 0; entries != null && i < entries.length; i++) {
                final File entry = entries[i];
                if (entry.isDirectory()) {
                    success &= deleteTree(entry);
                    if (success) {
                        LOG.log(Level.INFO, "Deleted directory {0}", root);
                        count--;
                    }
                } else if (entry.isFile()) {
                    success &= deleteFile(entry);
                    if (success) {
                        count--;
                    }
                }
            }
            success &= root.delete();
            if (success) {
                LOG.log(Level.INFO, "Deleted directory {0}", root);
            } else {
                if (count > 0) {
                    LOG.log(Level.WARNING, "Directory not empty {0}", root);
                } else {
                    LOG.log(Level.WARNING, "Directory empty but not deleted: {0}", root);
                }

            }
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Delete tree failed for: " + root, t);
            success = false;
        }

        return success;
    }

    public static void main(final String[] args) {
        final String dbPath = String.format("test%sdb%s", File.separator,
                File.separator);
        final String dbURI = "file:" + dbPath;
        final HsqlProperties props = new HsqlProperties();
        final Database database = DatabaseManager.getDatabase(
                DatabaseURL.S_FILE, dbPath, props);
        // will fail as the database is open
        HsqldbEmbeddedDatabaseDeleter.deleteDatabase(dbURI);
        // close and wait
        if (database != null) {
            database.close(Database.CLOSEMODE_NORMAL);
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        // try again; should succeed
        HsqldbEmbeddedDatabaseDeleter.deleteDatabase(dbURI);
    }

    private static File toCanonicalOrAbsoluteFile(String filePath) {
        assert filePath != null : "filePath paramter must not be null.";
        return toCanonicalOrAbsoluteFile(new File(filePath));

    }

    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    private static File toCanonicalOrAbsoluteFile(File file) {
        try {
            return file.getCanonicalFile();
        } catch (Throwable t) {
            return file.getAbsoluteFile();
        }
    }

    private HsqldbEmbeddedDatabaseDeleter() {
        throw new AssertionError("Pure utility class.");
    }

}
