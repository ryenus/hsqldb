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
package org.hsqldb.testbase;

import java.io.File;
import java.text.MessageFormat;
import org.hsqldb.DatabaseURL;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LockFile;
import org.hsqldb.persist.Logger;

/**
 * Provides support for deleting by database URL the files that compose an
 * HSQLDB database.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.2.5
 * @since 2.0.1
 */
public final class HsqldbEmbeddedDatabaseDeleter implements ConnectionFactory.ConnectionFactoryEventListener {

    //<editor-fold defaultstate="collapsed" desc="Logging Support">
    private static final java.util.logging.Level INFO_LEVEL = java.util.logging.Level.INFO;
    private static final java.util.logging.Level SEVERE_LEVEL = java.util.logging.Level.SEVERE;
    private static final java.util.logging.Logger s_logger = java.util.logging.Logger.getLogger(
            HsqldbEmbeddedDatabaseDeleter.class.getName());
    //</editor-fold>
    //
    //<editor-fold defaultstate="collapsed" desc="Constants">
    private static final String[] s_coreFileExt = new String[]{
        org.hsqldb.persist.Logger.backupFileExtension,
        org.hsqldb.persist.Logger.dataFileExtension,
        org.hsqldb.persist.Logger.dataFileExtension + ".tmp",
        org.hsqldb.persist.Logger.lobsFileExtension,
        org.hsqldb.persist.Logger.scriptFileExtension,
        org.hsqldb.persist.Logger.propertiesFileExtension,};
    private static final String[] s_infoLogExt = new String[]{
        org.hsqldb.persist.Logger.appLogFileExtension,
        org.hsqldb.persist.Logger.sqlLogFileExtension
    };

    private static final String s_fileScheme = DatabaseURL.S_FILE;
    private static final String s_jdbcUrlPrefix = DatabaseURL.S_URL_PREFIX;
    private static final String s_connectionTypePropertyKey = DatabaseURL.url_connection_type;
    private static final String s_databasePathPropertyKey = DatabaseURL.url_database;
    private static final String s_newFileExt = Logger.newFileExtension;
    private static final String s_oldFileExt = Logger.oldFileExtension;
    //</editor-fold>
    //
    //<editor-fold defaultstate="collapsed" desc="Fields">
    private final String m_dataseUrl;
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Constructor">
    public HsqldbEmbeddedDatabaseDeleter(final String databaseUrl) {
        m_dataseUrl = databaseUrl;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Methods">
    //<editor-fold defaultstate="collapsed" desc="closedRegisteredObjects(ConnectionFactory)">
    @Override
    public void finishedClosingRegisteredObjects(final ConnectionFactory source) {
        final boolean success = deleteDatabase(m_dataseUrl);

        if (success) {
            s_logger.log(INFO_LEVEL, "Database deletion succeeded for: {0}", m_dataseUrl);
        } else {
            s_logger.log(SEVERE_LEVEL, "Database deletion failed for: {0}", m_dataseUrl);
        }
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="deleteDatabase(String)">
    public static boolean deleteDatabase(final String databaseUrl) {
        if (databaseUrl == null) {
            s_logger.log(SEVERE_LEVEL, "null database url provided");

            return false;
        }

        final boolean hasPrefix = databaseUrl.trim().toLowerCase().startsWith(s_jdbcUrlPrefix);
        final HsqlProperties info = DatabaseURL.parseURL(databaseUrl, hasPrefix, false);
        final String connectionType = (info == null)
                ? null
                : info.getProperty(s_connectionTypePropertyKey);

        if (!s_fileScheme.equals(connectionType)) {
            s_logger.log(SEVERE_LEVEL, "Deletion not supported for database type: {0}", connectionType);

            return false;
        }

        final String databasePath = info.getProperty(s_databasePathPropertyKey);

        if (databasePath == null) {
            s_logger.log(SEVERE_LEVEL, "Malformed database url provided: {0}", databaseUrl);
            return false;
        }

        boolean success = true;
        LockFile lockFile = null;

        try {
            lockFile = LockFile.newLockFileLock(databasePath);

            for (int i = 0; i < s_coreFileExt.length; i++) {
                success &= deleteFile(databasePath + s_coreFileExt[i] + s_newFileExt);
                success &= deleteFile(databasePath + s_coreFileExt[i] + s_oldFileExt);
                success &= deleteFile(databasePath + s_coreFileExt[i]);
            }

            for (int i = 0; i < s_infoLogExt.length; i++) {
                success &= deleteFile(databasePath + s_infoLogExt[i]);
            }

            success &= deleteTree(new File(databasePath + ".tmp"));
        } catch (Exception ex) {
            success = false;
            String message = MessageFormat.format("LockFile.newLockFileLock({0})", databasePath);
            s_logger.log(SEVERE_LEVEL, message, ex);
        } finally {
            try {
                if (lockFile != null) {
                    lockFile.tryRelease();

                    success &= deleteFile(databasePath + ".lck");
                }
            } catch (Exception ex) {
                success = false;
                s_logger.log(SEVERE_LEVEL, "lockFile.tryRelease()", ex);
            }


        }

        return success;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="deleteFile(String file)">
    private static boolean deleteFile(final String file) {
        boolean exists = false;
        boolean deleted = false;
        boolean error = true;

        try {
            final File f = new File(file);

            exists = f.exists();

            if (exists) {
                deleted = f.delete();
            }

            error = false;
        } catch(NullPointerException npe) {
            s_logger.log(SEVERE_LEVEL, "null file parameter", npe);
        } catch (SecurityException se) {
            s_logger.log(SEVERE_LEVEL, "Security restriction", se);
        } catch (Exception e) {
            s_logger.log(SEVERE_LEVEL, file + " delete aborted.", e);
        }

        if (deleted) {
            s_logger.log(INFO_LEVEL, "{0} deleted.", file);
        } else if (!exists) {
            s_logger.log(INFO_LEVEL, "{0} did not exist.", file);
        }


        return !error & (deleted || !exists);
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="deleteTree(File)">
    private static boolean deleteTree(final File root) {
        try {
            if (root.isDirectory()) {
                final File[] files = root.listFiles();
                for (int i = 0; files != null && i < files.length; i++) {
                    final File file = files[i];
                    if (!deleteTree(file)) {
                        return false;
                    }
                }
            }
            return root.exists()
                    ? root.delete()
                    : true;
        } catch (Exception e) {
            s_logger.log(SEVERE_LEVEL, root + ": delete tree failed.", e);
            return false;
        }
    }
    //</editor-fold>
    //</editor-fold>
}
