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

/**
 * Deletes all a specific
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.1
 */
public final class HsqldbEmbeddedDatabaseDeleter implements ConnectionFactory.EventListener {

    private static final java.util.logging.Level INFO_LEVEL = java.util.logging.Level.INFO;
    private static final java.util.logging.Level SEVERE_LEVEL = java.util.logging.Level.SEVERE;
    private static final java.util.logging.Logger s_logger = java.util.logging.Logger.getLogger(
            HsqldbEmbeddedDatabaseDeleter.class.getName());
    //
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
    //
    private final String m_dataseUrl;

    public HsqldbEmbeddedDatabaseDeleter(final String databaseUrl) {
        m_dataseUrl = databaseUrl;
    }

    @Override
    public void closedRegisteredObjects(final ConnectionFactory source) {
        final boolean success = deleteDatabase(m_dataseUrl);

        if (success) {
            s_logger.log(INFO_LEVEL, "Database deletion succeeded for: {0}", m_dataseUrl);
        } else {
            s_logger.log(SEVERE_LEVEL, "Database deletion failed for: {0}", m_dataseUrl);
        }
    }

    public static boolean deleteDatabase(final String databaseUrl) {
        if (databaseUrl == null) {
            s_logger.log(SEVERE_LEVEL, "null database url provided");

            return false;
        }

        final boolean hasPrefix = databaseUrl.trim().toLowerCase().startsWith(DatabaseURL.S_URL_PREFIX);
        final HsqlProperties info = DatabaseURL.parseURL(databaseUrl, hasPrefix, false);
        final String type = (info == null)
                ? null
                : info.getProperty(DatabaseURL.url_connection_type);

        if (!DatabaseURL.S_FILE.equals(type)) {
            s_logger.log(SEVERE_LEVEL, "Deletion not supported for database type: {0}", type);

            return false;
        }

        final String path = info.getProperty(DatabaseURL.url_database);

        if (path == null) {
            s_logger.log(SEVERE_LEVEL, "Malformed database url provided: {0}", databaseUrl);
            return false;
        }

        boolean success = true;
        LockFile lockFile = null;

        try {
            lockFile = LockFile.newLockFileLock(path);

            for (int i = 0; i < s_coreFileExt.length; i++) {
                success &= deleteFile(path + s_coreFileExt[i] + org.hsqldb.persist.Logger.newFileExtension);
                success &= deleteFile(path + s_coreFileExt[i] + org.hsqldb.persist.Logger.oldFileExtension);
                success &= deleteFile(path + s_coreFileExt[i]);
            }

            for (int i = 0; i < s_infoLogExt.length; i++) {
                success &= deleteFile(path + s_infoLogExt[i]);
            }

            success &= deleteTree(new File(path + ".tmp"));
        } catch (Exception ex) {
            success = false;
            String message = MessageFormat.format("LockFile.newLockFileLock({0})", path);
            s_logger.log(SEVERE_LEVEL, message, ex);
        } finally {
            try {
                if (lockFile != null) {
                    lockFile.tryRelease();

                    success &= deleteFile(path + ".lck");
                }
            } catch (Exception ex) {
                success = false;
                s_logger.log(SEVERE_LEVEL, "lockFile.tryRelease()", ex);
            }


        }

        return success;
    }

    private static boolean deleteFile(final String file) {
        boolean exists = false;
        boolean deleted = false;
        boolean error = false;

        try {
            final File f = new File(file);

            exists = f.exists();

            if (exists) {
                deleted = f.delete();
            }

            if (deleted) {
                s_logger.log(INFO_LEVEL, "{0} deleted.", file);
            } else if (!exists) {
                s_logger.log(INFO_LEVEL, "{0} not exists.", file);
            } else {
                s_logger.log(SEVERE_LEVEL, "{0} delete failed.", file);
            }
        } catch (Exception e) {
            error = true;
            s_logger.log(SEVERE_LEVEL, file + " delete failed.", e);
        }

        return !error & (deleted || !exists);
    }

    private static boolean deleteTree(File root) {
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
            s_logger.log(SEVERE_LEVEL, root + " delete tree failed.", e);
            return false;
        }
    }
}
