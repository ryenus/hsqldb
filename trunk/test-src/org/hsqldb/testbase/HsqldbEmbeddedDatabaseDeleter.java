package org.hsqldb.testbase;

import java.io.File;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hsqldb.DatabaseURL;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LockFile;

/**
 *
 * @author cboucher
 */
public class HsqldbEmbeddedDatabaseDeleter implements ConnectionFactory.EventListener {

    private static final Logger s_logger = Logger.getLogger(HsqldbEmbeddedDatabaseDeleter.class.getName());

    @Override
    public void closedRegisteredObjects(ConnectionFactory source) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private static void deleteDatabase(final String databaseUrl) {
        if (databaseUrl == null) {
            s_logger.log(Level.SEVERE, "null database url provided");

            return;
        }

        final boolean hasPrefix = databaseUrl.trim().toLowerCase().startsWith(DatabaseURL.S_URL_PREFIX );
        final HsqlProperties info = DatabaseURL.parseURL(databaseUrl, hasPrefix, false);
        final String type = info == null ? null : info.getProperty(DatabaseURL.url_connection_type);

        if (!DatabaseURL.S_FILE.equals(type)) {
            s_logger.log(Level.SEVERE, "cannot delete databases of type: {0}", type);

            return;
        }

        final String path = info.getProperty(DatabaseURL.url_database);

        if (path == null) {
            s_logger.log(Level.SEVERE, "Malformed url provided: {0}", databaseUrl);
            return;
        }

        try {
            final LockFile lockFile = LockFile.newLockFileLock(path);

            deleteFile(path + ".backup");
            deleteFile(path + ".properties");
            deleteFile(path + ".script");
            deleteFile(path + ".data");
            deleteFile(path + ".log");
            deleteFile(path + ".lobs");

            try {
                lockFile.tryRelease();
            } catch (Exception ex) {
                s_logger.log(Level.SEVERE, "lockFile.tryRelease()", ex);
            }

            deleteFile(path + ".lck");
        } catch (Exception ex) {
            String message = MessageFormat.format("LockFile.newLockFileLock({0})", path);
            s_logger.log(Level.SEVERE, message, ex);
        }
    }

    private static boolean deleteFile(final String file) {
        boolean rval = false;

        try {
            rval = new File(file).delete();
        } catch (Exception e) {
            s_logger.log(Level.SEVERE, "Delete failed.", e);
        }

        if (rval) {
           s_logger.log(Level.INFO, "{0} deleted.", file);
        } else {
           s_logger.log(Level.SEVERE, "{0} not deleted.", file);
        }

        return rval;
    }
}
