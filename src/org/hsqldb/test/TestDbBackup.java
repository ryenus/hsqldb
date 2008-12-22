package org.hsqldb.test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.lib.tar.DbBackup;
import org.hsqldb.lib.tar.TarMalformatException;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class TestDbBackup extends junit.framework.TestCase {

    public TestDbBackup() throws IOException, SQLException {}

    static protected File baseDir =
        new File(System.getProperty("java.io.tmpdir"),
                 "TestDbBackup-" + System.getProperty("user.name"));

    static {
        try {
            Class.forName("org.hsqldb.jdbcDriver");

            // TODO:  Rename to upper-class JDBC driver class name
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                "<clinit> failed.  JDBC Driver class not in CLASSPATH");
        }
    }

    /**
     * Individual test methods may or may not need a Connection.
     * If they do, they run setupConn() then use 'conn', and it will be
     * automatically closed by the tearDown() method.
     *
     * @see #tearDown()
     */
    protected void setupConn(String id) throws SQLException {
        conn = getConnection(id);
        alreadyShut = false;
    }

    protected void shutdownAndCloseConn() throws SQLException {

        if (conn != null) {
            if (!alreadyShut) {
                conn.createStatement().executeUpdate("SHUTDOWN");
                alreadyShut = true;
            }
            if (verbose) {
                System.err.println("Shut down 'db1'");
            }
            conn.close();

            conn = null;
        }
    }

    /**
     * Use setupConn() to set up this Connection for just this individual test.
     *
     * @see #setupConn()
     */
    protected Connection conn = null;
    protected boolean alreadyShut = false;

    /**
     * Remove the specified directory and all of it's descendants.
     *
     * @throws IOException if unable to completely remove the specified dir
     */
    protected void rmR(File dir) throws IOException {

        if (!dir.exists()) {
            throw new IOException("Specified dir does not exist: "
                                  + dir.getAbsolutePath());
        }

        File[] children = dir.listFiles();

        for (int i = 0; i < children.length; i++) {
            if (children[i].isDirectory()) {
                rmR(children[i]);
            } else if (!children[i].delete()) {
                throw new IOException("Failed to remove '"
                                      + children[i].getAbsolutePath() + "'");
            }
        }

        if (!dir.delete()) {
            throw new IOException("Failed to remove '" + dir.getAbsolutePath()
                                  + "'");
        }
    }

    /**
     * I prefer to not use JUnit's unnecessary setUp() convention.
     */
    public TestDbBackup(String s) throws IOException, SQLException {
        super(s);
    }

    /**
     * JUnit convention for cleanup.
     */
    protected void tearDown() throws IOException, SQLException {

        if (baseDir.exists()) {
            rmR(baseDir);
            if (verbose) {
                System.err.println("Tore down");
            }
        }
    }

    static boolean verbose = Boolean.getBoolean("VERBOSE");

    /**
     * Specifically, this method sets up DB with id "db1".
     */
    protected void setUp() throws IOException, SQLException {
        if (verbose) {
            System.err.println("Set-upping");
        }

        if (baseDir.exists()) {
            throw new IOException(
                    "Please wipe out work directory '"
                    + baseDir + ", which is probably left over from an "
                    + "aborted test run");
        }

        try {
            setupConn("db1");

            Statement st = conn.createStatement();

            st.executeUpdate("CREATE TABLE t(i int);");
            st.executeUpdate("INSERT INTO t values(34);");
            conn.commit();
        } catch (SQLException se) {}
        finally {
            shutdownAndCloseConn();
        }
    }

    /**
     * Make sure to close after using the returned connection
     * (like in a finally block).
     */
    protected Connection getConnection(String id) throws SQLException {

        Connection c = DriverManager.getConnection("jdbc:hsqldb:file:"
            + baseDir.getAbsolutePath() + '/' + id + "/dbfile", "SA", "");

        if (verbose) {
            System.err.println("Opening JDBC URL '"
                    + "jdbc:hsqldb:file:" + baseDir.getAbsolutePath()
                    + '/' + id + "/dbfile");
        }

        c.setAutoCommit(false);

        return c;
    }

    static public void main(String[] sa) {

        if (sa.length > 0) {
            TestDbBackup.baseDir = new File(sa[0]);

            if (baseDir.exists()) {
                throw new IllegalArgumentException(
                    "If you specify a work directory, it must not exist "
                    + "yet.  (This makes it much easier for us to clean up "
                    + "after ourselves).");
            }

            System.err.println("Using user-specified base dir: "
                               + baseDir.getAbsolutePath());
        }

        junit.textui.TestRunner runner = new junit.textui.TestRunner();
        TestResult result =
            runner.run(runner.getTest(TestDbBackup.class.getName()));

        System.exit(result.wasSuccessful() ? 0
                                           : 1);
    }

    public void testSanity() throws SQLException {

        try {
            setupConn("db1");

            ResultSet rs =
                conn.createStatement().executeQuery("SELECT * FROM t;");

            rs.next();
            assertEquals("Wrong table 't' contents", 34, rs.getInt("i"));
        } finally {
            shutdownAndCloseConn();
        }
    }

    public void testBasicBackup()
    throws SQLException, IOException, TarMalformatException {
        mainBackupAndRestore("basic.tar");
    }

    public void testGzip()
    throws SQLException, IOException, TarMalformatException {
        mainBackupAndRestore("compressed.tar.gz");
    }

    public void testOnlineBackup()
    throws SQLException, IOException, TarMalformatException {
        onlineBackupAndRestore("online.tar");
    }

    public void onlineBackupAndRestore(String baseTarName)
    throws SQLException, IOException, TarMalformatException {

        try {
            setupConn("db1");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(2)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(3)");
            conn.commit();
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(4)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(5)");
            conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                                                 + baseDir.getAbsolutePath() + '/'
                                                 + baseTarName + "' BLOCKING");
            conn.createStatement().executeUpdate(
                    "INSERT INTO t VALUES(6)");
            conn.commit();
            conn.createStatement().executeUpdate("SHUTDOWN");
            alreadyShut = true;
            if (verbose) {
                    System.err.println("Shut down 'db1'");
            }
        } finally {
            shutdownAndCloseConn();
        }

        File destDir = new File(baseDir, "db2");

        if (!destDir.mkdir()) {
            throw new IOException("Failed to make new dir. to restore to: "
                                  + destDir.getAbsolutePath());
        }

        DbBackup.main(new String[] {
            "--extract", baseDir.getAbsolutePath() + '/' + baseTarName,
            destDir.getAbsolutePath()
        });

        try {
            setupConn("db2");
            conn.createStatement().executeUpdate("ROLLBACK");

            ResultSet rs =
                conn.createStatement().executeQuery("SELECT count(*) c FROM t;");

            rs.next();
            // 3 committed, 5 uncommited before saving:
            assertEquals("Wrong table 't' contents", 5, rs.getInt("c"));
        } finally {
            shutdownAndCloseConn();
        }
    }

    public void mainBackupAndRestore(String baseTarName)
    throws SQLException, IOException, TarMalformatException {

        DbBackup.main(new String[] {
            "--save", baseDir.getAbsolutePath() + '/' + baseTarName,
            baseDir.getAbsolutePath() + "/db1/dbfile"
        });

        File destDir = new File(baseDir, "db2");

        if (!destDir.mkdir()) {
            throw new IOException("Failed to make new dir. to restore to: "
                                  + destDir.getAbsolutePath());
        }

        DbBackup.main(new String[] {
            "--extract", baseDir.getAbsolutePath() + '/' + baseTarName,
            destDir.getAbsolutePath()
        });

        try {
            setupConn("db2");

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t;");

            rs.next();
            assertEquals("Wrong table 't' contents", 34, rs.getInt("i"));
        } finally {
            shutdownAndCloseConn();
        }
    }

    public void testMainOpen()
    throws SQLException, IOException, TarMalformatException {

        try {
            setupConn("db1");

            try {
                DbBackup.main(new String[] {
                    "--save", baseDir.getAbsolutePath() + "/mainOpen.tar",
                    baseDir.getAbsolutePath() + "/db1/dbfile"
                });
            } catch (IllegalStateException ioe) {
                return;
            }
        } finally {
            shutdownAndCloseConn();
        }

        fail("Backup from main() did not throw even though DB is open");
    }

    static public Test suite() throws IOException, SQLException {

        TestSuite newSuite = new TestSuite();

        newSuite.addTest(new TestDbBackup("testSanity"));
        newSuite.addTest(new TestDbBackup("testBasicBackup"));
        newSuite.addTest(new TestDbBackup("testMainOpen"));
        newSuite.addTest(new TestDbBackup("testGzip"));
        newSuite.addTest(new TestDbBackup("testOnlineBackup"));

        return newSuite;
    }
}
