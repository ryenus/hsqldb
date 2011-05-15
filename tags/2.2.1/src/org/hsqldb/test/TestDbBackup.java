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


package org.hsqldb.test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.hsqldb.lib.tar.DbBackup;
import org.hsqldb.lib.tar.TarMalformatException;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestDbBackup extends junit.framework.TestCase {

    public TestDbBackup() throws IOException, SQLException {}

    static protected File baseDir =
        new File(System.getProperty("java.io.tmpdir"),
                 "TestDbBackup-" + System.getProperty("user.name"));

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
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

        if (conn == null) {
            return;
        }
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

    /**
     * Use setupConn() to set up this Connection for just this individual test.
     *
     * @see #setupConn(String)
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
     * Accommodate JUnit's test-runner conventions.
     */
    public TestDbBackup(String s) throws IOException, SQLException {
        super(s);
    }

    /**
     * JUnit convention for cleanup.
     *
     * Called after each test*() method.
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
     * Specifically, this method creates and populates "db1", then closes it.
     *
     * Invoked before each test*() invocation by JUnit.
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

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(String[] sa) {
        if (sa.length > 0 && !sa[sa.length - 1].equals("-g")) {
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
            junit.framework.TestResult result =
                runner.run(runner.getTest(TestDbBackup.class.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
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

    /**
     * Test all forms of online backups with explicit filenames.
     */
    public void testOnlineBackup()
    throws SQLException, IOException, TarMalformatException {
        onlineBackupAndRestore("online.tar", true, false, "db11");
        onlineBackupAndRestore("online.tar.gz", false, true, "db12");
        onlineBackupAndRestore("online.tgz", false, true, "db13");
    }

    public void onlineBackupAndRestore(String baseTarName,
            boolean populate, boolean compress, String restoreDest)
    throws SQLException, IOException, TarMalformatException {

        try {
            setupConn("db1");
            conn.createStatement().executeUpdate("DELETE FROM t");
            // For this case, we wipe the data that we so carefully set up,
            // so that we can call this method repeatedly without worrying
            // about left-over data from a previous run.
            conn.commit();
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(1)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(2)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(3)");
            conn.commit();
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(4)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(5)");
            conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                                                 + baseDir.getAbsolutePath()
                                                 + '/' + baseTarName
                                                 + "' BLOCKING"
                    + (compress ? "" : " NOT COMPRESSED"));
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

        File destDir = new File(baseDir, restoreDest);

        if (!destDir.mkdir()) {
            throw new IOException("Failed to make new dir. to restore to: "
                                  + destDir.getAbsolutePath());
        }

        DbBackup.main(new String[] {
            "--extract", baseDir.getAbsolutePath() + '/' + baseTarName,
            destDir.getAbsolutePath()
        });

        try {
            setupConn(restoreDest);
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

        File destDir = new File(baseDir, "mainrestored");

        if (!destDir.mkdir()) {
            throw new IOException("Failed to make new dir. to restore to: "
                                  + destDir.getAbsolutePath());
        }

        DbBackup.main(new String[] {
            "--extract", baseDir.getAbsolutePath() + '/' + baseTarName,
            destDir.getAbsolutePath()
        });

        try {
            setupConn("mainrestored");

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t;");

            rs.next();
            assertEquals("Wrong table 't' contents", 34, rs.getInt("i"));
        } finally {
            shutdownAndCloseConn();
        }
    }

    public void testMainAlreadyOpen()
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

    /**
     * Test that bad explicit filenames are rejected for onilne backups.
     */
    public void testTarFileNames()
    throws SQLException, IOException, TarMalformatException {

        boolean caught;
        try {
            setupConn("db1");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(2)");
            conn.commit();

            // #1:  COMPRESSED -> no-extension
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad' BLOCKING COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested compression "
                        + "to file '/x/bad'");
            }
            // #2:  NOT COMPRESSED -> no-extension
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad' BLOCKING NOT COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested "
                        + "no-compression to file '/x/bad'");
            }
            // #3:  COMPRESSED -> *.txt
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.txt' BLOCKING COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested compression "
                        + "to file '/x/bad.txt'");
            }
            // #4:  NOT COMPRESSED -> *.txt
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.txt' BLOCKING NOT COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested "
                        + "no-compression to file '/x/bad.txt'");
            }
            // #5:  DEFAULT -> *.tar
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.tar' BLOCKING");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested default "
                        + "to file '/x/bad.tar'");
            }
            // #6:  COMPRESSION -> *.tar
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.tar' BLOCKING COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested compression "
                        + "to file '/x/bad.tar'");
            }
            // #7:  NOT COMPRESSED -> *.tar.gz
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.tar.gz' BLOCKING NOT COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested "
                        + "non-compression to file '/x/bad.tar.gz'");
            }
            // #8:  NOT COMPRESSED -> *.tgz
            caught = false;
            try {
                conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                        + baseDir.getAbsolutePath()
                        + "/x/bad.tgz' BLOCKING NOT COMPRESSED");
            } catch (SQLException se) {
                caught = true;
            }
            if (!caught) {
                fail("BACKUP did not throw even though requested "
                        + "non-compression to file '/x/bad.tgz'");
            }

            // Finally run a test to ensure that the attempts above didn't
            // fail for some unexpected reason.
            conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                    + baseDir.getAbsolutePath()
                    + "/positivetest.tar' BLOCKING NOT COMPRESSED");
        } finally {
            shutdownAndCloseConn();
        }

    }

    /**
     * Test that correct DB names are generated when user supplies just a
     * directory.
     *
     * N.b.  This test may not work right if tests are run at midnight.
     * This limitation will be removed once we can update the FilenameFilters
     * with Java 4's java.util.regex.
     */
    public void testAutoNaming()
    throws SQLException, IOException, TarMalformatException {

        boolean caught;
        int fileCount;
        try {
            setupConn("db1");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES(2)");
            conn.commit();

            fileCount = baseDir.listFiles(autoTarFilenameFilter).length;
            if (fileCount != 0)
                throw new IllegalStateException(Integer.toString(fileCount)
                        + " auto-tar files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' before starting testAutoNaming");
            fileCount = baseDir.listFiles(autoTarGzFilenameFilter).length;
            if (fileCount != 0)
                throw new IllegalStateException(Integer.toString(fileCount)
                        + " auto-tar.gz files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' before starting testAutoNaming");
            conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                    + baseDir.getAbsolutePath()
                    + "/' BLOCKING NOT COMPRESSED");
            fileCount = baseDir.listFiles(autoTarFilenameFilter).length;
            if (fileCount != 1)
                fail(Integer.toString(fileCount)
                        + " auto-tar files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' after writing a non-compressed backup");
            fileCount = baseDir.listFiles(autoTarGzFilenameFilter).length;
            if (fileCount != 0)
                fail(Integer.toString(fileCount)
                        + " auto-tar.gz files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' after writing a non-compressed backup");
            conn.createStatement().executeUpdate("BACKUP DATABASE TO '"
                    + baseDir.getAbsolutePath()
                    + "/' BLOCKING COMPRESSED");
            fileCount = baseDir.listFiles(autoTarFilenameFilter).length;
            if (fileCount != 1)
                fail(Integer.toString(fileCount)
                        + " auto-tar files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' after writing both backups");
            fileCount = baseDir.listFiles(autoTarGzFilenameFilter).length;
            if (fileCount != 1)
                fail(Integer.toString(fileCount)
                        + " auto-tar.gz files exist in baseDir '"
                        + baseDir.getAbsolutePath()
                        + "' after writing a compressed backup");
        } finally {
            shutdownAndCloseConn();
        }

    }
    public static Test suite() throws IOException, SQLException {

        TestSuite newSuite = new TestSuite();

        newSuite.addTest(new TestDbBackup("testSanity"));
        newSuite.addTest(new TestDbBackup("testBasicBackup"));
        newSuite.addTest(new TestDbBackup("testMainAlreadyOpen"));
        newSuite.addTest(new TestDbBackup("testGzip"));
        newSuite.addTest(new TestDbBackup("testOnlineBackup"));
        newSuite.addTest(new TestDbBackup("testTarFileNames"));
        newSuite.addTest(new TestDbBackup("testAutoNaming"));

        return newSuite;
    }

    private String autoMiddlingString = "-"
            + new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
            + 'T';

    FilenameFilter autoTarFilenameFilter = new FilenameFilter() {
        private String suffixFormat = "-yyyyMMddTHHmmss.tar";
        public boolean accept(File dir, String name) {
            if (name.length() < suffixFormat.length() + 1) {
                // Require variable name length >= 1 char
                return false;
            }
            int suffixPos = name.length() - suffixFormat.length();
            // Would like to use Java 1.4's java.util.regex here.
            return name.endsWith(".tar")
                    && name.substring(suffixPos,
                    suffixPos + autoMiddlingString.length())
                    .equals(autoMiddlingString);
        }
    };

    FilenameFilter autoTarGzFilenameFilter = new FilenameFilter() {
        private String suffixFormat = "-yyyyMMddTHHmmss.tar.gz";
        public boolean accept(File dir, String name) {
            if (name.length() < suffixFormat.length() + 1) {
                // Require variable name length >= 1 char
                return false;
            }
            int suffixPos = name.length() - suffixFormat.length();
            // Would like to use Java 1.4's java.util.regex here.
            return name.endsWith(".tar.gz")
                    && name.substring(suffixPos,
                    suffixPos + autoMiddlingString.length())
                    .equals(autoMiddlingString);
        }
    };
}
