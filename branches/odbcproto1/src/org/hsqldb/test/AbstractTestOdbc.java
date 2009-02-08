/* Copyright (c) 2001-2009, The HSQL Development Group * All rights reserved.
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;

import junit.framework.Test;
import junit.framework.TestSuite;

public abstract class AbstractTestOdbc extends junit.framework.TestCase {
    protected Connection netConn = null;
    protected Server server = null;
    static protected String portString = null;
    static protected String dsnName = null;

    public AbstractTestOdbc() {}

    /**
     * Accommodate JUnit's test-runner conventions.
     */
    public AbstractTestOdbc(String s) {
        super(s);
    }

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            // TODO:  Rename to upper-class JDBC driver class name
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                "<clinit> failed.  JDBC Driver class not in CLASSPATH");
        }
        portString = System.getProperty("test.hsqlodbc.port");
        dsnName = System.getProperty("test.hsqlodbc.dsnname");
        if (portString == null) {
            portString = "9797";
        }
        if (dsnName == null) {
            dsnName = "HSQLDB_UTEST";
        }
    }

    /**
     * JUnit convention for cleanup.
     *
     * Called after each test*() method.
     */
    protected void tearDown() throws SQLException {
        if (netConn != null) {
            netConn.rollback();
            // Necessary to prevent the SHUTDOWN command from causing implied
            // transaction control commands, which will not be able to
            // complete.
            netConn.createStatement().executeUpdate("SHUTDOWN");
            netConn.close();
            netConn = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
        if (server != null
            && server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
            throw new RuntimeException("Server failed to shut down");
        }
    }

    /**
     * Specifically, this opens a mem-only DB, populates it, starts a
     * HyperSQL Server to server it, and opens network JDBC Connection
     * "netConn" to it,
     *
     * Invoked before each test*() invocation by JUnit.
     */
    protected void setUp() {
        try {
            Connection setupConn = DriverManager.getConnection(
                "jdbc:hsqldb:mem:test", "SA", "");
            setupConn.setAutoCommit(false);
            populate(setupConn);
            setupConn.commit();
            setupConn.close();
        } catch (SQLException se) {
            throw new RuntimeException(
                "Failed to set up in-memory database", se);
        }
        try {
            server = new Server();
            HsqlProperties properties = new HsqlProperties();
            if (System.getProperty("VERBOSE") == null) {
                server.setLogWriter(null);
                server.setErrWriter(null);
            } else {
                properties.setProperty("server.silent", "false");
                properties.setProperty("server.trace", "true");
            }
            properties.setProperty("server.database.0", "mem:test");
            properties.setProperty("server.dbname.0", "");
            properties.setProperty("server.port", AbstractTestOdbc.portString);
            server.setProperties(properties);
            server.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to set up in-memory database", e);
        }
        if (server.getState() != ServerConstants.SERVER_STATE_ONLINE) {
            throw new RuntimeException("Server failed to start up");
        }
        try {
            netConn = DriverManager.getConnection(
                "jdbc:odbc:" + dsnName, "SA", "sapwd");
            //netConn.setAutoCommit(false);
        } catch (SQLException se) {
            if (se.getMessage().indexOf("No suitable driver") > -1) {
                throw new RuntimeException(
                    "You must install the native library for Sun's jdbc:odbc "
                    + "JDBC driver");
            }
            if (se.getMessage().indexOf("Data source name not found") > -1) {
                throw new RuntimeException(
                    "You must configure ODBC DSN '" + dsnName
                    + "' (you may change the name and/or port by setting Java "
                    + "system properties 'test.hsqlodbc.port' or "
                    + "'test.hsqlodbc.dsnname'");
            }
            throw new RuntimeException(
                "Failed to set up JDBC/ODBC network connection", se);
        }
    }

    protected void enableAutoCommit() {
        try {
            netConn.setAutoCommit(false);
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }


    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void staticRunner(Class c, String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(c);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(c.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }

    abstract protected void populate(Connection c) throws SQLException;
}
