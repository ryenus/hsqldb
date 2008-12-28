/* Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * For creating, tracking and closing the 
 * JDBC objects used by this test suite. <p>
 *
 * @author boucherb@users
 */
public class JdbcTestCaseConnectionFactory {
    
    private List<Connection> m_connections = new ArrayList<Connection>();
    private List<Statement> m_statements = new ArrayList<Statement>();
    private List<ResultSet> m_resultSets = new ArrayList<ResultSet>();
    
    /**
     * Creates a new instance of JdbcTestCaseConnectionFactory.
     */
    public JdbcTestCaseConnectionFactory() {
        //
    }
    
    /**
     * to be closed at teardown.
     *
     * @param conn to track for close.
     */
    public void registerConnection(Connection conn) {
        m_connections.add(conn);
    }
    
    /**
     * to be closed at teardown.
     *
     * @param stmt to track for close.
     */
    public void registerStatement(Statement stmt) {
        m_statements.add(stmt);
    }
    
    /**
     * to be closed at teardown.
     *
     * @param rs to track for close.
     */
    public void registerResultSet(ResultSet rs) {
        m_resultSets.add(rs);
    }
    
    /**
     * closes all registered JDBC objects.
     */
    public void closeRegisteredObjects() {
        for(ResultSet rs : m_resultSets) {
            if (rs != null) {
                try {
                    rs.close();
                } catch(Exception ex){}
            }
        }
        
        for(Statement stmt : m_statements) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch(Exception ex){}
            }
        }
        
        for(Connection conn : m_connections) {
            if (conn != null) {
                try {
                    conn.close();
                } catch(Exception ex){}
            }
        }
        
        org.hsqldb.DatabaseManager.closeDatabases(-1);
    }
    
    /**
     * with the specified driver, url, user and password. <p>
     *
     * @param driver fully qualified class name of a <tt>java.sql.Driver</tt>.
     * @param url of connection.
     * @param user of connection.
     * @param password of user.
     * @throws java.lang.Exception when the connection cannot be created.
     * @return a newly created and registered object.
     */
    public Connection newConnection(
            final String driver,
            final String url,
            final String user,
            final String password) throws Exception {
        // Not actually needed under JDBC4, as long as
        // a classpath jar has a compatible META-INF service
        // entry.  However, its not guaranteed that
        // every driver tested has a service entry...
        Class.forName(driver);
        
        final Connection conn = DriverManager.getConnection(
                url, 
                user, 
                password);
        
        registerConnection(conn);
        
        return conn;
    }
    
    /**
     * using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param conn with which to create.
     * @throws java.lang.Exception when the statement cannot be created.
     * @return a newly created and registered object.
     */
    public Statement createStatement(
            final Connection conn) throws Exception {
        final Statement stmt = conn.createStatement();
        
        registerStatement(stmt);
        
        return stmt;
    }
    
    /**
     * for the given <tt>sql</tt> using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param sql to prepare.
     * @param conn with which to prepare.
     * @throws java.lang.Exception when the statement cannot be prepared.
     * @return a newly prepared and registered object.
     */
    public PreparedStatement prepareStatement(
            final String sql,
            final Connection conn) throws Exception {
        final PreparedStatement pstmt = conn.prepareStatement(sql);
        
        registerStatement(pstmt);
        
        return pstmt;
    }
    
    /**
     * for the given <tt>sql</tt> using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param conn with which to prepare.
     * @throws java.lang.Exception when the call cannot be prepared.
     * @return the newly prepared and registered object.
     */
    public CallableStatement prepareCall(
            final String sql, 
            final Connection conn) throws Exception {
        final CallableStatement stmt = conn.prepareCall(sql);
        
        registerStatement(stmt);
        
        return stmt;
    }
    
    /**
     * using the given <tt>sql</tt> and statement object.
     *
     * The returned <tt>ResultSet</tt> is registered to be closed at teardown.
     * 
     * @param sql to execute.
     * @param stmt against which to execute.
     * @throws java.lang.Exception when execution fails.
     * @return the query result.
     */
    public ResultSet executeQuery(
            final String sql, 
            final Statement stmt) throws Exception {
        final ResultSet rs = stmt.executeQuery(sql);
        
        registerResultSet(rs);
        
        return rs;
    }
    
    /**
     * using the given statement object.
     *
     * The returned <tt>ResultSet</tt> is registered to be closed at teardown.
     *
     * @param stmt to execute.
     * @throws java.lang.Exception when execution fails.
     * @return the query result.
     */
    public final ResultSet executeQuery(
            final PreparedStatement stmt) throws Exception {
        final ResultSet rs = stmt.executeQuery();
        
        registerResultSet(rs);
        
        return rs;
    }
}
