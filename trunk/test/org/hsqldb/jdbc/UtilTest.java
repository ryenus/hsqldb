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

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.hsqldb.Trace;

/**
 * Test of class org.hsqldb.jdbc.Util.
 *
 * @author boucherb@users
 */
public class UtilTest extends JdbcTestCase {
    
// SQL 2003 Table 32 - SQLSTATE class and subclass values
//
//  connection exception 08 (no subclass)                     000
//
//                          SQL-client unable to establish    001
//                          SQL-connection
//
//                          connection name in use            002
//
//                          connection does not exist         003
//
//                          SQL-server rejected establishment 004
//                          of SQL-connection
//
//                          connection failure                006
//
//                          transaction resolution unknown    007
    
// org.hsqldb.Trace - sql-error-messages
//
// 080=08000 socket creation error                             - better 08001 ?
// 085=08000 Unexpected exception when setting up TLS
//
// 001=08001 The database is already in use by another process - better 08002 ?
//
// 002=08003 Connection is closed
// 003=08003 Connection is broken
// 004=08003 The database is shutdown
// 094=08003 Database does not exists                          - better 08001 ?
    
    private static final Object[][] exceptions = new Object[][]{
        {
            SQLTransientConnectionException.class,
                    new int[] {
                Trace.SOCKET_ERROR,
                Trace.DATABASE_LOCK_ACQUISITION_FAILURE,
                Trace.DATABASE_NOT_EXISTS
            }
        },
        {
            SQLNonTransientConnectionException.class,
                    new int[] {
                Trace.CONNECTION_IS_CLOSED,
                Trace.CONNECTION_IS_BROKEN,
                Trace.DATABASE_IS_SHUTDOWN,
                Trace.UNEXPECTED_EXCEPTION // when setting up TLS
            }
        },
        {
            SQLIntegrityConstraintViolationException.class,
                    new int[] {
                Trace.INTEGRITY_CONSTRAINT_VIOLATION,
                Trace.VIOLATION_OF_UNIQUE_INDEX,
                Trace.TRY_TO_INSERT_NULL,
                Trace.VIOLATION_OF_UNIQUE_CONSTRAINT,
                Trace.CHECK_CONSTRAINT_VIOLATION,
                Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,
                Trace.SEQUENCE_REFERENCED_BY_VIEW,
                Trace.TABLE_REFERENCED_CONSTRAINT,
                Trace.TABLE_REFERENCED_VIEW,
                Trace.COLUMN_IS_REFERENCED
            }
        },
        {
            SQLInvalidAuthorizationSpecException.class,
                    new int[] {
                Trace.GRANTEE_ALREADY_EXISTS,
                Trace.CIRCULAR_GRANT,
                Trace.NO_SUCH_RIGHT
            }
        },
        {
            SQLSyntaxErrorException.class,
                    new int[] {
                // TODO:
                //
                // First, the overview section of java.sql.SQLSyntaxErrorException
                // appears to be inaccurate or not in sync with the SQL 2003 standard:
                //
                // "...thrown when the SQLState class value is '<i>42</i>'"
                //
                // SQL 2003, Table 32  states:
                //
                // Condition                               Class SubClass
                // syntax error or access rule violation -  42   (no subclass) 000
                //
                // SQL 2003 describes an Access Rule Violation as refering to
                // the case where, in the course of preparing or executing
                // an SQL statement, an Access Rule section pertaining
                // to one of the elements of the statement is violated.
                //
                // Further, section 13.4 Calls to an <externally-invoked-procedure>
                // lists:
                //
                // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_NO_SUBCLASS:
                // constant SQLSTATE_TYPE :="42000";
                // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DIRECT_STATEMENT_NO_SUBCLASS:
                // constant SQLSTATE_TYPE :="2A000";
                // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DYNAMIC_STATEMENT_NO_SUBCLASS:
                // constant SQLSTATE_TYPE :="37000";
                //
                // Strangely, SQLSTATE "37000" and 2A000" are not mentioned
                // anywhere else in any of the SQL 2003 parts and are
                // conspicuously missing from 02 - Foundation, Table 32.
                //
                //  -----------------------------------
                ///
                // Our only Access Violation SQLSTATE so far is:
                //
                // Trace.NOT_AUTHORIZED 255=42000 User not authorized for action '$$'
                //
                // our syntax exceptions are apparently all sqlstate "37000"
                //
                // Clearly, we should differentiate between DIRECT and DYNAMIC
                // SQL forms.  And clearly, our current "37000" is possible
                // not correct, in that we do not actually support dynamic
                // SQL syntax, but rather only implement similar behaviour
                // through JDBC Prepared and Callable statements.
                Trace.UNEXPECTED_TOKEN,
                Trace.UNEXPECTED_END_OF_COMMAND,
                Trace.UNKNOWN_FUNCTION,
                Trace.NEED_AGGREGATE,
                Trace.SUM_OF_NON_NUMERIC,
                Trace.WRONG_DATA_TYPE,
                Trace.LABEL_REQUIRED,
                Trace.WRONG_DEFAULT_CLAUSE,
                Trace.OUTER_JOIN_CONDITION,
                Trace.MISSING_SOFTWARE_MODULE,
                Trace.NOT_IN_AGGREGATE_OR_GROUP_BY,
                Trace.INVALID_GROUP_BY,
                Trace.INVALID_HAVING,
                Trace.INVALID_ORDER_BY,
                Trace.INVALID_ORDER_BY_IN_DISTINCT_SELECT,
                Trace.NULL_LITERAL_NOT_ALLOWED,
                Trace.INVALID_CHARACTER_ENCODING,                
                Trace.MISSING_CLOSEBRACKET,
                Trace.COLUMN_IS_IN_INDEX,
                Trace.SINGLE_COLUMN_EXPECTED,
                Trace.INVALID_FUNCTION_ARGUMENT,
                Trace.COLUMN_IS_IN_CONSTRAINT,
                Trace.COLUMN_SIZE_REQUIRED,
                Trace.INVALID_SIZE_PRECISION,
                Trace.NOT_AUTHORIZED
            }
        },
        {
            SQLTransactionRollbackException.class,
                    new int[] {
                // TODO: our 40xxx exceptions are not currently used (correctly)
                //       for transaction rollback exceptions:
                //
                //       018=40001 Serialization failure
                //
                //       - currently used to indicate Java object serialization
                //         failures, which is just plain wrong.
                //
                //       019=40001 Transfer corrupted
                //
                //        - currently used to indicate IOExceptions related to
                //          PreparedStatement XXXStreamYYY operations and Result
                //          construction using RowInputBinary (e.g. when reading
                //          a result transmitted over the network), which is
                //          probably also just plain wrong.
                //
                // SQL 2003 02 - Foundation, Table 32 states:
                //
                // 40000  transaction rollback  - no subclass
                // 40001  transaction rollback  - (transaction) serialization failure
                // 40002  transaction rollback  - integrity constraint violation
                // 40003  transaction rollback  - statement completion unknown
                // 40004  transaction rollback  - triggered action exception
                //
            }
        },
        {
            SQLException.class,
                    null // calculated below, in static initializer
        }
        
    };
    
    private static final Map classMap = new HashMap();
    
    static {
        List list = new ArrayList();
        
        for (int i = 1; i < Trace.LAST_ERROR_HANDLE; i++) {
            for (int j = 0; j < exceptions.length - 1; j++) {
                int[]   codes = (int[]) exceptions[j][1];
                boolean found = false;
                
                for (int k = 0; k < codes.length; k++) {
                    if (i == codes[k]) {
                        found = true;
                        break;
                    }
                }
                
                if (!found) {
                    list.add(new Integer(i));
                }
            }
        }
        
        int[] nontransientcodes = new int[list.size()];
        
        for (int i = 0; i < list.size(); i++) {
            nontransientcodes[i] = ((Integer)list.get(i)).intValue();
        }
        
        exceptions[exceptions.length-1][1] = nontransientcodes;
        
        for (int i = 0; i < exceptions.length; i++) {
            classMap.put(exceptions[i][0], exceptions[i][1]);
        }
    }
    
    public UtilTest(String testName) {
        super(testName);
    }
    
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite(UtilTest.class);
        
        return suite;
    }
    
    protected void checkSQLException(SQLException se) throws Exception {
        String  sqlState  = se.getSQLState();
        
        if (sqlState.startsWith("08")) {
            if (sqlState.endsWith("003")) {
                assertTrue("se instanceof SQLNonTransientConnectionException",
                        se instanceof SQLNonTransientConnectionException);
                checkErrorCode(se, SQLNonTransientConnectionException.class);
            } else {
                assertTrue("se instanceof SQLTransientConnectionException",
                        se instanceof SQLTransientConnectionException);
                checkErrorCode(se, SQLTransientConnectionException.class);
            }
        } else if (sqlState.startsWith("23")) {
            assertTrue("se instanceof SQLIntegrityConstraintViolationException",
                    se instanceof SQLIntegrityConstraintViolationException);
            checkErrorCode(se, SQLIntegrityConstraintViolationException.class);
        } else if (sqlState.startsWith("28")) {
            assertTrue("se instanceof SQLInvalidAuthorizationSpecException",
                    se instanceof SQLInvalidAuthorizationSpecException);
            checkErrorCode(se, SQLInvalidAuthorizationSpecException.class);
        } else if (sqlState.startsWith("42")) {
            assertTrue("se instanceof SQLSyntaxErrorException",
                    se instanceof SQLSyntaxErrorException);
            checkErrorCode(se, SQLSyntaxErrorException.class);
        } else if (sqlState.startsWith("40")) {
            assertTrue("se instanceof SQLTransactionRollbackException",
                    se instanceof SQLTransactionRollbackException);
            checkErrorCode(se, SQLTransactionRollbackException.class);
        } else {
            checkErrorCode(se, SQLException.class);
        }
    }
    
    protected void checkErrorCode(SQLException se, Class clazz) throws Exception {
        int     errorCode    = Math.abs(se.getErrorCode());
        String  errorMessage = se.getMessage();
        String  sqlState     = se.getSQLState();
        int[]   codes        = (int[])classMap.get(clazz);
        boolean found        = false;
        
        for (int i = 0; i < codes.length; i++) {
            if (errorCode == codes[i]) {
                found = true;
                break;
            }
        }
        
        assertEquals("Allowable error code "
                + errorCode
                + " ("
                + sqlState
                + " "
                + errorMessage
                + ") for: "
                + clazz,
                true,
                found);
    }

    /**
     * Test of sqlException(Trace.DATABASE_LOCK_ACQUISITION_FAILURE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATABASE_LOCK_ACQUISITION_FAILURE() throws Exception {
        println("testSqlException_DATABASE_LOCK_ACQUISITION_FAILURE");
        
        SQLException ex = Util.sqlException(Trace.DATABASE_LOCK_ACQUISITION_FAILURE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CONNECTION_IS_CLOSED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CONNECTION_IS_CLOSED() throws Exception {
        println("testSqlException_CONNECTION_IS_CLOSED");
        
        SQLException ex = Util.sqlException(Trace.CONNECTION_IS_CLOSED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CONNECTION_IS_BROKEN,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CONNECTION_IS_BROKEN() throws Exception {
        println("testSqlException_CONNECTION_IS_BROKEN");
        
        SQLException ex = Util.sqlException(Trace.CONNECTION_IS_BROKEN,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATABASE_IS_SHUTDOWN,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATABASE_IS_SHUTDOWN() throws Exception {
        println("testSqlException_DATABASE_IS_SHUTDOWN");
        
        SQLException ex = Util.sqlException(Trace.DATABASE_IS_SHUTDOWN,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_COUNT_DOES_NOT_MATCH,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_COUNT_DOES_NOT_MATCH() throws Exception {
        println("testSqlException_COLUMN_COUNT_DOES_NOT_MATCH");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_COUNT_DOES_NOT_MATCH,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DIVISION_BY_ZERO,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DIVISION_BY_ZERO() throws Exception {
        println("testSqlException_DIVISION_BY_ZERO");
        
        SQLException ex = Util.sqlException(Trace.DIVISION_BY_ZERO,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_ESCAPE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_ESCAPE() throws Exception {
        println("testSqlException_INVALID_ESCAPE");
        
        SQLException ex = Util.sqlException(Trace.INVALID_ESCAPE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INTEGRITY_CONSTRAINT_VIOLATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INTEGRITY_CONSTRAINT_VIOLATION() throws Exception {
        println("testSqlException_INTEGRITY_CONSTRAINT_VIOLATION");
        
        SQLException ex = Util.sqlException(Trace.INTEGRITY_CONSTRAINT_VIOLATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.VIOLATION_OF_UNIQUE_INDEX,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_VIOLATION_OF_UNIQUE_INDEX() throws Exception {
        println("testSqlException_VIOLATION_OF_UNIQUE_INDEX");
        
        SQLException ex = Util.sqlException(Trace.VIOLATION_OF_UNIQUE_INDEX,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRY_TO_INSERT_NULL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRY_TO_INSERT_NULL() throws Exception {
        println("testSqlException_TRY_TO_INSERT_NULL");
        
        SQLException ex = Util.sqlException(Trace.TRY_TO_INSERT_NULL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNEXPECTED_TOKEN,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNEXPECTED_TOKEN() throws Exception {
        println("testSqlException_UNEXPECTED_TOKEN");
        
        SQLException ex = Util.sqlException(Trace.UNEXPECTED_TOKEN,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNEXPECTED_END_OF_COMMAND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNEXPECTED_END_OF_COMMAND() throws Exception {
        println("testSqlException_UNEXPECTED_END_OF_COMMAND");
        
        SQLException ex = Util.sqlException(Trace.UNEXPECTED_END_OF_COMMAND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNKNOWN_FUNCTION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNKNOWN_FUNCTION() throws Exception {
        println("testSqlException_UNKNOWN_FUNCTION");
        
        SQLException ex = Util.sqlException(Trace.UNKNOWN_FUNCTION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NEED_AGGREGATE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NEED_AGGREGATE() throws Exception {
        println("testSqlException_NEED_AGGREGATE");
        
        SQLException ex = Util.sqlException(Trace.NEED_AGGREGATE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SUM_OF_NON_NUMERIC,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SUM_OF_NON_NUMERIC() throws Exception {
        println("testSqlException_SUM_OF_NON_NUMERIC");
        
        SQLException ex = Util.sqlException(Trace.SUM_OF_NON_NUMERIC,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.WRONG_DATA_TYPE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_WRONG_DATA_TYPE() throws Exception {
        println("testSqlException_WRONG_DATA_TYPE");
        
        SQLException ex = Util.sqlException(Trace.WRONG_DATA_TYPE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CARDINALITY_VIOLATION_NO_SUBCLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CARDINALITY_VIOLATION_NO_SUBCLASS() throws Exception {
        println("testSqlException_CARDINALITY_VIOLATION_NO_SUBCLASS");
        
        SQLException ex = Util.sqlException(Trace.CARDINALITY_VIOLATION_NO_SUBCLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SERIALIZATION_FAILURE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SERIALIZATION_FAILURE() throws Exception {
        println("testSqlException_SERIALIZATION_FAILURE");
        
        SQLException ex = Util.sqlException(Trace.SERIALIZATION_FAILURE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRANSFER_CORRUPTED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRANSFER_CORRUPTED() throws Exception {
        println("testSqlException_TRANSFER_CORRUPTED");
        
        SQLException ex = Util.sqlException(Trace.TRANSFER_CORRUPTED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.FUNCTION_NOT_SUPPORTED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_FUNCTION_NOT_SUPPORTED() throws Exception {
        println("testSqlException_FUNCTION_NOT_SUPPORTED");
        
        SQLException ex = Util.sqlException(Trace.FUNCTION_NOT_SUPPORTED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TABLE_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TABLE_ALREADY_EXISTS() throws Exception {
        println("testSqlException_TABLE_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.TABLE_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TABLE_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TABLE_NOT_FOUND() throws Exception {
        println("testSqlException_TABLE_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.TABLE_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INDEX_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INDEX_ALREADY_EXISTS() throws Exception {
        println("testSqlException_INDEX_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.INDEX_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SECOND_PRIMARY_KEY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SECOND_PRIMARY_KEY() throws Exception {
        println("testSqlException_SECOND_PRIMARY_KEY");
        
        SQLException ex = Util.sqlException(Trace.SECOND_PRIMARY_KEY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DROP_PRIMARY_KEY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DROP_PRIMARY_KEY() throws Exception {
        println("testSqlException_DROP_PRIMARY_KEY");
        
        SQLException ex = Util.sqlException(Trace.DROP_PRIMARY_KEY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INDEX_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INDEX_NOT_FOUND() throws Exception {
        println("testSqlException_INDEX_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.INDEX_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_ALREADY_EXISTS() throws Exception {
        println("testSqlException_COLUMN_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_NOT_FOUND() throws Exception {
        println("testSqlException_COLUMN_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.FILE_IO_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_FILE_IO_ERROR() throws Exception {
        println("testSqlException_FILE_IO_ERROR");
        
        SQLException ex = Util.sqlException(Trace.FILE_IO_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.WRONG_DATABASE_FILE_VERSION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_WRONG_DATABASE_FILE_VERSION() throws Exception {
        println("testSqlException_WRONG_DATABASE_FILE_VERSION");
        
        SQLException ex = Util.sqlException(Trace.WRONG_DATABASE_FILE_VERSION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATABASE_IS_READONLY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATABASE_IS_READONLY() throws Exception {
        println("testSqlException_DATABASE_IS_READONLY");
        
        SQLException ex = Util.sqlException(Trace.DATABASE_IS_READONLY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATA_IS_READONLY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATA_IS_READONLY() throws Exception {
        println("testSqlException_DATA_IS_READONLY");
        
        SQLException ex = Util.sqlException(Trace.DATA_IS_READONLY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ACCESS_IS_DENIED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ACCESS_IS_DENIED() throws Exception {
        println("testSqlException_ACCESS_IS_DENIED");
        
        SQLException ex = Util.sqlException(Trace.ACCESS_IS_DENIED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INPUTSTREAM_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INPUTSTREAM_ERROR() throws Exception {
        println("testSqlException_INPUTSTREAM_ERROR");
        
        SQLException ex = Util.sqlException(Trace.INPUTSTREAM_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_DATA_IS_AVAILABLE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_DATA_IS_AVAILABLE() throws Exception {
        println("testSqlException_NO_DATA_IS_AVAILABLE");
        
        SQLException ex = Util.sqlException(Trace.NO_DATA_IS_AVAILABLE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.USER_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_USER_ALREADY_EXISTS() throws Exception {
        println("testSqlException_USER_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.USER_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.USER_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_USER_NOT_FOUND() throws Exception {
        println("testSqlException_USER_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.USER_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ASSERT_FAILED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ASSERT_FAILED() throws Exception {
        println("testSqlException_ASSERT_FAILED");
        
        SQLException ex = Util.sqlException(Trace.ASSERT_FAILED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.EXTERNAL_STOP,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_EXTERNAL_STOP() throws Exception {
        println("testSqlException_EXTERNAL_STOP");
        
        SQLException ex = Util.sqlException(Trace.EXTERNAL_STOP,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.GENERAL_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_GENERAL_ERROR() throws Exception {
        println("testSqlException_GENERAL_ERROR");
        
        SQLException ex = Util.sqlException(Trace.GENERAL_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.WRONG_OUT_PARAMETER,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_WRONG_OUT_PARAMETER() throws Exception {
        println("testSqlException_WRONG_OUT_PARAMETER");
        
        SQLException ex = Util.sqlException(Trace.WRONG_OUT_PARAMETER,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.FUNCTION_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_FUNCTION_NOT_FOUND() throws Exception {
        println("testSqlException_FUNCTION_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.FUNCTION_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRIGGER_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRIGGER_NOT_FOUND() throws Exception {
        println("testSqlException_TRIGGER_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.TRIGGER_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SAVEPOINT_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SAVEPOINT_NOT_FOUND() throws Exception {
        println("testSqlException_SAVEPOINT_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.SAVEPOINT_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.LABEL_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_LABEL_REQUIRED() throws Exception {
        println("testSqlException_LABEL_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.LABEL_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.WRONG_DEFAULT_CLAUSE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_WRONG_DEFAULT_CLAUSE() throws Exception {
        println("testSqlException_WRONG_DEFAULT_CLAUSE");
        
        SQLException ex = Util.sqlException(Trace.WRONG_DEFAULT_CLAUSE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.FOREIGN_KEY_NOT_ALLOWED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_FOREIGN_KEY_NOT_ALLOWED() throws Exception {
        println("testSqlException_FOREIGN_KEY_NOT_ALLOWED");
        
        SQLException ex = Util.sqlException(Trace.FOREIGN_KEY_NOT_ALLOWED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNKNOWN_DATA_SOURCE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNKNOWN_DATA_SOURCE() throws Exception {
        println("testSqlException_UNKNOWN_DATA_SOURCE");
        
        SQLException ex = Util.sqlException(Trace.UNKNOWN_DATA_SOURCE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.BAD_INDEX_CONSTRAINT_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_BAD_INDEX_CONSTRAINT_NAME() throws Exception {
        println("testSqlException_BAD_INDEX_CONSTRAINT_NAME");
        
        SQLException ex = Util.sqlException(Trace.BAD_INDEX_CONSTRAINT_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DROP_FK_INDEX,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DROP_FK_INDEX() throws Exception {
        println("testSqlException_DROP_FK_INDEX");
        
        SQLException ex = Util.sqlException(Trace.DROP_FK_INDEX,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.RESULTSET_FORWARD_ONLY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_RESULTSET_FORWARD_ONLY() throws Exception {
        println("testSqlException_RESULTSET_FORWARD_ONLY");
        
        SQLException ex = Util.sqlException(Trace.RESULTSET_FORWARD_ONLY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.VIEW_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_VIEW_ALREADY_EXISTS() throws Exception {
        println("testSqlException_VIEW_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.VIEW_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.VIEW_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_VIEW_NOT_FOUND() throws Exception {
        println("testSqlException_VIEW_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.VIEW_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NOT_A_TABLE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NOT_A_TABLE() throws Exception {
        println("testSqlException_NOT_A_TABLE");
        
        SQLException ex = Util.sqlException(Trace.NOT_A_TABLE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SYSTEM_INDEX,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SYSTEM_INDEX() throws Exception {
        println("testSqlException_SYSTEM_INDEX");
        
        SQLException ex = Util.sqlException(Trace.SYSTEM_INDEX,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_TYPE_MISMATCH,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_TYPE_MISMATCH() throws Exception {
        println("testSqlException_COLUMN_TYPE_MISMATCH");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_TYPE_MISMATCH,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.BAD_ADD_COLUMN_DEFINITION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_BAD_ADD_COLUMN_DEFINITION() throws Exception {
        println("testSqlException_BAD_ADD_COLUMN_DEFINITION");
        
        SQLException ex = Util.sqlException(Trace.BAD_ADD_COLUMN_DEFINITION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DROP_SYSTEM_CONSTRAINT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DROP_SYSTEM_CONSTRAINT() throws Exception {
        println("testSqlException_DROP_SYSTEM_CONSTRAINT");
        
        SQLException ex = Util.sqlException(Trace.DROP_SYSTEM_CONSTRAINT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CONSTRAINT_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CONSTRAINT_ALREADY_EXISTS() throws Exception {
        println("testSqlException_CONSTRAINT_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.CONSTRAINT_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CONSTRAINT_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CONSTRAINT_NOT_FOUND() throws Exception {
        println("testSqlException_CONSTRAINT_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.CONSTRAINT_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_INVALID_ARGUMENT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_INVALID_ARGUMENT() throws Exception {
        println("testSqlException_JDBC_INVALID_ARGUMENT");
        
        SQLException ex = Util.sqlException(Trace.JDBC_INVALID_ARGUMENT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATABASE_IS_MEMORY_ONLY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATABASE_IS_MEMORY_ONLY() throws Exception {
        println("testSqlException_DATABASE_IS_MEMORY_ONLY");
        
        SQLException ex = Util.sqlException(Trace.DATABASE_IS_MEMORY_ONLY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OUTER_JOIN_CONDITION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OUTER_JOIN_CONDITION() throws Exception {
        println("testSqlException_OUTER_JOIN_CONDITION");
        
        SQLException ex = Util.sqlException(Trace.OUTER_JOIN_CONDITION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NUMERIC_VALUE_OUT_OF_RANGE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NUMERIC_VALUE_OUT_OF_RANGE() throws Exception {
        println("testSqlException_NUMERIC_VALUE_OUT_OF_RANGE");
        
        SQLException ex = Util.sqlException(Trace.NUMERIC_VALUE_OUT_OF_RANGE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_SOFTWARE_MODULE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_SOFTWARE_MODULE() throws Exception {
        println("testSqlException_MISSING_SOFTWARE_MODULE");
        
        SQLException ex = Util.sqlException(Trace.MISSING_SOFTWARE_MODULE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NOT_IN_AGGREGATE_OR_GROUP_BY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NOT_IN_AGGREGATE_OR_GROUP_BY() throws Exception {
        println("testSqlException_NOT_IN_AGGREGATE_OR_GROUP_BY");
        
        SQLException ex = Util.sqlException(Trace.NOT_IN_AGGREGATE_OR_GROUP_BY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_GROUP_BY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_GROUP_BY() throws Exception {
        println("testSqlException_INVALID_GROUP_BY");
        
        SQLException ex = Util.sqlException(Trace.INVALID_GROUP_BY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_HAVING,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_HAVING() throws Exception {
        println("testSqlException_INVALID_HAVING");
        
        SQLException ex = Util.sqlException(Trace.INVALID_HAVING,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_ORDER_BY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_ORDER_BY() throws Exception {
        println("testSqlException_INVALID_ORDER_BY");
        
        SQLException ex = Util.sqlException(Trace.INVALID_ORDER_BY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_ORDER_BY_IN_DISTINCT_SELECT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_ORDER_BY_IN_DISTINCT_SELECT() throws Exception {
        println("testSqlException_INVALID_ORDER_BY_IN_DISTINCT_SELECT");
        
        SQLException ex = Util.sqlException(Trace.INVALID_ORDER_BY_IN_DISTINCT_SELECT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OUT_OF_MEMORY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OUT_OF_MEMORY() throws Exception {
        println("testSqlException_OUT_OF_MEMORY");
        
        SQLException ex = Util.sqlException(Trace.OUT_OF_MEMORY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OPERATION_NOT_SUPPORTED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OPERATION_NOT_SUPPORTED() throws Exception {
        println("testSqlException_OPERATION_NOT_SUPPORTED");
        
        SQLException ex = Util.sqlException(Trace.OPERATION_NOT_SUPPORTED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_IDENTIFIER,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_IDENTIFIER() throws Exception {
        println("testSqlException_INVALID_IDENTIFIER");
        
        SQLException ex = Util.sqlException(Trace.INVALID_IDENTIFIER,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_TABLE_SOURCE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_TABLE_SOURCE() throws Exception {
        println("testSqlException_TEXT_TABLE_SOURCE");
        
        SQLException ex = Util.sqlException(Trace.TEXT_TABLE_SOURCE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_FILE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_FILE() throws Exception {
        println("testSqlException_TEXT_FILE");
        
        SQLException ex = Util.sqlException(Trace.TEXT_FILE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ERROR_IN_SCRIPT_FILE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ERROR_IN_SCRIPT_FILE() throws Exception {
        println("testSqlException_ERROR_IN_SCRIPT_FILE");
        
        SQLException ex = Util.sqlException(Trace.ERROR_IN_SCRIPT_FILE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NULL_LITERAL_NOT_ALLOWED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NULL_LITERAL_NOT_ALLOWED() throws Exception {
        println("testSqlException_NULL_LITERAL_NOT_ALLOWED");
        
        SQLException ex = Util.sqlException(Trace.NULL_LITERAL_NOT_ALLOWED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SOCKET_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SOCKET_ERROR() throws Exception {
        println("testSqlException_SOCKET_ERROR");
        
        SQLException ex = Util.sqlException(Trace.SOCKET_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_CHARACTER_ENCODING,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_CHARACTER_ENCODING() throws Exception {
        println("testSqlException_INVALID_CHARACTER_ENCODING");
        
        SQLException ex = Util.sqlException(Trace.INVALID_CHARACTER_ENCODING,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNEXPECTED_EXCEPTION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNEXPECTED_EXCEPTION() throws Exception {
        println("testSqlException_UNEXPECTED_EXCEPTION");
        
        SQLException ex = Util.sqlException(Trace.UNEXPECTED_EXCEPTION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATABASE_NOT_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATABASE_NOT_EXISTS() throws Exception {
        println("testSqlException_DATABASE_NOT_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.DATABASE_NOT_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_CONVERSION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_CONVERSION() throws Exception {
        println("testSqlException_INVALID_CONVERSION");
        
        SQLException ex = Util.sqlException(Trace.INVALID_CONVERSION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ERROR_IN_BINARY_SCRIPT_1,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ERROR_IN_BINARY_SCRIPT_1() throws Exception {
        println("testSqlException_ERROR_IN_BINARY_SCRIPT_1");
        
        SQLException ex = Util.sqlException(Trace.ERROR_IN_BINARY_SCRIPT_1,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ERROR_IN_BINARY_SCRIPT_2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ERROR_IN_BINARY_SCRIPT_2() throws Exception {
        println("testSqlException_ERROR_IN_BINARY_SCRIPT_2");
        
        SQLException ex = Util.sqlException(Trace.ERROR_IN_BINARY_SCRIPT_2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.GENERAL_IO_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_GENERAL_IO_ERROR() throws Exception {
        println("testSqlException_GENERAL_IO_ERROR");
        
        SQLException ex = Util.sqlException(Trace.GENERAL_IO_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.EXPRESSION_NOT_SUPPORTED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_EXPRESSION_NOT_SUPPORTED() throws Exception {
        println("testSqlException_EXPRESSION_NOT_SUPPORTED");
        
        SQLException ex = Util.sqlException(Trace.EXPRESSION_NOT_SUPPORTED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Constraint_violation,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Constraint_violation() throws Exception {
        println("testSqlException_Constraint_violation");
        
        SQLException ex = Util.sqlException(Trace.Constraint_violation,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Database_dropTable,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Database_dropTable() throws Exception {
        println("testSqlException_Database_dropTable");
        
        SQLException ex = Util.sqlException(Trace.Database_dropTable,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ERROR_IN_CONSTRAINT_COLUMN_LIST,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ERROR_IN_CONSTRAINT_COLUMN_LIST() throws Exception {
        println("testSqlException_ERROR_IN_CONSTRAINT_COLUMN_LIST");
        
        SQLException ex = Util.sqlException(Trace.ERROR_IN_CONSTRAINT_COLUMN_LIST,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TABLE_HAS_NO_PRIMARY_KEY,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TABLE_HAS_NO_PRIMARY_KEY() throws Exception {
        println("testSqlException_TABLE_HAS_NO_PRIMARY_KEY");
        
        SQLException ex = Util.sqlException(Trace.TABLE_HAS_NO_PRIMARY_KEY,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.VIOLATION_OF_UNIQUE_CONSTRAINT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_VIOLATION_OF_UNIQUE_CONSTRAINT() throws Exception {
        println("testSqlException_VIOLATION_OF_UNIQUE_CONSTRAINT");
        
        SQLException ex = Util.sqlException(Trace.VIOLATION_OF_UNIQUE_CONSTRAINT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_DEFAULT_VALUE_FOR_COLUMN,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_DEFAULT_VALUE_FOR_COLUMN() throws Exception {
        println("testSqlException_NO_DEFAULT_VALUE_FOR_COLUMN");
        
        SQLException ex = Util.sqlException(Trace.NO_DEFAULT_VALUE_FOR_COLUMN,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NOT_A_CONDITION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NOT_A_CONDITION() throws Exception {
        println("testSqlException_NOT_A_CONDITION");
        
        SQLException ex = Util.sqlException(Trace.NOT_A_CONDITION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DatabaseManager_getDatabase,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DatabaseManager_getDatabase() throws Exception {
        println("testSqlException_DatabaseManager_getDatabase");
        
        SQLException ex = Util.sqlException(Trace.DatabaseManager_getDatabase,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DatabaseScriptReader_readDDL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DatabaseScriptReader_readDDL() throws Exception {
        println("testSqlException_DatabaseScriptReader_readDDL");
        
        SQLException ex = Util.sqlException(Trace.DatabaseScriptReader_readDDL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DatabaseScriptReader_readExistingData,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DatabaseScriptReader_readExistingData() throws Exception {
        println("testSqlException_DatabaseScriptReader_readExistingData");
        
        SQLException ex = Util.sqlException(Trace.DatabaseScriptReader_readExistingData,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Message_Pair,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Message_Pair() throws Exception {
        println("testSqlException_Message_Pair");
        
        SQLException ex = Util.sqlException(Trace.Message_Pair,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.LOAD_SAVE_PROPERTIES,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_LOAD_SAVE_PROPERTIES() throws Exception {
        println("testSqlException_LOAD_SAVE_PROPERTIES");
        
        SQLException ex = Util.sqlException(Trace.LOAD_SAVE_PROPERTIES,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_TRANSACTION_STATE_NO_SUBCLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_TRANSACTION_STATE_NO_SUBCLASS() throws Exception {
        println("testSqlException_INVALID_TRANSACTION_STATE_NO_SUBCLASS");
        
        SQLException ex = Util.sqlException(Trace.INVALID_TRANSACTION_STATE_NO_SUBCLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_ILLEGAL_BRI_SCOPE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_ILLEGAL_BRI_SCOPE() throws Exception {
        println("testSqlException_JDBC_ILLEGAL_BRI_SCOPE");
        
        SQLException ex = Util.sqlException(Trace.JDBC_ILLEGAL_BRI_SCOPE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_NO_RESULT_SET_METADATA,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_NO_RESULT_SET_METADATA() throws Exception {
        println("testSqlException_JDBC_NO_RESULT_SET_METADATA");
        
        SQLException ex = Util.sqlException(Trace.JDBC_NO_RESULT_SET_METADATA,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_NO_RESULT_SET,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_NO_RESULT_SET() throws Exception {
        println("testSqlException_JDBC_NO_RESULT_SET");
        
        SQLException ex = Util.sqlException(Trace.JDBC_NO_RESULT_SET,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_CLOSEBRACKET,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_CLOSEBRACKET() throws Exception {
        println("testSqlException_MISSING_CLOSEBRACKET");
        
        SQLException ex = Util.sqlException(Trace.MISSING_CLOSEBRACKET,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ITSNS_OVERWRITE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ITSNS_OVERWRITE() throws Exception {
        println("testSqlException_ITSNS_OVERWRITE");
        
        SQLException ex = Util.sqlException(Trace.ITSNS_OVERWRITE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_IS_IN_INDEX,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_IS_IN_INDEX() throws Exception {
        println("testSqlException_COLUMN_IS_IN_INDEX");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_IS_IN_INDEX,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.STRING_DATA_TRUNCATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_STRING_DATA_TRUNCATION() throws Exception {
        println("testSqlException_STRING_DATA_TRUNCATION");
        
        SQLException ex = Util.sqlException(Trace.STRING_DATA_TRUNCATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.QUOTED_IDENTIFIER_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_QUOTED_IDENTIFIER_REQUIRED() throws Exception {
        println("testSqlException_QUOTED_IDENTIFIER_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.QUOTED_IDENTIFIER_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.STATEMENT_IS_CLOSED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_STATEMENT_IS_CLOSED() throws Exception {
        println("testSqlException_STATEMENT_IS_CLOSED");
        
        SQLException ex = Util.sqlException(Trace.STATEMENT_IS_CLOSED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATA_FILE_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATA_FILE_ERROR() throws Exception {
        println("testSqlException_DATA_FILE_ERROR");
        
        SQLException ex = Util.sqlException(Trace.DATA_FILE_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlDateTime_null_string,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlDateTime_null_string() throws Exception {
        println("testSqlException_HsqlDateTime_null_string");
        
        SQLException ex = Util.sqlException(Trace.HsqlDateTime_null_string,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlDateTime_null_date,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlDateTime_null_date() throws Exception {
        println("testSqlException_HsqlDateTime_null_date");
        
        SQLException ex = Util.sqlException(Trace.HsqlDateTime_null_date,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlProperties_load,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlProperties_load() throws Exception {
        println("testSqlException_HsqlProperties_load");
        
        SQLException ex = Util.sqlException(Trace.HsqlProperties_load,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlSocketFactorySecure_verify,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlSocketFactorySecure_verify() throws Exception {
        println("testSqlException_HsqlSocketFactorySecure_verify");
        
        SQLException ex = Util.sqlException(Trace.HsqlSocketFactorySecure_verify,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlSocketFactorySecure_verify2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlSocketFactorySecure_verify2() throws Exception {
        println("testSqlException_HsqlSocketFactorySecure_verify2");
        
        SQLException ex = Util.sqlException(Trace.HsqlSocketFactorySecure_verify2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_CONNECTION_NATIVE_SQL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_CONNECTION_NATIVE_SQL() throws Exception {
        println("testSqlException_JDBC_CONNECTION_NATIVE_SQL");
        
        SQLException ex = Util.sqlException(Trace.JDBC_CONNECTION_NATIVE_SQL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.HsqlSocketFactorySecure_verify3,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_HsqlSocketFactorySecure_verify3() throws Exception {
        println("testSqlException_HsqlSocketFactorySecure_verify3");
        
        SQLException ex = Util.sqlException(Trace.HsqlSocketFactorySecure_verify3,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_STATEMENT_EXECUTE_UPDATE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_STATEMENT_EXECUTE_UPDATE() throws Exception {
        println("testSqlException_JDBC_STATEMENT_EXECUTE_UPDATE");
        
        SQLException ex = Util.sqlException(Trace.JDBC_STATEMENT_EXECUTE_UPDATE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.LockFile_checkHeartbeat,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_LockFile_checkHeartbeat() throws Exception {
        println("testSqlException_LockFile_checkHeartbeat");
        
        SQLException ex = Util.sqlException(Trace.LockFile_checkHeartbeat,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.LockFile_checkHeartbeat2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_LockFile_checkHeartbeat2() throws Exception {
        println("testSqlException_LockFile_checkHeartbeat2");
        
        SQLException ex = Util.sqlException(Trace.LockFile_checkHeartbeat2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_STRING_HAS_NEWLINE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_STRING_HAS_NEWLINE() throws Exception {
        println("testSqlException_TEXT_STRING_HAS_NEWLINE");
        
        SQLException ex = Util.sqlException(Trace.TEXT_STRING_HAS_NEWLINE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Result_Result,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Result_Result() throws Exception {
        println("testSqlException_Result_Result");
        
        SQLException ex = Util.sqlException(Trace.Result_Result,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SERVER_NO_DATABASE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SERVER_NO_DATABASE() throws Exception {
        println("testSqlException_SERVER_NO_DATABASE");
        
        SQLException ex = Util.sqlException(Trace.SERVER_NO_DATABASE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Server_openServerSocket,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Server_openServerSocket() throws Exception {
        println("testSqlException_Server_openServerSocket");
        
        SQLException ex = Util.sqlException(Trace.Server_openServerSocket,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Server_openServerSocket2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Server_openServerSocket2() throws Exception {
        println("testSqlException_Server_openServerSocket2");
        
        SQLException ex = Util.sqlException(Trace.Server_openServerSocket2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_TABLE_HEADER,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_TABLE_HEADER() throws Exception {
        println("testSqlException_TEXT_TABLE_HEADER");
        
        SQLException ex = Util.sqlException(Trace.TEXT_TABLE_HEADER,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_PARAMETER_NOT_SET,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_PARAMETER_NOT_SET() throws Exception {
        println("testSqlException_JDBC_PARAMETER_NOT_SET");
        
        SQLException ex = Util.sqlException(Trace.JDBC_PARAMETER_NOT_SET,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_LIMIT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_LIMIT() throws Exception {
        println("testSqlException_INVALID_LIMIT");
        
        SQLException ex = Util.sqlException(Trace.INVALID_LIMIT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_STATEMENT_NOT_ROW_COUNT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_STATEMENT_NOT_ROW_COUNT() throws Exception {
        println("testSqlException_JDBC_STATEMENT_NOT_ROW_COUNT");
        
        SQLException ex = Util.sqlException(Trace.JDBC_STATEMENT_NOT_ROW_COUNT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_STATEMENT_NOT_RESULTSET,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_STATEMENT_NOT_RESULTSET() throws Exception {
        println("testSqlException_JDBC_STATEMENT_NOT_RESULTSET");
        
        SQLException ex = Util.sqlException(Trace.JDBC_STATEMENT_NOT_RESULTSET,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.AMBIGUOUS_COLUMN_REFERENCE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_AMBIGUOUS_COLUMN_REFERENCE() throws Exception {
        println("testSqlException_AMBIGUOUS_COLUMN_REFERENCE");
        
        SQLException ex = Util.sqlException(Trace.AMBIGUOUS_COLUMN_REFERENCE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CHECK_CONSTRAINT_VIOLATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CHECK_CONSTRAINT_VIOLATION() throws Exception {
        println("testSqlException_CHECK_CONSTRAINT_VIOLATION");
        
        SQLException ex = Util.sqlException(Trace.CHECK_CONSTRAINT_VIOLATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_RESULTSET_IS_CLOSED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_RESULTSET_IS_CLOSED() throws Exception {
        println("testSqlException_JDBC_RESULTSET_IS_CLOSED");
        
        SQLException ex = Util.sqlException(Trace.JDBC_RESULTSET_IS_CLOSED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SINGLE_COLUMN_EXPECTED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SINGLE_COLUMN_EXPECTED() throws Exception {
        println("testSqlException_SINGLE_COLUMN_EXPECTED");
        
        SQLException ex = Util.sqlException(Trace.SINGLE_COLUMN_EXPECTED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TOKEN_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TOKEN_REQUIRED() throws Exception {
        println("testSqlException_TOKEN_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.TOKEN_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ORDER_LIMIT_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ORDER_LIMIT_REQUIRED() throws Exception {
        println("testSqlException_ORDER_LIMIT_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.ORDER_LIMIT_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRIGGER_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRIGGER_ALREADY_EXISTS() throws Exception {
        println("testSqlException_TRIGGER_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.TRIGGER_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ASSERT_DIRECT_EXEC_WITH_PARAM,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ASSERT_DIRECT_EXEC_WITH_PARAM() throws Exception {
        println("testSqlException_ASSERT_DIRECT_EXEC_WITH_PARAM");
        
        SQLException ex = Util.sqlException(Trace.ASSERT_DIRECT_EXEC_WITH_PARAM,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_compareValues,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_compareValues() throws Exception {
        println("testSqlException_Expression_compareValues");
        
        SQLException ex = Util.sqlException(Trace.Expression_compareValues,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_LIMIT_EXPRESSION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_LIMIT_EXPRESSION() throws Exception {
        println("testSqlException_INVALID_LIMIT_EXPRESSION");
        
        SQLException ex = Util.sqlException(Trace.INVALID_LIMIT_EXPRESSION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_TOP_EXPRESSION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_TOP_EXPRESSION() throws Exception {
        println("testSqlException_INVALID_TOP_EXPRESSION");
        
        SQLException ex = Util.sqlException(Trace.INVALID_TOP_EXPRESSION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_CONSTRAINT_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_CONSTRAINT_REQUIRED() throws Exception {
        println("testSqlException_SQL_CONSTRAINT_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.SQL_CONSTRAINT_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TableWorks_dropConstraint,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TableWorks_dropConstraint() throws Exception {
        println("testSqlException_TableWorks_dropConstraint");
        
        SQLException ex = Util.sqlException(Trace.TableWorks_dropConstraint,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_TABLE_SOURCE_FILENAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_TABLE_SOURCE_FILENAME() throws Exception {
        println("testSqlException_TEXT_TABLE_SOURCE_FILENAME");
        
        SQLException ex = Util.sqlException(Trace.TEXT_TABLE_SOURCE_FILENAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_TABLE_SOURCE_VALUE_MISSING,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_TABLE_SOURCE_VALUE_MISSING() throws Exception {
        println("testSqlException_TEXT_TABLE_SOURCE_VALUE_MISSING");
        
        SQLException ex = Util.sqlException(Trace.TEXT_TABLE_SOURCE_VALUE_MISSING,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_TABLE_SOURCE_SEPARATOR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_TABLE_SOURCE_SEPARATOR() throws Exception {
        println("testSqlException_TEXT_TABLE_SOURCE_SEPARATOR");
        
        SQLException ex = Util.sqlException(Trace.TEXT_TABLE_SOURCE_SEPARATOR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNSUPPORTED_PARAM_CLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNSUPPORTED_PARAM_CLASS() throws Exception {
        println("testSqlException_UNSUPPORTED_PARAM_CLASS");
        
        SQLException ex = Util.sqlException(Trace.UNSUPPORTED_PARAM_CLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.JDBC_NULL_STREAM,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_JDBC_NULL_STREAM() throws Exception {
        println("testSqlException_JDBC_NULL_STREAM");
        
        SQLException ex = Util.sqlException(Trace.JDBC_NULL_STREAM,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT() throws Exception {
        println("testSqlException_INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT");
        
        SQLException ex = Util.sqlException(Trace.INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.QuotedTextDatabaseRowInput_getField,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_QuotedTextDatabaseRowInput_getField() throws Exception {
        println("testSqlException_QuotedTextDatabaseRowInput_getField");
        
        SQLException ex = Util.sqlException(Trace.QuotedTextDatabaseRowInput_getField,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.QuotedTextDatabaseRowInput_getField2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_QuotedTextDatabaseRowInput_getField2() throws Exception {
        println("testSqlException_QuotedTextDatabaseRowInput_getField2");
        
        SQLException ex = Util.sqlException(Trace.QuotedTextDatabaseRowInput_getField2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextDatabaseRowInput_getField,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextDatabaseRowInput_getField() throws Exception {
        println("testSqlException_TextDatabaseRowInput_getField");
        
        SQLException ex = Util.sqlException(Trace.TextDatabaseRowInput_getField,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextDatabaseRowInput_getField2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextDatabaseRowInput_getField2() throws Exception {
        println("testSqlException_TextDatabaseRowInput_getField2");
        
        SQLException ex = Util.sqlException(Trace.TextDatabaseRowInput_getField2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextDatabaseRowInput_getField3,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextDatabaseRowInput_getField3() throws Exception {
        println("testSqlException_TextDatabaseRowInput_getField3");
        
        SQLException ex = Util.sqlException(Trace.TextDatabaseRowInput_getField3,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Parser_ambiguous_between1,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Parser_ambiguous_between1() throws Exception {
        println("testSqlException_Parser_ambiguous_between1");
        
        SQLException ex = Util.sqlException(Trace.Parser_ambiguous_between1,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SEQUENCE_REFERENCED_BY_VIEW,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SEQUENCE_REFERENCED_BY_VIEW() throws Exception {
        println("testSqlException_SEQUENCE_REFERENCED_BY_VIEW");
        
        SQLException ex = Util.sqlException(Trace.SEQUENCE_REFERENCED_BY_VIEW,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextCache_openning_file_error,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextCache_openning_file_error() throws Exception {
        println("testSqlException_TextCache_openning_file_error");
        
        SQLException ex = Util.sqlException(Trace.TextCache_openning_file_error,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextCache_closing_file_error,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextCache_closing_file_error() throws Exception {
        println("testSqlException_TextCache_closing_file_error");
        
        SQLException ex = Util.sqlException(Trace.TextCache_closing_file_error,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TextCache_purging_file_error,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TextCache_purging_file_error() throws Exception {
        println("testSqlException_TextCache_purging_file_error");
        
        SQLException ex = Util.sqlException(Trace.TextCache_purging_file_error,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SEQUENCE_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SEQUENCE_NOT_FOUND() throws Exception {
        println("testSqlException_SEQUENCE_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.SEQUENCE_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SEQUENCE_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SEQUENCE_ALREADY_EXISTS() throws Exception {
        println("testSqlException_SEQUENCE_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.SEQUENCE_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TABLE_REFERENCED_CONSTRAINT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TABLE_REFERENCED_CONSTRAINT() throws Exception {
        println("testSqlException_TABLE_REFERENCED_CONSTRAINT");
        
        SQLException ex = Util.sqlException(Trace.TABLE_REFERENCED_CONSTRAINT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TABLE_REFERENCED_VIEW,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TABLE_REFERENCED_VIEW() throws Exception {
        println("testSqlException_TABLE_REFERENCED_VIEW");
        
        SQLException ex = Util.sqlException(Trace.TABLE_REFERENCED_VIEW,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TEXT_SOURCE_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TEXT_SOURCE_EXISTS() throws Exception {
        println("testSqlException_TEXT_SOURCE_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.TEXT_SOURCE_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_IS_REFERENCED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_IS_REFERENCED() throws Exception {
        println("testSqlException_COLUMN_IS_REFERENCED");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_IS_REFERENCED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.FUNCTION_CALL_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_FUNCTION_CALL_ERROR() throws Exception {
        println("testSqlException_FUNCTION_CALL_ERROR");
        
        SQLException ex = Util.sqlException(Trace.FUNCTION_CALL_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRIGGERED_DATA_CHANGE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRIGGERED_DATA_CHANGE() throws Exception {
        println("testSqlException_TRIGGERED_DATA_CHANGE");
        
        SQLException ex = Util.sqlException(Trace.TRIGGERED_DATA_CHANGE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_FUNCTION_ARGUMENT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_FUNCTION_ARGUMENT() throws Exception {
        println("testSqlException_INVALID_FUNCTION_ARGUMENT");
        
        SQLException ex = Util.sqlException(Trace.INVALID_FUNCTION_ARGUMENT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNSUPPORTED_INTERNAL_OPERATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNSUPPORTED_INTERNAL_OPERATION() throws Exception {
        println("testSqlException_UNSUPPORTED_INTERNAL_OPERATION");
        
        SQLException ex = Util.sqlException(Trace.UNSUPPORTED_INTERNAL_OPERATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_PREPARED_STATEMENT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_PREPARED_STATEMENT() throws Exception {
        println("testSqlException_INVALID_PREPARED_STATEMENT");
        
        SQLException ex = Util.sqlException(Trace.INVALID_PREPARED_STATEMENT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CREATE_TRIGGER_COMMAND_1,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CREATE_TRIGGER_COMMAND_1() throws Exception {
        println("testSqlException_CREATE_TRIGGER_COMMAND_1");
        
        SQLException ex = Util.sqlException(Trace.CREATE_TRIGGER_COMMAND_1,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TRIGGER_FUNCTION_CLASS_NOT_FOUND,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TRIGGER_FUNCTION_CLASS_NOT_FOUND() throws Exception {
        println("testSqlException_TRIGGER_FUNCTION_CLASS_NOT_FOUND");
        
        SQLException ex = Util.sqlException(Trace.TRIGGER_FUNCTION_CLASS_NOT_FOUND,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_COLLATION_NAME_NO_SUBCLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_COLLATION_NAME_NO_SUBCLASS() throws Exception {
        println("testSqlException_INVALID_COLLATION_NAME_NO_SUBCLASS");
        
        SQLException ex = Util.sqlException(Trace.INVALID_COLLATION_NAME_NO_SUBCLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DataFileCache_makeRow,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DataFileCache_makeRow() throws Exception {
        println("testSqlException_DataFileCache_makeRow");
        
        SQLException ex = Util.sqlException(Trace.DataFileCache_makeRow,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DataFileCache_open,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DataFileCache_open() throws Exception {
        println("testSqlException_DataFileCache_open");
        
        SQLException ex = Util.sqlException(Trace.DataFileCache_open,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DataFileCache_close,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DataFileCache_close() throws Exception {
        println("testSqlException_DataFileCache_close");
        
        SQLException ex = Util.sqlException(Trace.DataFileCache_close,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypes1,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypes1() throws Exception {
        println("testSqlException_Expression_resolveTypes1");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypes1,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypes2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypes2() throws Exception {
        println("testSqlException_Expression_resolveTypes2");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypes2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypes3,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypes3() throws Exception {
        println("testSqlException_Expression_resolveTypes3");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypes3,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypes4,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypes4() throws Exception {
        println("testSqlException_Expression_resolveTypes4");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypes4,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNRESOLVED_PARAMETER_TYPE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNRESOLVED_PARAMETER_TYPE() throws Exception {
        println("testSqlException_UNRESOLVED_PARAMETER_TYPE");
        
        SQLException ex = Util.sqlException(Trace.UNRESOLVED_PARAMETER_TYPE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypes6,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypes6() throws Exception {
        println("testSqlException_Expression_resolveTypes6");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypes6,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.UNRESOLVED_TYPE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_UNRESOLVED_TYPE() throws Exception {
        println("testSqlException_UNRESOLVED_TYPE");
        
        SQLException ex = Util.sqlException(Trace.UNRESOLVED_TYPE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypeForLike,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypeForLike() throws Exception {
        println("testSqlException_Expression_resolveTypeForLike");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypeForLike,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Expression_resolveTypeForIn2,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Expression_resolveTypeForIn2() throws Exception {
        println("testSqlException_Expression_resolveTypeForIn2");
        
        SQLException ex = Util.sqlException(Trace.Expression_resolveTypeForIn2,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.Session_execute,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_Session_execute() throws Exception {
        println("testSqlException_Session_execute");
        
        SQLException ex = Util.sqlException(Trace.Session_execute,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATA_FILE_IS_FULL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATA_FILE_IS_FULL() throws Exception {
        println("testSqlException_DATA_FILE_IS_FULL");
        
        SQLException ex = Util.sqlException(Trace.DATA_FILE_IS_FULL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.TOO_MANY_IDENTIFIER_PARTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_TOO_MANY_IDENTIFIER_PARTS() throws Exception {
        println("testSqlException_TOO_MANY_IDENTIFIER_PARTS");
        
        SQLException ex = Util.sqlException(Trace.TOO_MANY_IDENTIFIER_PARTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_SCHEMA_NAME_NO_SUBCLASS() throws Exception {
        println("testSqlException_INVALID_SCHEMA_NAME_NO_SUBCLASS");
        
        SQLException ex = Util.sqlException(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DEPENDENT_DATABASE_OBJECT_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DEPENDENT_DATABASE_OBJECT_EXISTS() throws Exception {
        println("testSqlException_DEPENDENT_DATABASE_OBJECT_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.DEPENDENT_DATABASE_OBJECT_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_SUCH_ROLE_GRANT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_SUCH_ROLE_GRANT() throws Exception {
        println("testSqlException_NO_SUCH_ROLE_GRANT");
        
        SQLException ex = Util.sqlException(Trace.NO_SUCH_ROLE_GRANT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_SUCH_ROLE_REVOKE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_SUCH_ROLE_REVOKE() throws Exception {
        println("testSqlException_NO_SUCH_ROLE_REVOKE");
        
        SQLException ex = Util.sqlException(Trace.NO_SUCH_ROLE_REVOKE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NONMOD_ACCOUNT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NONMOD_ACCOUNT() throws Exception {
        println("testSqlException_NONMOD_ACCOUNT");
        
        SQLException ex = Util.sqlException(Trace.NONMOD_ACCOUNT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_SUCH_GRANTEE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_SUCH_GRANTEE() throws Exception {
        println("testSqlException_NO_SUCH_GRANTEE");
        
        SQLException ex = Util.sqlException(Trace.NO_SUCH_GRANTEE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_SYSAUTH,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_SYSAUTH() throws Exception {
        println("testSqlException_MISSING_SYSAUTH");
        
        SQLException ex = Util.sqlException(Trace.MISSING_SYSAUTH,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_GRANTEE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_GRANTEE() throws Exception {
        println("testSqlException_MISSING_GRANTEE");
        
        SQLException ex = Util.sqlException(Trace.MISSING_GRANTEE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CHANGE_GRANTEE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CHANGE_GRANTEE() throws Exception {
        println("testSqlException_CHANGE_GRANTEE");
        
        SQLException ex = Util.sqlException(Trace.CHANGE_GRANTEE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NULL_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NULL_NAME() throws Exception {
        println("testSqlException_NULL_NAME");
        
        SQLException ex = Util.sqlException(Trace.NULL_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ILLEGAL_ROLE_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ILLEGAL_ROLE_NAME() throws Exception {
        println("testSqlException_ILLEGAL_ROLE_NAME");
        
        SQLException ex = Util.sqlException(Trace.ILLEGAL_ROLE_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ROLE_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ROLE_ALREADY_EXISTS() throws Exception {
        println("testSqlException_ROLE_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.ROLE_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_SUCH_ROLE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_SUCH_ROLE() throws Exception {
        println("testSqlException_NO_SUCH_ROLE");
        
        SQLException ex = Util.sqlException(Trace.NO_SUCH_ROLE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_ROLEMANAGER,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_ROLEMANAGER() throws Exception {
        println("testSqlException_MISSING_ROLEMANAGER");
        
        SQLException ex = Util.sqlException(Trace.MISSING_ROLEMANAGER,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.GRANTEE_ALREADY_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_GRANTEE_ALREADY_EXISTS() throws Exception {
        println("testSqlException_GRANTEE_ALREADY_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.GRANTEE_ALREADY_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MISSING_PUBLIC_GRANTEE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MISSING_PUBLIC_GRANTEE() throws Exception {
        println("testSqlException_MISSING_PUBLIC_GRANTEE");
        
        SQLException ex = Util.sqlException(Trace.MISSING_PUBLIC_GRANTEE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NONMOD_GRANTEE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NONMOD_GRANTEE() throws Exception {
        println("testSqlException_NONMOD_GRANTEE");
        
        SQLException ex = Util.sqlException(Trace.NONMOD_GRANTEE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.CIRCULAR_GRANT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_CIRCULAR_GRANT() throws Exception {
        println("testSqlException_CIRCULAR_GRANT");
        
        SQLException ex = Util.sqlException(Trace.CIRCULAR_GRANT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.ALREADY_HAVE_ROLE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_ALREADY_HAVE_ROLE() throws Exception {
        println("testSqlException_ALREADY_HAVE_ROLE");
        
        SQLException ex = Util.sqlException(Trace.ALREADY_HAVE_ROLE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DONT_HAVE_ROLE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DONT_HAVE_ROLE() throws Exception {
        println("testSqlException_DONT_HAVE_ROLE");
        
        SQLException ex = Util.sqlException(Trace.DONT_HAVE_ROLE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.RETRIEVE_NEST_ROLE_FAIL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_RETRIEVE_NEST_ROLE_FAIL() throws Exception {
        println("testSqlException_RETRIEVE_NEST_ROLE_FAIL");
        
        SQLException ex = Util.sqlException(Trace.RETRIEVE_NEST_ROLE_FAIL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NO_SUCH_RIGHT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NO_SUCH_RIGHT() throws Exception {
        println("testSqlException_NO_SUCH_RIGHT");
        
        SQLException ex = Util.sqlException(Trace.NO_SUCH_RIGHT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.IN_SCHEMA_DEFINITION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_IN_SCHEMA_DEFINITION() throws Exception {
        println("testSqlException_IN_SCHEMA_DEFINITION");
        
        SQLException ex = Util.sqlException(Trace.IN_SCHEMA_DEFINITION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.PRIMARY_KEY_NOT_ALLOWED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_PRIMARY_KEY_NOT_ALLOWED() throws Exception {
        println("testSqlException_PRIMARY_KEY_NOT_ALLOWED");
        
        SQLException ex = Util.sqlException(Trace.PRIMARY_KEY_NOT_ALLOWED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_IS_IN_CONSTRAINT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_IS_IN_CONSTRAINT() throws Exception {
        println("testSqlException_COLUMN_IS_IN_CONSTRAINT");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_IS_IN_CONSTRAINT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.COLUMN_SIZE_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_COLUMN_SIZE_REQUIRED() throws Exception {
        println("testSqlException_COLUMN_SIZE_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.COLUMN_SIZE_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.INVALID_SIZE_PRECISION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_INVALID_SIZE_PRECISION() throws Exception {
        println("testSqlException_INVALID_SIZE_PRECISION");
        
        SQLException ex = Util.sqlException(Trace.INVALID_SIZE_PRECISION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.NOT_AUTHORIZED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_NOT_AUTHORIZED() throws Exception {
        println("testSqlException_NOT_AUTHORIZED");
        
        SQLException ex = Util.sqlException(Trace.NOT_AUTHORIZED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_SPECIFIC_ERROR_NO_SUBCLASS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_SPECIFIC_ERROR_NO_SUBCLASS() throws Exception {
        println("testSqlException_OLB_JRT_SPECIFIC_ERROR_NO_SUBCLASS");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_SPECIFIC_ERROR_NO_SUBCLASS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_INVALID_URL,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_INVALID_URL() throws Exception {
        println("testSqlException_OLB_JRT_INVALID_URL");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_INVALID_URL,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_INVALID_JAR_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_INVALID_JAR_NAME() throws Exception {
        println("testSqlException_OLB_JRT_INVALID_JAR_NAME");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_INVALID_JAR_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_INVALID_CLASS_DELETION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_INVALID_CLASS_DELETION() throws Exception {
        println("testSqlException_OLB_JRT_INVALID_CLASS_DELETION");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_INVALID_CLASS_DELETION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_INVALID_JAR_REPLACEMENT,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_INVALID_JAR_REPLACEMENT() throws Exception {
        println("testSqlException_OLB_JRT_INVALID_JAR_REPLACEMENT");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_INVALID_JAR_REPLACEMENT,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_ATTEMPT_TO_REPLACE_UNINSTALLED_JAR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_ATTEMPT_TO_REPLACE_UNINSTALLED_JAR() throws Exception {
        println("testSqlException_OLB_JRT_ATTEMPT_TO_REPLACE_UNINSTALLED_JAR");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_ATTEMPT_TO_REPLACE_UNINSTALLED_JAR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_ATTEMPT_TO_REMOVE_UNINSTALLED_JAR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_ATTEMPT_TO_REMOVE_UNINSTALLED_JAR() throws Exception {
        println("testSqlException_OLB_JRT_ATTEMPT_TO_REMOVE_UNINSTALLED_JAR");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_ATTEMPT_TO_REMOVE_UNINSTALLED_JAR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_INVALID_JAR_NAME_IN_PATH,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_INVALID_JAR_NAME_IN_PATH() throws Exception {
        println("testSqlException_OLB_JRT_INVALID_JAR_NAME_IN_PATH");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_INVALID_JAR_NAME_IN_PATH,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_JRT_UNRESOLVED_CLASS_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_JRT_UNRESOLVED_CLASS_NAME() throws Exception {
        println("testSqlException_OLB_JRT_UNRESOLVED_CLASS_NAME");
        
        SQLException ex = Util.sqlException(Trace.OLB_JRT_UNRESOLVED_CLASS_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_UNSUPPORTED_FEATURE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_UNSUPPORTED_FEATURE() throws Exception {
        println("testSqlException_OLB_UNSUPPORTED_FEATURE");
        
        SQLException ex = Util.sqlException(Trace.OLB_UNSUPPORTED_FEATURE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_INVALID_CLASS_DECLARATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_INVALID_CLASS_DECLARATION() throws Exception {
        println("testSqlException_OLB_INVALID_CLASS_DECLARATION");
        
        SQLException ex = Util.sqlException(Trace.OLB_INVALID_CLASS_DECLARATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_INVALID_COLUMN_NAME,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_INVALID_COLUMN_NAME() throws Exception {
        println("testSqlException_OLB_INVALID_COLUMN_NAME");
        
        SQLException ex = Util.sqlException(Trace.OLB_INVALID_COLUMN_NAME,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.OLB_INVALID_NUMBER_OF_COLUMNS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_OLB_INVALID_NUMBER_OF_COLUMNS() throws Exception {
        println("testSqlException_OLB_INVALID_NUMBER_OF_COLUMNS");
        
        SQLException ex = Util.sqlException(Trace.OLB_INVALID_NUMBER_OF_COLUMNS,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.GENERIC_WARNING,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_GENERIC_WARNING() throws Exception {
        println("testSqlException_GENERIC_WARNING");
        
        SQLException ex = Util.sqlException(Trace.GENERIC_WARNING,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.BLOB_IS_NO_LONGER_VALID,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_BLOB_IS_NO_LONGER_VALID() throws Exception {
        println("testSqlException_BLOB_IS_NO_LONGER_VALID");
        
        SQLException ex = Util.sqlException(Trace.BLOB_IS_NO_LONGER_VALID,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.BLOB_STREAM_IS_CLOSED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_BLOB_STREAM_IS_CLOSED() throws Exception {
        println("testSqlException_BLOB_STREAM_IS_CLOSED");
        
        SQLException ex = Util.sqlException(Trace.BLOB_STREAM_IS_CLOSED,"");       
           
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.BLOB_INCOMPATIBLE_SET_OPERATION,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_BLOB_INCOMPATIBLE_SET_OPERATION() throws Exception {
        println("testSqlException_BLOB_INCOMPATIBLE_SET_OPERATION");
        
        SQLException ex = Util.sqlException(Trace.BLOB_INCOMPATIBLE_SET_OPERATION,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_DATA_SUBSTRING_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_DATA_SUBSTRING_ERROR() throws Exception {
        println("testSqlException_SQL_DATA_SUBSTRING_ERROR");
        
        SQLException ex = Util.sqlException(Trace.SQL_DATA_SUBSTRING_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_DATA_TRIM_ERROR,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_DATA_TRIM_ERROR() throws Exception {
        println("testSqlException_SQL_DATA_TRIM_ERROR");
        
        SQLException ex = Util.sqlException(Trace.SQL_DATA_TRIM_ERROR,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.DATA_SEQUENCE_GENERATOR_LIMIT_EXCEEDED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_DATA_SEQUENCE_GENERATOR_LIMIT_EXCEEDED() throws Exception {
        println("testSqlException_DATA_SEQUENCE_GENERATOR_LIMIT_EXCEEDED");
        
        SQLException ex = Util.sqlException(Trace.DATA_SEQUENCE_GENERATOR_LIMIT_EXCEEDED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MERGE_ON_CONDITION_REQUIRED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MERGE_ON_CONDITION_REQUIRED() throws Exception {
        println("testSqlException_MERGE_ON_CONDITION_REQUIRED");
        
        SQLException ex = Util.sqlException(Trace.MERGE_ON_CONDITION_REQUIRED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MERGE_WHEN_MATCHED_ALREADY_USED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MERGE_WHEN_MATCHED_ALREADY_USED() throws Exception {
        println("testSqlException_MERGE_WHEN_MATCHED_ALREADY_USED");
        
        SQLException ex = Util.sqlException(Trace.MERGE_WHEN_MATCHED_ALREADY_USED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.MERGE_WHEN_NOT_MATCHED_ALREADY_USED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_MERGE_WHEN_NOT_MATCHED_ALREADY_USED() throws Exception {
        println("testSqlException_MERGE_WHEN_NOT_MATCHED_ALREADY_USED");
        
        SQLException ex = Util.sqlException(Trace.MERGE_WHEN_NOT_MATCHED_ALREADY_USED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_COLUMN_NAMES_NOT_UNIQUE() throws Exception {
        println("testSqlException_SQL_COLUMN_NAMES_NOT_UNIQUE");
        
        SQLException ex = Util.sqlException(Trace.SQL_COLUMN_NAMES_NOT_UNIQUE,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_SECOND_IDENTITY_COLUMN,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_SECOND_IDENTITY_COLUMN() throws Exception {
        println("testSqlException_SQL_SECOND_IDENTITY_COLUMN");
        
        SQLException ex = Util.sqlException(Trace.SQL_SECOND_IDENTITY_COLUMN,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_DEFAULT_CLAUSE_REQUITED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_DEFAULT_CLAUSE_REQUITED() throws Exception {
        println("testSqlException_SQL_DEFAULT_CLAUSE_REQUITED");
        
        SQLException ex = Util.sqlException(Trace.SQL_DEFAULT_CLAUSE_REQUITED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_INVALID_SEQUENCE_PARAMETER,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_INVALID_SEQUENCE_PARAMETER() throws Exception {
        println("testSqlException_SQL_INVALID_SEQUENCE_PARAMETER");
        
        SQLException ex = Util.sqlException(Trace.SQL_INVALID_SEQUENCE_PARAMETER,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_IDENTITY_DEFINITION_NOT_ALLOWED,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_IDENTITY_DEFINITION_NOT_ALLOWED() throws Exception {
        println("testSqlException_SQL_IDENTITY_DEFINITION_NOT_ALLOWED");
        
        SQLException ex = Util.sqlException(Trace.SQL_IDENTITY_DEFINITION_NOT_ALLOWED,"");
        
        checkSQLException(ex);
    }
    
    /**
     * Test of sqlException(Trace.SQL_IDENTITY_DEFINITION_NOT_EXISTS,"") method, of class org.hsqldb.jdbc.Util.
     */
    public void testSqlException_SQL_IDENTITY_DEFINITION_NOT_EXISTS() throws Exception {
        println("testSqlException_SQL_IDENTITY_DEFINITION_NOT_EXISTS");
        
        SQLException ex = Util.sqlException(Trace.SQL_IDENTITY_DEFINITION_NOT_EXISTS,"");
        
        checkSQLException(ex);
    }
    
    public static void main(java.lang.String[] argList) {
        
        junit.textui.TestRunner.run(suite());
    }
    
//    public void testGenerate() throws Exception {
//        java.lang.reflect.Field[] fields = Trace.class.getFields();
//        
//        for (int i = 0; i < fields.length-1; i++) {
//            if (int.class != fields[i].getType()) {
//                continue;
//            }
//            
//            if ("bundleHandle".equals(fields[i].getName())) {
//                continue;
//            }
//            if (fields[i].getName().startsWith("NOT_USED_")) {
//                continue;
//            }
//            if (fields[i].getName().startsWith("LAST_ERROR_HANDLE")) {
//                continue;
//            }
//            println("/**");
//            println("* Test of sqlException(Trace." + fields[i].getName() + ",\"\") method, of class org.hsqldb.jdbc.Util.");
//            println("*/");
//            println("public void testSqlException_" + fields[i].getName() + "() throws Exception {");
//            println("    println(\"testSqlException_" + fields[i].getName() + "\");");
//            println();
//            println("    SQLException ex = Util.sqlException(Trace." + fields[i].getName() + ",\"\");");
//            println();
//            println("    checkSQLException(ex);");
//            println("}");
//            println();
//        }
//    }
}
