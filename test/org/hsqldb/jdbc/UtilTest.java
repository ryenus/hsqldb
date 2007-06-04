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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    
    public UtilTest(String testName, int vendorCode) {
        super(testName);

        m_vendorCode = vendorCode;
    }
    
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
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
        
        if (!found) {
            assertEquals("Allowable error code "
                    + errorCode
                    + " ("
                    + sqlState
                    + " "
                    + errorMessage
                    + ") for: "
                    + clazz,
                    true,
                    false);
        }
    }

    public static void main(java.lang.String[] argList) throws Exception {
        
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("UtilTest Suite");
        

        Field[] fields = Trace.class.getFields();
        
        for (int i = 0; i < fields.length-1; i++) {
            if (int.class != fields[i].getType()) {
                continue;
            }
            
            Field field =  fields[i];
            String fieldName = field.getName();
            
            if ("bundleHandle".equals(fieldName)) {
                continue;
            }
            if (fieldName.startsWith("NOT_USED_")) {
                continue;
            }
            if (fieldName.startsWith("LAST_ERROR_HANDLE")) {
                continue;
            }
            
            String testName = "testSqlException_" + fieldName;
            int vendorCode = field.getInt(null);
            
            UtilTest test = new UtilTest(testName, vendorCode);
            
            suite.addTest(test);
        }
        
        return suite;
    }

    private int m_vendorCode;
    
    protected void runTest() throws Throwable {
        println(getName());
        
        SQLException ex = Util.sqlException(m_vendorCode,"");
        
        checkSQLException(ex);
    }
}
