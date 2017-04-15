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
package org.hsqldb.jdbc;

import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
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

import org.hsqldb.error.ErrorCode;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of class {@link org.hsqldb.jdbc.Util}.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCUtil.class)
public class UtilTest extends BaseJdbcTestCase {

    private static final Object[][] s_exceptions = new Object[][] {
        {
            SQLTransientConnectionException.class, new int[] {
                ErrorCode.X_08000,
                ErrorCode.X_08001,
                ErrorCode.X_08002,
                ErrorCode.X_08004,
                ErrorCode.X_08006,
                ErrorCode.X_08007,
                ErrorCode.X_08501,
                ErrorCode.X_08502,
            }
        }, {
            SQLNonTransientConnectionException.class, new int[] {
                ErrorCode.X_08003,
                ErrorCode.X_08503
            }
        }, {
            SQLIntegrityConstraintViolationException.class, new int[]{
                ErrorCode.X_23000, ErrorCode.X_23001, ErrorCode.X_23502,
                ErrorCode.X_23503, ErrorCode.X_23504, ErrorCode.X_23505,
                ErrorCode.X_23513
            }
        }, {
            SQLInvalidAuthorizationSpecException.class, new int[] {
                ErrorCode.X_28000, ErrorCode.X_28501, ErrorCode.X_28502,
                ErrorCode.X_28503
            }
        }, {
            SQLSyntaxErrorException.class, new int[] {

                // NOTES:
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
                ErrorCode.X_42000,
                ErrorCode.X_42501,
                ErrorCode.X_42502,
                ErrorCode.X_42503,
                ErrorCode.X_42504,
                ErrorCode.X_42505,
                ErrorCode.X_42506,
                ErrorCode.X_42507,
                ErrorCode.X_42508,
                ErrorCode.X_42509,
                ErrorCode.X_42510,
                ErrorCode.X_42512,
                ErrorCode.X_42513,
                ErrorCode.X_42520,
                ErrorCode.X_42521,
                ErrorCode.X_42522,
                ErrorCode.X_42523,
                ErrorCode.X_42524,
                ErrorCode.X_42525,
                ErrorCode.X_42526,
                ErrorCode.X_42527,
                ErrorCode.X_42528,
                ErrorCode.X_42529,
                ErrorCode.X_42530,
                ErrorCode.X_42531,
                ErrorCode.X_42532,
                ErrorCode.X_42533,
                ErrorCode.X_42534,
                ErrorCode.X_42535,
                ErrorCode.X_42536,
                ErrorCode.X_42537,
                ErrorCode.X_42538,
                ErrorCode.X_42539,
                ErrorCode.X_42541,
                ErrorCode.X_42542,
                ErrorCode.X_42543,
                ErrorCode.X_42544,
                ErrorCode.X_42545,
                ErrorCode.X_42546,
                ErrorCode.X_42547,
                ErrorCode.X_42548,
                ErrorCode.X_42549,
                ErrorCode.X_42551,
                ErrorCode.X_42555,
                ErrorCode.X_42556,
                ErrorCode.X_42561,
                ErrorCode.X_42562,
                ErrorCode.X_42563,
                ErrorCode.X_42564,
                ErrorCode.X_42565,
                ErrorCode.X_42566,
                ErrorCode.X_42567,
                ErrorCode.X_42568,
                ErrorCode.X_42569,
                ErrorCode.X_42570,
                ErrorCode.X_42571,
                ErrorCode.X_42572,
                ErrorCode.X_42573,
                ErrorCode.X_42574,
                ErrorCode.X_42575,
                ErrorCode.X_42576,
                ErrorCode.X_42577,
                ErrorCode.X_42578,
                ErrorCode.X_42579,
                ErrorCode.X_42580,
                ErrorCode.X_42581,
                ErrorCode.X_42582,
                ErrorCode.X_42583,
                ErrorCode.X_42584,
                ErrorCode.X_42585,
                ErrorCode.X_42586,
                ErrorCode.X_42587,
                ErrorCode.X_42588,
                ErrorCode.X_42589,
                ErrorCode.X_42590,
                ErrorCode.X_42591,
                ErrorCode.X_42592,
                ErrorCode.X_42593,
                ErrorCode.X_42594,
                ErrorCode.X_42595,
                ErrorCode.X_42596,
                ErrorCode.X_42597,
                ErrorCode.X_42598,
                ErrorCode.X_42599,
                ErrorCode.X_42601,
                ErrorCode.X_42602,
                ErrorCode.X_42603,
                ErrorCode.X_42604,
                ErrorCode.X_42605,
                ErrorCode.X_42606,
                ErrorCode.X_42607,
                ErrorCode.X_42608,
                ErrorCode.X_42609,
                ErrorCode.X_42610,
                ErrorCode.X_42611,
                ErrorCode.X_42612
            }
        }, {
            SQLTransactionRollbackException.class, new int[]{
                ErrorCode.X_40000, ErrorCode.X_40001, ErrorCode.X_40002,
                ErrorCode.X_40003, ErrorCode.X_40004, ErrorCode.X_40501,
                ErrorCode.X_40502
            }
        },

        {
            SQLException.class,
            null    // calculated below, in static initializer
        }
    };
    private static final Map<Class<?>,int[]> s_classMap = new HashMap<Class<?>,int[]>();

    static List<Integer> getErrorCodes()
    {
        List<Integer> list = new ArrayList<Integer>();

        Field[] fields = ErrorCode.class.getFields();

        for(int i = 0; i < fields.length; i++)
        {
            Field field = fields[i];

            try {
                int val = field.getInt(null);

                list.add(new Integer(val));
            } catch (Exception e){}
        }

        return list;
    }

    static {
        List<Integer> list = getErrorCodes();

        for (int j = 0; j < s_exceptions.length - 1; j++) {
            int[]   codes = (int[]) s_exceptions[j][1];

            for(int k = 0; k < codes.length; k++) {
                list.remove(Integer.valueOf(codes[k]));
            }
        }

        int[] nontransientcodes = new int[list.size()];

        for (int i = 0; i < list.size(); i++) {
            nontransientcodes[i] = list.get(i).intValue();
        }

        s_exceptions[s_exceptions.length - 1][1] = nontransientcodes;

        for (int i = 0; i < s_exceptions.length; i++) {
            s_classMap.put((Class<?>)s_exceptions[i][0], (int[]) s_exceptions[i][1]);
        }
    }

    public UtilTest(String testName, int vendorCode) {

        super(testName);

        m_vendorCode = vendorCode;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected void checkSQLException(SQLException se) throws Exception {

        String sqlState = se.getSQLState();

        if (sqlState.startsWith("08")) {
            if (sqlState.endsWith("3")) {
                assertTrue("se instanceof SQLNonTransientConnectionException",
                           se instanceof SQLNonTransientConnectionException);
                checkErrorCode(se, SQLNonTransientConnectionException.class);
            } else {
                assertTrue("se instanceof SQLTransientConnectionException",
                           se instanceof SQLTransientConnectionException);
                checkErrorCode(se, SQLTransientConnectionException.class);
            }
        } else if (sqlState.startsWith("23")) {
            assertTrue(
                "se instanceof SQLIntegrityConstraintViolationException",
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

    protected void checkErrorCode(SQLException se,
                                  Class<?> clazz) throws Exception {

        int     errorCode    = Math.abs(se.getErrorCode());
        String  errorMessage = se.getMessage();
        String  sqlState     = se.getSQLState();
        int[]   codes        = s_classMap.get(clazz);
        boolean found        = false;

        for (int i = 0; i < codes.length; i++) {
            if (errorCode == codes[i]) {
                found = true;

                break;
            }
        }

        if (!found) {
            assertEquals("Allowable error code " + errorCode + " (" + sqlState
                         + " " + errorMessage + ") for: " + clazz, true,
                             false);
        }
    }

    public static void main(String[] argList) throws Exception {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {

        TestSuite suite  = new TestSuite("UtilTest Suite");
        Field[]   fields = ErrorCode.class.getFields();

        for (int i = 0; i < fields.length; i++) {
            if (int.class != fields[i].getType()) {
                continue;
            }

            Field  field     = fields[i];
            String fieldName = field.getName();

            String testName = "testSqlException_" + fieldName;

            try {
                int      vendorCode = field.getInt(null);
                UtilTest test       = new UtilTest(testName + "_" + vendorCode, vendorCode);

                suite.addTest(test);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        return suite;
    }

    private int m_vendorCode;

    @Override
    @OfMethod("sqlException(int,java.lang.String)")
    protected void runTest() throws Throwable {

        println(getName());

        SQLException ex = JDBCUtil.sqlException(m_vendorCode, "");

        checkSQLException(ex);
    }
}
