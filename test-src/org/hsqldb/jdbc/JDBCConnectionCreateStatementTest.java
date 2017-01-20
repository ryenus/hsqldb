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

import java.sql.Connection;
import java.sql.SQLWarning;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCConnection.class)
public class JDBCConnectionCreateStatementTest extends BaseJdbcTestCase {

    protected static String computeTestName(
            int typeIndex,
            int concurrencyIndex) {
        String typeName = s_rstype[typeIndex][0];
        String concurName = s_rsconcurrency[concurrencyIndex][0];

        return "testCreateStatement_"
                + typeName
                + "_"
                + concurName;
    }

    protected static String computeTestName(
            int typeIndex,
            int concurrencyIndex,
            int holdabilityIndex) {
        String typeName = s_rstype[typeIndex][0];
        String concurName = s_rsconcurrency[concurrencyIndex][0];
        String holdName = s_rsholdability[holdabilityIndex][0];

        return "testCreateStatement_"
                + typeName
                + "_"
                + concurName
                + "_"
                + holdName;
    }

    protected final int m_holdabilityIndex;
    protected final int m_concurrencyIndex;
    protected final int m_typeIndex;
    protected final boolean m_holdabilitySpecified;

    public JDBCConnectionCreateStatementTest(
            int typeIndex,
            int concurrencyIndex,
            int holdabilityIndex) {
        super(computeTestName(typeIndex, concurrencyIndex, holdabilityIndex));

        m_typeIndex = typeIndex;
        m_concurrencyIndex = concurrencyIndex;
        m_holdabilityIndex = holdabilityIndex;
        m_holdabilitySpecified = true;
    }

    public JDBCConnectionCreateStatementTest(
            int typeIndex,
            int concurrencyIndex) {
        super(computeTestName(typeIndex, concurrencyIndex));

        m_typeIndex = typeIndex;
        m_concurrencyIndex = concurrencyIndex;
        m_holdabilityIndex = Integer.MIN_VALUE;
        m_holdabilitySpecified = false;
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("JDBCConnectionCreateStatementTest");

        for(int i = 0; i < s_rstype.length; i++) {
            for (int j = 0; j < s_rsconcurrency.length; j++) {
                suite.addTest(new JDBCConnectionCreateStatementTest(i, j));
            }
        }

        for(int i = 0; i < s_rstype.length; i++) {
            for (int j = 0; j < s_rsconcurrency.length; j++) {
                for (int k = 0; k < s_rsholdability.length; k++)
                suite.addTest(new JDBCConnectionCreateStatementTest(i, j, k));
            }
        }

        return suite;
    }

    public static void main(java.lang.String[] argList) {
        junit.textui.TestRunner.run(suite());
    }

    protected int getResultSetType() throws Exception {
        return getFieldValue(s_rstype[m_typeIndex][1]);
    }

    protected int getResultSetConcurrency() throws Exception {
        return getFieldValue(s_rsconcurrency[m_concurrencyIndex][1]);
    }

    protected int getResultSetHoldability() throws Exception {
        return getFieldValue(s_rsholdability[m_holdabilityIndex][1]);
    }

    @Override
    protected void runTest() throws Throwable {
        int type = getResultSetType();
        int concurrency = getResultSetConcurrency();
        int holdability = m_holdabilitySpecified
                ? getResultSetHoldability()
                : Integer.MIN_VALUE;

        Connection conn = newConnection();
        Statement stmt = m_holdabilitySpecified
                ? conn.createStatement(type, concurrency, holdability)
                : conn.createStatement(type, concurrency);

        connectionFactory().registerStatement(stmt);

        SQLWarning warning = conn.getWarnings();

        if(warning == null) {
            assertEquals(
                    "ResultSet Type",
                    type,
                    stmt.getResultSetType());
            assertEquals(
                    "ResultSet Concurrency",
                    concurrency,
                    stmt.getResultSetConcurrency());
            if(m_holdabilitySpecified) {
                assertEquals(
                        "ResultSet Holdability",
                        holdability,
                        stmt.getResultSetHoldability());
            }
        } else {
            while(warning != null) {
                printWarning(warning);
                warning = warning.getNextWarning();
            }
        }
    }
}
