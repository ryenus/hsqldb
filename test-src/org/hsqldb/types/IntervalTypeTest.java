/* Copyright (c) 2001-2022, The HSQL Development Group
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
package org.hsqldb.types;

import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.Tokens;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author fredt
 */
@ForSubject(IntervalType.class)
@OfMethod("getIntervalType(int,long,int)")
public class IntervalTypeTest extends BaseTestCase {

    private static TestParameters[] s_parameters = new TestParameters[]{
        new TestParameters(Types.SQL_INTERVAL_YEAR_TO_MONTH, 4, 0, "200-10", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 6, "200 10:12:12.456789", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 7, "200 10:12:12.456789", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:12.", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 7, "200 10:12:12.456789", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:12.", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:12", null),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:0:12", null),
        new TestParameters(Types.SQL_INTERVAL_YEAR_TO_MONTH, 5, 0, "20000-10", null),
        new TestParameters(Types.SQL_INTERVAL_YEAR_TO_MONTH, 4, 0, "20000-10", "first part too long"),
        new TestParameters(Types.SQL_INTERVAL_YEAR_TO_MONTH, 4, 0, "2000-90", "other part too large"),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:123.456789", "other part to long"),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12 12.456789", "bad separator"),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:12 456789", "bad separator"),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "200 10:12:12 .", "bad separator"),
        new TestParameters(Types.SQL_INTERVAL_DAY_TO_SECOND, 3, 5, "20000 10:12:12. ", "first part too long")
    };
    private static final Logger LOG = Logger.getLogger(IntervalTypeTest.class.getName());

    // hack to avoid null pointer in static initializer during test execution
    static {
        try {
            Collation.getDefaultInstance();
            IntervalType it = Type.SQL_INTERVAL_DAY;
        } catch (Throwable e) {
            LOG.log(Level.SEVERE, null, e);
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("IntervalType Test Suite");

        for (TestParameters s_parameter : s_parameters) {
            suite.addTest(new IntervalTypeTest(s_parameter));
        }

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    final TestParameters m_parameters;

    public IntervalTypeTest(final TestParameters parameters) {
        super(parameters.toString());

        m_parameters = parameters;
    }

    @Override
    protected void runTest() throws Exception {
        try {
            printProgress("****************************************");
            printProgress("Using: " + m_parameters);
            IntervalType t = IntervalType.getIntervalType(
                    m_parameters.intervalType,
                    m_parameters.intervalPrecision,
                    m_parameters.intervalFractionPrecision);
            printProgress("Actual type: " + t.getDefinition());
            if (m_parameters.intervalFailMessage != null) {
                try {
                    String sql = String.format("VALUES(%s)", m_parameters.toSQLLiteral());
                    printProgress("Executing query: " + sql);
                    ResultSet rs = newForwardOnlyReadOnlyResultSet(sql);
                    @SuppressWarnings("unchecked")
                    Object value = rs.next() ? rs.getObject(1, t.getJDBCClass()) : null;
                    String msg = String.format("Expected falure: %s, got value: %s%s",
                            m_parameters.intervalFailMessage,
                            value == null ? "" : value.getClass().getSimpleName() + ":",
                            value);
                    fail(msg);
                } catch (Exception ex) {
                    printProgress("SUCCESS: " + ex);
                }
            } else {
                try {
                    String sql = String.format("VALUES(%s)", m_parameters.toSQLLiteral());
                    printProgress("Executing query: " + sql);
                    ResultSet rs = newForwardOnlyReadOnlyResultSet(sql);
                    @SuppressWarnings("unchecked")
                    Object value = rs.next() ? rs.getObject(1, t.getJDBCClass()) : null;
                    String msg = String.format("SUCCESS: got value: %s%s",
                            value == null ? "" : value.getClass().getSimpleName() + ":",
                            value);
                    printProgress(msg);
                } catch (Exception ex) {
                    String msg = String.format("Expected success, got exception: " + ex);
                    fail(msg);
                }
            }
        } catch (Exception e) {
            if (m_parameters.intervalFailMessage == null) {
                fail(e + ": " + m_parameters);
            } else {
                printProgress("test failed as expected:" + m_parameters.intervalFailMessage + " " + e);
                println(m_parameters);
            }
        }
    }

    @SuppressWarnings({"FinalClass", "PublicInnerClass"})
    public static final class TestParameters {

        public final int intervalType;
        public final long intervalPrecision;
        public final int intervalFractionPrecision;
        public final String intervalLiteralText;
        public final String intervalFailMessage;

        /**
         *
         * @param type              interval type
         * @param precision         interval precision
         * @param fractionPrecision interval fraction precision
         * @param literalText       interval literal text
         * @param failMessage       string
         */
        public TestParameters(
                final int type,
                final long precision,
                final int fractionPrecision,
                final String literalText,
                final String failMessage) {
            intervalType = type;
            intervalPrecision = precision;
            intervalFractionPrecision = fractionPrecision;
            intervalLiteralText = literalText;
            intervalFailMessage = failMessage;
        }

        public String toSQLLiteral() {
                IntervalType t = IntervalType.getIntervalType(
                        intervalType,
                        intervalPrecision,
                        intervalFractionPrecision);
                final String fmt = t.getDefinition().replace(Tokens.T_INTERVAL, Tokens.T_INTERVAL + " '%s'");
                return String.format(fmt, this.intervalLiteralText);
        }

        @Override
        public String toString() {
            return String.format("TestParameters{\\%s\\"
                    + ", precision: [%s %s]"
                    + ", literal: [%s]"
                    + ", expect: [%s]}",
                    IntervalType.getQualifier(intervalType),
                    intervalPrecision,
                    intervalFractionPrecision,
                    intervalLiteralText,
                    intervalFailMessage == null ? "success" : intervalFailMessage);
        }
    }
}
