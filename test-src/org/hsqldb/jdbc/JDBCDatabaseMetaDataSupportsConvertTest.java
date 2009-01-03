/* Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb.jdbc.testbase.JdbcTestCase;
import java.sql.DatabaseMetaData;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Exhautively tests the supportsConvert(int,int) method of
 * interface java.sql.DatabaseMetaData.
 *
 * @author boucherb@users
 */
public class JDBCDatabaseMetaDataSupportsConvertTest
        extends JdbcTestCase {

    /**
     *  in type_name_value array
     */
    protected final int m_toIndex;

    /**
     * in type_name_value array
     */
    protected final int m_fromIndex;

    /**
     * to test.
     */
    protected static final String[][] TYPE_NAME_AND_FIELD = new String[][]
    {
        {"array", "java.sql.Types.ARRAY"},
        {"bigint", "java.sql.Types.BIGINT"},
        {"binary", "java.sql.Types.BINARY"},
        {"bit", "java.sql.Types.BIT"},
        {"blob", "java.sql.Types.BLOB"},
        {"boolean", "java.sql.Types.BOOLEAN"},
        {"char", "java.sql.Types.CHAR"},
        {"clob", "java.sql.Types.CLOB"},
        {"datalink","java.sql.Types.DATALINK"},
        {"date", "java.sql.Types.DATE"},
        {"decimal", "java.sql.Types.DECIMAL"},
        {"distinct", "java.sql.Types.DISTINCT"},
        {"double", "java.sql.Types.DOUBLE"},
        {"float", "java.sql.Types.FLOAT"},
        {"integer", "java.sql.Types.INTEGER"},
        {"java_object", "java.sql.Types.JAVA_OBJECT"},
        {"longnvarchar", "java.sql.Types.LONGNVARCHAR"},
        {"longvarchar", "java.sql.Types.LONGVARCHAR"},
        {"longvarbinary", "java.sql.Types.LONGVARBINARY"},
        {"nchar", "java.sql.Types.NCHAR"},
        {"nclob", "java.sql.Types.NCLOB"},
        {"null", "java.sql.Types.NULL"},
        {"numeric", "java.sql.Types.NUMERIC"},
        {"nvarchar", "java.sql.Types.NVARCHAR"},
        {"other", "java.sql.Types.OTHER"},
        {"real", "java.sql.Types.REAL"},
        {"ref", "java.sql.Types.REF"},
        {"rowid", "java.sql.Types.ROWID"},
        {"smallint", "java.sql.Types.SMALLINT"},
        {"struct", "java.sql.Types.STRUCT"},
        {"time", "java.sql.Types.TIME"},
        {"timestamp", "java.sql.Types.TIMESTAMP"},
        {"time_with_time_zone", "org.hsqldb.Types.SQL_TIME_WITH_TIME_ZONE"},
        {"timestamp_with_time_zone", "org.hsqldb.Types.SQL_TIMESTAMP_WITH_TIME_ZONE"},
        {"tinyint", "java.sql.Types.TINYINT"},
        {"varbinary", "java.sql.Types.VARBINARY"},
        {"varchar", "java.sql.Types.VARCHAR"},
        {"varchar_ignorecase", "org.hsqldb.Types.VARCHAR_IGNORECASE"},
        {"interval_year", "org.hsqldb.Types.SQL_INTERVAL_YEAR"},
        {"interval_month", "org.hsqldb.Types.SQL_INTERVAL_MONTH"},
        {"interval_day", "org.hsqldb.Types.SQL_INTERVAL_DAY"},
        {"interval_hour", "org.hsqldb.Types.SQL_INTERVAL_HOUR"},
        {"interval_minute", "org.hsqldb.Types.SQL_INTERVAL_MINUTE"},
        {"interval_second", "org.hsqldb.Types.SQL_INTERVAL_SECOND"},
        {"interval_year_to_month", "org.hsqldb.Types.SQL_INTERVAL_YEAR_TO_MONTH"},
        {"interval_day_to_hour", "org.hsqldb.Types.SQL_INTERVAL_DAY_TO_HOUR"},
        {"interval_day_to_minute", "org.hsqldb.Types.SQL_INTERVAL_DAY_TO_MINUTE"},
        {"interval_day_to_second", "org.hsqldb.Types.SQL_INTERVAL_DAY_TO_SECOND"},
        {"interval_hour_to_minute", "org.hsqldb.Types.SQL_INTERVAL_HOUR_TO_MINUTE"},
        {"interval_hour_to_second", "org.hsqldb.Types.SQL_INTERVAL_HOUR_TO_SECOND"},
        {"interval_minute_to_second", "org.hsqldb.Types.SQL_INTERVAL_MINUTE_TO_SECOND"}
    };

    /**
     * Constructs a new test case for the given pair of type index values.
     *
     * @param toIndex of target type
     * @param fromIndex of source type
     */
    public JDBCDatabaseMetaDataSupportsConvertTest(
            final int toIndex,
            final int fromIndex) {
        super(computeTestName(toIndex,fromIndex));

        m_toIndex = toIndex;
        m_fromIndex = fromIndex;
    }

    /**
     * for given pair of type index values.
     *
     * @param toIndex of target type
     * @param fromIndex of source type
     * @return test name.
     */
    protected static String computeTestName(
            int toIndex,
            int fromIndex) {
        String toType = TYPE_NAME_AND_FIELD[toIndex][0];
        String fromType = TYPE_NAME_AND_FIELD[fromIndex][0];
        String testName =
                "testSupportsConvert_to_"
                + toType.toUpperCase()
                + "_from_"
                + fromType.toUpperCase();

        return testName;
    }

    /**
     * to speed up getMetaData().
     */
    private static DatabaseMetaData m_dbmd;

    /**
     * subject of test.
     *
     * @throws java.lang.Exception raised by any internal operation.
     * @return test subject.
     */
    protected DatabaseMetaData getMetaData() throws Exception  {
        if (m_dbmd == null) {
            m_dbmd = super.newConnection().getMetaData();
        }

        return m_dbmd;
    }

    /**
     * Overriden to run the test and assert its state.
     *
     * @throws java.lang.Throwable if any exception is thrown
     */
    protected void runTest() throws Throwable {
        //println(super.getName()); // 2600+ printlns is too slow...

        final String toName = TYPE_NAME_AND_FIELD[m_toIndex][0];
        final String toField = TYPE_NAME_AND_FIELD[m_toIndex][1];
        final int    toCode = getFieldValue(toField);

        final String fromName = TYPE_NAME_AND_FIELD[m_fromIndex][0];
        final String fromField = TYPE_NAME_AND_FIELD[m_fromIndex][1];
        final int    fromCode = getFieldValue(fromField);

        final String propertyName =
                "dbmd.supports.convert.to."
                + toName
                + ".from."
                + fromName;

        final boolean expectedResult =
                getBooleanProperty(
                propertyName,
                false);
        final boolean actualResult = getMetaData().supportsConvert(
                fromCode,
                toCode);

        assertEquals(expectedResult, actualResult);
    }

    /**
     * of tests for this test case.
     *
     * @return the suite of tests for this test case.
     */
    public static Test suite() {
        final TestSuite suite = new TestSuite(
                "jdbcDatabaseMetaDataSupportsConvertTest");

        final int len = TYPE_NAME_AND_FIELD.length;

        for(int toIndex = 0; toIndex < len; toIndex++) {
            for (int fromIndex = 0; fromIndex < len; fromIndex++) {
                suite.addTest(new JDBCDatabaseMetaDataSupportsConvertTest(
                        toIndex,
                        fromIndex));
            }
        }

        return suite;
    }

    /**
     * runs the tests returned by suite().
     *
     * @param argList ignored.
     */
    public static void main(java.lang.String[] argList) {
        junit.textui.TestRunner.run(suite());
    }

}
