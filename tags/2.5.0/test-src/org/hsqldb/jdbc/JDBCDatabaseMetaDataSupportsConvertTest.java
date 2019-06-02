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

import org.hsqldb.jdbc.testbase.BaseDatabaseMetaDataSupportsConvertTestCase;
import junit.framework.Test;

public final class JDBCDatabaseMetaDataSupportsConvertTest
        extends BaseDatabaseMetaDataSupportsConvertTestCase {

    private static final String s_hsqldb_types_fqn = "org.hsqldb.types.Types";
    private static final String[][] s_type_name_and_field = new String[][]{
        {"array", "java.sql.Types.ARRAY"},
        {"bigint", "java.sql.Types.BIGINT"},
        {"binary", "java.sql.Types.BINARY"},
        {"bit", "java.sql.Types.BIT"},
        {"blob", "java.sql.Types.BLOB"},
        {"boolean", "java.sql.Types.BOOLEAN"},
        {"char", "java.sql.Types.CHAR"},
        {"clob", "java.sql.Types.CLOB"},
        {"datalink", "java.sql.Types.DATALINK"},
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
        {"time_with_time_zone", s_hsqldb_types_fqn + ".SQL_TIME_WITH_TIME_ZONE"},
        {"timestamp_with_time_zone", s_hsqldb_types_fqn + ".SQL_TIMESTAMP_WITH_TIME_ZONE"},
        {"tinyint", "java.sql.Types.TINYINT"},
        {"varbinary", "java.sql.Types.VARBINARY"},
        {"varchar", "java.sql.Types.VARCHAR"},
        {"varchar_ignorecase", s_hsqldb_types_fqn + ".VARCHAR_IGNORECASE"},
        {"interval_year", s_hsqldb_types_fqn + ".SQL_INTERVAL_YEAR"},
        {"interval_month", s_hsqldb_types_fqn + ".SQL_INTERVAL_MONTH"},
        {"interval_day", s_hsqldb_types_fqn + ".SQL_INTERVAL_DAY"},
        {"interval_hour", s_hsqldb_types_fqn + ".SQL_INTERVAL_HOUR"},
        {"interval_minute", s_hsqldb_types_fqn + ".SQL_INTERVAL_MINUTE"},
        {"interval_second", s_hsqldb_types_fqn + ".SQL_INTERVAL_SECOND"},
        {"interval_year_to_month", s_hsqldb_types_fqn + ".SQL_INTERVAL_YEAR_TO_MONTH"},
        {"interval_day_to_hour", s_hsqldb_types_fqn + ".SQL_INTERVAL_DAY_TO_HOUR"},
        {"interval_day_to_minute", s_hsqldb_types_fqn + ".SQL_INTERVAL_DAY_TO_MINUTE"},
        {"interval_day_to_second", s_hsqldb_types_fqn + ".SQL_INTERVAL_DAY_TO_SECOND"},
        {"interval_hour_to_minute", s_hsqldb_types_fqn + ".SQL_INTERVAL_HOUR_TO_MINUTE"},
        {"interval_hour_to_second", s_hsqldb_types_fqn + ".SQL_INTERVAL_HOUR_TO_SECOND"},
        {"interval_minute_to_second", s_hsqldb_types_fqn + ".SQL_INTERVAL_MINUTE_TO_SECOND"}
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
        super(toIndex, fromIndex);
    }

    @Override
    protected int getSQLTypeCode(int i) {
        try {
            return super.getFieldValue(s_type_name_and_field[i][1]);
        } catch (Exception e) {
            throw (e instanceof RuntimeException)
                    ? ((RuntimeException)e)
                    : new RuntimeException(e.toString(), e);
        }
    }

    @Override
    protected String getSQLTypeName(int i) {
        return s_type_name_and_field[i][0];
    }

    static int getSQLTypeCount() {
        return s_type_name_and_field.length;
    }
    
    /**
     * of tests for this test case.
     *
     * @return the suite of tests for this test case.
     */
    public static Test suite() {
        return BaseDatabaseMetaDataSupportsConvertTestCase.createTestSuite(
                JDBCDatabaseMetaDataSupportsConvertTest.class, getSQLTypeCount());
    }     

    /**
     * runs the tests returned by suite().
     *
     * @param args ignored.
     */
    public static void main(java.lang.String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
