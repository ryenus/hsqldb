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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * See AbstractTestOdbc for more general ODBC test information.
 *
 * @see AbstractTestOdbc
 */
public class TestOdbcTypes extends AbstractTestOdbc {
    /* HyperSQL types to be tested:
     * Exact Numeric
            TINYINT
            SMALLINT
            INTEGER
            BIGINT
            NUMERIC(?,?) = DECIMAL(?,?)   (default for decimal literals)
       Approximate Numeric
           REAL = FLOAT = DOUBLE    (default for literals with specified exponent)
       BOOLEAN
       Character Strings
           CHARACTER(1)* = CHAR(1)
           CHARACTER VARYING = VARCHAR = LONGVARCHAR
           CLOB(1) = CHARACTER LARGE OBJECT(1)
       Binary Strings
           BINARY(1)*
           BINARY VARYING = VARBINARY
           BLOB(1) = BINARY LARGE OBJECT()
       Bits
           BIT(1)
           BIT VARYING
           ? What is the difference between BIT and BIT VARYING ?  Are they synonyms?
       OTHER  (for holding serialized Java objects)
       Date/Times
           DATE
           TIME(?,?)
           TIMESTAMP(?,?)
           INTERVAL...(2,0)
     */

    public TestOdbcTypes() {}

    /**
     * Accommodate JUnit's test-runner conventions.
     */
    public TestOdbcTypes(String s) {
        super(s);
    }

    protected void populate(Statement st) throws SQLException {
        st.executeUpdate("DROP TABLE nullmix IF EXISTS");
        st.executeUpdate("CREATE TABLE nullmix "
                + "(i INT NOT NULL, vc VARCHAR(20), xtra VARCHAR(20))");

        // Would be more elegant and efficient to use a prepared statement
        // here, but our we want this setup to be as simple as possible, and
        // leave feature testing for the actual unit tests.
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(10, 'ten')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(5, 'five')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(15, 'fifteen')");
        st.executeUpdate(
                "INSERT INTO nullmix (i, vc) values(21, 'twenty one')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(40, 'forty')");
        st.executeUpdate("INSERT INTO nullmix (i) values(25)");
    }

    static public void main(String[] sa) {
        staticRunner(TestOdbcTypes.class, sa);
    }
}
