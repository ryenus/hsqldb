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
     *
     * Exact Numeric
     *      TINYINT
     *      SMALLINT
     *      INTEGER
     *      BIGINT
     *      NUMERIC(p?,s?) = DECIMAL()   (default for decimal literals)
     * Approximate Numeric
     *     FLOAT(p?) = FLOAT() = DOUBLE() (default for literals with exponent)
     *     DOUBLE = REAL
     * BOOLEAN
     * Character Strings
     *     CHARACTER(1l)* = CHAR()
     *     CHARACTER VARYING(1l) = VARCHAR() = LONGVARCHAR()
     *     CLOB(1l) = CHARACTER LARGE OBJECT(1)
     * Binary Strings
     *     BINARY(1l)*
     *     BINARY VARYING(1l) = VARBINARY()
     *     BLOB(1l) = BINARY LARGE OBJECT()
     * Bits
     *     BIT(1l)
     *     BIT VARYING(1l)
     *     ? What is the difference between BIT and BIT VARYING ?
     * OTHER  (for holding serialized Java objects)
     * Date/Times
     *     DATE
     *     TIME(p?,p?)
     *     TIMESTAMP(p?,p?)
     *     INTERVAL...(p2,p0)
     */

    public TestOdbcTypes() {}

    /**
     * Accommodate JUnit's test-runner conventions.
     */
    public TestOdbcTypes(String s) {
        super(s);
    }

    protected void populate(Statement st) throws SQLException {
        st.executeUpdate("DROP TABLE alltypes IF EXISTS");
        st.executeUpdate("CREATE TABLE alltypes (\n"
            + "    id INTEGER,\n"
            + "    ti TINYINT,\n"
            + "    si SMALLINT,\n"
            + "    i INTEGER,\n"
            + "    bi BIGINT,\n"
            + "    n NUMERIC(5,2),\n"
            + "    f FLOAT(5),\n"
            + "    r DOUBLE,\n"
            + "    b BOOLEAN,\n"
            + "    c CHARACTER(3),\n"
            + "    cv CHARACTER VARYING(3),\n"
            + "    cl CLOB(3),\n"
            + "    bin BINARY(3),\n"
            + "    bv BINARY VARYING(3),\n"
            + "    bl BLOB(3),\n"
            + "    bt BIT(3),\n"
            + "    btv BIT VARYING(3),\n"
            + "    o OTHER,\n"
            + "    d DATE,\n"
            + "    t TIME(2),\n"
            + "    ts TIMESTAMP(2),\n"
            + "    iv INTERVAL SECOND(2,2)\n"
           + ')');

        // Would be more elegant and efficient to use a prepared statement
        // here, but our we want this setup to be as simple as possible, and
        // leave feature testing for the actual unit tests.
        st.executeUpdate("INSERT INTO alltypes VALUES (\n"
            + "    1, 3, 4, 5, 6, 7.8, 8.9, 9.7, true, 'ab', 'cd', 'ef',\n"
            + "    'gh', 'ij', null, null, null, null, current_date,\n"
            + "    current_time, current_timestamp, null\n"
            + ')'
        );
        /*
         * How to write to BLOB with a text query?...
         * Can't write integers (smallest ones) to the bit fields.
         *  How is one to set multiple bits?
         * I set Object and Internal to null, because I don't want to deal with
         *  them yet.
         */
    }

    public void testInteger() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id = 1");
            assertTrue("Got no row with id of 1", rs.next());
            assertEquals(5, rs.getInt("i"));
System.err.println("Type is " + rs.getInt("i"));
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
            } catch(Exception e) {
            }
        }
    }

    static public void main(String[] sa) {
        staticRunner(TestOdbcTypes.class, sa);
    }
}
