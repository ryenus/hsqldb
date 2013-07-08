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


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;

/**
 * See AbstractTestOdbc for more general ODBC test information.
 *
 * @see AbstractTestOdbc
 */
public class TestOdbcTypes extends AbstractTestOdbc {
    /* HyperSQL types to be tested:
     *
     * Exact Numeric
     *     TINYINT
     *     SMALLINT
     *     INTEGER
     *     BIGINT
     *     NUMERIC(p?,s?) = DECIMAL()   (default for decimal literals)
     * Approximate Numeric
     *     FLOAT(p?)
     *     DOUBLE = REAL (default for literals with exponent)
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
            + "    bt BIT(9),\n"
            + "    btv BIT VARYING(3),\n"
            + "    d DATE,\n"
            + "    t TIME(2),\n"
            + "    tw TIME(2) WITH TIME ZONE,\n"
            + "    ts TIMESTAMP(2),\n"
            + "    tsw TIMESTAMP(2) WITH TIME ZONE,\n"
            + "    bin BINARY(4),\n"
            + "    vb VARBINARY(4),\n"
            + "    dsival INTERVAL DAY(5) TO SECOND(6),\n"
            + "    sival INTERVAL SECOND(6,4)\n"
           + ')');
        /** TODO:  This test class can't handle testing unlmited VARCHAR, since
         * we set up with strict size setting, which prohibits unlimited
         * VARCHARs.  Need to write a standalone test class to test that.
         */

        // Would be more elegant and efficient to use a prepared statement
        // here, but our we want this setup to be as simple as possible, and
        // leave feature testing for the actual unit tests.
        st.executeUpdate("INSERT INTO alltypes VALUES (\n"
            + "    1, 3, 4, 5, 6, 7.8, 8.9, 9.7, true, 'ab', 'cd',\n"
            + "    b'10', b'10', current_date, '13:14:00',\n"
            + "    '15:16:00', '2009-02-09 16:17:18', '2009-02-09 17:18:19',\n"
            + "    x'A103', x'A103', "
            + "INTERVAL '145 23:12:19.345' DAY TO SECOND,\n"
            + "    INTERVAL '1000.345' SECOND\n"
            + ')'
        );
        st.executeUpdate("INSERT INTO alltypes VALUES (\n"
            + "    2, 3, 4, 5, 6, 7.8, 8.9, 9.7, true, 'ab', 'cd',\n"
            + "    b'10', b'10', current_date, '13:14:00',\n"
            + "    '15:16:00', '2009-02-09 16:17:18', '2009-02-09 17:18:19',\n"
            + "    x'A103', x'A103', "
            + "    INTERVAL '145 23:12:19.345' DAY TO SECOND,\n"
            + "    INTERVAL '1000.345' SECOND\n"
            + ')'
        );
    }

    public void testIntegerSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Integer.class, rs.getObject("i").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(5, rs.getInt("i"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testTinyIntSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Integer.class, rs.getObject("ti").getClass());
            // Nb. HyperSQL purposefully returns an Integer for this type
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals((byte) 3, rs.getByte("ti"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testSmallIntSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Integer.class, rs.getObject("si").getClass());
            // Nb. HyperSQL purposefully returns an Integer for this type
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals((short) 4, rs.getShort("si"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testBigIntSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Long.class, rs.getObject("bi").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(6, rs.getLong("bi"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    /*
    public void testNumericSimpleRead() {
        // This is failing.
        // Looks like we inherited a real bug with numerics from psqlodbc,
        // because the problem exists with Postresql-supplied psqlodbc
        // connecting to a Postgresql server.
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(BigDecimal.class, rs.getObject("n").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(new BigDecimal(7.8), rs.getBigDecimal("n"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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
    */

    public void testFloatSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Double.class, rs.getObject("f").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(8.9D, rs.getDouble("f"), 0D);
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testDoubleSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Double.class, rs.getObject("r").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(9.7D, rs.getDouble("r"), 0D);
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testBooleanSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Boolean.class, rs.getObject("b").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertTrue(rs.getBoolean("b"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testCharSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(String.class, rs.getObject("c").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("ab ", rs.getString("c"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testVarCharSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(String.class, rs.getObject("cv").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("cd", rs.getString("cv"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testFixedStringSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT i, 'fixed str' fs, cv\n"
                    + "FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(String.class, rs.getObject("fs").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("fixed str", rs.getString("fs"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testDerivedStringSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT i, cv || 'appendage' app, 4\n"
                    + "FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(String.class, rs.getObject("app").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("cdappendage", rs.getString("app"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testDateSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(java.sql.Date.class, rs.getObject("d").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(
                new java.sql.Date(new java.util.Date().getTime()).toString(),
                rs.getDate("d").toString());
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testTimeSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(java.sql.Time.class, rs.getObject("t").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(Time.valueOf("13:14:00"), rs.getTime("t"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    /*
    public void testTimeWSimpleRead() {
        // This test is failing because the JDBC Driver is returning a
        // String instead of a Time oject for rs.getTime().
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(java.sql.Time.class, rs.getObject("tw").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(Time.valueOf("15:16:00"), rs.getTime("tw"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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
    */

    public void testTimestampSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try { st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Timestamp.class, rs.getObject("ts").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(Timestamp.valueOf("2009-02-09 16:17:18"),
                    rs.getTimestamp("ts"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testTimestampWSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals(Timestamp.class, rs.getObject("tsw").getClass());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals(Timestamp.valueOf("2009-02-09 17:18:19"),
                    rs.getTimestamp("tsw"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testBitSimpleRead() {
        // This test is failing because of a BIT padding bug in the engine.
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("100000000", rs.getString("bt"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testBitVaryingSimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            assertEquals("10", rs.getString("btv"));
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testBinarySimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        byte[] expectedBytes = new byte[] {
            (byte) 0xa1, (byte) 0x03, (byte) 0, (byte) 0
        };
        byte[] ba;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals("A1030000", rs.getString("bin"));
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            ba = rs.getBytes("bin");
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
        } catch (SQLException se) { junit.framework.AssertionFailedError ase
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
        assertEquals("Retrieved bye array length wrong",
            expectedBytes.length, ba.length);
        for (int i = 0; i < ba.length; i++) {
            assertEquals("Byte " + i + " wrong", expectedBytes[i], ba[i]);
        }
    }

    public void testVarBinarySimpleRead() {
        ResultSet rs = null;
        Statement st = null;
        byte[] expectedBytes = new byte[] { (byte) 0xa1, (byte) 0x03 };
        byte[] ba;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals("A103", rs.getString("vb"));
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            ba = rs.getBytes("vb");
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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
        assertEquals("Retrieved bye array length wrong",
            expectedBytes.length, ba.length);
        for (int i = 0; i < ba.length; i++) {
            assertEquals("Byte " + i + " wrong", expectedBytes[i], ba[i]);
        }
    }

    public void testDaySecIntervalSimpleRead() {
        /* Since our client does not support the INTERVAL precision
         * constraints, the returned value will always be toString()'d to
         * precision of microseconds. */
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals("145 23:12:19.345000", rs.getString("dsival"));
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            // Can't test the class, because jdbc:odbc or the driver returns
            // a String for getObject() for interval values.
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testSecIntervalSimpleRead() {
        /* Since our client does not support the INTERVAL precision
         * constraints, the returned value will always be toString()'d to
         * precision of microseconds. */
        ResultSet rs = null;
        Statement st = null;
        try {
            st = netConn.createStatement();
            rs = st.executeQuery("SELECT * FROM alltypes WHERE id in (1, 2)");
            assertTrue("Got no rows with id in (1, 2)", rs.next());
            assertEquals("1000.345000", rs.getString("sival"));
            assertTrue("Got only one row with id in (1, 2)", rs.next());
            // Can't test the class, because jdbc:odbc or the driver returns
            // a String for getObject() for interval values.
            assertFalse("Got too many rows with id in (1, 2)", rs.next());
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

    public void testIntegerComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, i) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setInt(2, 495);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE i = ?");
            ps.setInt(1, 495);
            rs = ps.executeQuery();
            assertTrue("Got no rows with i = 495", rs.next());
            assertEquals(Integer.class, rs.getObject("i").getClass());
            assertTrue("Got only one row with i = 495", rs.next());
            assertEquals(495, rs.getInt("i"));
            assertFalse("Got too many rows with i = 495", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testTinyIntComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, ti) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setByte(2, (byte) 200);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE ti = ?");
            ps.setByte(1, (byte) 200);
            rs = ps.executeQuery();
            assertTrue("Got no rows with ti = 200", rs.next());
            assertEquals(Integer.class, rs.getObject("ti").getClass());
            assertTrue("Got only one row with ti = 200", rs.next());
            assertEquals((byte) 200, rs.getByte("ti"));
            assertFalse("Got too many rows with ti = 200", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testSmallIntComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, si) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setShort(2, (short) 395);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE si = ?");
            ps.setShort(1, (short) 395);
            rs = ps.executeQuery();
            assertTrue("Got no rows with si = 395", rs.next());
            assertEquals(Integer.class, rs.getObject("si").getClass());
            // Nb. HyperSQL purposefully returns an Integer for this type
            assertTrue("Got only one row with si = 395", rs.next());
            assertEquals((short) 395, rs.getShort("si"));
            assertFalse("Got too many rows with si = 395", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testBigIntComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, bi) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setLong(2, 295L);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE bi = ?");
            ps.setLong(1, 295L);
            rs = ps.executeQuery();
            assertTrue("Got no rows with bi = 295L", rs.next());
            assertEquals(Long.class, rs.getObject("bi").getClass());
            assertTrue("Got only one row with bi = 295L", rs.next());
            assertEquals(295L, rs.getLong("bi"));
            assertFalse("Got too many rows with bi = 295L", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    /* TODO:  Implement this test after get testNumericSimpleRead() working.
     *        See that method above.
    public void testNumericComplex() {
    */

    public void testFloatComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, f) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setFloat(2, 98.765F);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE f = ?");
            ps.setFloat(1, 98.765F);
            rs = ps.executeQuery();
            assertTrue("Got no rows with f = 98.765F", rs.next());
            assertEquals(Double.class, rs.getObject("f").getClass());
            assertTrue("Got only one row with f = 98.765F", rs.next());
            assertEquals(98.765D, rs.getDouble("f"), .01D);
            assertFalse("Got too many rows with f = 98.765F", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testDoubleComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, r) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setDouble(2, 876.54D);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE r = ?");
            ps.setDouble(1, 876.54D);
            rs = ps.executeQuery();
            assertTrue("Got no rows with r = 876.54D", rs.next());
            assertEquals(Double.class, rs.getObject("r").getClass());
            assertTrue("Got only one row with r = 876.54D", rs.next());
            assertEquals(876.54D, rs.getDouble("r"), 0D);
            assertFalse("Got too many rows with r = 876.54D", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testBooleanComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, b) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setBoolean(2, false);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE b = ?");
            ps.setBoolean(1, false);
            rs = ps.executeQuery();
            assertTrue("Got no rows with b = false", rs.next());
            assertEquals(Boolean.class, rs.getObject("b").getClass());
            assertTrue("Got only one row with b = false", rs.next());
            assertEquals(false, rs.getBoolean("b"));
            assertFalse("Got too many rows with b = false", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testCharComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, c) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, "xy");
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE c = ?");
            ps.setString(1, "xy ");
            rs = ps.executeQuery();
            assertTrue("Got no rows with c = 'xy '", rs.next());
            assertEquals(String.class, rs.getObject("c").getClass());
            assertTrue("Got only one row with c = 'xy '", rs.next());
            assertEquals("xy ", rs.getString("c"));
            assertFalse("Got too many rows with c = 'xy '", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testVarCharComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, cv) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, "xy");
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE cv = ?");
            ps.setString(1, "xy");
            rs = ps.executeQuery();
            assertTrue("Got no rows with cv = 'xy'", rs.next());
            assertEquals(String.class, rs.getObject("cv").getClass());
            assertTrue("Got only one row with cv = 'xy'", rs.next());
            assertEquals("xy", rs.getString("cv"));
            assertFalse("Got too many rows with cv = 'xy'", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    /**
     * TODO:  Find out if there is a way to select based on an expression
     * using a named derived pseudo-column.
    public void testDerivedComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "SELECT id, cv || 'app' appendage FROM alltypes\n"
                + "WHERE appendage = ?");
            ps.setString(1, "cvapp");
            rs = ps.executeQuery();
            assertTrue("Got no rows appendage = 'cvapp'", rs.next());
            assertEquals(String.class, rs.getObject("r").getClass());
            assertTrue("Got only one row with appendage = 'cvapp'", rs.next());
            assertEquals("cvapp", rs.getString("r"));
            assertFalse("Got too many rows with appendage = 'cvapp'", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }
    */

    public void testDateComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        java.sql.Date tomorrow =
                new java.sql.Date(new java.util.Date().getTime()
                        + 1000 * 60 * 60 * 24);

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, d) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setDate(2, tomorrow);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE d = ?");
            ps.setDate(1, tomorrow);
            rs = ps.executeQuery();
            assertTrue("Got no rows with d = tomorrow", rs.next());
            assertEquals(java.sql.Date.class, rs.getObject("d").getClass());
            assertTrue("Got only one row with d = tomorrow", rs.next());
            assertEquals(tomorrow.toString(), rs.getDate("d").toString());
            // Compare the Strings since "tomorrow" has resolution to
            // millisecond, but getDate() is probably to the day.
            assertFalse("Got too many rows with d = tomorrow", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testTimeComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Time aTime = Time.valueOf("21:19:27");

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, t) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setTime(2, aTime);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE t = ?");
            ps.setTime(1, aTime);
            rs = ps.executeQuery();
            assertTrue("Got no rows with t = aTime", rs.next());
            assertEquals(Time.class, rs.getObject("t").getClass());
            assertTrue("Got only one row with t = aTime", rs.next());
            assertEquals(aTime, rs.getTime("t"));
            assertFalse("Got too many rows with t = aTime", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    /* TODO:  Implement this test after get testTimeWSimpleRead() working.
     *        See that method above.
    public void testTimeWComplex() {
    */

    public void testTimestampComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Timestamp aTimestamp = Timestamp.valueOf("2009-03-27 17:18:19");

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, ts) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setTimestamp(2, aTimestamp);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE ts = ?");
            ps.setTimestamp(1, aTimestamp);
            rs = ps.executeQuery();
            assertTrue("Got no rows with ts = aTimestamp", rs.next());
            assertEquals(Timestamp.class, rs.getObject("ts").getClass());
            assertTrue("Got only one row with ts = aTimestamp", rs.next());
            assertEquals(aTimestamp, rs.getTimestamp("ts"));
            assertFalse("Got too many rows with ts = aTimestamp", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testTimestampWComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Timestamp aTimestamp = Timestamp.valueOf("2009-03-27 17:18:19");

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, tsw) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setTimestamp(2, aTimestamp);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE tsw = ?");
            ps.setTimestamp(1, aTimestamp);
            rs = ps.executeQuery();
            assertTrue("Got no rows with tsw = aTimestamp", rs.next());
            assertEquals(Timestamp.class, rs.getObject("tsw").getClass());
            assertTrue("Got only one row with tsw = aTimestamp", rs.next());
            assertEquals(aTimestamp, rs.getTimestamp("tsw"));
            assertFalse("Got too many rows with tsw = aTimestamp", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    /*
     * Driver needs to be modified to transfer bits in byte (binary) fashion,
     * the same as is done for VARBINARY/bytea type.
    public void testBitComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, bt) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, "101");
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE bt = ?");
            ps.setString(1, "101");
            rs = ps.executeQuery();
            assertTrue("Got no rows with bt = 101", rs.next());
            assertEquals(String.class, rs.getObject("bt").getClass());
            assertTrue("Got only one row with bt = 101", rs.next());
            assertEquals("101000000", rs.getString("bt"));
            assertFalse("Got too many rows with bt = 101", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            } }
    }

    public void testBitVaryingComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, btv) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, "10101");
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE btv = ?");
            ps.setString(1, "10101");
            rs = ps.executeQuery();
            assertTrue("Got no rows with btv = 10101", rs.next());
            assertEquals(String.class, rs.getObject("btv").getClass());
            assertTrue("Got only one row with btv = 10101", rs.next());
            assertEquals("10101", rs.getString("btv"));
            assertFalse("Got too many rows with btv = 10101", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }
    */

    public void testBinaryComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        byte[] expectedBytes = new byte[] {
            (byte) 0xaa, (byte) 0x99, (byte) 0, (byte) 0
        };
        byte[] ba1, ba2;

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, bin) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setBytes(2, expectedBytes);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE bin = ?");
            ps.setBytes(1, expectedBytes);
            rs = ps.executeQuery();
            assertTrue("Got no rows with bin = b'AA99'", rs.next());
            ba1 = rs.getBytes("bin");
            assertTrue("Got only one row with bin = b'AA99'", rs.next());
            ba2 = rs.getBytes("bin");
            assertFalse("Got too many rows with bin = b'AA99'", rs.next());
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
                if (ps != null) {
                    ps.close();
                } } catch(Exception e) {
            }
        }
        assertEquals("Retrieved bye array length wrong (1)",
            expectedBytes.length, ba1.length);
        for (int i = 0; i < ba1.length; i++) {
            assertEquals("Byte " + i + " wrong (1)", expectedBytes[i], ba1[i]);
        }
        assertEquals("Retrieved bye array length wrong (2)",
            expectedBytes.length, ba2.length);
        for (int i = 0; i < ba2.length; i++) {
            assertEquals("Byte " + i + " wrong (2)", expectedBytes[i], ba2[i]);
        }
    }

    public void testVarBinaryComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        byte[] expectedBytes = new byte[] { (byte) 0xaa, (byte) 0x99 };
        byte[] ba1, ba2;

        try {
            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, vb) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setBytes(2, expectedBytes);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE vb = ?");
            ps.setBytes(1, expectedBytes);
            rs = ps.executeQuery();
            assertTrue("Got no rows with vb = b'AA99'", rs.next());
            ba1 = rs.getBytes("vb");
            assertTrue("Got only one row with vb = b'AA99'", rs.next());
            ba2 = rs.getBytes("vb");
            assertFalse("Got too many rows with vb = b'AA99'", rs.next());
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
                if (ps != null) {
                    ps.close();
                } } catch(Exception e) {
            }
        }
        assertEquals("Retrieved bye array length wrong (1)",
            expectedBytes.length, ba1.length);
        for (int i = 0; i < ba1.length; i++) {
            assertEquals("Byte " + i + " wrong (1)", expectedBytes[i], ba1[i]);
        }
        assertEquals("Retrieved bye array length wrong (2)",
            expectedBytes.length, ba2.length);
        for (int i = 0; i < ba2.length; i++) {
            assertEquals("Byte " + i + " wrong (2)", expectedBytes[i], ba2[i]);
        }
    }

    /*
     * TODO:  Learn how to set input params for INTERVAL types.
     *        I don't see how I could set the variant
     *        (HOUR, ...TO SECOND, etc.) with setString() or anything else.
    public void testDaySecIntervalComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            assertEquals("145 23:12:19.345000", rs.getString("dsival"));

            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, dsival) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, 876.54D);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE dsival = ?");
            ps.setString(1, 876.54D);
            rs = ps.executeQuery();
            assertTrue("Got no rows with dsival = 876.54D", rs.next());
            assertEquals(String.class, rs.getObject("dsival").getClass());
            assertTrue("Got only one row with dsival = 876.54D", rs.next());
            assertEquals(876.54D, rs.getString("dsival"));
            assertFalse("Got too many rows with dsival = 876.54D", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }

    public void testSecIntervalComplex() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            assertEquals("1000.345000", rs.getString("sival"));

            ps = netConn.prepareStatement(
                "INSERT INTO alltypes(id, sival) VALUES(?, ?)");
            ps.setInt(1, 3);
            ps.setString(2, 876.54D);
            assertEquals(1, ps.executeUpdate());
            ps.setInt(1, 4);
            assertEquals(1, ps.executeUpdate());
            ps.close();
            netConn.commit();
            ps = netConn.prepareStatement(
                "SELECT * FROM alltypes WHERE sival = ?");
            ps.setString(1, 876.54D);
            rs = ps.executeQuery();
            assertTrue("Got no rows with sival = 876.54D", rs.next());
            assertEquals(String.class, rs.getObject("sival").getClass());
            assertTrue("Got only one row with sival = 876.54D", rs.next());
            assertEquals(876.54D, rs.getString("sival"));
            assertFalse("Got too many rows with sival = 876.54D", rs.next());
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
                if (ps != null) {
                    ps.close();
                }
            } catch(Exception e) {
            }
        }
    }
    */

    public static void main(String[] sa) {
        staticRunner(TestOdbcTypes.class, sa);
    }

    /*
    static protected boolean closeEnough(Time t1, Time t2, int fudgeMin) {
        long delta = t1.getTime() - t2.getTime();
        if (delta < 0) {
            delta *= -1;
        }
        //System.err.println("Delta  " + delta);
        //System.err.println("exp  " + (fudgeMin * 1000 * 60));
        return delta < fudgeMin * 1000 * 60;
    }
    */
}
