/* Copyright (c) 2001-2021, The HSQL Development Group
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

/**
 * Test cases for HSQLDB aggregates and HAVING clause.
 *
 * @author Nicholas Quek (kocolipy@users dot sourceforge.net)
 * @version 2.5.1
 * @since 2.5.1
 */

public class TestDataCube extends TestCase {

    //------------------------------------------------------------
    // Class variables
    //------------------------------------------------------------
    private static final String databaseDriver   = "org.hsqldb.jdbc.JDBCDriver";
    private static final String databaseURL      = "jdbc:hsqldb:mem:.";
    private static final String databaseUser     = "sa";
    private static final String databasePassword = "";

    //------------------------------------------------------------
    // Instance variables
    //------------------------------------------------------------
    private Connection conn;
    private Statement  stmt;

    //------------------------------------------------------------
    // Constructors
    //------------------------------------------------------------

    /**
     * Constructs a new SubselectTest.
     */
    public TestDataCube(String s) {
        super(s);
    }

    //------------------------------------------------------------
    // Class methods
    //------------------------------------------------------------
    protected static Connection getJDBCConnection() throws SQLException {
        return DriverManager.getConnection(databaseURL, databaseUser,
                                           databasePassword);
    }

    protected void setUp() throws Exception {

        super.setUp();

        if (conn != null) {
            return;
        }

        Class.forName(databaseDriver);

        conn = getJDBCConnection();
        stmt = conn.createStatement();

        try {
            stmt.execute("DROP TABLE REVENUE IF EXISTS");
            stmt.execute("DROP TABLE LIABILITY IF EXISTS");
            stmt.execute("DROP TABLE TEST IF EXISTS");
        } catch (Exception x) {}

        stmt.execute("CREATE TABLE REVENUE(CHANNEL VARCHAR(20), YEAR INTEGER, " +
                        "COUNTRY VARCHAR(2), PROVINCE VARCHAR(20), SALES INTEGER);");

        //Channel: Internet
        addRevenueSource("INTERNET", 2009, "GB", "CAMBRIDGE", 10000);
        addRevenueSource("INTERNET", 2009, "GB", "OXFORD", 15000);
        addRevenueSource("INTERNET", 2009, "US", "STANFORD", 100000);        
        addRevenueSource("INTERNET", 2009, "US", "NEW YORK", 175000);
        addRevenueSource("INTERNET", 2010, "GB", "CAMBRIDGE", 20000);
        addRevenueSource("INTERNET", 2010, "GB", "OXFORD", 25000);
        addRevenueSource("INTERNET", 2010, "US", "STANFORD", 200000);
        addRevenueSource("INTERNET", 2010, "US", "NEW YORK", 300000);

        //Channel: Direct Sales
        addRevenueSource("DIRECT SALES", 2009, "GB", "CAMBRIDGE", 80000);
        addRevenueSource("DIRECT SALES", 2009, "GB", "OXFORD", 82000);
        addRevenueSource("DIRECT SALES", 2009, "US", "STANFORD", 802500);
        addRevenueSource("DIRECT SALES", 2009, "US", "NEW YORK", 800000);
        addRevenueSource("DIRECT SALES", 2010, "GB", "CAMBRIDGE", 90000);
        addRevenueSource("DIRECT SALES", 2010, "GB", "OXFORD", 91000);
        addRevenueSource("DIRECT SALES", 2010, "US", "STANFORD", 900000);
        addRevenueSource("DIRECT SALES", 2010, "US", "NEW YORK", 933000);

        stmt.execute("CREATE TABLE LIABILITY(CHANNEL VARCHAR(20), YEAR INTEGER, " +
                "COUNTRY VARCHAR(2), LOSSES INTEGER);");

        addLiabilitySource("INTERNET", 2009, "GB", 1000);
        addLiabilitySource("INTERNET", 2010, "GB", 2000);
        addLiabilitySource("INTERNET", 2009, "US", 4000);
        addLiabilitySource("INTERNET", 2010, "US", 5000);

        stmt.execute("CREATE TABLE TEST(SEL INTEGER, NAME1 VARCHAR(3), NAME2 VARCHAR(3));");

        stmt.execute("INSERT INTO TEST (SEL, NAME1, NAME2) VALUES (0, 'FOO', 'BAR')");
        stmt.execute("INSERT INTO TEST (SEL, NAME1, NAME2) VALUES (1, 'BAZ', 'FOO')");
        stmt.execute("INSERT INTO TEST (SEL, NAME1, NAME2) VALUES (1, 'FOO', 'QUX')");
    }

    protected void tearDown() throws Exception {

        try {
            stmt.execute("DROP TABLE REVENUE IF EXISTS;");
        } catch (Exception x) {}

        if (stmt != null) {
            stmt.close();

            stmt = null;
        }

        if (conn != null) {
            conn.close();

            conn = null;
        }

        super.tearDown();

    }

    private void addRevenueSource(String channel, int year, String country,
                             String province, int sales) throws Exception {

        stmt.execute("INSERT INTO REVENUE VALUES ('" + channel + "', '" + year
                     + "', '" + country + "', '" + province + "', "
                     + sales + ");");
    }
    private void addLiabilitySource(String channel, int year, String country, int losses) throws Exception {

        stmt.execute("INSERT INTO LIABILITY VALUES ('" + channel + "', '" + year
                + "', '" + country  + "', "
                + losses + ");");
    }
    //------------------------------------------------------------
    // CUBE OPERATOR TEST
    //------------------------------------------------------------
    /**
     * Tests aggregated selection using the CUBE operator with a <b>GROUP_BY</b> clause.
     * This is a normal use of the CUBE operator acting on one column.
     * The result set will contain two groupings: (CHANNEL), ()
     */
    public void testAggregatedGroupByCube() throws SQLException {
        String sql = "SELECT CHANNEL, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(CHANNEL);\n";
        Object[][] expected = new Object[][] {
            {
                null, Integer.valueOf(4623500)
            }, {
                "INTERNET", Integer.valueOf(845000)
            }, {
                "DIRECT SALES", Integer.valueOf(3778500)
            },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the CUBE operator with a <b>GROUP_BY</b> clause.
     * The CUBE operator acts on several column.
     * The result set will contain eight groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL, COUNTRY), (CHANNEL),
     * (YEAR, COUNTRY), (YEAR), (COUNTRY), ()
     */
    public void testAggregatedGroupByCube1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(CHANNEL, YEAR, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500)
            }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
            }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
            }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
            }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
            }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
            }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
            }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
            }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
            }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000)
            }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000)
            }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500)
            }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000)
            }, {
                "INTERNET", null, "GB", Integer.valueOf(70000)
            }, {
                "INTERNET", null, "US", Integer.valueOf(775000)
            }, {
                "DIRECT SALES", null, "GB", Integer.valueOf(343000)
            }, {
                "DIRECT SALES", null, "US", Integer.valueOf(3435500)
            }, {
                "INTERNET", null, null, Integer.valueOf(845000)
            }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
            }, {
                null, Integer.valueOf(2009), "GB", Integer.valueOf(187000)
            }, {
                null, Integer.valueOf(2009), "US", Integer.valueOf(1877500)
            }, {
                null, Integer.valueOf(2010), "GB", Integer.valueOf(226000)
            }, {
                null, Integer.valueOf(2010), "US", Integer.valueOf(2333000)
            }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
            }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
            }, {
                null, null, "GB", Integer.valueOf(413000)
            }, {
                null, null, "US", Integer.valueOf(4210500)
            },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a CUBE operator with no columns.
     * A SQLException should be thrown.
     */
    public void testInvalidCube() throws SQLException {
        String sql = "SELECT SUM(SALES)\n" +
                     "FROM REVENUE \n" +
                     "GROUP BY CUBE();\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a CUBE operator nested within parenthesis
     * A SQLException should be thrown.
     */
    public void testInvalidCube1() throws SQLException {
        String sql = "SELECT CHANNEL, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY (CUBE(CHANNEL));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    //------------------------------------------------------------
    // ROLLUP OPERATOR TEST
    //------------------------------------------------------------
    /**
     * Tests aggregated selection using the ROLLUP operator with a <b>GROUP_BY</b> clause.
     * This is a normal use of the ROLLUP operator acting on one column.
     * The result set will contain two groupings: (CHANNEL), ()
     */
    public void testAggregatedGroupByRollup() throws SQLException {
        String sql = "SELECT CHANNEL, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL);\n";
        Object[][] expected = new Object[][] {
                {
                        null, Integer.valueOf(4623500)
                }, {
                "INTERNET", Integer.valueOf(845000)
        }, {
                "DIRECT SALES", Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the ROLLUP operator with a <b>GROUP_BY</b> clause.
     * The ROLLUP operator acts on several column.
     * The result set will contain four groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL), (),
     */
    public void testAggregatedGroupByRollup1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL, YEAR, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a ROLLUP operator with no columns.
     * A SQLException should be thrown.
     */
    public void testInvalidRollup() throws SQLException {
        String sql = "SELECT SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP();\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a ROLLUP operator nested within parenthesis
     * A SQLException should be thrown.
     */
    public void testInvalidRollup1() throws SQLException {
        String sql = "SELECT CHANNEL, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY (ROLLUP(CHANNEL));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    //------------------------------------------------------------
    // GROUPING SETS OPERATOR TEST
    //------------------------------------------------------------
    /**
     * Tests aggregated selection using the GROUPING SETS operator with a <b>GROUP_BY</b> clause.
     * This is a trivial use of the GROUPING SETS operator acting on one group.
     * Equivalent to "GROUP BY CHANNEL"
     */
    public void testAggregatedGroupByGS() throws SQLException {
        String sql = "SELECT CHANNEL, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS(CHANNEL);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(845000)
        }, {
                "DIRECT SALES", Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the GROUPING SETS operator with a <b>GROUP_BY</b> clause.
     * The GROUPING SETS operator acts on several groups of dimension 1.
     * The result set will contain three groupings:
     * (CHANNEL), (YEAR), (COUNTRY)
     */
    public void testAggregatedGroupByGS1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS(CHANNEL, YEAR, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
        }, {
                null, null, "GB", Integer.valueOf(413000)
        }, {
                null, null, "US", Integer.valueOf(4210500)
        },
        };
        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the GROUPING SETS operator with a <b>GROUP_BY</b> clause.
     * The GROUPING SETS operator acts on several groups of various dimensions.
     * The result set will contain three groupings:
     * (CHANNEL, YEAR, COUNTRY), (YEAR), (COUNTRY, YEAR), ()
     */
    public void testAggregatedGroupByGS2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS((CHANNEL, YEAR, COUNTRY),(YEAR), (COUNTRY, YEAR), ());\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
        }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
        }, {
                null, Integer.valueOf(2009), "GB", Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", Integer.valueOf(2333000)
        }
        };
        compareResults(sql, expected, "00000");
    }


    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a GROUPING SETS operator with no columns.
     * A SQLException should be thrown.
     */
    public void testInvalidGS() throws SQLException {
        String sql = "SELECT SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS();\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains a GROUPING SETS operator nested within parenthesis
     * A SQLException should be thrown.
     */
    public void testInvalidGS1() throws SQLException {
        String sql = "SELECT COUNTRY, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY (GROUPING SETS(COUNTRY));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }


    //------------------------------------------------------------
    // GROUPING OPERATOR TEST
    //------------------------------------------------------------
    /**
     * Tests grouping display using the GROUPING operator.
     * The grouping column will return the bit mask of the columns specified
     * Only 1 column is specified, so it will return 0 if the corresponding
     * column is included in the grouping criteria of the grouping set for that row,
     * and 1 if it is not.
     *
     * The ROLLUP operator acts on several column, producing four groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL), ()
     */
    public void testAggregatedGrouping() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES), \n" +
                "GROUPING(CHANNEL) AS CH, GROUPING(YEAR) as YR, GROUPING(COUNTRY) AS CO\n" +
                "FROM REVENUE\n" +
                "GROUP BY ROLLUP(CHANNEL,YEAR,COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500), 1, 1, 1
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000), 0, 0, 0
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000), 0, 0, 0
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000), 0, 0, 0
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000), 0, 0, 0
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000), 0, 0, 0
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500), 0, 0, 0
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000), 0, 0, 0
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000), 0, 0, 0
        }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000), 0, 0, 1
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000), 0, 0, 1
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500), 0, 0, 1
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000), 0, 0, 1
        }, {
                "INTERNET", null, null, Integer.valueOf(845000), 0, 1, 1
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500), 0, 1, 1
        }
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests grouping display using the GROUPING operator with multiple parameters.
     *
     * The grouping column will return the bit mask of the columns specified
     * Bits are assigned with the rightmost argument being the least-significant bit;
     * Bits are set to 0 if the corresponding column is included in the grouping
     * criteria of the grouping set for that row, and 1 if it is not.
     *
     * The CUBE operator acts on several column, producing four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedGrouping1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES), GROUPING(CHANNEL, YEAR) " +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL,YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500), 3,
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000), 0,
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000), 0,
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500), 0,
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000), 0,
        }, {
                "INTERNET", null, Integer.valueOf(845000), 1,
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500), 1
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500), 2,
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000), 2,
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the interaction between the GROUPING operator and the
     * DECODE OPERATOR.
     *
     * If the row does not involve the column in the grouping set,
     * it will return 'MULTI-[COLUMN]' instead of NULLs,
     * else it will return the value of the column.
     *
     * The CUBE operator acts on several column, producing four groupings:
     * (CHANNEL, COUNTRY), (CHANNEL), (COUNTRY), ()
     */
    public void testAggregatedDecode() throws SQLException {
        String sql = "SELECT DECODE(GROUPING(CHANNEL), 1, 'MULTI-CHANNEL', CHANNEL), DECODE(GROUPING(COUNTRY), 0, COUNTRY, 'MULTI-COUNTRY'), SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                "MULTI-CHANNEL", "MULTI-COUNTRY", Integer.valueOf(4623500),
        }, {
                "INTERNET", "GB" , Integer.valueOf(70000),
        }, {
                "INTERNET", "US", Integer.valueOf(775000),
        }, {
                "DIRECT SALES", "GB", Integer.valueOf(343000),
        }, {
                "DIRECT SALES", "US", Integer.valueOf(3435500),
        }, {
                "INTERNET", "MULTI-COUNTRY", Integer.valueOf(845000),
        }, {
                "DIRECT SALES", "MULTI-COUNTRY", Integer.valueOf(3778500),
        }, {
                "MULTI-CHANNEL", "GB", Integer.valueOf(413000),
        }, {
                "MULTI-CHANNEL", "US", Integer.valueOf(4210500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the interaction between the GROUPING operator with multiple
     * dimensions and the DECODE OPERATOR.
     *
     * The CUBE operator acts on several column, producing four groupings:
     * (CHANNEL, COUNTRY), (CHANNEL), (COUNTRY), ()
     *
     * () evaluates to GRAND TOTAL, (CHANNEL) evaluate to the channel used,
     * (COUNTRY) evaluates to the country and (CHANNEL, COUNTRY) evaluates to NULL
     */
    public void testAggregatedDecode1() throws SQLException {
        String sql = "SELECT DECODE(GROUPING(CHANNEL, COUNTRY), 3, 'GRAND TOTAL', 2, COUNTRY, " +
                "1, CHANNEL, 0, null), SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                "GRAND TOTAL", Integer.valueOf(4623500),
        }, {
                null, Integer.valueOf(70000),
        }, {
                null,  Integer.valueOf(775000),
        }, {
                null,  Integer.valueOf(343000),
        }, {
                null, Integer.valueOf(3435500),
        }, {
                "INTERNET", Integer.valueOf(845000),
        }, {
                "DIRECT SALES", Integer.valueOf(3778500),
        }, {
                "GB", Integer.valueOf(413000),
        }, {
                "US", Integer.valueOf(4210500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * a GROUPING operator
     *
     * A SQLException should be thrown.
     */
    public void testInvalidGrouping() throws SQLException {
        String sql = "SELECT CHANNEL, COUNTRY, SUM(SALES)\n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, GROUPING(COUNTRY));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42572");
    }

    //------------------------------------------------------------
    // COMPOSITE COLUMNS TEST
    //------------------------------------------------------------
    /**
     * Tests interaction of composite columns using the CUBE operator.
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL), (YEAR, COUNTRY), ()
     */
    public void testAggregatedComposite() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, (YEAR, COUNTRY));\n";
        Object[][] expected = new Object[][] {
        {
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
        }, {
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), "GB", Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", Integer.valueOf(2333000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests interaction of composite columns using the ROLLUP operator.
     *
     * The ROLLUP operator produces three groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), ()
     */
    public void testAggregatedComposite1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY ROLLUP((CHANNEL, YEAR), COUNTRY);\n";
        Object[][] expected = new Object[][] {
        {
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests interaction of multiple composite columns using the CUBE operator.
     *
     * The ROLLUP operator produces four groupings:
     * (CHANNEL, YEAR, COUNTRY, PROVINCE), (CHANNEL, YEAR), (COUNTRY, PROVINCE), ()
     */
    public void testAggregatedComposite2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE((CHANNEL, YEAR), (COUNTRY, PROVINCE));\n";
        Object[][] expected = new Object[][] {
        {
                null, null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(15000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(100000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(175000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(20000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(200000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(82000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(802500)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(800000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(91000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(900000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(933000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null,Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        }, {
                null, null, "GB", "CAMBRIDGE", Integer.valueOf(200000)
        }, {
                null, null, "GB", "OXFORD", Integer.valueOf(213000)
        }, {
                null, null, "US", "STANFORD", Integer.valueOf(2002500)
        }, {
                null, null, "US", "NEW YORK", Integer.valueOf(2208000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // CONCATENATED GROUPINGS TEST
    //------------------------------------------------------------
    /**
     * Tests concatenated groupings using a column with GROUPING SETS.
     *
     * Two groupings should be produced:
     * (CHANNEL, YEAR), (CHANNEL, COUNTRY)
     */
    public void testAggregatedConcat() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CHANNEL, GROUPING SETS (YEAR, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000),
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000),
        }, {
                "INTERNET", null, "GB", Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US",  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB", Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US", Integer.valueOf(3435500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests concatenated groupings using GROUPING SETS.
     *
     * Four groupings should be produced:
     * (CHANNEL, COUNTRY), (YEAR, COUNTRY), (CHANNEL, PROVINCE), (YEAR, PROVINCE)
     */
    public void testAggregatedConcat1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS (CHANNEL, YEAR), GROUPING SETS (COUNTRY, PROVINCE);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                null, Integer.valueOf(2009), "GB", null, Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", null, Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", null, Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", null, Integer.valueOf(2333000)
        }, {
                "INTERNET", null, null, "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, null,  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, null,  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, null,  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, null, "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, null, "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, null, "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, null, "NEW YORK", Integer.valueOf(1733000)
        }, {
                null, Integer.valueOf(2009), null, "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                null, Integer.valueOf(2009), null, "OXFORD", Integer.valueOf(97000)
        }, {
                null, Integer.valueOf(2009), null, "STANFORD", Integer.valueOf(902500)
        }, {
                null, Integer.valueOf(2009), null, "NEW YORK", Integer.valueOf(975000)
        }, {
                null, Integer.valueOf(2010), null, "CAMBRIDGE", Integer.valueOf(110000)
        }, {
                null, Integer.valueOf(2010), null, "OXFORD", Integer.valueOf(116000)
        }, {
                null, Integer.valueOf(2010), null, "STANFORD", Integer.valueOf(1100000)
        }, {
                null, Integer.valueOf(2010), null, "NEW YORK", Integer.valueOf(1233000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests concatenated groupings using CUBE.
     *
     * Four groupings should be produced:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL, COUNTRY), (CHANNEL)
     */
    public void testAggregatedConcat2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CHANNEL, CUBE(YEAR, COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2009), "GB", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, "GB", Integer.valueOf(70000)
        }, {
                "INTERNET", null, "US", Integer.valueOf(775000)
        }, {
                "DIRECT SALES", null, "GB", Integer.valueOf(343000)
        }, {
                "DIRECT SALES", null, "US", Integer.valueOf(3435500)
        }, {
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests concatenated groupings using CUBE and GROUPING SETS.
     *
     * Eight groupings should be produced:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, COUNTRY), (YEAR, COUNTRY), (COUNTRY),
     * (CHANNEL, YEAR, PROVINCE), (CHANNEL, PROVINCE), (YEAR, PROVINCE), (PROVINCE)
     */
    public void testAggregatedConcat3() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR), GROUPING SETS(COUNTRY, PROVINCE);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2009), "GB", null, Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", null, Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", null, Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", null, Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", null, Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", null, Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", null, Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", null, Integer.valueOf(1833000)
        }, {
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                null, Integer.valueOf(2009), "GB", null, Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", null, Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", null, Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", null, Integer.valueOf(2333000)
        }, {
                null, null, "GB", null, Integer.valueOf(413000)
        }, {
                null, null, "US", null, Integer.valueOf(4210500)
        }, {
                "INTERNET", Integer.valueOf(2009), null, "CAMBRIDGE", Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, "OXFORD", Integer.valueOf(15000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, "STANFORD", Integer.valueOf(100000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, "NEW YORK", Integer.valueOf(175000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, "CAMBRIDGE", Integer.valueOf(20000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, "OXFORD", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, "STANFORD", Integer.valueOf(200000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, "NEW YORK", Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, "CAMBRIDGE", Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, "OXFORD", Integer.valueOf(82000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, "STANFORD", Integer.valueOf(802500)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, "NEW YORK", Integer.valueOf(800000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, "OXFORD", Integer.valueOf(91000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, "STANFORD", Integer.valueOf(900000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, "NEW YORK", Integer.valueOf(933000)
        }, {
                "INTERNET", null, null, "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, null,  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, null,  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, null,  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, null, "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, null, "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, null, "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, null, "NEW YORK", Integer.valueOf(1733000)
        }, {
                null, Integer.valueOf(2009), null, "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                null, Integer.valueOf(2009), null, "OXFORD", Integer.valueOf(97000)
        }, {
                null, Integer.valueOf(2009), null, "STANFORD", Integer.valueOf(902500)
        }, {
                null, Integer.valueOf(2009), null, "NEW YORK", Integer.valueOf(975000)
        }, {
                null, Integer.valueOf(2010), null, "CAMBRIDGE", Integer.valueOf(110000)
        }, {
                null, Integer.valueOf(2010), null, "OXFORD", Integer.valueOf(116000)
        }, {
                null, Integer.valueOf(2010), null, "STANFORD", Integer.valueOf(1100000)
        }, {
                null, Integer.valueOf(2010), null, "NEW YORK", Integer.valueOf(1233000)
        }, {
                null, null, null, "CAMBRIDGE", Integer.valueOf(200000)
        }, {
                null, null, null, "OXFORD", Integer.valueOf(213000)
        }, {
                null, null, null, "STANFORD", Integer.valueOf(2002500)
        }, {
                null, null, null, "NEW YORK", Integer.valueOf(2208000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests concatenated groupings using GROUPING SETS and ROLLUP.
     *
     * Six groupings should be produced:
     * (CHANNEL, COUNTRY, PROVINCE), (YEAR, COUNTRY, PROVINCE),
     * (CHANNEL, COUNTRY), (YEAR, COUNTRY),
     * (CHANNEL),(YEAR)
     */
    public void testAggregatedConcat4() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS(CHANNEL, YEAR), ROLLUP(COUNTRY, PROVINCE);\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, "GB", "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, "GB",  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, "US",  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, "US",  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, "GB", "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, "GB", "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, "US", "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, "US", "NEW YORK", Integer.valueOf(1733000)
        }, {
                null, Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                null, Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(97000)
        }, {
                null, Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(902500)
        }, {
                null, Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(975000)
        }, {
                null, Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(110000)
        }, {
                null, Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(116000)
        }, {
                null, Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(1100000)
        }, {
                null, Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(1233000)
        }, {
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                null, Integer.valueOf(2009), "GB", null, Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", null, Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", null, Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", null, Integer.valueOf(2333000)
        }, {
                "INTERNET", null, null, null,Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), null, null,Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, null, Integer.valueOf(2559000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests concatenated groupings using CUBE and ROLLUP.
     *
     * Twelve groupings should be produced:
     * (CHANNEL, YEAR, COUNTRY, PROVINCE), (CHANNEL, COUNTRY, PROVINCE), (YEAR, COUNTRY, PROVINCE), (COUNTRY, PROVINCE),
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, COUNTRY), (YEAR, COUNTRY), (COUNTRY),
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), (),
     */
    public void testAggregatedConcat5() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR), ROLLUP(COUNTRY, PROVINCE);\n";
        Object[][] expected = new Object[][] {{
                null, null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(15000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(100000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(175000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(20000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(200000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(82000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(802500)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(800000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(91000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(900000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(933000)
        }, {
                "INTERNET", null, "GB", "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, "GB",  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, "US",  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, "US",  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, "GB", "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, "GB", "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, "US", "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, "US", "NEW YORK", Integer.valueOf(1733000)
        }, {
                null, Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                null, Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(97000)
        }, {
                null, Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(902500)
        }, {
                null, Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(975000)
        }, {
                null, Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(110000)
        }, {
                null, Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(116000)
        }, {
                null, Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(1100000)
        }, {
                null, Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(1233000)
        }, {
                null, null, "GB", "CAMBRIDGE", Integer.valueOf(200000)
        }, {
                null, null, "GB", "OXFORD", Integer.valueOf(213000)
        }, {
                null, null, "US", "STANFORD", Integer.valueOf(2002500)
        }, {
                null, null, "US", "NEW YORK", Integer.valueOf(2208000)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", null, Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", null, Integer.valueOf(275000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", null, Integer.valueOf(45000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", null, Integer.valueOf(500000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", null, Integer.valueOf(162000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", null, Integer.valueOf(1602500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", null, Integer.valueOf(181000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", null, Integer.valueOf(1833000)
        }, {
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                null, Integer.valueOf(2009), "GB", null, Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", null, Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", null, Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", null, Integer.valueOf(2333000)
        }, {
                null, null, "GB", null, Integer.valueOf(413000)
        }, {
                null, null, "US", null, Integer.valueOf(4210500)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null,Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, null,Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), null, null,Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, null, Integer.valueOf(2559000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // NESTED GROUPING TEST
    //------------------------------------------------------------

    /**
     * Tests nested groupings using GROUPING SETS within GROUPING SETS.
     *
     * Three groupings should be produced:
     * (CHANNEL), (YEAR), (COUNTRY)
     */
    public void testAggregatedNest() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS(CHANNEL, GROUPING SETS (YEAR, COUNTRY));\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
        }, {
                null, null, "GB", Integer.valueOf(413000),
        }, {
                null, null, "US", Integer.valueOf(4210500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests nested groupings using CUBE within GROUPING SETS.
     *
     * Five groupings should be produced:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), (), (COUNTRY)
     */
    public void testAggregatedNest1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS(CUBE(CHANNEL, YEAR), COUNTRY);\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
        }, {
                null, null, "GB", Integer.valueOf(413000)
        }, {
                null, null, "US", Integer.valueOf(4210500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests nested groupings using ROLLUP within GROUPING SETS.
     *
     * Four groupings should be produced:
     * (CHANNEL), (YEAR, COUNTRY), (YEAR), ()
     */
    public void testAggregatedNest2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS(CHANNEL, ROLLUP(YEAR, COUNTRY));\n";
        Object[][] expected = new Object[][] {{
                null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), "GB", Integer.valueOf(187000)
        }, {
                null, Integer.valueOf(2009), "US", Integer.valueOf(1877500)
        }, {
                null, Integer.valueOf(2010), "GB", Integer.valueOf(226000)
        }, {
                null, Integer.valueOf(2010), "US", Integer.valueOf(2333000)
        }, {
                null, Integer.valueOf(2009), null, Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), null, Integer.valueOf(2559000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests nested groupings using multiple ROLLUP within GROUPING SETS.
     *
     * Six groupings should be produced:
     * (CHANNEL, YEAR), (CHANNEL), (), (COUNTRY, PROVINCE), (COUNTRY), ()
     */
    public void testAggregatedNest3() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY GROUPING SETS(ROLLUP(CHANNEL, YEAR), ROLLUP(COUNTRY, PROVINCE));\n";
        Object[][] expected = new Object[][] {{
                null, null, null, null, Integer.valueOf(4623500)
        }, {
                null, null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                null, null, "GB", "CAMBRIDGE", Integer.valueOf(200000)
        }, {
                null, null, "GB", "OXFORD", Integer.valueOf(213000)
        }, {
                null, null, "US", "STANFORD", Integer.valueOf(2002500)
        }, {
                null, null, "US", "NEW YORK", Integer.valueOf(2208000)
        }, {
                null, null, "GB", null, Integer.valueOf(413000)
        }, {
                null, null, "US", null, Integer.valueOf(4210500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests DISTINCT nested groupings using multiple ROLLUP within GROUPING SETS.
     *
     * Six groupings should be produced:
     * (CHANNEL, YEAR), (CHANNEL), (), (COUNTRY, PROVINCE), (COUNTRY), ()
     *
     * The second empty grouping is eliminated
     */
    public void testDistinctAggregatedNest3() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY DISTINCT GROUPING SETS(ROLLUP(CHANNEL, YEAR), ROLLUP(COUNTRY, PROVINCE));\n";
        Object[][] expected = new Object[][] {{
                null, null, null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null, Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                null, null, "GB", "CAMBRIDGE", Integer.valueOf(200000)
        }, {
                null, null, "GB", "OXFORD", Integer.valueOf(213000)
        }, {
                null, null, "US", "STANFORD", Integer.valueOf(2002500)
        }, {
                null, null, "US", "NEW YORK", Integer.valueOf(2208000)
        }, {
                null, null, "GB", null, Integer.valueOf(413000)
        }, {
                null, null, "US", null, Integer.valueOf(4210500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested ROLLUP within CUBE
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(CHANNEL, ROLLUP(YEAR));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested CUBE within CUBE
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(CHANNEL, CUBE(YEAR));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested GROUPING SETS within CUBE
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(GROUPING SETS(CHANNEL), YEAR);\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }
    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested ROLLUP within ROLLUP
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting3() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL, ROLLUP(YEAR));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested CUBE within ROLLUP
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting4() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CUBE(CHANNEL), YEAR);\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * nested GROUPING SETS within ROLLUP
     *
     * A SQLException should be thrown.
     */
    public void testInvalidNesting5() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL, GROUPING SETS(YEAR));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42581");
    }
    //------------------------------------------------------------
    // WHERE CLAUSE TEST
    //------------------------------------------------------------
    /**
     * Tests <b>WHERE</b> clause with text fields.
     *
     * Rows with {@code COUNTRY <> 'GB'} will be removed before grouping occurs
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedWhere() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "WHERE COUNTRY = 'GB'\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(413000),
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(25000),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(45000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(162000),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(181000),
        }, {
                "INTERNET", null, Integer.valueOf(70000),
        }, {
                "DIRECT SALES", null, Integer.valueOf(343000),
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(187000),
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(226000),
        }
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>WHERE</b> clause with numerical fields.
     *
     * Rows with {@code SALES <= 20000} will be removed before grouping occurs
     *
     * ROLLUP will produce three groupings:
     * (CHANNEL, YEAR), (CHANNEL), ()
     */
    public void testAggregatedWhere1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "WHERE SALES > 20000\n" +
                "GROUP BY ROLLUP(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4578500),
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(275000),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(525000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000),
        }, {
                "INTERNET", null, Integer.valueOf(800000),
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500),
        }
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>WHERE</b> clause with multiple fields.
     *
     * Rows with {@code COUNTRY <> 'US' or YEAR = 2009} will be removed before grouping occurs
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedWhere2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "WHERE COUNTRY = 'US' AND YEAR <> 2009\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(2333000),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(500000),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(1833000),
        }, {
                "INTERNET", null, Integer.valueOf(500000),
        }, {
                "DIRECT SALES", null, Integer.valueOf(1833000),
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2333000),
        }
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // HAVING CLAUSE TEST
    //------------------------------------------------------------

    /**
     * Tests <b>HAVING</b> clause with aggregated column and multiple
     * HAVING conditions
     *
     * ROLLUP will produce four groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL), ()
     * which are then filtered by the <B>HAVING</B> condition
     */
    public void testAggregatedHaving() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY ROLLUP(CHANNEL,YEAR,COUNTRY) \n" +
                "HAVING SUM(SALES) >= 500000 AND SUM(SALES) < 2000000;\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2010), "US", Integer.valueOf(500000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", Integer.valueOf(1602500),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000),
        }, {
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, Integer.valueOf(1764500),
        }, {
                "INTERNET", null, null, Integer.valueOf(845000),
        }
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>HAVING</b> clause with numerical column
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     * which are then filtered by the <B>HAVING</B> condition
     */
    public void testAggregatedHaving1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n" +
                "HAVING MIN(YEAR) <> 2010;\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>HAVING</b> clause with text column
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     * which are then filtered by the <B>HAVING</B> condition
     */
    public void testAggregatedHaving2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n" +
                "HAVING MIN(CHANNEL) = 'INTERNET';\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // ORDER BY CLAUSE TEST
    //------------------------------------------------------------

    /**
     * Tests <b>ORDER BY</b> clause
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR),()
     */
    public void testAggregatedOrder() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n" +
                "ORDER BY SALES;\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000),
        }, {
                "INTERNET", null, Integer.valueOf(845000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000),
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500),
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000),
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500),
        }, {
                null, null, Integer.valueOf(4623500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>ORDER BY</b> clause by descending order
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR),()
     */
    public void testAggregatedOrder1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n" +
                "ORDER BY SALES DESC;\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500),
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500),
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000),
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500),
        }, {
                "INTERNET", null, Integer.valueOf(845000),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000),
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000),
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests <b>ORDER BY</b> clause with multiple columns
     *
     * CUBE will produce four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR),()
     */
    public void testAggregatedOrder2() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n" +
                "ORDER BY YEAR DESC, SALES ASC;\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, Integer.valueOf(845000),
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500),
        }, {
                null, null, Integer.valueOf(4623500),
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000),
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000),
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000),
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000),
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500),
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500),
        },
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // Miscellaneous columns TEST
    //------------------------------------------------------------

    /**
     * Tests the interaction with renamed columns with
     * aggregated selection using the CUBE operator with a <b>GROUP_BY</b> clause.
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedRename() throws SQLException {
        String sql = "SELECT CHANNEL as ROUTES, YEAR, SUM(SALES) as SALES\n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(CHANNEL, YEAR)\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the interaction with renamed columns with
     * aggregated selection using the ROLLUP operator with a <b>GROUP_BY</b> clause.
     * The CUBE operates on renamed columns
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), ()
     */
    public void testAggregatedRename1() throws SQLException {
        String sql = "SELECT CHANNEL as ROUTES, YEAR as CALENDAR, SUM(SALES) as SALES\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL, CALENDAR)\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the CUBE operator with a <b>GROUP_BY</b> clause.
     * The order of columns is changed.
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR),()
     */
    public void testAggregatedReorder() throws SQLException {
        String sql = "SELECT YEAR, SUM(SALES) as SALES, CHANNEL\n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, Integer.valueOf(4623500), null
        }, {
                Integer.valueOf(2009), Integer.valueOf(300000), "INTERNET"
        }, {
                Integer.valueOf(2010), Integer.valueOf(545000), "INTERNET"
        }, {
                Integer.valueOf(2009), Integer.valueOf(1764500), "DIRECT SALES"
        }, {
                Integer.valueOf(2010), Integer.valueOf(2014000), "DIRECT SALES"
        }, {
                null, Integer.valueOf(845000), "INTERNET"
        }, {
                null, Integer.valueOf(3778500), "DIRECT SALES"
        }, {
                Integer.valueOf(2009), Integer.valueOf(2064500), null
        }, {
                Integer.valueOf(2010), Integer.valueOf(2559000), null
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the CUBE operator with a <b>GROUP_BY</b> clause.
     * The order of columns in CUBE is changed.
     *
     * The CUBE operator produces four groupings:
     * (YEAR, CHANNEL), (YEAR), (CHANNEL), ()
     */
    public void testAggregatedReorder1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, SUM(SALES) as SALES\n" +
                "FROM REVENUE \n" +
                "GROUP BY CUBE(YEAR, CHANNEL)\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(2064500)
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(2559000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the ROLLUP operator with a <b>GROUP_BY</b> clause.
     * The order of columns in ROLLUP is changed and renamed.
     *
     * The ROLLUP operator produces four groupings:
     * (YEAR, CHANNEL), (YEAR), ()
     */
    public void testAggregatedReorder2() throws SQLException {
        String sql = "SELECT CHANNEL as ROUTES, SUM(SALES) as SALES, YEAR AS CALENDAR\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CALENDAR, ROUTES)\n";
        Object[][] expected = new Object[][] {{
                null, Integer.valueOf(4623500), null
        }, {
                "INTERNET", Integer.valueOf(300000), Integer.valueOf(2009)
        }, {
                "INTERNET", Integer.valueOf(545000), Integer.valueOf(2010)
        }, {
                "DIRECT SALES", Integer.valueOf(1764500), Integer.valueOf(2009)
        }, {
                "DIRECT SALES", Integer.valueOf(2014000), Integer.valueOf(2010)
        }, {
                null, Integer.valueOf(2064500), Integer.valueOf(2009)
        }, {
                null, Integer.valueOf(2559000), Integer.valueOf(2010)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection using the GROUPING SETS operator
     * with a <b>GROUP_BY</b> clause.
     *
     * Not all columns involved in GROUP BY are displayed
     */
    public void testAggregatedMissingColumns() throws SQLException {
        String sql = "SELECT CHANNEL, COUNTRY, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS((), (CHANNEL), (YEAR, COUNTRY))\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        }, {
                null, "GB", Integer.valueOf(187000)
        }, {
                null, "US", Integer.valueOf(1877500)
        }, {
                null, "GB", Integer.valueOf(226000)
        }, {
                null, "US", Integer.valueOf(2333000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection with repeated columns using
     * the ROLLUP operator with a <b>GROUP_BY</b> clause.
     *
     * The ROLLUP operator produces five groupings:
     * (CHANNEL, YEAR), (CHANNEL, YEAR),(CHANNEL, YEAR),(CHANNEL), ()
     */
    public void testAggregatedRepeatedColumns() throws SQLException {
        String sql = "SELECT CHANNEL as ROUTES, YEAR as CALENDAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY ROLLUP(CHANNEL, YEAR, ROUTES, YEAR)\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests aggregated selection with repeated columns using
     * the ROLLUP operator with a <b>GROUP_BY</b> clause.
     *
     * The ROLLUP operator produces five groupings:
     * (CHANNEL, YEAR), (CHANNEL, YEAR),(CHANNEL, YEAR),(CHANNEL), ()
     *
     * The duplicate (CHANNEL, YEAR) groupings are eliminated.
     */
    public void testDistinctAggregatedRepeatedColumns() throws SQLException {
        String sql = "SELECT CHANNEL as ROUTES, YEAR as CALENDAR, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY DISTINCT ROLLUP(CHANNEL, YEAR, ROUTES, YEAR)\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(4623500)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(2014000)
        }, {
                "INTERNET", null, Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>SELECT</b> statement that contains
     * columns not in the <b>GROUP BY</b> clause
     *
     * A SQLException should be thrown.
     */
    public void testInvalidMissingColumns() throws SQLException {
        String sql = "SELECT CHANNEL, COUNTRY, SUM(SALES)\n" +
                "FROM REVENUE \n" +
                "GROUP BY GROUPING SETS(COUNTRY)\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42574");
    }

    //------------------------------------------------------------
    // Aggregate Functions TEST
    //------------------------------------------------------------

    /**
     * Tests the data-cube with the MIN aggregate function
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedMin() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, MIN(SALES) as SALES \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(20000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(90000)
        }, {
                "INTERNET", null, Integer.valueOf(10000)
        }, {
                "DIRECT SALES", null, Integer.valueOf(80000)
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(10000)
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(20000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the data-cube with the COUNT aggregate function
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedCount() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNT(*) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null, Integer.valueOf(16)
        }, {
                "INTERNET", Integer.valueOf(2009), Integer.valueOf(4)
        }, {
                "INTERNET", Integer.valueOf(2010), Integer.valueOf(4)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), Integer.valueOf(4)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), Integer.valueOf(4)
        }, {
                "INTERNET", null, Integer.valueOf(8)
        }, {
                "DIRECT SALES", null, Integer.valueOf(8)
        }, {
                null, Integer.valueOf(2009), Integer.valueOf(8)
        }, {
                null, Integer.valueOf(2010), Integer.valueOf(8)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the data-cube with multiple aggregate function (AVG, VAR, STDEV)
     *
     * The CUBE operator produces two groupings:
     * (CHANNEL), ()
     */
    public void testAggregatedAvg() throws SQLException {
        String sql = "SELECT CHANNEL, AVG(SALES), VAR_SAMP(SALES), STDDEV_SAMP(SALES), VAR_POP (SALES), STDDEV_POP(SALES)\n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL);\n";
        Object[][] expected = new Object[][] {{
                null, Integer.valueOf(288968), 1.21995215625E11, Integer.valueOf(349278), 1.143705146484375E11, 338187
        }, {
                "INTERNET", Integer.valueOf(105625), 1.1817410714285715E10, Integer.valueOf(108707), 1.0340234375E10, 101686
        }, {
                "DIRECT SALES", Integer.valueOf(472312), 1.7276678125E11, Integer.valueOf(415652), 1.5117093359375E11, 388807
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the data-cube without an aggregate function
     *
     * The CUBE operator produces four groupings:
     * (CHANNEL, YEAR), (CHANNEL), (YEAR), ()
     */
    public void testAggregatedMissing() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR \n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, YEAR);\n";
        Object[][] expected = new Object[][] {{
                null, null
        }, {
                "INTERNET", Integer.valueOf(2009)
        }, {
                "INTERNET", Integer.valueOf(2010)
        }, {
                "DIRECT SALES", Integer.valueOf(2009)
        }, {
                "DIRECT SALES", Integer.valueOf(2010)
        }, {
                "INTERNET", null
        }, {
                "DIRECT SALES", null
        }, {
                null, Integer.valueOf(2009)
        }, {
                null, Integer.valueOf(2010)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests an invalid <b>GROUP BY</b> clause that contains
     * an aggregate function
     *
     * A SQLException should be thrown.
     */
    public void testInvalidAggregate() throws SQLException {
        String sql = "SELECT CHANNEL, COUNTRY, SUM(SALES)\n" +
                "FROM REVENUE\n" +
                "GROUP BY CUBE(CHANNEL, MIN(COUNTRY));\n";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42572");
    }

    //------------------------------------------------------------
    // Functions in GROUP BY TEST
    //------------------------------------------------------------

    /**
     * Tests the interaction of functions within the GROUP BY clause
     * and datacube operators.
     */
    public void testFunctionGroupBy() throws SQLException {
        String sql = "SELECT CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END AS NAME,\n" +
                "  COUNT(A.NAME1) AS COUNTER FROM TEST A \n" +
                "  GROUP BY CUBE(CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END, A.SEL);\n";
        Object[][] expected = new Object[][]{{
                null, 3
        }, {
                "FOO", 1
        }, {
                "FOO", 1
        }, {
                "QUX", 1
        }, {
                "FOO", 2
        }, {
                "QUX", 1
        }, {
                null, 1
        }, {
                null, 2
        }};
        compareResults(sql, expected, "00000");
    }

    public void testFunctionGroupBy1() throws SQLException {
        String sql = "SELECT CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END AS NAME,\n" +
                "  COUNT(A.NAME1) AS COUNTER FROM TEST A \n" +
                "  GROUP BY ROLLUP(CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END);\n";
        Object[][] expected = new Object[][]{{
                null, 3
        }, {
                "FOO", 2
        }, {
                "QUX", 1
        }};
        compareResults(sql, expected, "00000");
    }

    public void testFunctionGroupBy2() throws SQLException {
        String sql = "SELECT CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END AS NAME,\n" +
                "  COUNT(A.NAME1) AS COUNTER FROM TEST A \n" +
                "  GROUP BY GROUPING SETS(CASE WHEN A.SEL=1 THEN A.NAME2 ELSE A.NAME1 END, A.SEL);\n";
        Object[][] expected = new Object[][]{{
                "FOO", 2
        }, {
                "QUX", 1
        }, {
                null, 1
        }, {
                null, 2
        }};
        compareResults(sql, expected, "00000");
    }

    public void testFunctionGroupBy3() throws SQLException {
        String sql = "SELECT A.SEL, COALESCE(A.NAME1, A.NAME2) AS NAME,\n" +
                "COUNT(A.SEL) AS COUNTER FROM TEST A \n" +
                "GROUP BY CUBE(COALESCE(A.NAME1, A.NAME2), A.SEL);\n";
        Object[][] expected = new Object[][]{{
                null, null, 3
        }, {
                0, "FOO", 1
        }, {
                1, "BAZ", 1
        }, {
                1, "FOO", 1
        }, {
                null, "FOO", 2
        }, {
                null, "BAZ", 1
        }, {
                0, null, 1
        }, {
                1, null, 2
        }};
        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // Complex Query TEST
    //------------------------------------------------------------

    /**
     * Tests the interaction of multiple features
     * Concatenated grouping, nesting and composite columns
     *
     * Produces seven groupings:
     * (CHANNEL, COUNTRY, PROVINCE), (CHANNEL, COUNTRY), (CHANNEL, PROVINCE), (CHANNEL)
     * (CHANNEL, YEAR, COUNTRY, PROVINCE), (CHANNEL, YEAR), (CHANNEL)
     */
    public void testAggregatedComplex() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY CHANNEL, GROUPING SETS(CUBE(COUNTRY, PROVINCE), ROLLUP(YEAR, (COUNTRY, PROVINCE)));\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, "GB", "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, "GB",  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, "US",  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, "US",  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, "GB", "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, "GB", "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, "US", "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, "US", "NEW YORK", Integer.valueOf(1733000)
        }, {
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                "INTERNET", null, null, "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, null,  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, null,  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, null,  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, null, "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, null, "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, null, "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, null, "NEW YORK", Integer.valueOf(1733000)
        }, {
                "INTERNET", null, null, null,Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(15000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(100000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(175000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(20000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(200000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(82000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(802500)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(800000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(91000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(900000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(933000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null,Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        }, {
                "INTERNET", null, null, null,Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the interaction of multiple features
     * Concatenated grouping, nesting and composite columns
     *
     * Produces seven groupings:
     * (CHANNEL, COUNTRY, PROVINCE), (CHANNEL, COUNTRY), (CHANNEL, PROVINCE), (CHANNEL)
     * (CHANNEL, YEAR, COUNTRY, PROVINCE), (CHANNEL, YEAR), (CHANNEL)
     *
     * The duplicate are eliminated
     */
    public void testDistinctAggregatedComplex() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, PROVINCE, SUM(SALES) \n" +
                "FROM REVENUE\n" +
                "GROUP BY DISTINCT CHANNEL, GROUPING SETS(CUBE(COUNTRY, PROVINCE), ROLLUP(YEAR, (COUNTRY, PROVINCE)));\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", null, "GB", "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, "GB",  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, "US",  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, "US",  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, "GB", "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, "GB", "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, "US", "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, "US", "NEW YORK", Integer.valueOf(1733000)
        }, {
                "INTERNET", null, "GB" , null, Integer.valueOf(70000),
        }, {
                "INTERNET", null, "US" , null,  Integer.valueOf(775000),
        }, {
                "DIRECT SALES", null, "GB" , null, Integer.valueOf(343000),
        }, {
                "DIRECT SALES", null, "US" , null, Integer.valueOf(3435500),
        }, {
                "INTERNET", null, null, "CAMBRIDGE", Integer.valueOf(30000)
        }, {
                "INTERNET", null, null,  "OXFORD", Integer.valueOf(40000)
        }, {
                "INTERNET", null, null,  "STANFORD", Integer.valueOf(300000)
        }, {
                "INTERNET", null, null,  "NEW YORK", Integer.valueOf(475000)
        }, {
                "DIRECT SALES", null, null, "CAMBRIDGE", Integer.valueOf(170000)
        }, {
                "DIRECT SALES", null, null, "OXFORD", Integer.valueOf(173000)
        }, {
                "DIRECT SALES", null, null, "STANFORD", Integer.valueOf(1702500)
        }, {
                "DIRECT SALES", null, null, "NEW YORK", Integer.valueOf(1733000)
        }, {
                "INTERNET", null, null, null,Integer.valueOf(845000)
        }, {
                "DIRECT SALES", null, null, null, Integer.valueOf(3778500)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(10000)
        }, {
                "INTERNET", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(15000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(100000)
        }, {
                "INTERNET", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(175000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(20000)
        }, {
                "INTERNET", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(25000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(200000)
        }, {
                "INTERNET", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(300000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "CAMBRIDGE", Integer.valueOf(80000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "GB", "OXFORD", Integer.valueOf(82000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "STANFORD", Integer.valueOf(802500)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), "US", "NEW YORK", Integer.valueOf(800000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "CAMBRIDGE", Integer.valueOf(90000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "GB", "OXFORD", Integer.valueOf(91000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "STANFORD", Integer.valueOf(900000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", "NEW YORK", Integer.valueOf(933000)
        }, {
                "INTERNET", Integer.valueOf(2009), null, null,Integer.valueOf(300000)
        }, {
                "INTERNET", Integer.valueOf(2010), null, null, Integer.valueOf(545000)
        }, {
                "DIRECT SALES", Integer.valueOf(2009), null, null, Integer.valueOf(1764500)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, null, Integer.valueOf(2014000)
        },
        };

        compareResults(sql, expected, "00000");
    }

    /**
     * Tests the data-cube operators acting on values from multiple tables
     * with all the other clauses
     *
     * Produces Eight groupings:
     * (CHANNEL, YEAR, COUNTRY), (CHANNEL, YEAR), (CHANNEL, COUNTRY), (CHANNEL)
     * (YEAR, COUNTRY), (YEAR), (COUNTRY), ()
     */
    public void testAggregatedComplex1() throws SQLException {
        String sql = "SELECT CHANNEL, YEAR, COUNTRY, SUM(SALES), SUM(LOSSES)\n" +
                "FROM REVENUE\n" +
                "LEFT JOIN LIABILITY ON REVENUE.CHANNEL = LIABILITY.CHANNEL and REVENUE.YEAR = LIABILITY.YEAR and\n" +
                "REVENUE.COUNTRY = LIABILITY.COUNTRY\n" +
                "WHERE YEAR = '2010'\n" +
                "GROUP BY CUBE(CHANNEL, YEAR, COUNTRY) \n" +
                "HAVING SUM(SALES) > 500000 \n" +
                "ORDER BY SUM(SALES)\n" +
                "LIMIT 5;\n";
        Object[][] expected = new Object[][] {{
                "INTERNET", Integer.valueOf(2010), null, Integer.valueOf(545000), Integer.valueOf(14000)
        }, {
                "INTERNET", null, null, Integer.valueOf(545000), Integer.valueOf(14000)
        }, {
                "DIRECT SALES", Integer.valueOf(2010), "US", Integer.valueOf(1833000), null
        }, {
                "DIRECT SALES", null, "US", Integer.valueOf(1833000), null
        }, {
                "DIRECT SALES", Integer.valueOf(2010), null, Integer.valueOf(2014000), null
        },
        };

        compareResults(sql, expected, "00000");
    }

    //------------------------------------------------------------
    // Helper methods
    //------------------------------------------------------------
    private void compareResults(String sql, Object[][] rows,
                                String sqlState) throws SQLException {

        ResultSet rs = null;

        try {
            rs = stmt.executeQuery(sql);

            assertTrue("Statement <" + sql + "> \nexpecting error code: "
                       + sqlState, ("00000".equals(sqlState)));
        } catch (SQLException sqlx) {
            if (!sqlx.getSQLState().equals(sqlState)) {
                sqlx.printStackTrace();
            }

            // compare and report SqlState, rather then ErrorCode
            assertTrue("Statement <" + sql + "> \nthrows wrong error code: "
                       + sqlx.getSQLState() + " expecting error code: "
                       + sqlState, (sqlx.getSQLState().equals(sqlState)));

            return;
        }

        int rowCount = 0;
        int colCount = rows.length > 0 ? rows[0].length
                                       : 0;

        while (rs.next()) {
            assertTrue("Statement <" + sql + "> \nreturned too many rows.",
                       (rowCount < rows.length));

            Object[] columns = rows[rowCount];

            for (int col = 1, i = 0; i < colCount; i++, col++) {
                Object result   = null;
                Object expected = columns[i];

                if (expected == null) {
                    result = rs.getString(col);
                } else if (expected instanceof String) {
                    result = rs.getString(col);
                } else if (expected instanceof Double) {
                    result = Double.valueOf(rs.getString(col));
                } else if (expected instanceof Integer) {
                    result = Integer.valueOf(rs.getInt(col));
                }

                result = rs.wasNull() ? null
                                      : result;

                if (columns[i] != null && !columns[i].equals(result)) {
                    columns[i] = columns[i];
                }

                assertEquals("Statement <" + sql
                             + "> \nreturned wrong value at row " + rowCount + " ", columns[i],
                                 result);
            }

            rowCount++;
        }

        System.out.println(sql);

        assertEquals("Statement <" + sql
                     + "> \nreturned wrong number of rows.", rows.length,
                         rowCount);
    }
}
