/* Copyright (c) 2001-2016, The HSQL Development Group
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
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

/**
 * Base tests for table definition with period.
 *
 * @author Pascal-Eric Servais (peservais@users dot sourceforge.net)
 * @version 2.3.4
 * @since 2.3.4
 */
public class TestCreateTableWithPeriod extends TestBase {

    private Connection conn;

    public TestCreateTableWithPeriod(String name) throws Exception {

        super(name,  "jdbc:hsqldb:mem:test", true, false);
    }
    
    public void setUp() throws Exception {
        super.setUp();
        conn = newConnection();
        Statement stmt = conn.createStatement();
		stmt.executeUpdate("DROP TABLE PUBLIC.emp IF EXISTS");
		stmt.close();
    }

    public void tearDown() {
    	super.tearDown();
    }
    
    public void testCreateTableWithValidApplicationPeriod() throws SQLException {
        Statement stmt = conn.createStatement();
        
        stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL,bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR business_time (bus_start, bus_end))");
        
        stmt.close();
    }

    public void testCannotHave2ApplicationPeriod() throws SQLException {
        Statement stmt = conn.createStatement();
        
        try {
            stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR P1 (bus_start, bus_end), PERIOD FOR P2 (bus_start, bus_end))");
            Assert.fail("Cannot Have 2 Application Periods");
        } catch (SQLException e) {
            Assert.assertEquals("46526", e.getSQLState());
        }
        
        stmt.close();
    }

    public void testPeriodNameCannotBeColumnName() throws SQLException {
        Statement stmt = conn.createStatement();
        
        try {
            stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, MY_COLUMN VARCHAR(10),bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR MY_COLUMN (bus_start, bus_end))");
            Assert.fail("Period Name Cannot Be The Same As Column Names");
        } catch (SQLException e) {
            Assert.assertEquals("46520", e.getSQLState());
        }
        
        stmt.close();
    }

    public void testPeriodMustHaveTwoColumnNames() throws SQLException {
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR business_time (bus_start))");
			Assert.fail("Period Must Have Two Columns");
		} catch (SQLException e) {
			Assert.assertEquals("46521", e.getSQLState());
		}
    	
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR business_time (bus_start, bus_end, emp_id))");
			Assert.fail("Period Must Have Two Columns");
		} catch (SQLException e) {
			Assert.assertEquals("46521", e.getSQLState());
		}
    	
		stmt.close();
    }

    public void testPeriodColumnsMustExists() throws SQLException {
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR business_time (pStart, bus_end))");
			Assert.fail("Period Columns Must Exists");
		} catch (SQLException e) {
			Assert.assertEquals("46522", e.getSQLState());
		}
    	
		stmt.close();
    }

    public void testPeriodColumnsMustBeNullable() throws SQLException {
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME ,bus_end DATETIME NOT NULL, PERIOD FOR business_time (bus_start, bus_end))");
			Assert.fail("Period Columns Must Not Be Nullable");
		} catch (SQLException e) {
			Assert.assertEquals("46523", e.getSQLState());
		}
    	
		stmt.close();
    }

    public void testPeriodColumnsDateType() throws SQLException {
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start TIME NOT NULL,bus_end DATE NOT NULL, PERIOD FOR business_time (bus_start, bus_end))");
			Assert.fail("Period Must Be Of The Same Type");
		} catch (SQLException e) {
			Assert.assertEquals("46524", e.getSQLState());
		}
    	
		stmt.close();
    }
    
    public void testPeriodColumnsOfSameType() throws SQLException {
		Statement stmt = conn.createStatement();
		
		try {
			stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL, bus_start DATETIME NOT NULL,bus_end DATE NOT NULL, PERIOD FOR business_time (bus_start, bus_end))");
			Assert.fail("Period Must Be Of The Same Type");
		} catch (SQLException e) {
			Assert.assertEquals("46525", e.getSQLState());
		}
    	
		stmt.close();
    }

    public void testPeriodRangeIntegrityWithInsert() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("CREATE TABLE PUBLIC.emp (emp_id INTEGER NOT NULL,bus_start DATETIME NOT NULL,bus_end DATETIME NOT NULL, PERIOD FOR business_time (bus_start, bus_end))");
		
		try {
			stmt.executeUpdate("INSERT INTO PUBLIC.EMP ( EMP_ID, BUS_START, BUS_END ) VALUES ( 1, DATE '2000-01-01' , DATE '1999-12-31')");
			Assert.fail("Period Start Must Be Before Period End");
		} catch (java.sql.SQLIntegrityConstraintViolationException e) {
			Assert.assertTrue(e.getMessage().startsWith("integrity constraint violation: check constraint;"));
		}

		stmt.close();
    }

}
