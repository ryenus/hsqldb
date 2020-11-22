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

import java.io.PrintWriter;
import java.sql.Connection;
//import java.sql.DataSet;
//import java.sql.QueryObjectGenerator;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCDataSourceFactory.class)
public class JDBCDataSourceTest extends BaseJdbcTestCase {

    public JDBCDataSourceTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCDataSourceTest.class);

        return suite;
    }

    protected void setUpSampleData() throws Exception {
        executeScript("setup-sample-data-tables.sql");
    }

    protected DataSource newDataSource() {
        JDBCDataSource ds = new JDBCDataSource();

        ds.setDatabase(getUrl());
        ds.setUser(getUser());
        ds.setPassword(getPassword());

        return ds;
    }

    protected Class getExpectedWrappedClass() {
        return JDBCDataSource.class;
    }

    protected Object getExpectedWrappedObject(DataSource ds, Class<?> ifc) {
        return ds;
    }

    /**
     * Test of getConnection method, of interface javax.sql.DataSource.
     */
    public void testGetConnection() throws Exception {
        DataSource ds   = newDataSource();
        Connection conn = ds.getConnection();
    }

    /**
     * Test of createQueryObject method, of interface javax.sql.DataSource.
     */
//    public void testCreateQueryObject() throws Exception {
//        println("createQueryObject");
//
//        setUpSampleData();
//
//        Class<CustomerDao> ifc         = CustomerDao.class;
//        Connection         conn        = newConnection();
//        CustomerDao        customerDao = conn.createQueryObject(ifc);
//        DataSet<Customer>  customers   = customerDao.getAllCustomers();
//        ResultSet          rs          = conn.createStatement()
//                                            .executeQuery(
//                "select id, firstname, lastname, street, city from customer order by 1");
//
//        for (Customer customer: customers) {
//            rs.next();
//
//            assertEquals("customer.id", rs.getInt(1), customer.id.intValue());
//            assertEquals("customer.firstname", rs.getString(2), customer.firstname);
//            assertEquals("customer.lastname", rs.getString(3), customer.lastname);
//            assertEquals("customer.street", rs.getString(4), customer.street);
//            assertEquals("customer.city", rs.getString(5), customer.city);
//        }
//
//        // TODO:  test update and delete functions.
//    }

    /**
     * Test of getQueryObjectGenerator method, of interface javax.sql.DataSource.
     */
//    public void testGetQueryObjectGenerator() throws Exception {
//        println("getQueryObjectGenerator");
//
//        DataSource instance = new jdbcDataSource();
//
//
//        setUpSampleData();
//
//        Class<CustomerDao>   ifc       = CustomerDao.class;
//        DataSource           ds        = newDataSource();
//        QueryObjectGenerator gen       = ds.getQueryObjectGenerator();
//
//        if (gen == null) {
//            return;
//        }
//
//        CustomerDao        customerDao = gen.createQueryObject(ifc, ds);
//        DataSet<Customer>  customers   = customerDao.getAllCustomers();
//        ResultSet          rs          = ds.getConnection().createStatement()
//                                            .executeQuery(
//                "select id, firstname, lastname, street, city from customer order by 2, 1");
//
//        for (Customer customer: customers) {
//            rs.next();
//
//            assertEquals(rs.getInt(1), customer.id.intValue());
//            assertEquals(rs.getString(2), customer.firstname);
//            assertEquals(rs.getString(3), customer.lastname);
//            assertEquals(rs.getString(4), customer.street);
//            assertEquals(rs.getString(5), customer.city);
//        }
//
//        // TODO:  test update and delete functions.
//    }

    /**
     * Test of unwrap method, of interface javax.sql.DataSource.
     */
    public void testUnwrap() throws Exception {
        DataSource ds   = newDataSource();
        Class      wcls = getExpectedWrappedClass();
        Object     wobj = getExpectedWrappedObject(ds, wcls);

        assertEquals("ds.unwrap(" + wcls + ").equals(" + wobj + ")",
                     wobj,
                     ds.unwrap(wcls));
    }

    /**
     * Test of isWrapperFor method, of interface javax.sql.DataSource.
     */
    public void testIsWrapperFor() throws Exception {
        DataSource ds   = newDataSource();
        Class      wcls = getExpectedWrappedClass();

        assertEquals("ds.isWrapperFor(" + wcls + ")",
                      true,
                      ds.isWrapperFor(wcls));
    }

    /**
     * Test of getLoginTimeout method, of interface javax.sql.DataSource.
     */
    public void testGetLoginTimeout() throws Exception {
        int result = newDataSource().getLoginTimeout();
    }

    /**
     * Test of getLogWriter method, of interface javax.sql.DataSource.
     */
    public void testGetLogWriter() throws Exception {
        PrintWriter result = newDataSource().getLogWriter();
    }

    /**
     * Test of setLoginTimeout method, of interface javax.sql.DataSource.
     */
    public void testSetLoginTimeout() throws Exception {
        newDataSource().setLoginTimeout(10);
    }

    /**
     * Test of setLogWriter method, of interface javax.sql.DataSource.
     */
    public void testSetLogWriter() throws Exception {
        newDataSource().setLogWriter(new PrintWriter(System.out));
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
