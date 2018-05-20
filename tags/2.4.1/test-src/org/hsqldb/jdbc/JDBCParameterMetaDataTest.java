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

import java.sql.ParameterMetaData;
import java.sql.Types;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class JDBCParameterMetaDataTest extends BaseJdbcTestCase {

    public JDBCParameterMetaDataTest(String testName) {
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
        TestSuite suite = new TestSuite(JDBCParameterMetaDataTest.class);

        return suite;
    }

    protected ParameterMetaData getMetaData() throws Exception {
        String sql = "select * from information_schema.system_tables where table_name = ?";
        return newConnection().prepareStatement(sql).getParameterMetaData();
    }

    protected Class getExpectedWrappedClass() {
        return JDBCParameterMetaData.class;
    }

    protected Object getExpectedWrappedObject(ParameterMetaData pmd, Class<?> ifc) {
        return pmd;
    }

    /**
     * Test of getParameterClassName method, of interface java.sql.ParameterMetaData.
     */
    public void testGetParameterClassName() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        String            expResult = "java.lang.String";
        String            result    = pmd.getParameterClassName(param);

        assertEquals("pmd.getParameterClassName(" + param + ")",
                      expResult,
                      result);
    }

    /**
     * Test of getParameterCount method, of interface java.sql.ParameterMetaData.
     */
    public void testGetParameterCount() throws Exception {
        ParameterMetaData pmd       = getMetaData();
        int               expResult = 1;
        int               result    = pmd.getParameterCount();

        assertEquals("pmd.getParameterCount()", expResult, result);
    }

    /**
     * Test of getParameterMode method, of interface java.sql.ParameterMetaData.
     */
    public void testGetParameterMode() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        int               expResult = ParameterMetaData.parameterModeIn;
        int               result    = pmd.getParameterMode(param);

        assertEquals("pmd.getParameterMode(" + param + ")", expResult, result);
    }

    /**
     * Test of getParameterType method, of interface java.sql.ParameterMetaData.
     */
    public void testGetParameterType() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        int               expResult = Types.VARCHAR;
        int               result    = pmd.getParameterType(param);

        assertEquals("pmd.getParameterType(" + param + ")", expResult, result);
    }

    /**
     * Test of getParameterTypeName method, of interface java.sql.ParameterMetaData.
     */
    public void testGetParameterTypeName() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        String            expResult = "VARCHAR";
        String            result    = pmd.getParameterTypeName(param);

        assertEquals("pmd.getParameterTypeName(" + param + ")",
                     expResult,
                     result);
    }

    /**
     * Test of getPrecision method, of interface java.sql.ParameterMetaData.
     */
    public void testGetPrecision() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        int               expResult = 128; // max length of schema object name
        int               result    = pmd.getPrecision(param);

        assertEquals("pmd.getPrecision(" + param + ")", expResult, result);
    }

    /**
     * Test of getScale method, of interface java.sql.ParameterMetaData.
     */
    public void testGetScale() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        int               expResult = 0;
        int               result    = pmd.getScale(param);

        assertEquals("pmd.getScale(" + param + ")", expResult, result);
    }

    /** @todo use an insert or update statement here, where nullability is
     *  returned more precisely.
     */
    /**
     * Test of isNullable method, of interface java.sql.ParameterMetaData.
     */
    public void testIsNullable() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        int               expResult = ParameterMetaData.parameterNullableUnknown;
        int               result    = pmd.isNullable(param);

        assertEquals("pmd.isNullable(" + param + ")", expResult, result);
    }

    /**
     * Test of isSigned method, of interface java.sql.ParameterMetaData.
     */
    public void testIsSigned() throws Exception {
        int               param     = 1;
        ParameterMetaData pmd       = getMetaData();
        boolean           expResult = false;
        boolean           result    = pmd.isSigned(param);

        assertEquals("pmd.isSigned(" + param + ")", expResult, result);
    }

    /**
     * Test of unwrap method, of interface java.sql.ParameterMetaData.
     */
    public void testUnwrap() throws Exception {
        ParameterMetaData pmd  = getMetaData();
        Class<?>          wcls = getExpectedWrappedClass();
        Object            wobj = getExpectedWrappedObject(pmd, wcls);

        assertEquals("pmd.unwrap(" + wcls + ")",
                      wobj,
                      pmd.unwrap(wcls));
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.ParameterMetaData.
     */
    public void testIsWrapperFor() throws Exception {
        ParameterMetaData pmd = getMetaData();
        Class<?>          wcls = getExpectedWrappedClass();

        assertEquals("pmd.isWrapperFor(" + wcls + ")",
                      true,
                      pmd.isWrapperFor(wcls));
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
