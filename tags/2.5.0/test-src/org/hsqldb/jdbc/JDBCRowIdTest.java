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

import java.sql.RowId;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of class org.hsqldb.jdbc.jdbcRowId.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(RowId.class)
public class JDBCRowIdTest extends BaseJdbcTestCase {

    public JDBCRowIdTest(String testName) {
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
        TestSuite suite = new TestSuite(JDBCRowIdTest.class);

        return suite;
    }

    public RowId newRowId(String s) throws Exception {
        return new JDBCRowId(s);
    }

    public RowId newRowId(byte[] b) throws Exception {
        return new JDBCRowId(b);
    }

    /**
     * Test of equals method, of interface java.sql.RowId.
     */
    @OfMethod("equals(java.lang.Object")
    public void testEquals() throws Exception {
        RowId id1         = newRowId("02b7abfe");
        RowId id2         = newRowId(StringConverter.hexStringToByteArray("02b7abfe"));
        boolean expResult = true;
        boolean result    = id1.equals(id2);

        assertEquals(expResult, result);
    }

    /**
     * Test of getBytes method, of interface java.sql.RowId.
     */
    @OfMethod("getBytes()")
    public void testGetBytes() throws Exception {
        byte[] expResult = StringConverter.hexStringToByteArray("02b7abfe");
        byte[] result    = newRowId("02b7abfe").getBytes();

        assertJavaArrayEquals(expResult, result);
    }

    /**
     * Test of toString method, of interface java.sql.RowId.
     */
    @OfMethod("toString()")
    public void testToString() throws Exception {
        RowId  rid       = newRowId("02b7abfe");
        String expResult = "02b7abfe";
        String result    = rid.toString();

        assertEquals(expResult, result);
    }

    /**
     * Test of hashCode method, of interface java.sql.RowId.
     */
    @OfMethod("hashCode()")
    public void testHashCode() throws Exception {
        byte[] bytes  = StringConverter.hexStringToByteArray("02b7abfe");
        RowId  rid    = newRowId("02b7abfe");
        int expResult = Arrays.hashCode(bytes);
        int result    = rid.hashCode();

        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
