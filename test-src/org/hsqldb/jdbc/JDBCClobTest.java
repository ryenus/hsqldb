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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Reader;
import java.io.Writer;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseTestCase;

/**
 * Test of class org.hsqldb.jdbc.jdbcClob.
 *
 * @author boucherb@users
 */
public class JDBCClobTest extends BaseTestCase {

    public JDBCClobTest(String testName) {
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
        TestSuite suite = new TestSuite(JDBCClobTest.class);

        return suite;
    }

    /**
     * Test of length method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testLength() throws Exception {
        println("length");

        JDBCClob clob       = new JDBCClob("Test");
        long     expResult  = "Test".length();
        long     result     = clob.length();

        assertEquals(expResult, result);
    }

    /**
     * Test of getSubString method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testGetSubString() throws Exception {
        println("getSubString");

        JDBCClob clob      = new JDBCClob("Test");
        String   result    = clob.getSubString(2, 2);

        assertEquals("es", result);
    }

    /**
     * Test of getCharacterStream method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testGetCharacterStream() throws Exception {
        println("getCharacterStream");

        JDBCClob clob      = new JDBCClob("Test");
        Reader   expResult = new StringReader("Test");
        Reader   result    = clob.getCharacterStream();

        assertReaderEquals(expResult, result);
    }

    /**
     * Test of getAsciiStream method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testGetAsciiStream() throws Exception {
        println("getAsciiStream");

        StringBuffer sb = new StringBuffer();

        for (int i = Character.MAX_VALUE; i <= Character.MAX_VALUE; i++) {
            sb.append((char)i);
        }

        String      testVal   = sb.toString();
        JDBCClob    clob      = new JDBCClob(testVal);
        InputStream expResult = new ByteArrayInputStream(
                testVal.getBytes("US-ASCII"));
        InputStream result    = clob.getAsciiStream();

        assertStreamEquals(expResult, result);
    }

    /**
     * Test of position method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testPosition() throws Exception {
        println("position");

        JDBCClob clob   = new JDBCClob("Test");
        long     result = clob.position("est", 1);

        assertEquals(2L, result);
    }

    /**
     * Test of setString method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testSetString() throws Exception {
        println("setString");

        JDBCClob clob = (JDBCClob) newConnection().createClob();

        try {
            clob.setString(1, "T");

            assertEquals(1L, clob.length());
            assertEquals(3, clob.setString(2L, "ask"));
            assertEquals(4L, clob.length());
            assertEquals("Task", clob.getSubString(1L, 4));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of setAsciiStream method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testSetAsciiStream() throws Exception {
        println("setAsciiStream");

        JDBCClob clob = (JDBCClob) newConnection().createClob();

        try {
            clob.setString(1L, "T");

            assertEquals(1L, clob.length());

            OutputStream result = clob.setAsciiStream(2);

            result.write("ask".getBytes("US-ASCII"));
            result.flush();

            assertEquals(1L, clob.length());

            result.close();

            assertEquals(4L, clob.length());

            assertEquals("Task", clob.getSubString(1, 4));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of setCharacterStream method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testSetCharacterStream() throws Exception {
        println("setCharacterStream");

        JDBCClob clob = (JDBCClob) newConnection().createClob();

        try {
            clob.setString(1L, "T");

            assertEquals(1L, clob.length());

            Writer result = clob.setCharacterStream(2);

            result.write("ask");
            result.flush();

            assertEquals(1L, clob.length());

            result.close();

            assertEquals(4L, clob.length());

            assertEquals("Task", clob.getSubString(1, 4));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of truncate method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testTruncate() throws Exception {
        println("truncate");

        JDBCClob clob = new JDBCClob("Test");

        try {
            clob.truncate(2);
            assertEquals(2L, clob.length());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of free method, of class org.hsqldb.jdbc.jdbcClob.
     */
    public void testFree() throws Exception {
        println("free");

        JDBCClob clob = new JDBCClob("Test");

        try {
            clob.free();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            clob.getCharacterStream();
            assertTrue("getCharacterStream operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.getCharacterStream(1,2);
            assertTrue("getCharacterStream(pos, len) operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.getSubString(1,2);
            assertTrue("getSubString operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.length();
            assertTrue("length operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.position("est",1);
            assertTrue("position(String,long) operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.position(new JDBCClob("est"),1);
            assertTrue("position(Clob,long) operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.setAsciiStream(1);
            assertTrue("setAsciiStream operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.setCharacterStream(1);
            assertTrue("setCharacterStream operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.setString(2, "est");
            assertTrue("setString(long,String) operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.setString(2, "est", 0, 3);
            assertTrue("setString(long,String,int,int) operation allowed after free", false);
        } catch (Exception e){ }

        try {
            clob.truncate(1);
            assertTrue("truncate operation allowed after free", false);
        } catch (Exception e){ }
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
