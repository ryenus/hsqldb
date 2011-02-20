package org.hsqldb.jdbc.testbase;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import java.sql.Clob;

import org.hsqldb.jdbc.JDBCClob;

import org.hsqldb.testbase.ForSubject;

/**
 * Test of class org.hsqldb.jdbc.jdbcClob.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(java.sql.Clob.class)
public abstract class BaseClobTest extends BaseJdbcTestCase {

    private String m_encoding;

    public void setEncoding(String encoding) {
        m_encoding = encoding;
    }

    public String getEncoding() {
        return m_encoding;
    }

    public BaseClobTest(String name) {
        super(name);
    }

    protected abstract Clob handleCreateClob() throws Exception;

    public final Clob createClob() throws Exception {
        final Clob clob = handleCreateClob();

        connectionFactory().registerClob(clob);

        return clob;
    }

    protected Clob newClob(String data) throws Exception {
        Clob clob = createClob();
        Writer writer = null;
        if (data != null) {
            try {
                writer = clob.setCharacterStream(1);
                writer.write(data);
            } finally {
                // Tmportant - typically must close (not just flush)
                // the stream to ensure all characters are flushed
                // to the underlying clob.
                try {
                    writer.close();
                } catch (Exception ex) {
                }
            }
        }
        return clob;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testFree() throws Exception {
        Clob clob = newClob("testFree");
        try {
            clob.free();
        } catch (Exception e) {
            fail(e.toString());
        }
        try {
            clob.getCharacterStream();
            assertTrue("getCharacterStream operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.getCharacterStream(1, 2);
            assertTrue("getCharacterStream(pos, len) operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.getSubString(1, 2);
            assertTrue("getSubString operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.length();
            assertTrue("length operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.position("est", 1);
            assertTrue("position(String,long) operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.position(new JDBCClob("est"), 1);
            assertTrue("position(Clob,long) operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.setAsciiStream(1);
            assertTrue("setAsciiStream operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.setCharacterStream(1);
            assertTrue("setCharacterStream operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.setString(2, "est");
            assertTrue("setString(long,String) operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.setString(2, "est", 0, 3);
            assertTrue("setString(long,String,int,int) operation allowed after free", false);
        } catch (Exception e) {
        }
        try {
            clob.truncate(1);
            assertTrue("truncate operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    public void testGetAsciiStream() throws Exception {
        StringBuffer sb = new StringBuffer();
        for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {
            sb.append((char) i);
        }
        String testVal = sb.toString();
        this.setEncoding("US-ASCII");
        Clob clob = newClob(testVal);
        InputStream expResult = new ByteArrayInputStream(testVal.getBytes("US-ASCII"));
        InputStream result = clob.getAsciiStream();
        assertStreamEquals(expResult, result);
    }

    public void testGetCharacterStream() throws Exception {
        Clob clob = newClob("testGetCharacterStream()");
        Reader expResult = new StringReader("testGetCharacterStream()");
        Reader result = clob.getCharacterStream();
        assertReaderEquals(expResult, result);
    }

    public void testGetSubString() throws Exception {
        Clob clob = newClob("testGetSubString()");
        String result = clob.getSubString(2, 2);
        assertEquals("es", result);
    }

    public void testLength() throws Exception {
        JDBCClob clob = new JDBCClob("testLength()");
        long expResult = "testLength()".length();
        long result = clob.length();
        assertEquals(expResult, result);
    }

    public void testPosition() throws Exception {
        Clob clob = newClob("testPosition()");
        long result = clob.position("Pos", 1);
        assertEquals(5L, result);
    }

    public void testSetAsciiStream() throws Exception {
        setEncoding("US-ASCII");
        Clob clob = handleCreateClob();
        try {
            clob.setString(1L, "T");
            assertEquals(1L, clob.length());
            OutputStream result = clob.setAsciiStream(2);
            result.write("ask".getBytes("US-ASCII"));
            result.close();
            assertEquals(4L, clob.length());
            assertEquals("Task", clob.getSubString(1, 4));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    public void testSetCharacterStream() throws Exception {
        Clob clob = handleCreateClob();
        try {
            clob.setString(1L, "T");
            assertEquals(1L, clob.length());
            Writer result = clob.setCharacterStream(2);
            result.write("ask");
            result.close();
            assertEquals(4L, clob.length());
            assertEquals("Task", clob.getSubString(1, 4));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    public void testSetString() throws Exception {
        Clob clob = handleCreateClob();
        try {
            clob.setString(1, "T");
            assertEquals(1L, clob.length());
            assertEquals(3, clob.setString(2L, "ask"));
            assertEquals(4L, clob.length());
            assertEquals("Task", clob.getSubString(1L, 4));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    public void testTruncate() throws Exception {
        Clob clob = newClob("testTruncate");
        try {
            clob.truncate(2);
            assertEquals(2L, clob.length());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }
}
