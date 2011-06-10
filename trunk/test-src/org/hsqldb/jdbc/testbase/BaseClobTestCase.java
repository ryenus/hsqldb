package org.hsqldb.jdbc.testbase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import java.net.URL;

import java.sql.Clob;

import org.hsqldb.jdbc.JDBCClob;
import org.hsqldb.lib.InOutUtil;

import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of class org.hsqldb.jdbc.jdbcClob.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(java.sql.Clob.class)
public abstract class BaseClobTestCase extends BaseJdbcTestCase {

    private String m_encoding;

    /**
     * used by default when creating file-backed / file-based Clobs.
     *
     * @param encoding to use
     */
    public void setEncoding(String encoding) {
        m_encoding = encoding;
    }

    /**
     * used by default when creating file-backed / file-based Clobs.
     *
     * @return encoding used
     */
    public String getEncoding() {
        return m_encoding;
    }

    /**
     * Standard constructor; delegates directly to base.
     *
     * @param name of test.
     */
    public BaseClobTestCase(String name) {
        super(name);
    }

    /**
     * supplies new Clob instances to createClob and hence newClob.
     *
     * @return
     * @throws Exception
     * @see #createClob()
     * @see #newClob(java.lang.String)
     */
    protected abstract Clob handleCreateClob() throws Exception;

    /**
     * for testing purposes.
     *
     * @return a newly created Clob instance.
     * @throws Exception
     */
    public final Clob createClob() throws Exception {
        final Clob clob = handleCreateClob();

        connectionFactory().registerClob(clob);

        return clob;
    }

    /**
     * with the given character content.
     *
     * @param data
     * @return
     * @throws Exception
     */
    protected Clob newClobFromString(final String data) throws Exception {
        final Clob clob = createClob();
        Writer writer = null;
        if (data != null) {
            try {
                writer = clob.setCharacterStream(1);
                writer.write(data);
            } finally {
                // Important - typically must close (not just flush)
                // the stream to ensure all characters are flushed
                // to the underlying clob.
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (Exception ex) {
                    }
                }
            }
        }
        return clob;
    }

    /**
     * with the given character content.
     *
     * @param data
     * @return
     * @throws Exception
     */
    protected Clob newClobFromResource(final String resource) throws Exception {
        Clob clob = createClob();
        InputStream inputStream = null;
        Reader reader = null;
        Writer writer = null;
        try {
            final URL url = getResource(resource);
            inputStream = url.openStream();
            reader = new InputStreamReader(inputStream);
            writer = clob.setCharacterStream(1);

            InOutUtil.copy(reader, writer);
        } finally {
            // Important - typically must close (not just flush)
            // the stream to ensure all characters are flushed
            // to the underlying clob.
            if (writer != null) {
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

    @OfMethod("free()")
    public void testFree() throws Exception {
        Clob clob = newClobFromString("testFree");
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

    /**
     *
     * @throws Exception
     */
    @OfMethod("getAsciiStream()")
    public void testGetAsciiStream() throws Exception {
        StringBuffer sb = new StringBuffer();
        for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {
            sb.append((char) i);
        }
        String testVal = sb.toString();
        this.setEncoding("US-ASCII");
        Clob clob = newClobFromString(testVal);
        InputStream expResult = new ByteArrayInputStream(testVal.getBytes("US-ASCII"));
        InputStream result = clob.getAsciiStream();
        assertStreamEquals(expResult, result);
    }

    @OfMethod("getCharacterStream()")
    public void testGetCharacterStream() throws Exception {
        Clob clob = newClobFromString("testGetCharacterStream()");
        Reader expResult = new StringReader("testGetCharacterStream()");
        Reader result = clob.getCharacterStream();
        assertReaderEquals(expResult, result);
    }

    @OfMethod("getSubString(long,int)")
    public void testGetSubString() throws Exception {
        Clob clob = newClobFromString("testGetSubString()");
        String result = clob.getSubString(2, 2);
        assertEquals("es", result);
    }

    @OfMethod("length()")
    public void testLength() throws Exception {
        String[] encodings = new String[]{"UTF-16BE","US-ASCII","UTF-8",null};
        String data = "testLength()\u00a1\u2002";
        long expResult = data.length();

        for (int i = 0; i < encodings.length; i++) {
            setEncoding(encodings[i]);
            Clob clob = newClobFromString(data);
            long result = clob.length();
            assertEquals(expResult, result);
        }
    }

    @OfMethod("position(java.lang.String, long)")
    public void testPosition() throws Exception {
        Clob clob = newClobFromString("testPosition()");
        long result = 0;
        try {
            result = clob.position("Pos", 1);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.toString());
        }
        assertEquals(5L, result);
    }

    @OfMethod("setAsciiStream(long)")
    public void testSetAsciiStream() throws Exception {
        setEncoding("US-ASCII");
        Clob clob = handleCreateClob();
        OutputStream outputStream = null;
        try {
            outputStream = clob.setAsciiStream(1);

            byte[] bytes = "testSetAsciiStream()".getBytes("US-ASCII");
            String expValue = new String(bytes, "US-ASCII");
            int expLen = expValue.length();

            outputStream.write(bytes);
            outputStream.close();

            int len = (int) clob.length();

            assertEquals("Clob Length", expLen, len);

            String value = clob.getSubString(1, len);

            assertEquals("Clob Value", expValue, value);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @OfMethod("setCharacterStream(long)")
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
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @OfMethod("setString(long,java.lang.String)")
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

    @OfMethod("truncate()")
    public void testTruncate() throws Exception {
        Clob clob = newClobFromString("testTruncate");
        try {
            clob.truncate(2);
            assertEquals(2L, clob.length());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @OfMethod("position(java.lang.String,long)")
    public void testPostionInResource() throws Exception {
        Clob clob = newClobFromResource("/org/hsqldb/jdbc/resources/sql/TestSelf.txt");

        String pattern = "-- correlated subquery together with group and aggregates";

        long position = 0;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 200; i++) {
            try {
                position = clob.position(pattern, i+1);
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.toString());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("position: " + position);
        System.out.println("elapsed: " + elapsed);
    }
}
