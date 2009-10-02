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
package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.resources.BundleHandler;

/**
 * Abstract HSQLDB-targeted Junit test case. <p>
 *
 * @author  boucherb@users
 * @version 1.9.0
 * @since 1.9.0
 */
public abstract class BaseTestCase extends junit.framework.TestCase {

    public static final String DEFAULT_DRIVER = "org.hsqldb.jdbcDriver";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_URL = "jdbc:hsqldb:mem:testcase";
    public static final String DEFAULT_USER = "SA";
    protected static final IntValueHashMap fieldValueMap = new IntValueHashMap();
    protected static final String[][] rsconcurrency = new String[][]{{"concur_read_only", "java.sql.ResultSet.CONCUR_READ_ONLY"}, {"concur_updatable", "java.sql.ResultSet.CONCUR_UPDATABLE"}};
    protected static final String[][] rsholdability = new String[][]{{"close_cursors_at_commit", "java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT"}, {"hold_cursors_over_commit", "java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT"}};
    protected static final String[][] rstype = new String[][]{{"type_forward_only", "java.sql.ResultSet.TYPE_FORWARD_ONLY"}, {"type_scroll_insensitive", "java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE"}, {"type_scroll_sensitive", "java.sql.ResultSet.TYPE_SCROLL_SENSITIVE"}};
    //
    public static final int testBundleHandle;
    public static final int testConvertBundleHandle;

    static {
        testBundleHandle = BundleHandler.getBundleHandle("test", null);
        testConvertBundleHandle = BundleHandler.getBundleHandle("test-dbmd-convert", null);
    }

    /**
     * by computing a shallow string representation
     * of the given array. <p>
     * @param array for which to produce the string representation.
     * @return the string representation.
     */
    protected static String arrayToString(Object array) {
        if (array == null) {
            return "null";
        }
        int length = Array.getLength(array);
        StringBuffer sb = new StringBuffer(2 + 3 * length);
        sb.append('[');
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Array.get(array, i));
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * for the given array objects.
     * @param expected array
     * @param actual array
     * @return an "arrays not equal" failure message
     * describing the given values.
     */
    protected static String arraysNotEqualMessage(Object expected, Object actual) {
        return "expected:<" + arrayToString(expected) + "> but was:<" + arrayToString(actual) + ">";
    }

    /**
     * with special handling for Java array objects.
     * @param expected object
     * @param actual object
     * @throws java.lang.Exception as thrown by any internal operation.
     */
    protected static void assertJavaArrayEquals(Object expected, Object actual) throws Exception {
        switch (getComponentDescriptor(expected)) {
            case 'X': {
                if (actual != null) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'B': {
                if (!Arrays.equals((byte[]) expected, (byte[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'C': {
                if (!Arrays.equals((char[]) expected, (char[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'D': {
                if (!Arrays.equals((double[]) expected, (double[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'F': {
                if (!Arrays.equals((float[]) expected, (float[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'I': {
                if (!Arrays.equals((int[]) expected, (int[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'J': {
                if (!Arrays.equals((long[]) expected, (long[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'L': {
                if (!Arrays.deepEquals((Object[]) expected, (Object[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'S': {
                if (!Arrays.equals((short[]) expected, (short[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'Z': {
                if (!Arrays.equals((boolean[]) expected, (boolean[]) actual)) {
                    fail(arraysNotEqualMessage(expected, actual));
                }
                break;
            }
            case 'N': {
                if (actual == null) {
                    fail("expected:<" + arrayToString(expected) + "> but was:<null>");
                } else if (Object[].class.isAssignableFrom(expected.getClass()) && Object[].class.isAssignableFrom(actual.getClass())) {
                    if (!Arrays.deepEquals((Object[]) expected, (Object[]) actual)) {
                        fail(arraysNotEqualMessage(expected, actual));
                    }
                } else {
                    assertEquals("Array Class", expected.getClass(), actual.getClass());
                    assertEquals("Array Length", Array.getLength(expected), Array.getLength(actual));
                    int len = Array.getLength(expected);
                    for (int i = 0; i < len; i++) {
                        assertJavaArrayEquals(Array.get(expected, i), Array.get(actual, i));
                    }
                }
                break;
            }
            case '0':
            default: {
                assertEquals(expected, actual);
                break;
            }
        }
    }

    /**
     * in terms of producing the same character sequence. <p>
     * @param expected reader; must not be null.
     * @param actual reader; must not be null.
     * @throws java.lang.Exception if an I/0 error occurs.
     */
    protected static void assertReaderEquals(Reader expected, Reader actual) throws Exception {
        if (expected == actual) {
            return;
        }
        assertTrue("expected != null", expected != null);
        assertTrue("actual != null", actual != null);
        int count = 0;
        String sexp = expected.getClass().getName() + Integer.toHexString(System.identityHashCode(expected));
        String sact = actual.getClass().getName() + Integer.toHexString(System.identityHashCode(actual));
        while (true) {
            int expChar = expected.read();
            int actChar = actual.read();
            // More efficient than generating the message for every
            // stream element.
            if (expChar != actChar) {
                String msg = sexp + "(" + count + ") == " + sact + "(" + count + ")";
                assertEquals(msg, expChar, actChar);
            }
            if (expChar == -1) {
                assertEquals("End of expected sequence", -1, actual.read());
                break;
            }
            count++;
        }
    }

    /**
     * in terms of column count and order-sensitive row content.
     *
     * @param expected result
     * @param actual result
     * @throws java.lang.Exception thrown by any internal operation.
     */
    protected static void assertResultSetEquals(ResultSet expected, ResultSet actual) throws Exception {
        if (expected == actual) {
            return;
        }
        assertTrue("expected != null", expected != null);
        assertTrue("actual != null", actual != null);
        ResultSetMetaData expRsmd = expected.getMetaData();
        ResultSetMetaData actRsmd = actual.getMetaData();
        assertEquals("expRsmd.getColumnCount() == actRsmd.getColumnCount()", expRsmd.getColumnCount(), actRsmd.getColumnCount());
        int columnCount = actRsmd.getColumnCount();
        int rowCount = 0;
        while (expected.next()) {
            rowCount++;
            assertTrue("actual.next() [row: " + rowCount + "]", actual.next());
            for (int i = 1; i <= columnCount; i++) {
                Object expObject = expected.getObject(i);
                Object actObject = actual.getObject(i);
                if (expObject != null && expObject.getClass().isArray()) {
                    assertJavaArrayEquals(expObject, actObject);
                } else if (expObject instanceof Blob) {
                    Blob expBlob = (Blob) expObject;
                    Blob actBlob = (Blob) actObject;
                    assertEquals("expBlob.length(), actBlob.length()", expBlob.length(), actBlob.length());
                    assertEquals("expBlob.position(actBlob, 1L), 1L", expBlob.position(actBlob, 1L), 1L);
                } else if (expObject instanceof Clob) {
                    Clob expClob = (Clob) expObject;
                    Clob actClob = (Clob) actObject;
                    assertEquals("expClob.length(), actClob.length()", expClob.length(), actClob.length());
                    assertEquals("expClob.position(actClob, 1L), 1L", expClob.position(actClob, 1L), 1L);
                } else {
                    String msg = "expected.getObject(" + i + "), actual.getObject(" + i + ")";
                    assertEquals(msg, expObject, actObject);
                }
            }
        }
    }

    /**
     * in terms of producing the same octet sequence. <p>
     * @param expected octet sequence, as an InputStream; must not be null
     * @param actual octet sequence, as an InputStream; must not be null
     * @throws java.lang.Exception if an I/0 error occurs.
     */
    protected static void assertStreamEquals(final InputStream expected, final InputStream actual) throws Exception {
        if (expected == actual) {
            return;
        }
        assertTrue("expected != null", expected != null);
        assertTrue("actual != null", actual != null);
        int count = 0;
        String sexp = expected.getClass().getName() + Integer.toHexString(System.identityHashCode(expected));
        String sact = actual.getClass().getName() + Integer.toHexString(System.identityHashCode(actual));
        while (true) {
            int expByte = expected.read();
            int actByte = actual.read();
            // More efficient than generating the message for every
            // stream element.
            if (expByte != actByte) {
                String msg = sexp + "(" + count + ") == " + sact + "(" + count + ")";
                assertEquals(msg, expByte, actByte);
            }
            if (expByte == -1) {
                // Assert that the actual stream is also at the end
                assertEquals("End of expected stream", -1, actual.read());
                break;
            }
            count++;
        }
    }

    /**
     * indicating the given object's component type. <p>
     *
     * For null, returns 'X' (unknown). <p>
     *
     * For 1D arrays, returns: <p>
     *
     * <pre>
     * BaseType Character 	 Type            Interpretation
     *
     * B                        byte            signed byte
     * C                        char            Unicode character
     * D                        double          double-precision floating-point value
     * F                        float           single-precision floating-point value
     * I                        int             integer
     * J                        long            long integer
     * L                        reference 	  an instance of class
     * S                        short           signed short
     * Z                        boolean 	  true or false
     * </pre>
     *
     * for multi-arrays, returns 'N' (n-dimensional). <p>
     *
     * for (non-null, non-array) object ref, returns 'O' (Object).
     * @return a character code representing the component type.
     * @param o for which to produce the component descriptor.
     */
    protected static char getComponentDescriptor(Object o) {
        if (o == null) {
            return 'X';
        }
        Class cls = o.getClass();
        if (cls.isArray()) {
            Class comp = cls.getComponentType();
            String className = cls.getName();
            int count = 0;
            for (; className.charAt(count) == '['; count++) {
            }
            if (count > 1) {
                return 'N';
            } else if (comp.isPrimitive()) {
                return className.charAt(1);
            } else {
                return 'L';
            }
        } else {
            // L...; is JMV field descriptor
            return 'O';
        }
    }

    /**
     *
     * @param packageName
     * @throws java.io.IOException
     * @return fully expe
     */
    protected static String[] getResoucesInPackage(final String packageName) throws IOException {
        String packagePath = packageName.replace('.', '/');
        if (!packagePath.endsWith("/")) {
            packagePath = packagePath + '/';
        }
        //Enumeration resources = ClassLoader.getSystemResources(packagePath);
        Enumeration resources = BaseTestCase.class.getClassLoader().getResources(packagePath);
        OrderedHashSet set = new OrderedHashSet();
        while (resources.hasMoreElements()) {
            URL resource = (URL) resources.nextElement();
            String protocol = resource.getProtocol();
            if ("file".equals(protocol)) {
                try {
                    File[] files = new File(new URI(resource.toString()).getPath()).listFiles();
                    if (files == null) {
                        continue;
                    }
                    for (int i = 0; i < files.length; i++) {
                        File file = files[i];
                        if (file.isDirectory()) {
                            continue;
                        }
                        set.add('/' + packagePath + file.getName());
                    }
                } catch (Exception ex) {
                }
            } else if ("jar".equals(protocol)) {
                Enumeration entries = ((JarURLConnection) resource.openConnection()).getJarFile().entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = (JarEntry) entries.nextElement();
                    String entryName = entry.getName();
                    if (entryName.equals(packagePath)) {
                        continue;
                    }
                    int slashPos = entryName.lastIndexOf('/');
                    String directoryPath = entryName.substring(0, slashPos + 1);
                    if (!directoryPath.equals(packagePath)) {
                        continue;
                    }
                    set.add(entryName);
                }
            }
        }
        String[] names = new String[set.size()];
        set.toArray(names);
        return names;
    }
    private ConnectionFactory m_connectionFactory;

    // for subclasses
    protected BaseTestCase(){
        super();
    }

    /**
     * Constructs a new TestCase.
     *
     * @param name test name
     */
    public BaseTestCase(String name) {
        super(name);
    }

    /**
     *
     * that produces, tracks and closes the JDBC
     * objects used by this test suite. <p>
     * @return the factory.
     */
    protected ConnectionFactory connectionFactory() {
        if (m_connectionFactory == null) {
            m_connectionFactory = new ConnectionFactory();
        }
        return m_connectionFactory;
    }

    /**
     * using the default connection.
     *
     * @param resource on class path.
     * @throws java.lang.Exception thrown by any internal operation.
     */
    protected void executeScript(String resource) throws Exception {
        if (resource == null) {
            throw new RuntimeException("resource parameter must not be null.");
        }
        resource = resource.trim();
        URL url = this.getClass().getResource(resource);
        if (url == null) {
            String fullResource = (resource.startsWith("/")) ? resource : '/' + this.getClass().getPackage().getName().replace('.', '/') + '/' + resource;
            throw new RuntimeException("No such resource on CLASSPATH: [" + fullResource + "]");
        }
        ScriptIterator it = new ScriptIterator(url);
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = newConnection();
            stmt = conn.createStatement();
            while (it.hasNext()) {
                String sql = (String) it.next();
                //System.out.println("sql:");
                //System.out.println(sql);
                stmt.execute(sql);
            }
            conn.commit();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException se) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException se) {
                }
            }
        }
    }

    /**
     * for the given key.
     *
     * @param key to match.
     * @param defaultValue when there is no matching property.
     * @return the matching value.
     */
    public boolean getBooleanProperty(final String key, final boolean defaultValue) {
        try {
            String value = getProperty(key, String.valueOf(defaultValue));
            if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("on") || value.equals("1")) {
                return true;
            } else if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("off") || value.equals("0")) {
                return false;
            } else {
                return defaultValue;
            }
        } catch (SecurityException se) {
            return defaultValue;
        }
    }

    /**
     * as defined in system properties.
     * @return defined value.
     */
    public String getDriver() {
        return getProperty("driver", BaseTestCase.DEFAULT_DRIVER);
    }

    /**
     * with which named public static int field is initialized.
     *
     * @param fieldName fully qualified public static int field name.
     * @throws java.lang.Exception if no such field or access denied.
     * @return value with which field is initialized.
     */
    protected int getFieldValue(final String fieldName) throws Exception {
        int fieldValue = BaseTestCase.fieldValueMap.get(fieldName, Integer.MIN_VALUE);
        if (fieldValue > Integer.MIN_VALUE) {
            return fieldValue;
        }
        final int lastIndexofDot = fieldName.lastIndexOf('.');
        final String className = fieldName.substring(0, lastIndexofDot);
        final Class clazz = Class.forName(className);
        final String bareFieldName = fieldName.substring(lastIndexofDot + 1);
        fieldValue = clazz.getField(bareFieldName).getInt(null);
        BaseTestCase.fieldValueMap.put(fieldName, fieldValue);
        return fieldValue;
    }

    /**
     * for the given key.
     *
     * @param key to match.
     * @param defaultValue when there is no matching property.
     * @return the matching value.
     */
    public int getIntProperty(final String key, final int defaultValue) {
        String propertyValue = this.getProperty(key, null);
        int rval = defaultValue;

        if (propertyValue != null) {
            propertyValue = propertyValue.trim();

            if (propertyValue.length() > 0) {
                try {
                    rval = Integer.parseInt(propertyValue);
                } catch (Exception ex) {
                }
            }
        }

        return rval;
    }

    /**
     * as defined in system properties.
     * @return defined value.
     */
    public String getPassword() {
        return getProperty("password", BaseTestCase.DEFAULT_PASSWORD);
    }

    /**
     * for the given key.
     *
     * @param key to match.
     * @param defaultValue when there is no matching property.
     * @return the matching value.
     */
    public String getProperty(final String key, final String defaultValue) {
        String value = null;
        String translatedPropertyKey = translatePropertyKey(key);
        // Note: some properties may be submitted on the command line and
        // should override property file resources on the class path.
        try {
            value = System.getProperty(translatedPropertyKey, null);
        } catch (SecurityException se) {
        }
        if (value == null) {
            value = BundleHandler.getString(BaseTestCase.testBundleHandle, translatedPropertyKey);
        }
        if (value == null) {
            value = BundleHandler.getString(BaseTestCase.testConvertBundleHandle, translatedPropertyKey);
        }
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    /**
     * as defined in system properties.
     * @return defined value.
     */
    public String getUrl() {
        return getProperty("url", BaseTestCase.DEFAULT_URL);
    }

    /**
     * as defined in system properties.
     * @return defined value.
     */
    public String getUser() {
        return getProperty("user", BaseTestCase.DEFAULT_USER);
    }

    /**
     * with the driver, url, user and password
     * specifed by the corresponding protected
     * accessors of this class. <p>
     *
     * @return a new connection.
     * @throws java.lang.Exception thrown by any internal operation.
     */
    protected Connection newConnection() throws Exception {
        final String driver = getDriver();
        // not actually needed under JDBC4, as long as jar has META-INF service entry
        Class.forName(driver);
        final String url = getUrl();
        final String user = getUser();
        final String password = getPassword();
        return connectionFactory().newConnection(driver, url, user, password);
    }

    /**
     * to standard output.
     * @param msg to print
     */
    protected void print(Object msg) {
        System.out.print(msg);
    }

    /**
     * to standard output.
     * @param msg to print
     */
    protected void println(Object msg) {
        System.out.println(getClass().getName() + ":" + msg);
    }

    /**
     * Performs test setup.
     *
     * @throws java.lang.Exception probably never
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Performs test teardown.
     *
     * @throws java.lang.Exception probably never
     */
    @Override
    protected void tearDown() throws Exception {
        connectionFactory().closeRegisteredObjects();
        super.tearDown();
    }

    /**
     * by prepending the test suite property prefix.
     *
     * @param key to translate.
     * @return the given key, prepending with the test suite property prefix.
     */
    protected String translatePropertyKey(final String key) {
        return "hsqldb.test.suite." + key;
    }
}
