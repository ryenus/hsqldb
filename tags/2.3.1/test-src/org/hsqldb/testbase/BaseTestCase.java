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
package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCResultSetTest;
import org.hsqldb.jdbc.testbase.SqlState;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.testbase.ConnectionFactory.ConnectionFactoryEventListener;

/**
 * Abstract HSQLDB-targeted JUnit 3.8 test case. <p>
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public abstract class BaseTestCase extends junit.framework.TestCase {

    public static final String BTCK_CLOSE_EMBEDDED_DATABASES_HANDLER = "close.embedded.databases.handler";
    public static final String DEFAULT_PROPERTY_KEY_PREFIX = "hsqldb.test.suite.";
    public static final String DEFAULT_PROPERTY_KEY_PREFIX_KEY = DEFAULT_PROPERTY_KEY_PREFIX + "properties.prefix";
    public static final String DEFAULT_DRIVER = "org.hsqldb.jdbcDriver";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_URL = "jdbc:hsqldb:mem:testcase";
    public static final String DEFAULT_USER = "SA";
    public static final String BTCK_CLOSE_EMBEDDED_DATABASES_ON_TEARDOWN = "close.embedded.databases.on.teardown";
    public static final int DEFAULT_INCOMPATIBLE_DATA_TYPE_CONVERSION_ERROR_CODE = -ErrorCode.X_42561;
    public static final int DEFAULT_RESULT_SET_AFTER_LAST_ERROR_CODE = -ErrorCode.X_24504;
    public static final int DEFAULT_RESULT_SET_BEFORE_FIRST_ERROR_CODE = -ErrorCode.X_24504;
    public static final int DEFAULT_RESULT_SET_CLOSED_ERROR_CODE = -ErrorCode.X_24501;
    //
    protected static final IntValueHashMap s_fieldValueMap =
            new IntValueHashMap();
    //
    protected static final String[][] s_rsconcurrency = new String[][]{
        {
            "concur_read_only",
            "java.sql.ResultSet.CONCUR_READ_ONLY"
        },
        {
            "concur_updatable",
            "java.sql.ResultSet.CONCUR_UPDATABLE"
        }
    };
    //
    protected static final String[][] s_rsholdability = new String[][]{
        {
            "close_cursors_at_commit",
            "java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT"
        },
        {
            "hold_cursors_over_commit",
            "java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT"
        }
    };
    //
    protected static final String[][] s_rstype = new String[][]{
        {
            "type_forward_only",
            "java.sql.ResultSet.TYPE_FORWARD_ONLY"
        },
        {
            "type_scroll_insensitive",
            "java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE"
        },
        {
            "type_scroll_sensitive",
            "java.sql.ResultSet.TYPE_SCROLL_SENSITIVE"
        }
    };
    //
    private static final ConnectionFactory.ConnectionFactoryEventListener s_embeddedDatabaseCloser = HsqldbEmbeddedDatabaseCloser.Instance;
    //
    private static final String LINE_SEPARATOR =
            PropertyGetter.getProperty("line.separator", "\n");
    //
    private static final String TEST_LABEL_FORMAT_1 =
            LINE_SEPARATOR
            + "--------------------------------------------------------------------------------" + LINE_SEPARATOR
            + "TEST SUBJECT : {0}.{1}" + LINE_SEPARATOR
            + "TEST FIXTURE : {2}.{3}" + LINE_SEPARATOR
            + "TEST MESSAGE : {4}" + LINE_SEPARATOR
            + "--------------------------------------------------------------------------------" + LINE_SEPARATOR;
    //
    private static final String TEST_LABEL_FORMAT_2 =
            LINE_SEPARATOR
            + "--------------------------------------------------------------------------------" + LINE_SEPARATOR
            + "TEST SUBJECT : {0}.{1}" + LINE_SEPARATOR
            + "TEST FIXTURE : {2}.{3}" + LINE_SEPARATOR
            + "--------------------------------------------------------------------------------" + LINE_SEPARATOR;
    //
    private static final String PROGRESS_FORMAT =
            "{0}[PROGRESS]: {1}" + LINE_SEPARATOR;
    //
    private static final String EXCEPTION_FORMAT =
            "{0}[EXCEPTION]: {1}" + LINE_SEPARATOR
            + "{2}" + LINE_SEPARATOR;
    //
    private static final String WARNING_FORMAT =
            "{0}[WARNING]: {1}" + LINE_SEPARATOR;
    //
    private static final String PRINTLN_FORMAT = "{0}" + LINE_SEPARATOR;
    //
    private static final Class<?>[] NO_PARMS = new Class<?>[0];
    //
    private ConnectionFactory m_connectionFactory;
    private ConnectionFactory.ConnectionFactoryEventListener m_embeddedDatabaseCloser;

    /**
     * by computing a shallow string representation of the given array. <p>
     *
     * @param array for which to produce the string representation.
     * @return the string representation.
     */
    protected static String arrayToString(Object array) {
        if (array == null) {
            return "null";
        }
        int length = Array.getLength(array);
        StringBuilder sb = new StringBuilder(2 + 3 * length);
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
     *
     * @param expected array
     * @param actual array
     * @return an "arrays not equal" failure message describing the given
     * values.
     */
    protected static String arraysNotEqualMessage(Object expected, Object actual) {
        // TODO:  implement cf-style message, but with either context info
        //        (i.e. start/end index of common prefix/suffic) or with
        //        full content oncluded and possibly something more like a DIFF
        //        style (multiple differences handled) output.
        //return (new ComparisonFailure(null, arrayToString(expected), arrayToString(actual))).getMessage();
        return "expected:<" + arrayToString(expected) + "> but was:<" + arrayToString(actual) + ">";
    }

    /**
     * with special handling for Java array objects.
     *
     * @param expected object
     * @param actual object
     * @throws java.lang.Exception as thrown by any internal operation.
     */
    protected static void assertJavaArrayEquals(Object expected, Object actual) throws Exception {
        switch (getComponentDescriptor(expected)) {
            case 'X': {
                // null component descriptor
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
                } else if (Object[].class.isAssignableFrom(expected.getClass())
                        && Object[].class.isAssignableFrom(actual.getClass())) {
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
     *
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
                // TODO java.sql.Array, java.sql.Struct, etc.
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
     *
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
     *
     * @return a character code representing the component type.
     * @param o for which to produce the component descriptor.
     */
    protected static char getComponentDescriptor(Object o) {
        if (o == null) {
            return 'X';
        }
        Class<?> cls = o.getClass();
        if (cls.isArray()) {
            Class<?> comp = cls.getComponentType();
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
            return 'O'; // non-array object instance
        }
    }
    protected boolean m_resultSetConcurrencyDetermined;
    protected boolean m_supportsForwardOnlyUpdates;
    protected boolean m_supportsScrollInsensitiveUpdates;
    protected boolean m_supportsScrollSensitiveUpdates;
    protected boolean m_supportsUpdates;

    /**
     * for file: or jar: resources.
     *
     * @param packageName
     * @throws java.io.IOException
     * @return array of paths to the resources in the given package
     */
    protected String[] getResoucesInPackage(final String packageName) throws IOException {
        String packagePath = packageName.replace('.', '/');
        if (!packagePath.endsWith("/")) {
            packagePath += '/';
        }

        Enumeration<URL> resources = getResources(packagePath);
        OrderedHashSet set = new OrderedHashSet();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
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
                Enumeration<JarEntry> entries = ((JarURLConnection) resource.openConnection()).getJarFile().entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
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

    // for subclasses
    protected BaseTestCase() {
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

    protected URL getResource(String resource) throws Exception {
        return getClass().getResource(resource);
    }

    protected Enumeration<URL> getResources(String resource) throws IOException {
        return getClass().getClassLoader().getResources(resource);
    }

    /**
     *
     * that produces, tracks and closes the JDBC objects used by this test
     * suite. <p>
     *
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
    @SuppressWarnings({"AssignmentToMethodParameter"})
    protected void executeScript(String resource) throws Exception {
        if (resource == null) {
            throw new RuntimeException("resource parameter must not be null.");
        }
        resource = resource.trim();
        URL url = getResource(resource);
        String packageName = this.getClass().getPackage().getName();
        if (url == null) {
            String fullResource = (resource.startsWith("/"))
                    ? resource
                    : '/' + packageName.replace('.', '/') + '/' + resource;
            throw new RuntimeException(
                    "No such resource on CLASSPATH: [" + fullResource + "]");
        }
        Connection conn = null;
        Statement stmt = null;
        ScriptIterator it = new ScriptIterator(url);
        String sql = null;
        try {
            conn = newConnection();
            stmt = connectionFactory().createStatement(conn);
            while (it.hasNext()) {
                sql = (String) it.next();
                stmt.execute(sql);
            }
            conn.commit();
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String sqlState = ((SQLException) e).getSQLState();
                int sqlCode = ((SQLException) e).getErrorCode();

                println("SQL State: " + sqlState);
                println("SQL Code: " + sqlCode);
            }
            println("error executing sql:");
            println(sql);

            throw e;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
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
    public boolean getBooleanProperty(final String key,
            final boolean defaultValue) {
        return PropertyGetter.getBooleanProperty(
                translatePropertyKey(key),
                defaultValue);
    }

    /**
     * as defined in system properties.
     *
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
        int fieldValue = BaseTestCase.s_fieldValueMap.get(fieldName, Integer.MIN_VALUE);
        if (fieldValue > Integer.MIN_VALUE) {
            return fieldValue;
        }
        final int lastIndexofDot = fieldName.lastIndexOf('.');
        final String className = fieldName.substring(0, lastIndexofDot);
        final Class<?> clazz = Class.forName(className);
        final String bareFieldName = fieldName.substring(lastIndexofDot + 1);
        fieldValue = clazz.getField(bareFieldName).getInt(null);
        BaseTestCase.s_fieldValueMap.put(fieldName, fieldValue);
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
        return PropertyGetter.getIntProperty(
                translatePropertyKey(key),
                defaultValue);
    }
    
    /**
     * for the given key.
     *
     * @param key to match.
     * @param defaultValue when there is no matching property.
     * @return the matching value.
     */
    public double getDoubleProperty(final String key, final double defaultValue) {
        return PropertyGetter.getDoubleProperty(
                translatePropertyKey(key),
                defaultValue);
    }    

    /**
     * as defined in system properties.
     *
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
        String translatedPropertyKey = translatePropertyKey(key);

        String value = PropertyGetter.getProperty(translatedPropertyKey, defaultValue);
        return value;
    }

    /**
     * as defined in system properties.
     *
     * @return defined value.
     */
    public String getUrl() {
        return getProperty("url", BaseTestCase.DEFAULT_URL);
    }

    /**
     * as defined in system properties.
     *
     * @return defined value.
     */
    public String getUser() {
        return getProperty("user", BaseTestCase.DEFAULT_USER);
    }

    /**
     * with the driver, url, user and password specified by the corresponding
     * protected accessors of this class. <p>
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

    protected boolean isPrint() {
        return getBooleanProperty("print.test.output", true);
    }

    /**
     * to standard output.
     *
     * @param msg to print
     */
    protected void print(Object msg) {
        if (isPrint()) {
            final PrintStream ps = System.out;

            if (ps != null) {
                synchronized (ps) {
                    ps.print(msg);
                }
            }
        }
    }

    /**
     * to standard output.
     */
    protected final void println() {
        print(LINE_SEPARATOR);
    }

    /**
     * to standard output.
     *
     * @param msg to print
     */
    protected final void println(final Object msg) {
        print(MessageFormat.format(PRINTLN_FORMAT, msg));
    }

    protected boolean isPrintProgress() {
        return isPrint() && getBooleanProperty("print.test.progress", true);
    }

    protected final void printProgress(final Object msg) {
        if (isPrintProgress()) {
            print(MessageFormat.format(
                    PROGRESS_FORMAT,
                    getClass().getName(),
                    msg));
        }
    }

    protected boolean isPrintExceptions() {
        return isPrint() && getBooleanProperty("print.test.exceptions", true);
    }

    protected final void printException(Throwable t) {
        if (isPrintExceptions()) {
            StringWriter sw = new StringWriter();

            t.printStackTrace(new PrintWriter(sw));

            print(MessageFormat.format(
                    EXCEPTION_FORMAT,
                    getClass().getName(),
                    t.getMessage(),
                    sw.toString()));
        }
    }

    protected boolean isPrintWarnings() {
        return isPrint() && getBooleanProperty("print.test.warnings", true);
    }

    protected boolean isFailStubTestCase() {
        return getBooleanProperty("fail.stubbed.testcase", true);
    }

    protected final void printWarning(Throwable t) {
        if (isPrintWarnings()) {
            print(MessageFormat.format(
                    WARNING_FORMAT,
                    getClass().getName(),
                    t.getMessage(),
                    LINE_SEPARATOR));
        }
    }

    protected boolean isPrintTestLabels() {
        return this.getBooleanProperty("print.test.lables", true);
    }

//    protected void printTestLabel() {
//        printTestLabel0(null, null);
//    }
//
//    protected void printTestLabel(final Object msg) {
//        printTestLabel0(null, msg);
//    }
    /**
     * to standard output.
     *
     * @param method
     * @param msg to print
     */
    @SuppressWarnings("AssignmentToMethodParameter")
    private void printTestLabel0(Method method, final Object msg) {
        if (!isPrintTestLabels()) {
            return;
        }

        if (method == null) {
            method = this.getStackTraceMethod(
                    Thread.currentThread().getStackTrace(),
                    3);
        }

        String subjectClassName = getSubjectClassName(method);
        String subjectMethodName = getSubjectMethodName(method);

        if (subjectClassName == null) {
            subjectClassName = "<<Unspecified Subject Class>>";
        }
        if (subjectMethodName == null) {
            subjectMethodName = "<<Unspecified Subject Method>>";
        }

        String callerClassName = getClass().getName();
        String callerMethodName = method.getName();

        String fullMessage = null;

        if (msg == null) {
            fullMessage = MessageFormat.format(
                    TEST_LABEL_FORMAT_2,
                    subjectClassName,
                    subjectMethodName,
                    callerClassName,
                    callerMethodName);
        } else {
            fullMessage = MessageFormat.format(
                    TEST_LABEL_FORMAT_1,
                    subjectClassName,
                    subjectMethodName,
                    callerClassName,
                    callerMethodName,
                    msg);
        }

        print(fullMessage);
    }

    private String getSubjectClassName(Method method) {
        if (method != null && method.isAnnotationPresent(ForSubject.class)) {
            return method.getAnnotation(ForSubject.class).value().getCanonicalName();
        }

        if (getClass().isAnnotationPresent(ForSubject.class)) {
            return getClass().getAnnotation(ForSubject.class).value().getCanonicalName();
        }

        if (getClass().getName().endsWith("Test")) {
            String name = getClass().getName();
            return "[?]" + name.substring(0, name.length() - 4);
        }

        return null;
    }

    private Method getStackTraceMethod(
            final StackTraceElement[] stackTrace,
            final int index) {
        if (stackTrace == null && stackTrace.length <= index) {
            return null;
        }

        Method method = null;
        String methodName = stackTrace[index].getMethodName();

        try {
            method = getClass().getMethod(methodName, NO_PARMS);
        } catch (NoSuchMethodException ex) {
        } catch (SecurityException ex) {
        }

        return method;
    }

    private String getSubjectMethodName(Method method) {

        if (method != null && method.isAnnotationPresent(OfMethod.class)) {
            String[] result = method.getAnnotation(OfMethod.class).value();

            return (result.length == 1) ? result[0] : Arrays.toString(result);
        }

        if (getClass().isAnnotationPresent(OfMethod.class)) {
            String[] result = getClass().getAnnotation(OfMethod.class).value();

            return (result.length == 1) ? result[0] : Arrays.toString(result);
        }

        if (method == null) {
            return null;
        }

        String methodName = method.getName();

        if (methodName.startsWith("test")) {
            String name = methodName.substring(4);

            if (name.length() > 1) {
                char ch1 = name.charAt(0);
                char ch2 = name.charAt(1);

                if (Character.isLowerCase(ch2)
                        && Character.isUpperCase(ch1)) {
                    name = Character.toLowerCase(ch1)
                            + name.substring(1);
                }
            } else {
                name = methodName.toLowerCase();
            }

            return "[?]" + name;
        }

        return null;
    }

    @Override
    protected void runTest() throws Throwable {
        String testCaseName = this.getName();
        assertNotNull("TestCase name must not be null", testCaseName);
        Method runMethod = null;
        try {
            // use getMethod to get all public inherited
            // methods. getDeclaredMethods returns all
            // methods of this class but excludes the
            // inherited ones.
            runMethod = getClass().getMethod(testCaseName, NO_PARMS);
        } catch (NoSuchMethodException e) {
            fail("Fixture Method \"" + testCaseName + "()\" not found");
        }
        if (!Modifier.isPublic(runMethod.getModifiers())) {
            fail("Fixture Method \"" + testCaseName + "()\" should be public");
        }

        this.printTestLabel0(runMethod, null);

        try {
            runMethod.invoke(this);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();

            // fillInStackTrace does not produce the desired result
            // (i.e. without this, resulting failure trace *does not* include
            // the line that caused the assertion failure)
            t.setStackTrace(t.getStackTrace());

            throw t;
        } catch (IllegalAccessException e) {
            e.setStackTrace(e.getStackTrace());
            throw e;
        }
    }

    /**
     * Performs test setup.
     *
     * @throws java.lang.Exception probably never
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        connectionFactory().closeRegisteredObjects();
    }

    /**
     * Determines if teardown attempts to closes any open in-process database
     * instances.
     *
     * @return true if so, else false.
     */
    protected boolean isCloseEmbeddedDatabasesOnTeardown() {
        return getBooleanProperty(BTCK_CLOSE_EMBEDDED_DATABASES_ON_TEARDOWN, false);
    }

    protected ConnectionFactoryEventListener getEmbeddedDatabaseCloser() {
        ConnectionFactoryEventListener closer = null;
        String closerFQN = getProperty(BTCK_CLOSE_EMBEDDED_DATABASES_HANDLER,
                HsqldbEmbeddedDatabaseCloser.class.getName());

        if (!HsqldbEmbeddedDatabaseCloser.class.getName().equals(closerFQN)) {
            try {
                Class<?> closerClass = Class.forName(closerFQN);
                closer = (ConnectionFactory.ConnectionFactoryEventListener) closerClass.newInstance();
            } catch (ClassNotFoundException cnfe) {
            } catch (IllegalAccessException iae) {
            } catch (InstantiationException ie) {
            }
        }
        if (closer == null) {
            closer = s_embeddedDatabaseCloser;
        }

        return closer;
    }

    // invoked on teardown to ensure that the built-in listeners always 
    // get notified *after* all subclass listener registrations.
    protected void activateConnectionFactoryListeners() {
        if (isCloseEmbeddedDatabasesOnTeardown()) {
            m_embeddedDatabaseCloser = getEmbeddedDatabaseCloser();

            if (m_embeddedDatabaseCloser != null) {
                connectionFactory().addEventListener(m_embeddedDatabaseCloser);
            }
        }
    }

    protected void deactivateConnectionFactoryListeners() {
        if (m_embeddedDatabaseCloser != null) {
            connectionFactory().removeEventListener(m_embeddedDatabaseCloser);
        }
    }

    /**
     * Activates any configured ConnectionFactoryEventLiseners. <p>
     *
     * Invoked before the main teardown behavior occurs, providing a facility
     * for controlling event listener registration / invocation
     *
     * @throws Exception
     */
    protected void preTearDown() throws Exception {
        activateConnectionFactoryListeners();
    }

    /**
     * Performs test teardown, which includes preTeardown and postTeardown<p>
     *
     * It is highly recommended to use the pre and post methods, rather than
     * overriding the teardown method, which contains important base
     * functionality while other teardown work may need to come either before or
     * after this.
     *
     * @throws java.lang.Exception if any, thrown as part of tearing down the
     * test fixture.
     */
    @Override
    protected void tearDown() throws Exception {

        Exception preTearDownException = null;
        Exception tearDownException = null;
        Exception postTearDownException = null;


        try {
            preTearDown();
        } catch (Exception ex) {
            preTearDownException = ex;
        }

        connectionFactory().closeRegisteredObjects();

        try {
            super.tearDown();
        } catch (Exception ex) {
            tearDownException = ex;
        } finally {
            try {
                postTearDown();
            } catch (Exception ex2) {
                postTearDownException = ex2;
            }
        }

        if (tearDownException != null) {
            throw tearDownException;
        } else if (postTearDownException != null) {
            throw postTearDownException;
        } else if (preTearDownException != null) {
            throw preTearDownException;
        }
    }

    /**
     * Invoked before the main teardown behavior occurs.
     *
     * @throws Exception
     */
    protected void postTearDown() throws Exception {
        deactivateConnectionFactoryListeners();
    }

    /**
     * by prepending the test suite property prefix.
     *
     * @param key to translate.
     * @return the given key, prepended with the test suite property prefix.
     */
    protected String translatePropertyKey(final String key) {
        String prefix = DEFAULT_PROPERTY_KEY_PREFIX;

        try {
            prefix = System.getProperty(
                    DEFAULT_PROPERTY_KEY_PREFIX_KEY,
                    DEFAULT_PROPERTY_KEY_PREFIX);
        } catch (SecurityException se) {
        }

        return prefix + key;
    }

    protected void stubTestResult() {
        stubTestResult("The test case is only a stub.");
    }

    protected void stubTestResult(String message) {
        if (isFailStubTestCase()) {
            fail(message);
        }
    }

    protected boolean isTestARRAY() {
        return getBooleanProperty("test.types.array", false);
    }

        
    protected boolean isTestDISTINCT() {
        return getBooleanProperty("test.types.distinct", false);
    }
    
    protected boolean isTestJAVA_OBJECT() {
        return getBooleanProperty("test.types.distinct", false);
    } 
    
    protected boolean isTestOTHER() {
        return getBooleanProperty("test.types.distinct", false);
    }     
    
    protected boolean isTestREF() {
        return getBooleanProperty("test.types.ref", false);
    }

    protected boolean isTestROWID() {
        return getBooleanProperty("test.types.rowid", false);
    }
    
    protected boolean isTestSTRUCT() {
        return getBooleanProperty("test.types.struct", false);
    }  

    protected boolean isTestSQLXML() {
        return getBooleanProperty("test.types.sqlxml", false);
    }

    protected boolean isTestUpdates() throws Exception {
        return supportsUpdates() && getBooleanProperty("test.result.set.updates", true);
    }

    /**
     * Checks to ensure either sql feature not supported with sql state '0A...'
     * or sql exception with error code that indicates statement is closed.
     *
     * @param ex to check
     */
    protected void checkResultSetAfterLastOrNotSupportedException(SQLException ex) {
        if (ex instanceof SQLFeatureNotSupportedException) {
            assertEquals("0A", ex.getSQLState().substring(0, 2));
        } else {
            assertEquals("sql state", SqlState.Exception.InvalidCursorState.IdentifiedCursorNotPositionedOnRowIn_UPDATE_DELETE_SET_or_GET_Statement.Value, ex.getSQLState());
            assertEquals("Error code for: " + ex.toString(), this.getResultSetAfterLastErrorCode(), ex.getErrorCode());
        }
    }

    /**
     * Checks to ensure either sql feature not supported with sql state '0A...'
     * or sql exception with error code that indicates statement is closed.
     *
     * @param ex to check
     */
    protected void checkResultSetBeforeFirstOrNotSupportedException(SQLException ex) {
        if (ex instanceof SQLFeatureNotSupportedException) {
            assertEquals("0A", ex.getSQLState().substring(0, 2));
        } else {
            assertEquals("sql state", SqlState.Exception.InvalidCursorState.IdentifiedCursorNotPositionedOnRowIn_UPDATE_DELETE_SET_or_GET_Statement.Value, ex.getSQLState());
            assertEquals("Error code for: " + ex.toString(), getResultSetBeforeFirstErrorCode(), ex.getErrorCode());
        }
    }

    /**
     * Checks to ensure either sql feature not supported with sql state '0A...'
     * or sql exception with error code that indicates statement is closed.
     *
     * @param ex to check
     */
    protected void checkResultSetClosedOrNotSupportedException(SQLException ex) {
        if (ex instanceof SQLFeatureNotSupportedException) {
            assertEquals("0A", ex.getSQLState().substring(0, 2));
        } else {
            assertEquals("sql state", SqlState.Exception.InvalidCursorState.IdentifiedCursorIsNotOpen.Value, ex.getSQLState());
            assertEquals("Error code for: " + ex.toString(), getResultSetClosedErrorCode(), ex.getErrorCode());
        }
    }

    protected void determineResultSetConcurrency() throws Exception {
        if (m_resultSetConcurrencyDetermined) {
            return;
        }
        Connection conn = newConnection();
        DatabaseMetaData dbmd = conn.getMetaData();
        m_supportsForwardOnlyUpdates = dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        m_supportsScrollInsensitiveUpdates = dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        m_supportsScrollSensitiveUpdates = dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        m_supportsUpdates = m_supportsForwardOnlyUpdates || m_supportsScrollInsensitiveUpdates || m_supportsScrollSensitiveUpdates;
        m_resultSetConcurrencyDetermined = true;
    }

    protected int getIncompatibleDataTypeConversionErrorCode() {
        return getIntProperty("result.set.incompatible.data.type.conversion.error.code", DEFAULT_INCOMPATIBLE_DATA_TYPE_CONVERSION_ERROR_CODE);
    }

    protected int getResultSetAfterLastErrorCode() {
        return getIntProperty("result.set.after.last.error.code", DEFAULT_RESULT_SET_AFTER_LAST_ERROR_CODE);
    }

    protected int getResultSetBeforeFirstErrorCode() {
        return getIntProperty("result.set.before.first.error.code", DEFAULT_RESULT_SET_BEFORE_FIRST_ERROR_CODE);
    }

    protected int getResultSetClosedErrorCode() {
        return getIntProperty("result.set.closed.error.code", DEFAULT_RESULT_SET_CLOSED_ERROR_CODE);
    }

    // Forward-Only, Read-Only
    protected ResultSet newForwardOnlyReadOnlyResultSet(String query) throws Exception {
        return newReadOnlyResultSet(ResultSet.TYPE_FORWARD_ONLY, query);
    }

    protected ResultSet newReadOnlyResultSet(int type, String query) throws Exception {
        return newResultSet(type, ResultSet.CONCUR_READ_ONLY, query);
    }

    protected ResultSet newResultSet(int type, int concur, String query) throws Exception, SQLException {
        Connection conn = newConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(type, concur);
        connectionFactory().registerStatement(stmt);
        ResultSet rs = connectionFactory().executeQuery(query, stmt);
        return rs;
    }

    // Scrollable, Read-Only
    protected ResultSet newScrollableInsensitiveReadOnlyResultSet(String query) throws Exception {
        return newReadOnlyResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE, query);
    }

    // Scrollable, Updatable
    protected ResultSet newScrollableInsensitiveUpdateableResultSet(String query) throws Exception {
        return newResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, query);
    }

    protected boolean supportsForwardOnlyUpdates() throws Exception {
        determineResultSetConcurrency();
        return m_supportsForwardOnlyUpdates;
    }

    protected boolean supportsScrollInsensitiveUpdates() throws Exception {
        determineResultSetConcurrency();
        return m_supportsScrollInsensitiveUpdates;
    }

    protected boolean supportsScrollSensitiveUpdates() throws Exception {
        determineResultSetConcurrency();
        return m_supportsScrollSensitiveUpdates;
    }

    protected boolean supportsUpdates() throws Exception {
        determineResultSetConcurrency();
        return m_supportsUpdates;
    }

    protected boolean isTestDATALINK() {
        return getBooleanProperty("test.types.datalink", true);
    }
}
