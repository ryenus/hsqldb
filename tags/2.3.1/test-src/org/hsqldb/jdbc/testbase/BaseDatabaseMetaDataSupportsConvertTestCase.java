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
package org.hsqldb.jdbc.testbase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Exhaustively tests the supportsConvert(int,int) method of
 * interface java.sql.DatabaseMetaData. <p>
 *
 * Note that concrete subclasses <em>must</em> provide a public default
 * constructor that delegates to the protected default constructor of this
 * class (in order to provide access to concrete implementations of
 * {@link #getSQLTypeCode(int)} and {@link #getSQLTypeName(int)}. <p>
 *
 * In order to automate exhaustive combinatoric testing, concrete subclasses
 * must also provide a <tt>public static TestCase suite()</tt> method that
 * delegates to {@link #createTestSuite(java.lang.Class)}, passing their own
 * @{link Class} and the supported type count (one greater than the maximum
 * valid index than can be passed to the concrete implementation of
 * {@link #getSQLTypeCode(int)} or  {@link #getSQLTypeName(int)}.  <p>
 *
 * Finally, for each supported conversion, there must be a <tt>'true'</tt>
 * valued entry in either the <tt>/org/hsqldb/resources/test.properties</tt>
 * or the <tt>/org/hsqldb/resources/test-dbmd-convert.properties</tt> resource
 * (in that order of precedence) whose key matches the output of
 * {@link #translatePropertyKey(java.lang.String)} when the input value is of
 * the form  <tt>'dbmd.supports.convert.to.${target-type-name}.from.${source-type-name}'</tt>
 * where <tt>${target-type-name}</tt> and <tt>${source-type-name}</tt> are values
 * returned from invocation of {@link #getSQLTypeName(int)}
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.2.9
 * @since HSQLDB 2.1.0
 */
@ForSubject(java.sql.DatabaseMetaData.class)
@OfMethod("supportsConvert(int,int")
public abstract class BaseDatabaseMetaDataSupportsConvertTestCase extends BaseJdbcTestCase {

    private final int m_toIndex;
    private final int m_fromIndex;

    private static DatabaseMetaData s_dbmd;


    /**
     * Constructs a new test case for the given pair of type index values.
     *
     * @param toIndex of target type
     * @param fromIndex of source type
     */
    protected BaseDatabaseMetaDataSupportsConvertTestCase(
            final int toIndex,
            final int fromIndex) {
        setName(computeTestName(toIndex, fromIndex));

        m_toIndex = toIndex;
        m_fromIndex = fromIndex;
    }

    private String computeTestName(int toIndex, int fromIndex) {
        String toType = getSQLTypeName(toIndex);
        String fromType = getSQLTypeName(fromIndex);
        String testName = "testSupportsConvert_to_" + toType.toUpperCase() + "_from_" + fromType.toUpperCase();
        return testName;
    }

    private int getFromIndex() {
        return m_fromIndex;
    }

    private int getToIndex() {
        return m_toIndex;
    }

    /**
     * Suite execution performance optimization property getter.
     *
     * This value is used to optimize suite execution performance.  In particular,
     * if this method returns false, then a single DatabaseMetaData instance
     * is used for all tests in the suite and the Connection from which it is
     * obtained is closed immediately after the instance is first obtained,
     * in order to prevent resource leakage.  This can save a tremedous amount
     * of execution time over being required to open and close a new connection
     * (and possibly a new embedded database instance) for each pair of types
     * to be tested.
     *
     * @return true if the Connection instance associated with a
     *         DatabaseMetaData instance must be open in order to
     *         successfully invoke the supportsConvert(int,int) method;
     *         otherwise false
     */
    protected boolean isSupportsConvertInvocationRequiresOpenConnection(){
        return getBooleanProperty("dbmd.supports.convert.invocation.requires.open.connection", false);
    }

    protected DatabaseMetaData getMetaData() throws Exception {
        if (isSupportsConvertInvocationRequiresOpenConnection()) {
            return newConnection().getMetaData();
        } else {
            if (s_dbmd == null) {
                Connection conn = newConnection();
                s_dbmd = conn.getMetaData();
                conn.close();
            }

            return s_dbmd;
        }
    }

    /**
     * @param i index of type for which to fetch the code
     * @return the SQL type's JDBC type code for the ith type to be tested.
     */
    protected abstract int getSQLTypeCode(int i);

    /**
     *
     * @param i index of type for which to fetch the name
     * @return the SQL type name for the ith type to be tested.
     */
    protected abstract String getSQLTypeName(int i);

    /**
     *
     * @param clazz that provides the concrete test case implementation.
     * @param typeCount one greater than the maximum valid index than can be
     *        passed to {@link #getSQLTypeCode(int)} or
     *        {@link #getSQLTypeName(int)}
     * @return a suite of test cases; 1 for each possible from/to data type pair
     */
    protected static TestSuite createTestSuite(
            Class<? extends BaseDatabaseMetaDataSupportsConvertTestCase> clazz,
            int typeCount) {
        TestSuite suite = new TestSuite(clazz.getName());
        Class<?>[] parameterTypes = new Class<?>[]{int.class, int.class};

        try {
            Constructor<? extends Test> ctor = clazz.getConstructor(parameterTypes);

            for (int toIndex = 0; toIndex < typeCount; toIndex++) {
                for (int fromIndex = 0; fromIndex < typeCount; fromIndex++) {
                    suite.addTest(ctor.newInstance(toIndex, fromIndex));
                }
            }

            return suite;
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();

            if (t == null) {
                t = e;
            }

            throw new RuntimeException(t.toString(), t);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e.toString(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.toString(), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e.toString(), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e.toString(), e);
        } catch (SecurityException e) {
            throw new RuntimeException(e.toString(), e);
        }

    }

    @Override
    protected final void runTest() throws Throwable {
        final int fromIndex = getFromIndex();
        final int toIndex = getToIndex();
        //
        final String toName = getSQLTypeName(toIndex);
        final int toCode = getSQLTypeCode(toIndex);
        final String fromName = getSQLTypeName(fromIndex);
        final int fromCode = getSQLTypeCode(fromIndex);
        final String propertyName = "dbmd.supports.convert.to." + toName + ".from." + fromName;
        final boolean expectedResult = getBooleanProperty(propertyName, false);
        //long start = System.currentTimeMillis();
        final boolean actualResult = getMetaData().supportsConvert(fromCode, toCode);
        //long end = System.currentTimeMillis();

        //if ((end-start) > 0) {
            //println('!');
        //}
        if (expectedResult != actualResult) {
            println("CHECK FOR MISSING TEST PROPERTY: " + translatePropertyKey(propertyName) + "=" + actualResult);
        }
        assertEquals(expectedResult, actualResult);
    }
}
