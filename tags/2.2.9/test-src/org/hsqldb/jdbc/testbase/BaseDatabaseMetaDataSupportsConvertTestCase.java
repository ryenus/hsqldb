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
import java.sql.DatabaseMetaData;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Exhaustively tests the supportsConvert(int,int) method of
 * interface java.sql.DatabaseMetaData.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1.0
 * @since HSQLDB 2.1.0
 */
@ForSubject(java.sql.DatabaseMetaData.class)
public abstract class BaseDatabaseMetaDataSupportsConvertTestCase extends BaseJdbcTestCase {

    private final int m_toIndex;
    private final int m_fromIndex;

    // for subclasses
    protected BaseDatabaseMetaDataSupportsConvertTestCase() {
        this(0, 0);
    }

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

    protected DatabaseMetaData getMetaData() throws Exception {
        return newConnection().getMetaData();
    }

    /**
     * name from which the JDBC type code is reflectively determined.
     * @param i
     * @return the SQL type's JDBC type code field name for the ith type to be tested.
     */
    protected abstract int getSQLTypeCode(int i);

    /**
     *
     * @param i
     * @return the SQL type name for the ith type to be tested.
     */
    protected abstract String getSQLTypeName(int i);

    /**
     *
     * @return the number of types to be combinatorially tested.
     */
    protected abstract int getSQLTypeCount();

    protected final TestSuite createTestSuite(String name) {
        final TestSuite suite = new TestSuite(name);
        final int count = getSQLTypeCount();
        final Class clazz = getClass();
        final Class[] signature = new Class[]{int.class, int.class};

        Constructor<? extends Test> constructor;
        try {
            constructor = clazz.getConstructor(signature);
            
            for (int toIndex = 0; toIndex < count; toIndex++) {
                for (int fromIndex = 0; fromIndex < count; fromIndex++) {
                    suite.addTest(constructor.newInstance(toIndex, fromIndex));
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
    @OfMethod("supportsConvert(int,int)")
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
            //System.out.println('!');
        //}
        if (expectedResult != actualResult) {
            System.out.println("CHECK FOR MISSING TEST PROPERTY: " + translatePropertyKey(propertyName) + "=" + actualResult);
        }
        assertEquals(expectedResult, actualResult);
    }
}
