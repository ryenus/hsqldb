/* Copyright (c) 2001-2022, The HSQL Development Group
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
package org.hsqldb.lib;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of {@link KMPSearchAlgorithm}.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(KMPSearchAlgorithm.class)
@SuppressWarnings("ClassWithoutLogger")
public class KMPSearchAlgorithmTest extends BaseTestCase {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(KMPSearchAlgorithmTest.class);
    }

    /**
     * Constructs a new test case with the given {@code testName}.
     * 
     * @param testName 
     */
    public KMPSearchAlgorithmTest(String testName) {
        super(testName);
    }

    /**
     * Test of computeTable method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("computeTable(byte[])")
    public void testComputeTableForByteArray() throws Exception {
        byte[] pattern = null;
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(byte[]) null)");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new byte[0];
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new byte[0])");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new byte[1];
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new byte[1])");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }

        pattern = new byte[2];
        int[] expected = new int[]{-1, 0};
        int[] actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = new byte[10];
        expected = new int[]{-1, 0, 1, 2, 3, 4, 5, 6, 7, 8};
        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = SearchAlgorithmProperty.SearchPatternSuccess.value.getBytes();
        expected = new int[]{-1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
            0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 1, 0};

        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

    }

    /**
     * Test of computeTable method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("computeTable(char[])")
    public void testComputeTableForCharArray() throws Exception {
        char[] pattern = null;
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(char[]) null)");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new char[0];
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new char[0])");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new char[1];
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new char[1])");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }

        pattern = new char[2];
        int[] expected = new int[]{-1, 0};
        int[] actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = new char[10];
        expected = new int[]{-1, 0, 1, 2, 3, 4, 5, 6, 7, 8};
        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = SearchAlgorithmProperty.SearchPatternSuccess.value.toCharArray();
        expected = new int[]{-1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
            0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 1, 0};

        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);
    }

    /**
     * Test of computeTable method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("computeTable(java.lang.String)")
    public void testComputeTableForString() throws Exception {
        String pattern = null;
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable((String) null)");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new String(new char[0]);
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new String(new char[0]))");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }
        pattern = new String(new char[1]);
        try {
            KMPSearchAlgorithm.computeTable(pattern);
            fail("Allowed computeTable(new String(new char[1]))");
        } catch (Exception e) {
            assertEquals("Exception class", IllegalArgumentException.class, e.getClass());
        }

        pattern = new String(new char[2]);
        int[] expected = new int[]{-1, 0};
        int[] actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = new String(new char[10]);
        expected = new int[]{-1, 0, 1, 2, 3, 4, 5, 6, 7, 8};
        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);

        pattern = SearchAlgorithmProperty.SearchPatternSuccess.value;
        expected = new int[]{-1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
            0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 1, 0};

        actual = KMPSearchAlgorithm.computeTable(pattern);

        assertJavaArrayEquals(expected, actual);
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     */
    @OfMethod("search(byte[],byte[],int[],int)")
    public void testSearchByteArrayForByteArrayFromStartPosition() {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final byte[] source
                    = SearchAlgorithmProperty.SearchTarget.value.getBytes();
            final byte[] pattern = sap.value == null
                    ? null
                    : sap.value.getBytes();
            final long expected = sap.expectedCharPosition;
            final long actual = KMPSearchAlgorithm.search(source, pattern, null, 0);
            final String msg = "position of " + sap.name()
                    + " in source";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     */
    @OfMethod("search(char[],char[],int[],int)")
    public void testSearchCharArrayForCharArrayFromStartPostion() {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final char[] source
                    = SearchAlgorithmProperty.SearchTarget.value.toCharArray();
            final char[] pattern = sap.value == null
                    ? null
                    : sap.value.toCharArray();
            final long expected = sap.expectedCharPosition;
            final long actual = KMPSearchAlgorithm.search(source, pattern, null, 0);
            final String msg = "position of " + sap.name()
                    + " in source";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("search(java.io.InputStream,byte[],int[])")
    public void testSearchInputStreamForByteArray() throws Exception {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final InputStream stream = new ByteArrayInputStream(
                    SearchAlgorithmProperty.SearchTarget.value.getBytes());
            final byte[] pattern = sap.value == null
                    ? null
                    : sap.value.getBytes();
            final long expected = sap.expectedBytePosition;
            final long actual = KMPSearchAlgorithm.search(stream, pattern, null);
            final String msg = "position of " + sap.name()
                    + " in input stream";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("search(java.io.Reader,char[],int[])")
    public void testSearchReaderForCharArray() throws Exception {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final Reader reader = new StringReader(
                    SearchAlgorithmProperty.SearchTarget.value);
            char[] pattern = sap.value == null ? null : sap.value.toCharArray();
            final long expected = sap.expectedCharPosition;
            final long actual = KMPSearchAlgorithm.search(reader, pattern, null);
            final String msg = "position of " + sap.name()
                    + " in reader stream";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     *
     * @throws java.lang.Exception
     */
    @OfMethod("search(java.io.Reader,java.lang.String,int[])")
    public void testSearchReaderForString() throws Exception {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final Reader reader = new StringReader(
                    SearchAlgorithmProperty.SearchTarget.value);
            String pattern = sap.value == null ? null : sap.value;
            final long expected = sap.expectedCharPosition;
            final long actual = KMPSearchAlgorithm.search(reader, pattern, null);
            final String msg = "position of " + sap.name()
                    + " in reader stream";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class KMPSearchAlgorithm.
     */
    @OfMethod("search(java.lang.String,java.lang.String,int[],int)")
    public void testSearchStringForStringFromStartPostition() {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final String source = SearchAlgorithmProperty.SearchTarget.value;
            final String pattern = sap.value;
            final long expected = sap.expectedCharPosition;
            final long actual = KMPSearchAlgorithm.search(source, pattern, null, 0);
            final String msg = "position of " + sap.name()
                    + " in source";
            assertEquals(msg, expected, actual);
        }
    }

}
