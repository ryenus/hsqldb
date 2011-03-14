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


package org.hsqldb.lib.tar;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;

public class PIFGeneratorTest extends junit.framework.TestCase {
    static protected byte[] loadResByteFile(String resPath) {
        InputStream is = null;
        int bytesRead = 0;
        int retval;
        byte[] ba = null;

        try {
            is = PIFGeneratorTest.class.getResourceAsStream(resPath);
            if (is == null) {
                throw new RuntimeException("CLASSPATH not set properly.  "
                        + "Res file '" + resPath + "' not accessible");
            }
            ba = new byte[is.available()];
            while (bytesRead < ba.length &&
                    (retval = is.read(ba, bytesRead, ba.length - bytesRead))
                    > 0) {
                bytesRead += retval;
            }
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Unexpected resource problem with Res file '"
                    + resPath + "' not accessible", ioe);
        } finally {
            if (is != null) try {
                is.close();
            } catch (IOException ioe) {
                // Intentionally doing nothing
            }
        }
        if (bytesRead != ba.length) {
            throw new RuntimeException("I/O problem reading res file '"
                    + resPath + "'");
        }
        return ba;
    }

    public void testXrecord() {
        byte[] expectedHeaderData = PIFGeneratorTest.loadResByteFile(
                "/org/hsqldb/resources/pif.data");
        // Would like to load this one time with a JUnit v. 4 @BeforeClass
        // method
        File f = new File("build.xml");
        if (!f.exists()) {
            throw new RuntimeException(
                "Test environment misconfigured.  File 'build.xml' inaccessible");
        }
        PIFGenerator pif = new PIFGenerator(f);
        try {
            populate(pif);
        } catch (IOException ioe) {
            fail("Failed to populate PIF:" + ioe);
        } catch (TarMalformatException tme) {
            fail("Failed to populate PIF:" + tme);
        }
        assertTrue("Bad PIF record name", pif.getName().endsWith("/build.xml"));

        //assertArrayEquals(expectedHeaderData, pif.toByteArray());
        // Arg.  All of the following work can be done with the single line
        // above with JUnit v. 4.
        byte[] pifBytes = pif.toByteArray();
        assertEquals(expectedHeaderData.length, pifBytes.length);
        for (int i = 0; i < expectedHeaderData.length; i++) {
            assertEquals(expectedHeaderData[i], pifBytes[i]);
        }
    }

    public void testGrecord() {
        byte[] expectedHeaderData = PIFGeneratorTest.loadResByteFile(
                "/org/hsqldb/resources/pif.data");
        // Would like to load this one time with a JUnit v. 4 @BeforeClass
        // method
        PIFGenerator pif = new PIFGenerator(1);
        try {
            populate(pif);
        } catch (TarMalformatException tme) {
            fail("Failed to populate PIF:" + tme);
        } catch (IOException ioe) {
            fail("Failed to populate PIF:" + ioe);
        }
        assertTrue("Bad PIF record name",
                pif.getName().indexOf("GlobalHead") > 0);

        //assertArrayEquals(expectedHeaderData, pif.toByteArray());
        // Arg.  All of the following work can be done with the single line
        // above with JUnit v. 4.
        byte[] pifBytes = pif.toByteArray();
        assertEquals(expectedHeaderData.length, pifBytes.length);
        for (int i = 0; i < expectedHeaderData.length; i++) {
            assertEquals(expectedHeaderData[i], pifBytes[i]);
        }
    }

    protected void populate(PIFGenerator pif)
            throws TarMalformatException, IOException {
        pif.addRecord("o", "n");       // Shortest possible
        pif.addRecord("k1", "23");     // total 8.  Impossible to get total of 9.
        pif.addRecord("k2", "234");    // total 10
        pif.addRecord("k3", "2345");   // total 11
        pif.addRecord("k4",
                      "2345678901234567890123456789012345678901234567890"
                      + "123456789012345678901234567890123456789012");     //total 98

        // Impossible to get total of 99.
        pif.addRecord("k5",
                      "2345678901234567890123456789012345678901234567890"
                      + "1234567890123456789012345678901234567890123");    //total 100
        pif.addRecord("int1234", 1234);
        pif.addRecord("long1234", 1234);
        pif.addRecord("boolTrue", true);
        pif.addRecord("boolFalse", false);
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(PIFGeneratorTest.class);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(PIFGeneratorTest.class.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
