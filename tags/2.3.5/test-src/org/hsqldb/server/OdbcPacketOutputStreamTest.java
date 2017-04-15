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


package org.hsqldb.server;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

@ForSubject(OdbcPacketOutputStream.class)
public class OdbcPacketOutputStreamTest extends BaseTestCase {
    protected OdbcPacketOutputStream targetPacket = null;
    protected long distinguishableLong = 0x0203040506070809L;

    @OfMethod({"writeShort(short)",
        "writeInt(int)",
        "writeLong(long)",
        "writeByte(byte)",
        "writeByteChar(char)",
        "getSize()",
        "write(java.lang.String,boolean)",
        "writeSized(java.lang.String)",
        "xmit(char,org.hsqldb.lib.DataOutputStream)"})
    public void testTypeMix() throws IOException {
        byte[] buffer = new byte[1024];
        String resPath = "/org/hsqldb/resources/odbcPacket.data";
        InputStream is = OdbcPacketOutputStreamTest.class.getResourceAsStream(
                resPath);
        if (is == null) {
            throw new RuntimeException("CLASSPATH not set properly.  "
                    + "Res file '" + resPath + "' not accessible");
        }
        ByteArrayOutputStream baosA = new ByteArrayOutputStream();
        ByteArrayOutputStream baosE = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baosA);

        int i;
        while ((i = is.read(buffer)) > -1) {
            baosE.write(buffer, 0, i);
        }
        byte[] expectedBa = baosE.toByteArray();
        baosE.close();

        targetPacket.writeShort((short) distinguishableLong);
        targetPacket.writeInt((int) distinguishableLong);
        targetPacket.writeLong(distinguishableLong);
        targetPacket.writeByte((byte) distinguishableLong);
        targetPacket.writeByteChar('k');
         // the writeByteChar() method is explicitly not for ext. characters.

        int preLength = targetPacket.getSize();
        targetPacket.write("Ein gro\u00df Baum\nwith blossom", false);
        if (targetPacket.getSize() - preLength != 27) {
            throw new RuntimeException(
                 "Assertion failed.  Fix test because encoding size changed");
        }
        targetPacket.write("Another string", true);
        targetPacket.writeSized("Ein gro\u00df Baum\nmit blossom");
        targetPacket.xmit('a', dos);
        dos.flush();
        dos.close();
        byte[] actualBa = baosA.toByteArray();

        /* Use this to regenerate the test data (which is also used to test
         * the reader).
        java.io.FileOutputStream fos =
            new java.io.FileOutputStream("/tmp/fix.bin");
        fos.write(actualBa);
        fos.flush();
        fos.close();
        */

        // JUnit 4.x has a built-in byte-array comparator.  Until then...
        assertEquals("Byte stream size is wrong", expectedBa.length,
            actualBa.length);
        for (i = 0; i < expectedBa.length; i++) {
            assertEquals("Bye stream corruption at offset " + i,
                expectedBa[i], actualBa[i]);
        }
    }

    /**
     * Invoked before each test*() invocation, by JUnit.
     */
    protected void setUp() throws IOException {
        targetPacket = OdbcPacketOutputStream.newOdbcPacketOutputStream();
    }

    /**
     * Invoked after each test*() invocation, by JUnit.
     */
    protected void tearDown() throws IOException {
        targetPacket.close();
        targetPacket = null;
    }

    static int shortFromByteArray(byte[] ba, int offset) {
        return (short) (((ba[offset] & 0xff) << 8) + (ba[++offset] & 0xff));
    }
    static int intFromByteArray(byte[] ba, int offset) {
        return (int) (((ba[offset] & 0xff) << 24) + ((ba[++offset] & 0xff) <<16)
            + ((ba[++offset] & 0xff) << 8) + (ba[++offset] & 0xff));
    }
    static long longFromByteArray(byte[] ba, int offset) {
        return (long) (((ba[offset] & 0xffL) << 56)
            + ((ba[++offset] & 0xffL) <<48)
            + ((ba[++offset] & 0xffL) << 40) + ((ba[++offset] & 0xffL) <<32)
            + ((ba[++offset] & 0xffL) << 24) + ((ba[++offset] & 0xffL) <<16)
            + ((ba[++offset] & 0xffL) << 8) + (ba[++offset] & 0xffL));
    }

    @OfMethod("writeShort(short)")
    public void testShort() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        targetPacket.writeShort((short) distinguishableLong);
        assertEquals("Write of first short wrote wrong number of bytes",
            6, targetPacket.getSize());
        targetPacket.writeShort((short) distinguishableLong);
        assertEquals("Write of second short wrote wrong number of bytes",
            8, targetPacket.getSize());
        int packetSize = targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        assertEquals("Size header in packet is wrong",
            packetSize, OdbcPacketOutputStreamTest.intFromByteArray(ba, 1));
        assertEquals("Retrieved data does not match stream's write count",
            packetSize + 1, ba.length);
        assertEquals("Value of first short got mangled",
            (short) (distinguishableLong & 0xffff),
            OdbcPacketOutputStreamTest.shortFromByteArray(ba, 5));
        assertEquals("Value of second short got mangled",
            (short) (distinguishableLong & 0xffff),
            OdbcPacketOutputStreamTest.shortFromByteArray(ba, 7));
    }

    @OfMethod("writeInt(int)")
    public void testInt() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        targetPacket.writeInt((int) distinguishableLong);
        assertEquals("Write of first int wrote wrong number of bytes",
            8, targetPacket.getSize());
        targetPacket.writeInt((int) distinguishableLong);
        assertEquals("Write of second int wrote wrong number of bytes",
            12, targetPacket.getSize());
        int packetSize = targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        assertEquals("Size header in packet is wrong",
            packetSize, OdbcPacketOutputStreamTest.intFromByteArray(ba, 1));
        assertEquals("Retrieved data does not match stream's write count",
            packetSize + 1, ba.length);
        assertEquals("Value of first int got mangled",
            (int) (distinguishableLong & 0xffffffff),
            OdbcPacketOutputStreamTest.intFromByteArray(ba, 5));
        assertEquals("Value of second int got mangled",
            (int) (distinguishableLong & 0xffffffff),
            OdbcPacketOutputStreamTest.intFromByteArray(ba, 9));
    }

    @OfMethod("writeLong(long)")
    public void testLong() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        targetPacket.writeLong(distinguishableLong);
        assertEquals("Write of first long wrote wrong number of bytes",
            12, targetPacket.getSize());
        targetPacket.writeLong(distinguishableLong);
        assertEquals("Write of second long wrote wrong number of bytes",
            20, targetPacket.getSize());
        int packetSize = targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        assertEquals("Size header in packet is wrong",
            packetSize, OdbcPacketOutputStreamTest.intFromByteArray(ba, 1));
        assertEquals("Retrieved data does not match stream's write count",
            packetSize + 1, ba.length);
        assertEquals("Value of first long got mangled", distinguishableLong,
            OdbcPacketOutputStreamTest.longFromByteArray(ba, 5));
        assertEquals("Value of second long got mangled", distinguishableLong,
            OdbcPacketOutputStreamTest.longFromByteArray(ba, 13));
    }

    @OfMethod("writeByte(byte)")
    public void testeByte() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        targetPacket.writeByte((byte) distinguishableLong);
        assertEquals("Write of first byte wrote wrong number of bytes",
            5, targetPacket.getSize());
        targetPacket.writeByte((byte) distinguishableLong);
        assertEquals("Write of second byte wrote wrong number of bytes",
            6, targetPacket.getSize());
        int packetSize = targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        assertEquals("Size header in packet is wrong",
            packetSize, OdbcPacketOutputStreamTest.intFromByteArray(ba, 1));
        assertEquals("Retrieved data does not match stream's write count",
            packetSize + 1, ba.length);
        assertEquals("Value of first byte got mangled",
            (byte) (distinguishableLong & 0xff), ba[5]);
        assertEquals("Value of second byte got mangled",
            (byte) (distinguishableLong & 0xff), ba[6]);
    }

    @OfMethod("writeByteChar(char)")
    public void testChar() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        targetPacket.writeByteChar('k');
        targetPacket.writeByteChar('X');
        int packetSize = targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Size header in packet is wrong",
            packetSize, OdbcPacketOutputStreamTest.intFromByteArray(ba, 1));
        assertEquals("Retrieved data does not match stream's write count",
            packetSize + 1, ba.length);
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        BufferedReader utfReader = new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(ba, 5, ba.length - 5), "UTF-8"));
        assertTrue("Packet did not provide a good character stream (1)",
            utfReader.ready());
        assertEquals("Value of first char got mangled", 'k',
            (char) utfReader.read());
        assertTrue("Packet did not provide a good character stream (2)",
            utfReader.ready());
        assertEquals("Value of first char got mangled", 'X',
            (char) utfReader.read());
        assertFalse("Packet has extra stuff after character stream",
            utfReader.ready());
        utfReader.close();
    }

    @OfMethod("write(java.lang.String,boolean)")
    public void testString() throws IOException {
        /** TODO:  Test high-order characters */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String testString1 = "Ein gro\u00df Baum\nwith blossom";
        String testString2 = "Another string";
        targetPacket.write(testString1, false);
        targetPacket.write(testString2, false);
        targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        BufferedReader utfReader = new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(ba, 5, ba.length - 5), "UTF-8"));
        assertTrue("Packet did not provide a good character stream (1)",
            utfReader.ready());
        char[] ca = new char[100];
        int charsRead = utfReader.read(ca);
        assertEquals("A test string got mangled (length mismatch)",
            (testString1 + testString2).length(), charsRead);
        assertEquals("A test string got mangled",
            testString1 + testString2, new String(ca, 0, charsRead));
        assertFalse("Packet has extra stuff after character stream",
            utfReader.ready());
    }

    @OfMethod("write(java.lang.String,boolean)")
    public void testNullTermdString() throws IOException {
        /** TODO:  Test high-order characters */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String testString1 = "Ein gro\u00df Baum\nwith blossom";
        String testString2 = "Another string";
        targetPacket.write(testString1, true);
        targetPacket.write(testString2, true);
        targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        BufferedReader utfReader = new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(ba, 5, ba.length - 5), "UTF-8"));
        char[] ca = new char[100];
        int charsRead;

        assertTrue("Packet did not provide a good character stream (1)",
            utfReader.ready());
        charsRead = utfReader.read(ca, 0, testString1.length());
        assertEquals("First test string got mangled (length mismatch)",
            testString1.length(), charsRead);
        assertEquals("First test string got mangled",
            testString1, new String(ca, 0, charsRead));
        assertTrue("Packet did not provide a good character stream (2)",
            utfReader.ready());
        assertEquals("No null delimiter after first string",
            (char) 0, (char) utfReader.read());

        assertTrue("Packet did not provide a good character stream (3)",
            utfReader.ready());
        charsRead = utfReader.read(ca, 0, testString2.length());
        assertEquals("Second test string got mangled (length mismatch)",
            testString2.length(), charsRead);
        assertEquals("Second test string got mangled",
            testString2, new String(ca, 0, charsRead));
        assertTrue("Packet did not provide a good character stream (4)",
            utfReader.ready());
        assertEquals("No null delimiter after second string",
            (char) 0, (char) utfReader.read());

        assertFalse("Packet has extra stuff after character stream",
            utfReader.ready());
    }

    @OfMethod("writeSized(java.lang.String)")
    public void testSizedString() throws IOException {
        /** TODO:  Test high-order characters */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        String testString1 = "Ein gro\u00df Baum\nwith blossom";
        String testString2 = "Another string";
        targetPacket.writeSized(testString1);
        int str2BytePos = targetPacket.getSize();
        targetPacket.writeSized(testString2);
        targetPacket.xmit('R', new DataOutputStream(baos));

        byte[] ba = baos.toByteArray();
        assertEquals("Packet type character got mangled", 'R', (char) ba[0]);
        BufferedReader utfReader;
        char[] ca = new char[100];
        int charsRead;

        utfReader= new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(ba, 9,
            OdbcPacketOutputStreamTest.intFromByteArray(ba, 5)), "UTF-8"));
        assertTrue("Packet did not provide a good character stream (1)",
            utfReader.ready());
        charsRead = utfReader.read(ca);
        assertEquals("First test string got mangled (length mismatch)",
            testString1.length(), charsRead);
        assertEquals("First test string got mangled",
            testString1, new String(ca, 0, charsRead));
        assertFalse("Packet has extra stuff after character stream",
            utfReader.ready());
        utfReader.close();

        utfReader= new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(ba, str2BytePos + 5,
            OdbcPacketOutputStreamTest.intFromByteArray(ba, str2BytePos + 1)),
            "UTF-8"));
        charsRead = utfReader.read(ca);
        assertEquals("Second test string got mangled (length mismatch)",
            testString2.length(), charsRead);
        assertEquals("Second test string got mangled",
            testString2, new String(ca, 0, charsRead));
        assertFalse("Packet has extra stuff after character stream",
            utfReader.ready());
        utfReader.close();
    }

    @OfMethod("getSize()")
    public void testReset() throws IOException {
        assertEquals("New packet size wrong", 4, targetPacket.getSize());
        testInt();
        assertEquals("New packet size wrong", 4, targetPacket.getSize());
        testShort();
        assertEquals("New packet size wrong", 4, targetPacket.getSize());
    }

    public static Test suite() {
        return new TestSuite(OdbcPacketOutputStreamTest.class);
    }

     /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(java.lang.String[] args) {
        if (args.length > 0 && args[0].startsWith("-g")) {
//            junit.swingui.TestRunner.run(OdbcPacketOutputStreamTest.class);
        } else {
            junit.framework.TestResult result = junit.textui.TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
