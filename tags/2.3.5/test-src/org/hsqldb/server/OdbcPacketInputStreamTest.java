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

import java.io.IOException;
import java.io.InputStream;

import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

@ForSubject(OdbcPacketInputStream.class)
public class OdbcPacketInputStreamTest extends BaseTestCase {

    protected long distinguishableLong = 0x0203040506070809L;

    @OfMethod({"newOdbcPacketInputStream(char,java.io.InputStream)",
        "readShort()",
        "readInt()",
        "readLong()",
        "readByte()",
        "readChar()",
        "readString()",
        "readSizedString()",
        "available()",
        "close()"})
    public void testTypeMix() throws IOException {
        byte[] buffer = new byte[1024];
        String resPath = "/org/hsqldb/resources/odbcPacket.data";
        InputStream is = OdbcPacketOutputStreamTest.class.getResourceAsStream(
                resPath);
        if (is == null) {
            throw new RuntimeException("CLASSPATH not set properly.  "
                    + "Res file '" + resPath + "' not accessible");
        }
        OdbcPacketInputStream inPacket = null;
        int packetType = is.read();
        if (packetType < 0) {
            throw new IOException("Failed to read first byte of packet stream");
        }
        try {
            inPacket = OdbcPacketInputStream.newOdbcPacketInputStream(
                    (char) packetType, is);
        } catch (IOException ioe) {
            fail("Failed to instantiate OdbcPacketInputStream object: "
                    + ioe);
        }

        assertEquals("Wrong packet type", 'a', inPacket.packetType);
        assertEquals("Mungled short", (short) distinguishableLong,
                inPacket.readShort());
        assertEquals("Mungled int", (int) distinguishableLong,
                inPacket.readInt());
        assertEquals("Mungled long", distinguishableLong, inPacket.readLong());
        assertEquals("Mungled byte", (byte) distinguishableLong,
                inPacket.readByte());
        assertEquals("Mungled char", 'k', inPacket.readByteChar());
        assertEquals("Mungled String", "Ein gro\u00df Baum\nwith blossom",
                inPacket.readString(27));
        // I know this length from manual testing when writing the string.
        assertEquals("Mungled String", "Another string", inPacket.readString());
        assertEquals("Mungled String", "Ein gro\u00df Baum\nmit blossom",
                inPacket.readSizedString());

        assertEquals("Bytes left over", 0, inPacket.available());
        inPacket.close();
    }

    public static Test suite() {
        return new TestSuite(OdbcPacketInputStreamTest.class);
    }

     /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(java.lang.String[] args) {
        if (args.length > 0 && args[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(ServerSuite.class);
        } else {
            junit.framework.TestResult result = junit.textui.TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
