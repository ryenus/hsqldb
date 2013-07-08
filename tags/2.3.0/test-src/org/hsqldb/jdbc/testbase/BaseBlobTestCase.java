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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.sql.Blob;
import java.sql.SQLException;

import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of interface java.sql.Blob.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(java.sql.Blob.class)
public abstract class BaseBlobTestCase extends BaseJdbcTestCase {

    private static final byte[] s_data = new byte[]{0, 1, 2, 1, 2, 3, 2, 3, 4, 2, 3, 4, 5, 2, 3, 4, 5, 0, 1, 2, 1, 2, 3, 2, 3, 4, 2, 3, 4, 5, 2, 3, 4};
    private static final byte[] s_pattern = new byte[]{2, 3, 4, 5};
    private static final int[] s_expectedPosition = new int[]{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 14, 14, 14, 14, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, 27, -1, -1, -1, -1, -1, -1, -1, -1};

    public BaseBlobTestCase(String name) {
        super(name);
    }

    protected abstract Blob handleCreateBlob() throws Exception;

    public final Blob createBlob() throws Exception {
         final Blob blob = handleCreateBlob();

         connectionFactory().registerBlob(blob);

         return blob;
    }

    protected Blob newBlob(byte[] bytes) throws Exception {
        final Blob blob = handleCreateBlob();

        if (bytes != null) {
            final OutputStream os = blob.setBinaryStream(1);
            // important - must close the stream to ensure all
            // bytes are flushed to the blob.
            try {
                os.write(bytes);
            } finally {
                try {
                    os.close();
                } catch (Exception ex) {
                }
            }

        }

        return blob;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @OfMethod(value = "free()")
    public void testFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        try {
            blob.free();
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @OfMethod(value = "getBinaryStream()")
    public void testGetBinaryStream() throws Exception {
        byte[] bytes = "getBinaryStream".getBytes();
        Blob blob = newBlob(bytes);
        InputStream expResult = new ByteArrayInputStream(bytes);
        InputStream result = blob.getBinaryStream();
        assertStreamEquals(expResult, result);
        blob.free();
    }

    @OfMethod(value = "getBinaryStream()")
    public void testGetBinaryStreamAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.getBinaryStream();
            assertTrue("getBinaryStream operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "getBytes(long,int)")
    public void testGetBytes() throws Exception {
        byte[] bytes = "getBytes".getBytes();
        Blob blob = newBlob(bytes);
        byte[] result = blob.getBytes(1, bytes.length);
        assertEquals(bytes.length, result.length);
        assertJavaArrayEquals(bytes, result);
    }

    @OfMethod(value = "getBytes(long,int)")
    public void testGetBytesAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.getBytes(1, 1);
            assertTrue("getBytes operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "length()")
    public void testLength() throws Exception {
        Blob blob = newBlob(new byte[100]);
        long result = blob.length();
        assertEquals(100L, result);
    }

    @OfMethod(value = "length()")
    public void testLengthAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.length();
            assertTrue("length operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "position(byte[],long)")
    public void testPosition_byte_array_pattern() throws Exception {
        assertEquals(-1, (newBlob(new byte[0])).position(new byte[]{1}, 1));
        assertEquals(-1, (newBlob(new byte[]{1})).position(new byte[0], 1));
        assertEquals(-1, (newBlob(new byte[]{1})).position((byte[]) null, 1));

        final Blob blob = newBlob(s_data);

        for (int i = 0; i < s_data.length; i++) {
            long expected =  s_expectedPosition[i];
            long actual = blob.position(s_pattern, i + 1);
            assertEquals("With start position: " + (i + 1),expected,actual);
        }
    }

    @OfMethod(value = "position(java.sql.Blob,long)")
    public void testPosition_blob_pattern() throws Exception {
        assertEquals(-1, (newBlob(new byte[0])).position(newBlob(new byte[]{1}), 1));
        assertEquals(-1, (newBlob(new byte[]{1})).position(newBlob(new byte[0]), 1));
        assertEquals(-1, (newBlob(new byte[]{1})).position(newBlob((byte[]) null), 1));

        final Blob blobData = newBlob(s_data);
        final Blob blobPattern = newBlob(s_pattern);

        for (int i = 0; i < s_data.length; i++) {
            assertEquals("With start position: " + (i + 1),
                    s_expectedPosition[i],
                    blobData.position(blobPattern, i + 1));
        }

    }

    @OfMethod(value = "position(byte[],long)")
    public void testPositionAfterFree_byte_array_pattern() throws Exception {
        Blob blob = newBlob(new byte[10]);

        blob.free();

        try {
            blob.position(new byte[1], 1);
            assertTrue("position(byte[],long) operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "position(java.sql.Blob,long)")
    public void testPositionAfterFree_blob_pattern() throws Exception {
        Blob blob = newBlob(new byte[10]);

        blob.free();

        try {
            blob.position(newBlob(new byte[1]), 1);
            assertTrue("position(byte[],long) operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "setBinaryStream(long)")
    public void testSetBinaryStream() throws Exception {
        Blob blob = handleCreateBlob();
        try {
            OutputStream result = blob.setBinaryStream(1);
            result.write(new byte[10]);
            assertEquals(0L, blob.length());
            result.close();
            assertEquals(10L, blob.length());
            assertJavaArrayEquals(new byte[10], blob.getBytes(1, 10));
        } catch (SQLException e) {
            fail(e.toString());
        }
    }

    @OfMethod(value = "setBinaryStream(long)")
    public void testSetBinaryStreamAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.setBinaryStream(1);
            assertTrue("setBinaryStream operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "setBytes(long,byte[])")
    public void testSetBytes() throws Exception {
        byte[] bytes = new byte[1];
        Blob blob = handleCreateBlob();
        int result = 0;
        try {
            result = blob.setBytes(1, bytes);
        } catch (Exception e) {
            fail(e.toString());
        }
        assertEquals(1, result);
    }

    @OfMethod(value = "setBytes(long,byte[])")
    public void testSetBytesAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.setBytes(1, new byte[1]);
            assertTrue("setBytes(long,byte[]) operation allowed after free", false);
        } catch (Exception e) {
        }
    }

    @OfMethod(value = "truncate(long)")
    public void testTruncate() throws Exception {
        Blob blob = newBlob(new byte[10]);
        try {
            blob.truncate(5);
        } catch (Exception e) {
            fail(e.toString());
        }
        assertEquals(5L, blob.length());
    }

    @OfMethod(value = "truncate(long)")
    public void testTruncateAfterFree() throws Exception {
        Blob blob = newBlob(new byte[10]);
        blob.free();
        try {
            blob.truncate(1);
            assertTrue("truncate operation allowed after free", false);
        } catch (Exception e) {
        }
    }
}
