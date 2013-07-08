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
package org.hsqldb.lib;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.hsqldb.lib.FileAccess.FileSync;
import org.hsqldb.lib.FileUtil.FileAccessRes;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(FileAccessRes.class)
public class FileAccessResTest extends BaseTestCase {

    public FileAccessResTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected FileAccessRes getTestSubject() {
        return (FileAccessRes) FileUtil.getFileAccess(/*isResource*/true);
    }

    /**
     * Test of openInputStreamElement method, of class FileAccessRes.
     */
    @OfMethod("openInputStreamElement(java.lang.String)")
    public void testOpenInputStreamElement() throws Exception {
        String streamName = "/org/hsqldb/resources/sql-state-messages.properties";
        FileAccessRes testSubject = getTestSubject();
        InputStream result = null;

        try {
            result = testSubject.openInputStreamElement(streamName);
        } catch (IOException ex) {
            fail("" + ex);
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (IOException ex) {
                }
            }
        }

        assertNotNull(result);
    }

    /**
     * Test of openOutputStreamElement method, of class FileAccessRes.
     */
    @OfMethod("openInputStreamElement(java.lang.String)")
    public void testOpenOutputStreamElement() throws Exception {
        String streamName = "doesnotmatter";
        FileAccessRes testSubject = getTestSubject();
        InputStream result = null;

        try {
            result = testSubject.openInputStreamElement(streamName);
        } catch (Exception ex) {
            assertTrue(ex instanceof IOException);
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (IOException ex) {
                }
            }
        }

        assertNull(result);
    }

    /**
     * Test of isStreamElement method, of class FileAccessRes.
     */
    @OfMethod("isStreamElement(java.lang.String)")
    public void testIsStreamElement() {
        System.out.println("isStreamElement");
        FileAccessRes testSubject = getTestSubject();
        assertTrue(testSubject.isStreamElement("/org/hsqldb/resources/webserver-content-types.properties"));
        assertFalse(testSubject.isStreamElement("/org/hsqldb/resources/webserver-content-types.unexpected-extension"));
    }

    /**
     * Test of createParentDirs method, of class FileAccessRes.
     */
    @OfMethod("createParentDirs(java.lang.String)")
    public void testCreateParentDirs() {
        String filename = "foo";
        FileAccessRes testSubject = getTestSubject();
        // defined as NO-OP
        testSubject.createParentDirs(filename);
    }

    /**
     * Test of removeElement method, of class FileAccessRes.
     */
    @OfMethod("removeElement(java.lang.String)")
    public void testRemoveElement() {
        String filename = "foo";
        FileAccessRes testSubject = getTestSubject();

        // defined as NO-OP
        testSubject.removeElement(filename);
    }

    /**
     * Test of renameElement method, of class FileAccessRes.
     */
    @OfMethod("renameElement(java.lang.String,java.lang.String)")
    public void testRenameElement() {
        String oldName = "foo";
        String newName = "bar";
        FileAccess testSubject = getTestSubject();

        // defined as NO-OP
        testSubject.renameElement(oldName, newName);
    }

    /**
     * Test of getFileSync method, of class FileAccessRes.
     */
    @OfMethod("getFileSync(java.io.OutputStream)")
    public void testGetFileSync() throws Exception {
        OutputStream os = null;
        FileSync result = null;
        FileAccessRes testSubject = getTestSubject();

        try {
            os = new FileOutputStream(".");
            result = testSubject.getFileSync(os);
        } catch (IOException ex) {
            //
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException ex) {
                }
            }
        }

        assertNull(result);
    }


    public static Test suite() {
        return new TestSuite(FileAccessResTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
