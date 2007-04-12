/* Copyright (c) 2001-2006, The HSQL Development Group
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

package org.hsqldb.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.sql.SQLXML;
import javax.xml.transform.stream.StreamSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.lib.StringConverter;

/**
 *
 * @author boucherb@users
 */
public class jdbcSQLXMLTest extends JdbcTestCase {

    public jdbcSQLXMLTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected java.net.URL getResource(String path) throws Exception {
        return getClass().getResource(path);
    }

    protected InputStream getZipEntryInputStream(String zipPath,
            String entryPath) throws Exception {
        URL         zipUrl    =  getResource(zipPath);
        String      spec     = "jar:" + zipUrl + "!/" + entryPath;
        URL         entryUrl = new URL(spec);
        InputStream is        = entryUrl.openStream();

        return is;
    }

    protected String zipEntryToString(String zip, String entry)
    throws Exception {
        InputStream is = getZipEntryInputStream(zip, entry);
        String      s  = StringConverter.inputStreamToString(is,
                Integer.MAX_VALUE);

        return s;
    }

    protected SQLXML newMyDoc() throws Exception {
        InputStream is = getZipEntryInputStream("resources/xml/MyDoc.xml.zip", "MyDoc.xml");
        StreamSource ss = new StreamSource();

        ss.setInputStream(is);
        ss.setSystemId(getResource("resources/xml/MyDoc.xml.zip").toExternalForm());

        return new jdbcSQLXML(ss);
    }

    protected String newMyDocString() throws Exception {
        return zipEntryToString("resources/xml/MyDoc.xml.zip", "MyDoc.xml");
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(jdbcSQLXMLTest.class);

        return suite;
    }

    /**
     * Test of free method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testFree() throws Exception {
        System.out.println("free");

        SQLXML instance = newMyDoc();

        instance.free();
    }

    /**
     * Test of getBinaryStream method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetBinaryStream() throws Exception {
        System.out.println("getBinaryStream");

        SQLXML instance = newMyDoc();
    }

    /**
     * Test of setBinaryStream method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetBinaryStream() throws Exception {
        System.out.println("setBinaryStream");

        SQLXML       instance = new jdbcSQLXML();
        OutputStream os       = instance.setBinaryStream();

        os.close();
    }

    /**
     * Test of getCharacterStream method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetCharacterStream() throws Exception {
        System.out.println("getCharacterStream");

        SQLXML instance = newMyDoc();
        Reader reader   = instance.getCharacterStream();

        reader.close();
    }

    /**
     * Test of setCharacterStream method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetCharacterStream() throws Exception {
        System.out.println("setCharacterStream");

        SQLXML instance = new jdbcSQLXML();
        Writer writer   = instance.setCharacterStream();

        writer.close();
    }

    /**
     * Test of getString method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetString() throws Exception {
        System.out.println("getString");

        SQLXML instance = newMyDoc();

        String result = instance.getString();
    }

    /**
     * Test of setString method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetString() throws Exception {
        System.out.println("setString");

        String value = this.newMyDocString();
        jdbcSQLXML instance = new jdbcSQLXML();

        instance.setString(value);
    }

    /**
     * Test of getSource method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetDOMSource() throws Exception {
        System.out.println("getDOMSource");
    }

    /**
     * Test of getSource method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetSAXSource() throws Exception {
        System.out.println("getDOMSource");
    }

    /**
     * Test of getSource method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetStAXSource() throws Exception {
        System.out.println("getDOMSource");
    }

    /**
     * Test of getSource method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testGetStreamSource() throws Exception {
        System.out.println("getDOMSource");
    }

    /**
     * Test of setResult method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetDOMResult() throws Exception {
        System.out.println("setResult");
    }

    /**
     * Test of setResult method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetSAXResult() throws Exception {
        System.out.println("setResult");
    }

    /**
     * Test of setResult method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetStAXResult() throws Exception {
        System.out.println("setResult");
    }

    /**
     * Test of setResult method, of class org.hsqldb.jdbc.jdbcSQLXML.
     */
    public void testSetStreamResult() throws Exception {
        System.out.println("setResult");
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
