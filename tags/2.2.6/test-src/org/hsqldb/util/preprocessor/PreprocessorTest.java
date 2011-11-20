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

package org.hsqldb.util.preprocessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URLDecoder;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/* $Id$ */

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
public class PreprocessorTest extends TestCase {

    public PreprocessorTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(PreprocessorTest.class);

        return suite;
    }

    protected String fileToString(File file) throws Exception {
        InputStream    is = new FileInputStream(file);
        Reader         r  = new InputStreamReader(is, "UTF8");
        BufferedReader br = new BufferedReader(r);
        StringBuffer   sb = new StringBuffer((int)file.length());
        StringWriter   sw = new StringWriter();
        PrintWriter    pw = new PrintWriter(sw);
        String         line;

        while(null != (line = br.readLine())) {
            pw.println(line);
        }

        return sw.toString();
    }

    /**
     * Test of preprocessBatch method, of class org.hsqldb.util.preprocessor.Preprocessor.
     */
    public void testPreprocessBatch() throws Exception {
        System.out.println("preprocessBatch");

        String srcpath      = URLDecoder.decode(getClass().getResource("ATest.src").getFile());
        File      srcfile   = new File(srcpath);
        File      sourceDir = srcfile.getParentFile();
        File      targetDir = sourceDir;
        String[]  files     = new String[]{srcfile.getName()};
        String    defines   = "jdbc_version=4.0";
        int       options   = Option.BACKUP | Option.FILTER | Option.VERBOSE;
        String    altExt    = ".java";
        String    encoding  = "UTF8";
        IResolver resolver  = null;

        Preprocessor.preprocessBatch(sourceDir, targetDir, files, altExt,
                encoding, options, defines, resolver);

        File dstfile = new File(targetDir, "ATest.java");
        File expFile = new File(sourceDir, "ATest.exp");
        String dst   = fileToString(dstfile);
        String exp   = fileToString(expFile);

        assertTrue("Preprocessed output does not match expected output", dst.equals(exp));
    }
}
